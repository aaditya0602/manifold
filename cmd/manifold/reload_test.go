package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaditya0602/manifold/internal/config"
	"github.com/aaditya0602/manifold/internal/proxy"
	"github.com/aaditya0602/manifold/internal/reload"
)

// gateConfig is the config the reload gate cycles between.
//
// Active health checking is on, because a reload has to start one set of
// checkers and stop another and the gate should exercise that. Its thresholds
// are deliberately unreachable within the test's lifetime: a probe that times
// out because the machine is busy running the load generator must not eject a
// backend and turn a scheduling hiccup into a "dropped connection" that has
// nothing to do with reloading.
func gateConfig(upstream string) string {
	return fmt.Sprintf(`listen: "127.0.0.1:18080"
admin: "127.0.0.1:19090"
server:
  drain_timeout: 10s
pools:
  - name: api
    strategy: round_robin
    upstreams:
      - url: %s
    health:
      active:
        enabled: true
        path: /
        interval: 200ms
        timeout: 150ms
        healthy_threshold: 1
        unhealthy_threshold: 50
      passive:
        enabled: false
    breaker:
      enabled: false
    retry:
      max_attempts: 1
routes:
  - match:
      path_prefix: /
    pool: api
`, upstream)
}

func writeGateConfig(t *testing.T, path, upstream string) {
	t.Helper()
	// Written the way real tooling writes it: to a temporary file, then
	// renamed over the target. An in-place write can be read half-finished.
	tmp := path + ".new"
	if err := os.WriteFile(tmp, []byte(gateConfig(upstream)), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename config: %v", err)
	}
}

// TestReload_TenReloadsUnderLoadDropZeroConnections is the acceptance gate.
//
// Ten reloads back to back while real HTTP clients are hammering a real
// listener over real TCP connections, and every single response is counted:
// not sampled, not rate-limited, not averaged. The pass condition is exactly
// zero non-2xx responses and exactly zero transport errors across every
// request made for the duration of the run.
//
// It runs through an http.Server on a real socket rather than through
// ServeHTTP with a recorder, because half of what can go wrong in a reload is
// invisible to a handler-level test: keep-alive connections held across the
// swap, a transport closed while a response body is still being copied, a
// listener rebuilt underneath the accept loop.
func TestReload_TenReloadsUnderLoadDropZeroConnections(t *testing.T) {
	const (
		reloads     = 10
		workers     = 16
		settleAfter = 150 * time.Millisecond
	)

	// Two backends the config alternates between, so every reload is a real
	// change and the traffic can be proven to have moved.
	var hits [2]atomic.Int64
	backends := make([]*httptest.Server, 2)
	for i := range backends {
		name := fmt.Sprintf("backend-%d", i)
		counter := &hits[i]
		backends[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			counter.Add(1)
			_, _ = io.WriteString(w, name)
		}))
		defer backends[i].Close()
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeGateConfig(t, path, backends[0].URL)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	px, err := proxy.New(cfg)
	if err != nil {
		t.Fatalf("build proxy: %v", err)
	}

	var successes, failures atomic.Int64
	sup := reload.New(path, cfg, px, reload.Options{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnResult: func(r string) {
			if r == reload.ResultSuccess {
				successes.Add(1)
				return
			}
			failures.Add(1)
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sup.Start(ctx)
	defer sup.Close()

	// The one http.Server for the whole run, with the supervisor as its
	// handler, set once. This is the arrangement main.go uses and the reason a
	// reload can be invisible: nothing about the listener changes.
	front := httptest.NewServer(sup)
	defer front.Close()

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        workers * 2,
			MaxIdleConnsPerHost: workers * 2,
			IdleConnTimeout:     30 * time.Second,
		},
		Timeout: 10 * time.Second,
	}
	defer client.CloseIdleConnections()

	var (
		total    atomic.Int64
		nonOK    atomic.Int64
		connErrs atomic.Int64
		firstErr atomic.Value
	)
	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}

				res, err := client.Get(front.URL + "/api/things")
				total.Add(1)
				if err != nil {
					connErrs.Add(1)
					firstErr.CompareAndSwap(nil, err.Error())
					continue
				}
				body, readErr := io.ReadAll(res.Body)
				_ = res.Body.Close()
				if readErr != nil {
					// A body that cannot be read to completion is a dropped
					// connection with a 200 status line in front of it.
					connErrs.Add(1)
					firstErr.CompareAndSwap(nil, "read body: "+readErr.Error())
					continue
				}
				if res.StatusCode < 200 || res.StatusCode > 299 {
					nonOK.Add(1)
					firstErr.CompareAndSwap(nil, fmt.Sprintf("status %d: %s", res.StatusCode, strings.TrimSpace(string(body))))
				}
			}
		}()
	}

	// Let the load reach a steady state so the reloads land on live traffic
	// and warm keep-alive connections rather than on a cold listener.
	time.Sleep(settleAfter)
	before := total.Load()
	if before == 0 {
		t.Fatal("no traffic before the first reload; the gate would prove nothing")
	}

	start := time.Now()
	for i := 1; i <= reloads; i++ {
		writeGateConfig(t, path, backends[i%2].URL)
		if err := sup.Reload(path); err != nil {
			t.Fatalf("reload %d failed: %v", i, err)
		}
		// A short gap between reloads, not for the implementation's benefit
		// but for the test's: it guarantees every generation actually serves
		// requests, so the ten swaps are ten handovers of live traffic rather
		// than nine of them landing on a proxy nobody has called yet.
		time.Sleep(20 * time.Millisecond)
	}
	elapsed := time.Since(start)

	// Traffic keeps running past the last reload, so requests that were in
	// flight during the final swap are counted rather than cut off by the test
	// itself.
	time.Sleep(settleAfter)
	close(stop)
	wg.Wait()

	requests, bad, errs := total.Load(), nonOK.Load(), connErrs.Load()
	t.Logf("gate: %d requests, %d reloads in %s, %d non-2xx, %d connection errors; backend hits: %d / %d",
		requests, reloads, elapsed.Round(time.Millisecond), bad, errs, hits[0].Load(), hits[1].Load())

	if bad != 0 || errs != 0 {
		detail, _ := firstErr.Load().(string)
		t.Fatalf("dropped connections: %d non-2xx and %d transport errors out of %d requests (first: %s)",
			bad, errs, requests, detail)
	}
	if got := successes.Load(); got != reloads {
		t.Errorf("%d reloads reported success, want %d (failures: %d)", got, reloads, failures.Load())
	}
	if requests-before < workers {
		t.Errorf("only %d requests were served across the reload window; the gate needs sustained load", requests-before)
	}
	// Both backends must have served traffic, or the reloads changed nothing
	// and zero drops is a statement about an idle process.
	if hits[0].Load() == 0 || hits[1].Load() == 0 {
		t.Errorf("backend hits %d / %d: the config did not actually change under load",
			hits[0].Load(), hits[1].Load())
	}
}
