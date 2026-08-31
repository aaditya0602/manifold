package reload

import (
	"bytes"
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
	"syscall"
	"testing"
	"time"

	"github.com/aaditya0602/manifold/internal/config"
	"github.com/aaditya0602/manifold/internal/proxy"
)

// --- helpers ---------------------------------------------------------------

// syncBuffer is a log sink a test can read after the goroutine that wrote to
// it has finished. slog handlers are called from the reload goroutine, so the
// buffer needs its own lock even though the test reads it later.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// backend is an origin that identifies itself in the response body, so a test
// can tell which generation served a request by reading it.
func backend(t *testing.T, name string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, name)
	}))
	t.Cleanup(srv.Close)
	return srv
}

type configOpts struct {
	// activeHealth turns on probing, which is how the "old generation is
	// really closed" test observes that a retired server's checkers stopped.
	activeHealth bool
	// extraUpstreams are appended verbatim, for the config that is valid to
	// the schema and rejected by proxy.New.
	extraUpstreams []string
}

func configYAML(urls []string, opts configOpts) string {
	var b strings.Builder
	b.WriteString("listen: \"127.0.0.1:18080\"\n")
	b.WriteString("admin: \"127.0.0.1:19090\"\n")
	b.WriteString("server:\n  drain_timeout: 10s\n")
	b.WriteString("pools:\n  - name: api\n    strategy: round_robin\n    upstreams:\n")
	for _, u := range urls {
		fmt.Fprintf(&b, "      - url: %s\n", u)
	}
	for _, u := range opts.extraUpstreams {
		fmt.Fprintf(&b, "      - url: %s\n", u)
	}
	b.WriteString("    health:\n      active:\n")
	if opts.activeHealth {
		b.WriteString("        enabled: true\n        path: /\n        interval: 25ms\n        timeout: 20ms\n")
	} else {
		b.WriteString("        enabled: false\n")
	}
	b.WriteString("      passive:\n        enabled: false\n")
	b.WriteString("    breaker:\n      enabled: false\n")
	b.WriteString("    retry:\n      max_attempts: 1\n")
	b.WriteString("routes:\n  - match:\n      path_prefix: /\n    pool: api\n")
	return b.String()
}

func writeConfig(t *testing.T, path string, urls []string, opts configOpts) {
	t.Helper()
	if err := os.WriteFile(path, []byte(configYAML(urls, opts)), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
}

// newSupervisor loads path, builds the first generation, and returns a started
// Supervisor plus the sink its logs go to.
func newSupervisor(t *testing.T, path string, onResult func(string)) (*Supervisor, *syncBuffer) {
	t.Helper()

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load initial config: %v", err)
	}
	srv, err := proxy.New(cfg)
	if err != nil {
		t.Fatalf("build initial proxy: %v", err)
	}

	logs := &syncBuffer{}
	sup := New(path, cfg, srv, Options{
		Logger:   slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
		OnResult: onResult,
	})
	sup.Start(context.Background())
	t.Cleanup(sup.Close)
	return sup, logs
}

// get sends one request through the Supervisor and returns status and body.
func get(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code, rec.Body.String()
}

// --- tests -----------------------------------------------------------------

// TestReload_SwapsConfig is the basic contract: after a successful reload the
// next request is served by the new config's backends.
func TestReload_SwapsConfig(t *testing.T) {
	a, b := backend(t, "A"), backend(t, "B")
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeConfig(t, path, []string{a.URL}, configOpts{})

	sup, _ := newSupervisor(t, path, nil)

	if code, body := get(t, sup, "/"); code != http.StatusOK || body != "A" {
		t.Fatalf("before reload: got %d %q, want 200 \"A\"", code, body)
	}

	writeConfig(t, path, []string{b.URL}, configOpts{})
	if err := sup.Reload(path); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	if code, body := get(t, sup, "/"); code != http.StatusOK || body != "B" {
		t.Errorf("after reload: got %d %q, want 200 \"B\"", code, body)
	}
	if got := sup.Config().Pools[0].Upstreams[0].URL; got != b.URL {
		t.Errorf("Config() still reports %q, want %q", got, b.URL)
	}
}

// TestReload_InvalidConfigKeepsServing is the property that decides whether
// hot reload is safe to enable in production at all.
//
// Both halves of "invalid" are covered, because they fail at different stages
// and only one of them is a schema problem:
//
//   - a file that does not parse, caught by config.Load;
//   - a file that parses and validates cleanly but that proxy.New refuses,
//     caught only when the server is built.
//
// In both cases the reload must fail, the running proxy must go on serving the
// previous configuration without a single interrupted request, and the reason
// must reach the log.
func TestReload_InvalidConfigKeepsServing(t *testing.T) {
	cases := []struct {
		name  string
		write func(t *testing.T, path, goodURL string)
		want  string
	}{
		{
			name: "parse error",
			write: func(t *testing.T, path, _ string) {
				t.Helper()
				if err := os.WriteFile(path, []byte("listen: \":8080\"\npools: [ unterminated\n"), 0o600); err != nil {
					t.Fatalf("write bad config: %v", err)
				}
			},
			want: "parse",
		},
		{
			name: "same origin written twice with different casing",
			write: func(t *testing.T, path, goodURL string) {
				t.Helper()
				// Two upstreams for one origin, differing only in scheme
				// case. This used to validate clean and then fail in
				// proxy.New, which normalises the origin -- the class of
				// error a schema check missed, and the reason step 2 of the
				// reload sequence exists at all.
				//
				// Validation now dedupes on the canonical origin too, so it
				// is caught a stage earlier and `manifold -check` sees it.
				// The assertion here is deliberately about the outcome that
				// matters and has not changed: the reload fails, and traffic
				// keeps being served by the previous config.
				writeConfig(t, path, []string{goodURL}, configOpts{
					extraUpstreams: []string{strings.Replace(goodURL, "http://", "HTTP://", 1)},
				})
			},
			want: "duplicate upstream",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := backend(t, "A")
			path := filepath.Join(t.TempDir(), "config.yaml")
			writeConfig(t, path, []string{a.URL}, configOpts{})

			var results []string
			var resultMu sync.Mutex
			sup, logs := newSupervisor(t, path, func(r string) {
				resultMu.Lock()
				defer resultMu.Unlock()
				results = append(results, r)
			})

			// Traffic runs across the failed reload, not just before and
			// after it: "kept serving" means no request was dropped while the
			// reload was being attempted.
			stop := make(chan struct{})
			var requests, bad atomic.Int64
			var wg sync.WaitGroup
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case <-stop:
							return
						default:
						}
						rec := httptest.NewRecorder()
						sup.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
						requests.Add(1)
						if rec.Code != http.StatusOK || rec.Body.String() != "A" {
							bad.Add(1)
						}
					}
				}()
			}

			tc.write(t, path, a.URL)
			err := sup.Reload(path)

			close(stop)
			wg.Wait()

			if err == nil {
				t.Fatal("Reload accepted an invalid config")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
			if n := bad.Load(); n != 0 {
				t.Errorf("%d of %d requests were disrupted by a failed reload, want 0", n, requests.Load())
			}
			if requests.Load() == 0 {
				t.Error("no traffic ran during the failed reload; the assertion above proved nothing")
			}
			if code, body := get(t, sup, "/"); code != http.StatusOK || body != "A" {
				t.Errorf("after failed reload: got %d %q, want the previous config to still serve 200 \"A\"", code, body)
			}
			resultMu.Lock()
			defer resultMu.Unlock()
			if len(results) != 1 || results[0] != ResultFailure {
				t.Errorf("OnResult got %v, want exactly [%s]", results, ResultFailure)
			}
			if out := logs.String(); !strings.Contains(out, "config reload failed") {
				t.Errorf("failure was not logged; log was:\n%s", out)
			}
		})
	}
}

// TestReload_InFlightRequestSurvivesSwap asserts the ordering that step 5 of
// the reload sequence exists for: a request already inside the old server runs
// to completion against it, and the old server is not closed until it does.
//
// The backend blocks on a channel the test controls, so this is deterministic
// rather than a race the test hopes to win.
func TestReload_InFlightRequestSurvivesSwap(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Only the first request blocks. The test polls the supervisor while
		// that request is pinned, and those polls land on this same backend
		// until the swap happens -- blocking them too would deadlock the test
		// rather than test anything.
		held := false
		once.Do(func() { held = true; close(entered) })
		if held {
			<-release
		}
		_, _ = io.WriteString(w, "A")
	}))
	defer slow.Close()

	b := backend(t, "B")
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeConfig(t, path, []string{slow.URL}, configOpts{})

	sup, _ := newSupervisor(t, path, nil)

	type result struct {
		code int
		body string
	}
	inflight := make(chan result, 1)
	go func() {
		rec := httptest.NewRecorder()
		sup.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		inflight <- result{rec.Code, rec.Body.String()}
	}()

	<-entered // the request is now inside the old generation

	writeConfig(t, path, []string{b.URL}, configOpts{})
	reloaded := make(chan error, 1)
	go func() { reloaded <- sup.Reload(path) }()

	// The swap happens immediately, so new traffic must already be on B while
	// the old request is still blocked. Poll: the reload goroutine has to get
	// as far as the store, and there is no hook for that moment.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if code, body := get(t, sup, "/"); code == http.StatusOK && body == "B" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("new traffic never reached the new generation while an old request was in flight")
		}
		time.Sleep(2 * time.Millisecond)
	}

	// And the reload must not have finished: closing the old server now would
	// pull the pool out from under the request still running in it.
	select {
	case err := <-reloaded:
		t.Fatalf("Reload returned (%v) while a request was still in flight in the old generation", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	select {
	case got := <-inflight:
		if got.code != http.StatusOK || got.body != "A" {
			t.Errorf("in-flight request got %d %q, want 200 \"A\" from the old generation", got.code, got.body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request never completed")
	}

	select {
	case err := <-reloaded:
		if err != nil {
			t.Fatalf("Reload: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Reload did not finish after the drain completed")
	}
}

// TestReload_SignalTriggersReload exercises the SIGHUP path by calling the
// handler directly. Signalling the test binary's own process would be a
// process-wide side effect in a package whose tests run in parallel with
// nothing to catch a stray HUP.
func TestReload_SignalTriggersReload(t *testing.T) {
	a, b := backend(t, "A"), backend(t, "B")
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeConfig(t, path, []string{a.URL}, configOpts{})

	results := make(chan string, 4)
	sup, _ := newSupervisor(t, path, func(r string) { results <- r })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		sup.HandleSignals(ctx, sig)
	}()

	writeConfig(t, path, []string{b.URL}, configOpts{})
	sig <- syscall.SIGHUP

	select {
	case got := <-results:
		if got != ResultSuccess {
			t.Fatalf("SIGHUP reload reported %q, want %q", got, ResultSuccess)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SIGHUP did not trigger a reload")
	}

	if code, body := get(t, sup, "/"); code != http.StatusOK || body != "B" {
		t.Errorf("after SIGHUP: got %d %q, want 200 \"B\"", code, body)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("HandleSignals did not return when its context was cancelled")
	}
}

// TestReload_ClosesRetiredServer asserts the old generation is actually shut
// down rather than leaked.
//
// A leaked *proxy.Server is invisible in a functional test — traffic goes to
// the right place either way — and lethal in production: every reload would
// add a permanent set of prober goroutines hammering backends the config no
// longer mentions. Probe traffic is therefore the observable, and it is
// measured on the retired backend after the swap.
func TestReload_ClosesRetiredServer(t *testing.T) {
	var oldProbes, newProbes atomic.Int64
	oldBE := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		oldProbes.Add(1)
		_, _ = io.WriteString(w, "A")
	}))
	defer oldBE.Close()
	newBE := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		newProbes.Add(1)
		_, _ = io.WriteString(w, "B")
	}))
	defer newBE.Close()

	path := filepath.Join(t.TempDir(), "config.yaml")
	writeConfig(t, path, []string{oldBE.URL}, configOpts{activeHealth: true})

	sup, _ := newSupervisor(t, path, nil)

	// Wait until the old generation is definitely probing, so that "it stopped"
	// is a real observation and not a checker that never started.
	deadline := time.Now().Add(5 * time.Second)
	for oldProbes.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("active health checking never started on the first generation")
		}
		time.Sleep(5 * time.Millisecond)
	}

	writeConfig(t, path, []string{newBE.URL}, configOpts{activeHealth: true})
	if err := sup.Reload(path); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	// Reload returns only after the old server is drained and closed, and
	// proxy.Close waits for its prober goroutines to exit — so the count is
	// already frozen here, with no sleep needed to make that true.
	frozen := oldProbes.Load()
	time.Sleep(200 * time.Millisecond) // eight probe intervals

	if after := oldProbes.Load(); after != frozen {
		t.Errorf("retired generation issued %d more probes after being closed; it was leaked", after-frozen)
	}
	if newProbes.Load() == 0 {
		t.Error("the new generation is not health checking; the test above would pass on a proxy that never probes")
	}
}

// TestReload_WatchReloadsOnAtomicReplace wires the file watcher to the
// supervisor, which is the combination the -watch flag ships. The config file
// is replaced by rename, the way real tooling writes it.
func TestReload_WatchReloadsOnAtomicReplace(t *testing.T) {
	a, b := backend(t, "A"), backend(t, "B")
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeConfig(t, path, []string{a.URL}, configOpts{})

	results := make(chan string, 8)
	sup, _ := newSupervisor(t, path, func(r string) { results <- r })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchDone := make(chan error, 1)
	go func() { watchDone <- sup.Watch(ctx, 40*time.Millisecond) }()

	// Retried, because the OS-level watch is established asynchronously and
	// this test must not depend on winning that race. Every attempt writes the
	// same new config, so repeats are harmless.
	next := configYAML([]string{b.URL}, configOpts{})
	deadline := time.After(15 * time.Second)
	for {
		tmp := filepath.Join(dir, "config.yaml.new")
		if err := os.WriteFile(tmp, []byte(next), 0o600); err != nil {
			t.Fatalf("write temp config: %v", err)
		}
		if err := os.Rename(tmp, path); err != nil {
			t.Fatalf("rename over config: %v", err)
		}
		select {
		case got := <-results:
			if got != ResultSuccess {
				t.Fatalf("watch-triggered reload reported %q", got)
			}
			if code, body := get(t, sup, "/"); code != http.StatusOK || body != "B" {
				t.Errorf("after watch reload: got %d %q, want 200 \"B\"", code, body)
			}
			cancel()
			select {
			case err := <-watchDone:
				if err != nil {
					t.Errorf("Watch returned %v, want nil on cancellation", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("Watch did not return after cancellation")
			}
			return
		case <-time.After(300 * time.Millisecond):
		case <-deadline:
			t.Fatal("replacing the config file never triggered a reload")
		}
	}
}

// TestReload_AfterCloseIsRefused: a supervisor whose process is shutting down
// must not build a new generation that nothing will ever close.
func TestReload_AfterCloseIsRefused(t *testing.T) {
	a := backend(t, "A")
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeConfig(t, path, []string{a.URL}, configOpts{})

	sup, _ := newSupervisor(t, path, nil)
	sup.Close()
	sup.Close() // idempotent

	if err := sup.Reload(path); err == nil {
		t.Fatal("Reload succeeded after Close")
	}
}
