package proxy

import (
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaditya0602/manifold/internal/config"
)

// Tests build config.Config values literally rather than parsing YAML. The
// loader is another package's work and is being written in parallel; coupling
// the data-plane tests to it would mean a bug there fails these tests for the
// wrong reason. The cost is that these helpers must restate the handful of
// defaults the data plane actually reads.

func testTransport() config.TransportConfig {
	return config.TransportConfig{
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     config.Duration(90 * time.Second),
		DialTimeout:         config.Duration(2 * time.Second),
		// No ResponseHeaderTimeout: a test that wants one sets it, and a
		// background timeout firing under a loaded CI machine is a flake
		// factory.
		DisableCompression: true,
	}
}

// pool builds a single pool of round-robin upstreams over the given origins.
func pool(name string, retry config.RetryConfig, urls ...string) config.PoolConfig {
	ups := make([]config.UpstreamConfig, len(urls))
	for i, u := range urls {
		ups[i] = config.UpstreamConfig{URL: u, Weight: 1}
	}
	return config.PoolConfig{
		Name:      name,
		Strategy:  config.StrategyRoundRobin,
		Upstreams: ups,
		Retry:     retry,
		Transport: testTransport(),
	}
}

// noRetry is the default retry policy for tests that are not about retries.
var noRetry = config.RetryConfig{MaxAttempts: 1}

func retryN(n int, idempotentOnly bool) config.RetryConfig {
	return config.RetryConfig{MaxAttempts: n, IdempotentOnly: idempotentOnly}
}

// catchAll routes every request to the named pool.
func catchAll(poolName string) config.RouteConfig {
	return config.RouteConfig{
		Match: config.MatchConfig{PathPrefix: "/"},
		Pool:  poolName,
	}
}

func newServer(t *testing.T, cfg *config.Config) *Server {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// simpleConfig is the common shape: one pool, one catch-all route.
func simpleConfig(retry config.RetryConfig, urls ...string) *config.Config {
	return &config.Config{
		Listen: ":0",
		Pools:  []config.PoolConfig{pool("api", retry, urls...)},
		Routes: []config.RouteConfig{catchAll("api")},
	}
}

// backend is a live httptest origin that counts the requests it serves.
type backend struct {
	*httptest.Server
	hits atomic.Int64
	// last is the most recent request's headers, captured so tests can assert
	// on what the proxy forwarded.
	lastHeader atomic.Pointer[http.Header]
	lastHost   atomic.Pointer[string]
}

// newBackend starts an origin. h may be nil for a plain 200 "ok".
func newBackend(t *testing.T, h http.HandlerFunc) *backend {
	t.Helper()
	b := &backend{}
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.hits.Add(1)
		hdr := r.Header.Clone()
		b.lastHeader.Store(&hdr)
		host := r.Host
		b.lastHost.Store(&host)
		if h != nil {
			h(w, r)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(b.Close)
	return b
}

func (b *backend) Hits() int64 { return b.hits.Load() }

// deadURL returns the origin of a server that has already been shut down, so
// dialing it is refused. This is the cleanest way to simulate a backend that
// is down without waiting on a dial timeout.
func deadURL(t *testing.T) string {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	u := s.URL
	s.Close()
	return u
}

// sink accepts TCP connections, counts them, and closes them immediately
// without speaking HTTP. Unlike a refused dial this is countable, which is how
// the "retries are bounded" test proves the attempt loop cannot run away.
//
// Each proxy attempt opens a brand-new connection (a different backend, and a
// closed connection is never returned to the idle pool), and http.Transport
// only retries internally on a *reused* connection — so accepts here equal
// proxy attempts exactly.
type sink struct {
	ln     net.Listener
	accept atomic.Int64
}

func newSink(t *testing.T) *sink {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &sink{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.accept.Add(1)
			_ = c.Close()
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

func (s *sink) URL() string    { return "http://" + s.ln.Addr().String() }
func (s *sink) Accepts() int64 { return s.accept.Load() }
