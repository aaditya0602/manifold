package upstream

import (
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/aaditya0602/manifold/internal/balance"
	"github.com/aaditya0602/manifold/internal/config"
)

func poolCfg(name string, urls ...string) config.PoolConfig {
	ups := make([]config.UpstreamConfig, len(urls))
	for i, u := range urls {
		ups[i] = config.UpstreamConfig{URL: u, Weight: 1}
	}
	return config.PoolConfig{
		Name:      name,
		Strategy:  config.StrategyRoundRobin,
		Upstreams: ups,
		Retry:     config.RetryConfig{MaxAttempts: 1},
		Transport: config.TransportConfig{
			MaxIdleConns:        128,
			MaxIdleConnsPerHost: 32,
			IdleConnTimeout:     config.Duration(90 * time.Second),
			DialTimeout:         config.Duration(2 * time.Second),
		},
	}
}

func TestNewPool_BuildsBackends(t *testing.T) {
	p, err := NewPool(poolCfg("api", "http://127.0.0.1:9001", "http://127.0.0.1:9002"))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	if p.Name() != "api" {
		t.Errorf("Name = %q", p.Name())
	}
	if p.Strategy().Name() != balance.StrategyRoundRobin {
		t.Errorf("Strategy = %q", p.Strategy().Name())
	}
	if p.Gen() != 0 {
		t.Errorf("Gen = %d, want 0 before any membership change", p.Gen())
	}

	for i, want := range []string{"http://127.0.0.1:9001", "http://127.0.0.1:9002"} {
		b := p.Backend(i)
		if b == nil {
			t.Fatalf("Backend(%d) = nil", i)
		}
		if b.ID() != i {
			t.Errorf("Backend(%d).ID() = %d", i, b.ID())
		}
		if b.Key() != want {
			t.Errorf("Backend(%d).Key() = %q, want %q", i, b.Key(), want)
		}
		if b.URL().String() != want {
			t.Errorf("Backend(%d).URL() = %q, want %q", i, b.URL(), want)
		}
	}

	if p.Backend(-1) != nil || p.Backend(2) != nil {
		t.Error("Backend must return nil for an out-of-range id rather than panicking")
	}
}

// TestNewPool_KeyIsCanonical: the key feeds consistent hashing's ring
// placement, so scheme/host casing in the YAML must not change it.
func TestNewPool_KeyIsCanonical(t *testing.T) {
	p, err := NewPool(poolCfg("api", "HTTP://127.0.0.1:9001"))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	if got := p.Backend(0).Key(); got != "http://127.0.0.1:9001" {
		t.Fatalf("Key = %q, want lowercase canonical form", got)
	}
}

// TestNewPool_UnimplementedStrategySurfaces is the point of the whole factory
// contract: a Week 2 strategy named in config must fail at startup, not
// silently degrade to round-robin under traffic.
func TestNewPool_UnimplementedStrategySurfaces(t *testing.T) {
	for _, s := range []config.Strategy{config.StrategyLeastConn, config.StrategyConsistentHash} {
		cfg := poolCfg("api", "http://127.0.0.1:9001")
		cfg.Strategy = s

		_, err := NewPool(cfg)
		if err == nil {
			t.Fatalf("strategy %q: NewPool succeeded, want an error", s)
		}
		var notImpl *balance.ErrNotImplemented
		if !errors.As(err, &notImpl) {
			t.Fatalf("strategy %q: error %v does not wrap *balance.ErrNotImplemented", s, err)
		}
	}
}

func TestNewPool_Rejects(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.PoolConfig
	}{
		{"no name", poolCfg("", "http://127.0.0.1:9001")},
		{"no upstreams", poolCfg("api")},
		{"url without scheme", poolCfg("api", "127.0.0.1:9001")},
		{"url without host", poolCfg("api", "http://")},
		{"unknown strategy", func() config.PoolConfig {
			c := poolCfg("api", "http://127.0.0.1:9001")
			c.Strategy = "random"
			return c
		}()},
		{"duplicate upstream", poolCfg("api", "http://127.0.0.1:9001", "HTTP://127.0.0.1:9001")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewPool(tc.cfg); err == nil {
				t.Fatal("NewPool succeeded, want an error")
			}
		})
	}
}

// TestPool_CandidatesAreFresh: strategies may retain the slice they are given
// (round-robin caches a table derived from it), so Candidates must never hand
// back aliased storage.
func TestPool_CandidatesAreFresh(t *testing.T) {
	p, err := NewPool(poolCfg("api", "http://127.0.0.1:9001", "http://127.0.0.1:9002"))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	first := p.Candidates()
	first[0].Key = "mutated"

	second := p.Candidates()
	if second[0].Key != "http://127.0.0.1:9001" {
		t.Fatalf("mutating a returned candidate slice corrupted the pool: %q", second[0].Key)
	}
	if &first[0] == &second[0] {
		t.Fatal("Candidates returned aliased storage")
	}
}

// TestPool_CandidatesReflectInFlight is what least_conn will read in Week 2.
func TestPool_CandidatesReflectInFlight(t *testing.T) {
	p, err := NewPool(poolCfg("api", "http://127.0.0.1:9001", "http://127.0.0.1:9002"))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	b := p.Backend(1)
	b.Acquire()
	b.Acquire()
	defer func() { b.Release(); b.Release() }()

	c := p.Candidates()
	if c[0].InFlight != 0 {
		t.Errorf("candidate 0 in-flight = %d, want 0", c[0].InFlight)
	}
	if c[1].InFlight != 2 {
		t.Errorf("candidate 1 in-flight = %d, want 2", c[1].InFlight)
	}
}

// TestBackend_InFlightIsAtomic asserts an exact final count from many
// goroutines. On the Windows dev host the race detector is unavailable, so an
// exact-count assertion is what catches a non-atomic counter: a lost update
// leaves the total short deterministically often at this concurrency.
func TestBackend_InFlightIsAtomic(t *testing.T) {
	p, err := NewPool(poolCfg("api", "http://127.0.0.1:9001"))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	b := p.Backend(0)

	const goroutines = 64
	const iterations = 500

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				b.Acquire()
			}
		}()
	}
	wg.Wait()

	if got := b.InFlight(); got != goroutines*iterations {
		t.Fatalf("in-flight = %d, want %d", got, goroutines*iterations)
	}

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				b.Release()
			}
		}()
	}
	wg.Wait()

	if got := b.InFlight(); got != 0 {
		t.Fatalf("in-flight = %d after balanced releases, want 0", got)
	}
}

// TestPool_TransportIsSharedAndHTTP1 pins both the one-transport-per-pool
// decision and the deliberate HTTP/1.1-only stance.
func TestPool_TransportIsSharedAndHTTP1(t *testing.T) {
	cfg := poolCfg("api", "http://127.0.0.1:9001", "http://127.0.0.1:9002")
	cfg.Transport.MaxConnsPerHost = 7
	cfg.Transport.ResponseHeaderTimeout = config.Duration(3 * time.Second)
	cfg.Transport.DisableCompression = true

	p, err := NewPool(cfg)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	rt := p.Transport()
	if rt != p.Transport() {
		t.Fatal("Transport must return the same instance for every backend in the pool")
	}
	tr, ok := rt.(*http.Transport)
	if !ok {
		t.Fatalf("Transport is %T, want *http.Transport", rt)
	}
	if tr.ForceAttemptHTTP2 {
		t.Error("ForceAttemptHTTP2 must stay false: manifold is deliberately HTTP/1.1 upstream")
	}
	if tr.MaxConnsPerHost != 7 {
		t.Errorf("MaxConnsPerHost = %d, want 7", tr.MaxConnsPerHost)
	}
	if tr.ResponseHeaderTimeout != 3*time.Second {
		t.Errorf("ResponseHeaderTimeout = %v", tr.ResponseHeaderTimeout)
	}
	if !tr.DisableCompression {
		t.Error("DisableCompression not carried through")
	}
	if tr.DialContext == nil {
		t.Error("DialContext must be set from a net.Dialer carrying dial_timeout")
	}

	// Must not panic on a transport with no open connections.
	p.CloseIdleConnections()
}

func TestPool_ConfigRoundTrips(t *testing.T) {
	cfg := poolCfg("api", "http://127.0.0.1:9001")
	cfg.Retry = config.RetryConfig{MaxAttempts: 3, IdempotentOnly: true}

	p, err := NewPool(cfg)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	got := p.Config()
	if got.Retry.MaxAttempts != 3 || !got.Retry.IdempotentOnly {
		t.Fatalf("Config().Retry = %+v", got.Retry)
	}
	if got.Name != "api" {
		t.Fatalf("Config().Name = %q", got.Name)
	}
}

// TestPool_WeightNormalisation guards the strategies against a divide by zero
// if they are ever handed an unvalidated config.
func TestPool_WeightNormalisation(t *testing.T) {
	cfg := poolCfg("api", "http://127.0.0.1:9001")
	cfg.Upstreams[0].Weight = 0

	p, err := NewPool(cfg)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	if got := p.Candidates()[0].Weight; got != 1 {
		t.Fatalf("weight = %d, want normalised to 1", got)
	}
}
