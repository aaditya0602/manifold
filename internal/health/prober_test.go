package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaditya0602/manifold/internal/config"
	"github.com/aaditya0602/manifold/internal/upstream"
)

// testPool builds a pool over the given origins with active checking
// configured and passive checking off, so prober tests observe only the
// prober's transitions.
func testPool(t *testing.T, active config.ActiveHealthConfig, urls ...string) *upstream.Pool {
	t.Helper()

	ups := make([]config.UpstreamConfig, len(urls))
	for i, u := range urls {
		ups[i] = config.UpstreamConfig{URL: u, Weight: 1}
	}
	p, err := upstream.NewPool(config.PoolConfig{
		Name:      "test",
		Strategy:  config.StrategyRoundRobin,
		Upstreams: ups,
		Health:    config.HealthConfig{Active: active},
		Transport: config.TransportConfig{
			MaxIdleConns:        16,
			MaxIdleConnsPerHost: 4,
			IdleConnTimeout:     config.Duration(time.Second),
			DialTimeout:         config.Duration(time.Second),
		},
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	return p
}

func activeCfg(healthy, unhealthy int) config.ActiveHealthConfig {
	return config.ActiveHealthConfig{
		Enabled:            true,
		Path:               "/healthz",
		Method:             http.MethodGet,
		Interval:           config.Duration(30 * time.Millisecond),
		Timeout:            config.Duration(10 * time.Millisecond),
		HealthyThreshold:   healthy,
		UnhealthyThreshold: unhealthy,
	}
}

// statusServer answers /healthz with whatever code is currently set, counting
// hits. Driving the code from the test rather than from timing is what keeps
// these assertions exact.
type statusServer struct {
	srv  *httptest.Server
	code atomic.Int64
	hits atomic.Int64
}

func newStatusServer(t *testing.T, code int) *statusServer {
	t.Helper()
	s := &statusServer{}
	s.code.Store(int64(code))
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.hits.Add(1)
		if r.URL.Path != "/healthz" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(int(s.code.Load()))
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *statusServer) setCode(c int) { s.code.Store(int64(c)) }
func (s *statusServer) url() string   { return s.srv.URL }

// Test 1: a backend returning 500 goes unhealthy after exactly
// unhealthy_threshold consecutive failures, and not one probe earlier.
func TestProber_UnhealthyAfterExactlyThreshold(t *testing.T) {
	const unhealthy = 3
	srv := newStatusServer(t, http.StatusInternalServerError)
	pool := testPool(t, activeCfg(2, unhealthy), srv.url())

	pr, err := NewProber(pool)
	if err != nil {
		t.Fatalf("NewProber: %v", err)
	}
	b := pool.Backend(0)
	var st probeState

	for i := 1; i < unhealthy; i++ {
		pr.probe(context.Background(), b, &st)
		if !b.ActiveHealthy() {
			t.Fatalf("backend went unhealthy after %d failures, want %d", i, unhealthy)
		}
		if pool.Gen() != 0 {
			t.Fatalf("Gen = %d after %d failures, want 0 before the threshold", pool.Gen(), i)
		}
	}

	pr.probe(context.Background(), b, &st)
	if b.ActiveHealthy() {
		t.Fatalf("backend still healthy after %d consecutive failures", unhealthy)
	}
	if pool.Gen() != 1 {
		t.Errorf("Gen = %d after ejection, want exactly 1", pool.Gen())
	}

	// Further failures are steady state and must not churn the generation.
	pr.probe(context.Background(), b, &st)
	pr.probe(context.Background(), b, &st)
	if pool.Gen() != 1 {
		t.Errorf("Gen = %d after repeated identical verdicts, want 1", pool.Gen())
	}
}

// Test 2: it recovers after exactly healthy_threshold consecutive successes.
func TestProber_RecoversAfterExactlyHealthyThreshold(t *testing.T) {
	const healthy = 2
	srv := newStatusServer(t, http.StatusInternalServerError)
	pool := testPool(t, activeCfg(healthy, 1), srv.url())

	pr, err := NewProber(pool)
	if err != nil {
		t.Fatalf("NewProber: %v", err)
	}
	b := pool.Backend(0)
	var st probeState

	pr.probe(context.Background(), b, &st)
	if b.ActiveHealthy() {
		t.Fatal("backend healthy after reaching unhealthy_threshold of 1")
	}

	srv.setCode(http.StatusOK)
	for i := 1; i < healthy; i++ {
		pr.probe(context.Background(), b, &st)
		if b.ActiveHealthy() {
			t.Fatalf("backend recovered after %d successes, want %d", i, healthy)
		}
	}
	pr.probe(context.Background(), b, &st)
	if !b.ActiveHealthy() {
		t.Fatalf("backend still unhealthy after %d consecutive successes", healthy)
	}
	if got := len(pool.Candidates()); got != 1 {
		t.Errorf("Candidates = %d after recovery, want 1", got)
	}
}

// Test 3: failures that never form a run of unhealthy_threshold never eject.
// The thresholds count consecutive outcomes; one success resets the run.
func TestProber_IntermittentFailuresNeverEject(t *testing.T) {
	srv := newStatusServer(t, http.StatusOK)
	pool := testPool(t, activeCfg(2, 3), srv.url())

	pr, err := NewProber(pool)
	if err != nil {
		t.Fatalf("NewProber: %v", err)
	}
	b := pool.Backend(0)
	var st probeState

	// Two failures, one success, repeated: never three in a row.
	for round := 0; round < 5; round++ {
		srv.setCode(http.StatusBadGateway)
		pr.probe(context.Background(), b, &st)
		pr.probe(context.Background(), b, &st)
		srv.setCode(http.StatusOK)
		pr.probe(context.Background(), b, &st)

		if !b.ActiveHealthy() {
			t.Fatalf("round %d: backend ejected without %d consecutive failures", round, 3)
		}
	}
	if pool.Gen() != 0 {
		t.Errorf("Gen = %d, want 0: no transition should have occurred", pool.Gen())
	}
}

// Test 4: the candidate set shrinks when a backend goes unhealthy and grows
// again when it recovers.
func TestProber_CandidatesShrinkAndGrow(t *testing.T) {
	good := newStatusServer(t, http.StatusOK)
	bad := newStatusServer(t, http.StatusInternalServerError)
	pool := testPool(t, activeCfg(1, 2), good.url(), bad.url())

	pr, err := NewProber(pool)
	if err != nil {
		t.Fatalf("NewProber: %v", err)
	}
	badBackend := pool.Backend(1)
	var st probeState

	if got := len(pool.Candidates()); got != 2 {
		t.Fatalf("Candidates = %d at start, want 2", got)
	}

	pr.probe(context.Background(), badBackend, &st)
	pr.probe(context.Background(), badBackend, &st)

	cands := pool.Candidates()
	if len(cands) != 1 {
		t.Fatalf("Candidates = %d after one backend failed, want 1", len(cands))
	}
	if cands[0].Key != pool.Backend(0).Key() {
		t.Errorf("surviving candidate = %q, want the healthy backend %q", cands[0].Key, pool.Backend(0).Key())
	}

	bad.setCode(http.StatusOK)
	pr.probe(context.Background(), badBackend, &st)
	if got := len(pool.Candidates()); got != 2 {
		t.Errorf("Candidates = %d after recovery, want 2", got)
	}
}

// Test 9: with every backend unhealthy the candidate set is empty. The proxy
// turns that into a 503 rather than sending traffic to origins it knows are
// broken.
func TestProber_AllUnhealthyLeavesNoCandidates(t *testing.T) {
	a := newStatusServer(t, http.StatusInternalServerError)
	b := newStatusServer(t, http.StatusInternalServerError)
	pool := testPool(t, activeCfg(1, 1), a.url(), b.url())

	pr, err := NewProber(pool)
	if err != nil {
		t.Fatalf("NewProber: %v", err)
	}
	var st0, st1 probeState
	pr.probe(context.Background(), pool.Backend(0), &st0)
	pr.probe(context.Background(), pool.Backend(1), &st1)

	if got := pool.Candidates(); len(got) != 0 {
		t.Fatalf("Candidates = %v, want empty when every backend is unhealthy", got)
	}
}

// A dead origin (nothing listening) is a transport failure, which counts
// exactly like a bad status.
func TestProber_TransportErrorIsAFailure(t *testing.T) {
	srv := newStatusServer(t, http.StatusOK)
	addr := srv.url()
	srv.srv.Close() // nothing is listening on addr any more

	pool := testPool(t, activeCfg(1, 1), addr)
	pr, err := NewProber(pool)
	if err != nil {
		t.Fatalf("NewProber: %v", err)
	}
	var st probeState
	pr.probe(context.Background(), pool.Backend(0), &st)
	if pool.Backend(0).ActiveHealthy() {
		t.Error("backend healthy after a probe to a dead origin")
	}
}

// expect_status replaces the default "any 2xx", so a backend answering 204 is
// unhealthy when only 200 is expected, and a 418 is healthy when listed.
func TestProber_ExpectStatus(t *testing.T) {
	srv := newStatusServer(t, http.StatusTeapot)
	cfg := activeCfg(1, 1)
	cfg.ExpectStatus = []int{http.StatusTeapot}
	pool := testPool(t, cfg, srv.url())

	pr, err := NewProber(pool)
	if err != nil {
		t.Fatalf("NewProber: %v", err)
	}
	var st probeState
	pr.probe(context.Background(), pool.Backend(0), &st)
	if !pool.Backend(0).ActiveHealthy() {
		t.Error("backend unhealthy on a status listed in expect_status")
	}

	srv.setCode(http.StatusNoContent)
	pr.probe(context.Background(), pool.Backend(0), &st)
	if pool.Backend(0).ActiveHealthy() {
		t.Error("backend healthy on a 2xx that expect_status does not list")
	}
}

// A probe must never use the pool's data-plane transport: sharing one lets
// probes evict warm connections, and a saturated data plane starve probes.
func TestProber_UsesItsOwnTransport(t *testing.T) {
	srv := newStatusServer(t, http.StatusOK)
	pool := testPool(t, activeCfg(1, 1), srv.url())

	pr, err := NewProber(pool)
	if err != nil {
		t.Fatalf("NewProber: %v", err)
	}
	if rt, ok := pool.Transport().(*http.Transport); ok && rt == pr.transport {
		t.Fatal("prober shares the pool's data-plane transport")
	}
	if pr.transport.MaxConnsPerHost != probeMaxConnsPerHost {
		t.Errorf("probe MaxConnsPerHost = %d, want the small cap %d",
			pr.transport.MaxConnsPerHost, probeMaxConnsPerHost)
	}
}

// Test 10: Start returns immediately, probing actually happens, and
// cancellation stops every goroutine — Wait returns and no further probe is
// issued afterwards.
func TestProber_StartWaitCleanShutdown(t *testing.T) {
	srv := newStatusServer(t, http.StatusOK)
	pool := testPool(t, activeCfg(1, 1), srv.url())

	pr, err := NewProber(pool)
	if err != nil {
		t.Fatalf("NewProber: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	pr.Start(ctx)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Start blocked for %s, want an immediate return", elapsed)
	}

	waitFor(t, time.Second, func() bool { return pr.probes.Load() > 0 })

	cancel()
	done := make(chan struct{})
	go func() { pr.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after cancellation: a goroutine is still running")
	}

	after := pr.probes.Load()
	time.Sleep(120 * time.Millisecond) // several probe intervals
	if got := pr.probes.Load(); got != after {
		t.Errorf("probes went from %d to %d after Wait returned; a goroutine outlived shutdown", after, got)
	}
}

// The first probe is staggered by a random fraction of the interval so a pool
// does not fire a synchronised burst at its own upstreams every interval.
func TestProber_StaggersFirstProbe(t *testing.T) {
	srv := newStatusServer(t, http.StatusOK)
	cfg := activeCfg(1, 1)
	cfg.Interval = config.Duration(400 * time.Millisecond)
	cfg.Timeout = config.Duration(50 * time.Millisecond)
	pool := testPool(t, cfg, srv.url())

	pr, err := NewProber(pool)
	if err != nil {
		t.Fatalf("NewProber: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); pr.Wait() }()

	pr.Start(ctx)
	// With no stagger the first probe lands within microseconds of Start.
	// A random phase over a 400ms interval makes that overwhelmingly
	// unlikely, but the assertion that survives every seed is only that the
	// probe is not synchronous with Start.
	time.Sleep(2 * time.Millisecond)
	if pr.probes.Load() > 0 && srv.hits.Load() > 1 {
		t.Errorf("probing began immediately at Start; the initial stagger is missing")
	}
}

// A prober for a pool with active checks disabled is usable and inert.
func TestProber_DisabledIsInert(t *testing.T) {
	srv := newStatusServer(t, http.StatusInternalServerError)
	pool := testPool(t, config.ActiveHealthConfig{Enabled: false}, srv.url())

	pr, err := NewProber(pool)
	if err != nil {
		t.Fatalf("NewProber: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr.Start(ctx)
	pr.Wait()

	time.Sleep(20 * time.Millisecond)
	if srv.hits.Load() != 0 {
		t.Errorf("disabled prober issued %d probes", srv.hits.Load())
	}
	if !pool.Backend(0).Available() {
		t.Error("backend unavailable under a disabled prober")
	}
}

func TestNewProber_RejectsBadInput(t *testing.T) {
	if _, err := NewProber(nil); err == nil {
		t.Error("NewProber(nil) = nil error")
	}

	cfg := activeCfg(1, 1)
	cfg.Interval = 0
	srv := newStatusServer(t, http.StatusOK)
	if _, err := NewProber(testPool(t, cfg, srv.url())); err == nil {
		t.Error("NewProber accepted a zero interval")
	}
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, limit time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", limit)
}
