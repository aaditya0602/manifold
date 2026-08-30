package health

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/aaditya0602/manifold/internal/config"
	"github.com/aaditya0602/manifold/internal/upstream"
)

// passivePool builds a pool with passive checking configured and active
// checking off, so tracker tests observe only the tracker's transitions. The
// origins are never dialled: passive health is driven entirely by recorded
// outcomes.
func passivePool(t *testing.T, passive config.PassiveHealthConfig, urls ...string) *upstream.Pool {
	t.Helper()

	ups := make([]config.UpstreamConfig, len(urls))
	for i, u := range urls {
		ups[i] = config.UpstreamConfig{URL: u, Weight: 1}
	}
	p, err := upstream.NewPool(config.PoolConfig{
		Name:      "test",
		Strategy:  config.StrategyRoundRobin,
		Upstreams: ups,
		Health:    config.HealthConfig{Passive: passive},
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

func passiveCfg(minRequests int, errorRate float64, window, ejectFor time.Duration) config.PassiveHealthConfig {
	return config.PassiveHealthConfig{
		Enabled:     true,
		Window:      config.Duration(window),
		MinRequests: minRequests,
		ErrorRate:   errorRate,
		EjectFor:    config.Duration(ejectFor),
	}
}

// A failure is a connection error or a 5xx. A 4xx is the client's fault and
// must never count against the backend.
func TestFailed_Classification(t *testing.T) {
	cases := []struct {
		name   string
		status int
		err    error
		want   bool
	}{
		{"transport error", 0, errors.New("dial tcp: refused"), true},
		{"500", http.StatusInternalServerError, nil, true},
		{"502", http.StatusBadGateway, nil, true},
		{"503", http.StatusServiceUnavailable, nil, true},
		{"400", http.StatusBadRequest, nil, false},
		{"401", http.StatusUnauthorized, nil, false},
		{"404", http.StatusNotFound, nil, false},
		{"429", http.StatusTooManyRequests, nil, false},
		{"200", http.StatusOK, nil, false},
		{"304", http.StatusNotModified, nil, false},
	}
	for _, c := range cases {
		if got := Failed(c.status, c.err); got != c.want {
			t.Errorf("Failed(%s) = %v, want %v", c.name, got, c.want)
		}
	}
}

// Test 6: below min_requests the backend is never ejected, however bad the
// ratio. The very next sample, which reaches min_requests, does eject it —
// that is what pins the assertion to the sample-size guard rather than to some
// other reason for not ejecting.
func TestTracker_BelowMinRequestsNeverEjects(t *testing.T) {
	const minRequests = 10
	pool := passivePool(t, passiveCfg(minRequests, 0.5, 5*time.Second, time.Minute), "http://127.0.0.1:9001")
	tr := NewTracker(pool)
	// Started: ejection only happens when a sweeper exists to readmit.
	trCtx, trCancel := context.WithCancel(context.Background())
	tr.Start(trCtx)
	t.Cleanup(func() { trCancel(); tr.Wait() })

	for i := 0; i < minRequests-1; i++ {
		tr.Record(0, true) // a 100% failure rate, still too small a sample
		if pool.Backend(0).Ejected() {
			t.Fatalf("ejected after %d failures, below min_requests=%d", i+1, minRequests)
		}
	}
	if pool.Gen() != 0 {
		t.Fatalf("Gen = %d, want 0: nothing should have changed", pool.Gen())
	}

	tr.Record(0, true)
	if !pool.Backend(0).Ejected() {
		t.Fatalf("not ejected at min_requests=%d with a 100%% failure rate", minRequests)
	}
}

// The threshold is a strict inequality: an error rate exactly at error_rate is
// not over it.
func TestTracker_RatioAtThresholdDoesNotEject(t *testing.T) {
	pool := passivePool(t, passiveCfg(4, 0.5, 5*time.Second, time.Minute), "http://127.0.0.1:9001")
	tr := NewTracker(pool)
	// Started: ejection only happens when a sweeper exists to readmit.
	trCtx, trCancel := context.WithCancel(context.Background())
	tr.Start(trCtx)
	t.Cleanup(func() { trCancel(); tr.Wait() })

	tr.Record(0, true)
	tr.Record(0, false)
	tr.Record(0, true)
	tr.Record(0, false) // 2/4 = 0.5, exactly the configured rate
	if pool.Backend(0).Ejected() {
		t.Fatal("ejected at a failure ratio exactly equal to error_rate")
	}

	tr.Record(0, true) // 3/5 = 0.6, over
	if !pool.Backend(0).Ejected() {
		t.Fatal("not ejected at a failure ratio above error_rate")
	}
}

// Test 7: above the threshold the backend is ejected, and the sweeper readmits
// it automatically once eject_for has elapsed.
func TestTracker_EjectsAndReadmitsAutomatically(t *testing.T) {
	const ejectFor = 80 * time.Millisecond
	pool := passivePool(t, passiveCfg(5, 0.5, time.Second, ejectFor),
		"http://127.0.0.1:9001", "http://127.0.0.1:9002")
	tr := NewTracker(pool)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); tr.Wait() }()
	tr.Start(ctx)

	ejectedAt := time.Now()
	for i := 0; i < 5; i++ {
		tr.Record(0, true)
	}
	if !pool.Backend(0).Ejected() {
		t.Fatal("not ejected after 5 consecutive failures over min_requests=5")
	}
	if got := len(pool.Candidates()); got != 1 {
		t.Fatalf("Candidates = %d while one backend is ejected, want 1", got)
	}
	if until := pool.EjectedUntil(0); until.Before(ejectedAt.Add(ejectFor)) {
		t.Errorf("EjectedUntil = %v, want at least %v", until, ejectedAt.Add(ejectFor))
	}
	genEjected := pool.Gen()

	waitFor(t, 2*time.Second, func() bool { return !pool.Backend(0).Ejected() })

	if elapsed := time.Since(ejectedAt); elapsed < ejectFor {
		t.Errorf("readmitted after %s, before eject_for=%s elapsed", elapsed, ejectFor)
	}
	if got := len(pool.Candidates()); got != 2 {
		t.Errorf("Candidates = %d after readmission, want 2", got)
	}
	if pool.Gen() == genEjected {
		t.Error("Gen did not move on readmission")
	}

	// The window was reset on ejection, so the readmitted backend starts from
	// a clean slate: four fresh failures are below min_requests and must not
	// re-eject it instantly on stale samples.
	for i := 0; i < 4; i++ {
		tr.Record(0, true)
	}
	if pool.Backend(0).Ejected() {
		t.Error("re-ejected below min_requests; the window was not reset")
	}
}

// Test 8: a flood of 4xx never ejects. The sample size and the request count
// are far past every threshold; only the classification keeps the backend in.
func TestTracker_FloodOf4xxNeverEjects(t *testing.T) {
	pool := passivePool(t, passiveCfg(10, 0.5, 5*time.Second, time.Minute), "http://127.0.0.1:9001")
	tr := NewTracker(pool)
	// Started: ejection only happens when a sweeper exists to readmit.
	trCtx, trCancel := context.WithCancel(context.Background())
	tr.Start(trCtx)
	t.Cleanup(func() { trCancel(); tr.Wait() })

	for i := 0; i < 500; i++ {
		tr.Record(0, Failed(http.StatusNotFound, nil))
	}
	if pool.Backend(0).Ejected() {
		t.Fatal("ejected by a flood of 404s; 4xx is the client's fault, not the backend's")
	}
	if pool.Gen() != 0 {
		t.Errorf("Gen = %d, want 0", pool.Gen())
	}
	if got := len(pool.Candidates()); got != 1 {
		t.Errorf("Candidates = %d, want 1", got)
	}

	// The identical flood of 5xx, against a fresh pool, does eject — which is
	// what proves the 404 flood was large enough to have ejected the backend
	// had it been counted, and that only the classification spared it.
	control := passivePool(t, passiveCfg(10, 0.5, 5*time.Second, time.Minute), "http://127.0.0.1:9001")
	ctrlTracker := NewTracker(control)
	// Started: ejection only happens when a sweeper exists to readmit.
	ctrlTrackerCtx, ctrlTrackerCancel := context.WithCancel(context.Background())
	ctrlTracker.Start(ctrlTrackerCtx)
	t.Cleanup(func() { ctrlTrackerCancel(); ctrlTracker.Wait() })
	for i := 0; i < 500; i++ {
		ctrlTracker.Record(0, Failed(http.StatusInternalServerError, nil))
	}
	if !control.Backend(0).Ejected() {
		t.Fatal("not ejected by an identical flood of 500s")
	}
}

// Ejecting one backend must not disturb another's window.
func TestTracker_WindowsArePerBackend(t *testing.T) {
	pool := passivePool(t, passiveCfg(4, 0.5, 5*time.Second, time.Minute),
		"http://127.0.0.1:9001", "http://127.0.0.1:9002")
	tr := NewTracker(pool)
	// Started: ejection only happens when a sweeper exists to readmit.
	trCtx, trCancel := context.WithCancel(context.Background())
	tr.Start(trCtx)
	t.Cleanup(func() { trCancel(); tr.Wait() })

	for i := 0; i < 4; i++ {
		tr.Record(0, true)
		tr.Record(1, false)
	}
	if !pool.Backend(0).Ejected() {
		t.Error("failing backend not ejected")
	}
	if pool.Backend(1).Ejected() {
		t.Error("healthy backend ejected by its neighbour's failures")
	}
}

// Samples age out of the sliding window: failures older than window must not
// combine with fresh ones to cross the threshold.
func TestTracker_SamplesAgeOut(t *testing.T) {
	const window = 60 * time.Millisecond
	pool := passivePool(t, passiveCfg(6, 0.5, window, time.Minute), "http://127.0.0.1:9001")
	tr := NewTracker(pool)
	// Started: ejection only happens when a sweeper exists to readmit.
	trCtx, trCancel := context.WithCancel(context.Background())
	tr.Start(trCtx)
	t.Cleanup(func() { trCancel(); tr.Wait() })

	for i := 0; i < 5; i++ {
		tr.Record(0, true)
	}
	if pool.Backend(0).Ejected() {
		t.Fatal("ejected below min_requests")
	}

	time.Sleep(2 * window)

	for i := 0; i < 5; i++ {
		tr.Record(0, true)
	}
	if pool.Backend(0).Ejected() {
		t.Error("ejected on a total that only reaches min_requests by counting expired samples")
	}
}

// Record is called concurrently from every request goroutine. With no race
// detector on this host, the assertion is an exact count: every sample must
// land, and none may be double counted.
func TestTracker_RecordIsConcurrencySafe(t *testing.T) {
	const (
		goroutines = 16
		perG       = 200
	)
	// min_requests far above the sample count, so nothing ejects and the
	// window is never reset underneath the count.
	pool := passivePool(t, passiveCfg(1_000_000, 0.5, 10*time.Second, time.Minute), "http://127.0.0.1:9001")
	tr := NewTracker(pool)
	// Started: ejection only happens when a sweeper exists to readmit.
	trCtx, trCancel := context.WithCancel(context.Background())
	tr.Start(trCtx)
	t.Cleanup(func() { trCancel(); tr.Wait() })

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				tr.Record(0, i%2 == 0)
			}
		}()
	}
	wg.Wait()

	slot := time.Now().UnixNano() / int64(tr.bucketDur)
	total, fail := tr.counts(tr.windows[0], slot)
	if total != goroutines*perG {
		t.Errorf("total = %d, want %d", total, goroutines*perG)
	}
	if fail != goroutines*perG/2 {
		t.Errorf("fail = %d, want %d", fail, goroutines*perG/2)
	}
}

// Test 10, passive half: Start returns immediately and the sweeper exits on
// cancellation.
func TestTracker_StartWaitCleanShutdown(t *testing.T) {
	pool := passivePool(t, passiveCfg(2, 0.5, time.Second, 30*time.Millisecond), "http://127.0.0.1:9001")
	tr := NewTracker(pool)

	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	tr.Start(ctx)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Start blocked for %s, want an immediate return", elapsed)
	}

	tr.Record(0, true)
	tr.Record(0, true)
	if !pool.Backend(0).Ejected() {
		t.Fatal("not ejected")
	}
	waitFor(t, time.Second, func() bool { return !pool.Backend(0).Ejected() })

	cancel()
	done := make(chan struct{})
	go func() { tr.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after cancellation: the sweeper is still running")
	}

	// With the sweeper stopped, an ejection stays put: nothing is readmitting
	// behind the caller's back.
	tr.Record(0, true)
	tr.Record(0, true)
	if !pool.Backend(0).Ejected() {
		t.Fatal("not ejected after shutdown")
	}
	time.Sleep(100 * time.Millisecond) // several sweep intervals' worth
	if !pool.Backend(0).Ejected() {
		t.Error("backend readmitted after the sweeper was shut down")
	}
}

// A tracker for a pool with passive checks disabled is usable and inert.
func TestTracker_DisabledIsInert(t *testing.T) {
	pool := passivePool(t, config.PassiveHealthConfig{Enabled: false}, "http://127.0.0.1:9001")
	tr := NewTracker(pool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)
	tr.Wait()

	for i := 0; i < 100; i++ {
		tr.Record(0, true)
	}
	if pool.Backend(0).Ejected() {
		t.Error("disabled tracker ejected a backend")
	}
	if pool.Gen() != 0 {
		t.Errorf("Gen = %d, want 0", pool.Gen())
	}
}

// An out-of-range backend ID is ignored rather than panicking: IDs travel
// through strategy and retry code, and a bug there must not take the proxy
// down.
func TestTracker_UnknownBackendIDIsIgnored(t *testing.T) {
	pool := passivePool(t, passiveCfg(1, 0.5, time.Second, time.Minute), "http://127.0.0.1:9001")
	tr := NewTracker(pool)
	// Started: ejection only happens when a sweeper exists to readmit.
	trCtx, trCancel := context.WithCancel(context.Background())
	tr.Start(trCtx)
	t.Cleanup(func() { trCancel(); tr.Wait() })

	tr.Record(-1, true)
	tr.Record(99, true)
	if pool.Gen() != 0 {
		t.Errorf("Gen = %d, want 0", pool.Gen())
	}
}

// Record must not allocate: it runs once per proxied request.
func TestTracker_RecordDoesNotAllocate(t *testing.T) {
	pool := passivePool(t, passiveCfg(1_000_000, 0.5, 10*time.Second, time.Minute), "http://127.0.0.1:9001")
	tr := NewTracker(pool)
	// Started: ejection only happens when a sweeper exists to readmit.
	trCtx, trCancel := context.WithCancel(context.Background())
	tr.Start(trCtx)
	t.Cleanup(func() { trCancel(); tr.Wait() })

	if got := testing.AllocsPerRun(200, func() { tr.Record(0, true) }); got != 0 {
		t.Errorf("Record allocated %v objects per call, want 0", got)
	}
}

// TestTracker_NeverEjectsWithoutStart pins the invariant that ejection is only
// performed when a sweeper exists to undo it. Without this, a caller that
// records outcomes but forgets Start silently loses backends forever.
func TestTracker_NeverEjectsWithoutStart(t *testing.T) {
	pool := passivePool(t, config.PassiveHealthConfig{
		Enabled:     true,
		Window:      config.Duration(time.Second),
		MinRequests: 10,
		ErrorRate:   0.5,
		EjectFor:    config.Duration(time.Second),
	}, "http://127.0.0.1:19001", "http://127.0.0.1:19002")
	tr := NewTracker(pool)

	// Far more failures than the threshold requires.
	for i := 0; i < 500; i++ {
		tr.Record(0, true)
	}
	if pool.Backend(0).Ejected() {
		t.Fatal("tracker ejected a backend with no sweeper running to readmit it")
	}
	if got := len(pool.Candidates()); got != 2 {
		t.Fatalf("candidates = %d, want 2 (nothing should have left rotation)", got)
	}
}
