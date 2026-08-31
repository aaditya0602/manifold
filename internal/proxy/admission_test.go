package proxy

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaditya0602/manifold/internal/config"
	"github.com/aaditya0602/manifold/internal/observe"
)

// --- helpers --------------------------------------------------------------

func breakerCfg(threshold int, openFor time.Duration, probes int) config.BreakerConfig {
	return config.BreakerConfig{
		Enabled:          true,
		FailureThreshold: threshold,
		OpenFor:          config.Duration(openFor),
		HalfOpenProbes:   probes,
	}
}

// admissionConfig is simpleConfig plus breaker and limit policy. The existing
// helpers deliberately leave both at their zero values -- breaker disabled,
// max_in_flight 0 -- so that every test written before this change keeps
// measuring what it was written to measure.
func admissionConfig(retry config.RetryConfig, br config.BreakerConfig, lim config.LimitConfig, urls ...string) *config.Config {
	p := pool("api", retry, urls...)
	p.Breaker = br
	p.Limits = lim
	return &config.Config{
		Listen: ":0",
		Pools:  []config.PoolConfig{p},
		Routes: []config.RouteConfig{catchAll("api")},
	}
}

func get(t *testing.T, s *Server) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://x.test/api/thing", nil))
	return rec
}

// blocker is an origin that holds every request inside its handler until it is
// told to let go. It is how a test occupies in-flight slots without racing:
// entered reports that a request is definitely inside the handler, so the test
// never has to sleep and hope.
type blocker struct {
	*httptest.Server
	entered chan struct{}
	release chan struct{}
}

func newBlocker(t *testing.T) *blocker {
	t.Helper()
	b := &blocker{
		entered: make(chan struct{}, 64),
		release: make(chan struct{}),
	}
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		b.entered <- struct{}{}
		<-b.release
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(func() {
		select {
		case <-b.release:
		default:
			close(b.release)
		}
		b.Close()
	})
	return b
}

func (b *blocker) waitEntered(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-b.entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d requests reached the backend", i, n)
		}
	}
}

func (b *blocker) releaseAll() { close(b.release) }

// --- breaker integration --------------------------------------------------

// TestBreaker_OpenSkipsTheUpstreamEntirely is the assertion the whole feature
// stands on. It is not enough that the client gets a 503: the backend handler
// must never run, because a breaker that still pays the connection attempt has
// saved nothing at all -- it has only moved where the timeout is spent, while
// continuing to load a machine that has already told us it is sick.
func TestBreaker_OpenSkipsTheUpstreamEntirely(t *testing.T) {
	be := newBackend(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	s := newServer(t, admissionConfig(noRetry, breakerCfg(1, time.Minute, 1), config.LimitConfig{}, be.URL))

	// One failure at threshold 1 trips the breaker. The client sees the
	// backend's own 500 for this one.
	if rec := get(t, s); rec.Code != http.StatusInternalServerError {
		t.Fatalf("first request: status %d, want 500", rec.Code)
	}
	if got := be.Hits(); got != 1 {
		t.Fatalf("backend hits = %d after one request, want 1", got)
	}

	for i := 0; i < 20; i++ {
		rec := get(t, s)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("request %d: status %d, want 503 from the open breaker", i, rec.Code)
		}
	}
	if got := be.Hits(); got != 1 {
		t.Fatalf("backend was invoked %d times; an open breaker must not reach it at all", got)
	}
}

// TestBreaker_TrafficShiftsToHealthyBackends is the end-to-end claim: one dead
// replica in a pool of three costs the client nothing.
//
// The sink counts TCP accepts, so "traffic stopped going there" is a number
// rather than an inference. Once the breaker is open the count must be frozen:
// not merely lower, not mostly stable -- frozen.
func TestBreaker_TrafficShiftsToHealthyBackends(t *testing.T) {
	dead := newSink(t)
	a := newBackend(t, nil)
	b := newBackend(t, nil)

	cfg := admissionConfig(
		retryN(3, true),
		breakerCfg(1, time.Minute, 1),
		config.LimitConfig{},
		dead.URL(), a.URL, b.URL,
	)
	s := newServer(t, cfg)

	const warmup = 30
	for i := 0; i < warmup; i++ {
		rec := get(t, s)
		if rec.Code != http.StatusOK {
			t.Fatalf("warmup request %d: status %d, want 200", i, rec.Code)
		}
	}

	settled := dead.Accepts()
	if settled == 0 {
		t.Fatal("the dead backend was never dialled; the test proves nothing")
	}
	if settled > 4 {
		t.Fatalf("dialled the dead backend %d times before the breaker took hold", settled)
	}

	const steady = 120
	for i := 0; i < steady; i++ {
		rec := get(t, s)
		if rec.Code != http.StatusOK {
			t.Fatalf("steady request %d: status %d, want 200", i, rec.Code)
		}
		if body := rec.Body.String(); body != "ok" {
			t.Fatalf("steady request %d: body %q", i, body)
		}
	}

	if got := dead.Accepts(); got != settled {
		t.Fatalf("dead backend took %d more connections after its breaker opened", got-settled)
	}
	if a.Hits()+b.Hits() < warmup+steady {
		t.Fatalf("healthy backends served %d of %d requests", a.Hits()+b.Hits(), warmup+steady)
	}
}

// TestBreaker_IsPerUpstream. A pool-wide breaker would take the whole pool out
// of service because one replica out of two is broken, which is a strictly
// worse outage than the one it was installed to prevent.
func TestBreaker_IsPerUpstream(t *testing.T) {
	dead := newSink(t)
	good := newBackend(t, nil)

	s := newServer(t, admissionConfig(
		retryN(2, true),
		breakerCfg(1, time.Minute, 1),
		config.LimitConfig{},
		dead.URL(), good.URL,
	))

	for i := 0; i < 40; i++ {
		if rec := get(t, s); rec.Code != http.StatusOK {
			t.Fatalf("request %d: status %d, want 200 from the healthy replica", i, rec.Code)
		}
	}
	if got := good.Hits(); got < 40 {
		t.Fatalf("healthy replica served %d of 40 requests", got)
	}
}

// TestBreaker_Disabled leaves the data plane exactly as it was: the pool keeps
// hammering a dead backend, which is the pre-breaker behaviour and must remain
// available to an operator who wants it.
func TestBreaker_Disabled(t *testing.T) {
	dead := newSink(t)
	good := newBackend(t, nil)

	s := newServer(t, admissionConfig(
		retryN(2, true),
		config.BreakerConfig{Enabled: false},
		config.LimitConfig{},
		dead.URL(), good.URL,
	))

	const n = 30
	for i := 0; i < n; i++ {
		if rec := get(t, s); rec.Code != http.StatusOK {
			t.Fatalf("request %d: status %d", i, rec.Code)
		}
	}
	if got := dead.Accepts(); got < n/3 {
		t.Fatalf("dead backend accepted only %d connections; the breaker is not disabled", got)
	}
}

// --- limiter integration --------------------------------------------------

// TestLimiter_ShedsWith503AndRetryAfter. Retry-After is the part that makes a
// shed actionable rather than merely rude: without it a client has no
// information beyond "no", and the usual response to an uninformative 503 is an
// immediate retry, which is the last thing an overloaded proxy needs.
func TestLimiter_ShedsWith503AndRetryAfter(t *testing.T) {
	be := newBlocker(t)
	s := newServer(t, admissionConfig(noRetry, config.BreakerConfig{},
		config.LimitConfig{MaxInFlight: 1}, be.URL))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if rec := get(t, s); rec.Code != http.StatusOK {
			t.Errorf("held request: status %d, want 200", rec.Code)
		}
	}()

	// The slot is definitely taken once the backend handler has been entered.
	be.waitEntered(t, 1)

	start := time.Now()
	rec := get(t, s)
	elapsed := time.Since(start)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("shed request: status %d, want 503", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want \"1\"", got)
	}
	if elapsed > time.Second {
		t.Fatalf("shed took %s; queue_timeout 0 must refuse immediately", elapsed)
	}

	be.releaseAll()
	wg.Wait()

	// The slot came back, so the pool serves again.
	if rec := get(t, s); rec.Code != http.StatusOK {
		t.Fatalf("after release: status %d, want 200", rec.Code)
	}
}

// TestLimiter_QueueTimeoutWaitsThenSheds exercises the waiting configuration
// end to end: a request that would have been shed instantly is given a chance,
// and is still refused deterministically when the chance runs out.
func TestLimiter_QueueTimeoutWaitsThenSheds(t *testing.T) {
	const wait = 100 * time.Millisecond
	be := newBlocker(t)
	s := newServer(t, admissionConfig(noRetry, config.BreakerConfig{},
		config.LimitConfig{MaxInFlight: 1, QueueTimeout: config.Duration(wait)}, be.URL))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if rec := get(t, s); rec.Code != http.StatusOK {
			t.Errorf("held request: status %d", rec.Code)
		}
	}()
	be.waitEntered(t, 1)

	start := time.Now()
	rec := get(t, s)
	elapsed := time.Since(start)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("queued request: status %d, want 503", rec.Code)
	}
	if elapsed < wait {
		t.Fatalf("gave up after %s, before queue_timeout %s", elapsed, wait)
	}

	be.releaseAll()
	wg.Wait()
}

// TestLimiter_UnlimitedNeverSheds. This is the default for an operator who
// writes max_in_flight: 0, and it must hold under real concurrency rather than
// only in a serial loop.
func TestLimiter_UnlimitedNeverSheds(t *testing.T) {
	be := newBackend(t, nil)
	s := newServer(t, admissionConfig(noRetry, config.BreakerConfig{},
		config.LimitConfig{MaxInFlight: 0}, be.URL))

	const n = 200
	var wg sync.WaitGroup
	var bad atomic.Int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://x.test/api/thing", nil))
			if rec.Code != http.StatusOK {
				bad.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := bad.Load(); got != 0 {
		t.Fatalf("%d of %d requests were not 200 with max_in_flight 0", got, n)
	}
}

// TestLimiter_SlotIsReleasedOnEveryExitPath. dispatch has several 503 exits
// that never touch an upstream, and a slot leaked on one of them lowers
// max_in_flight for the rest of the process's life. Here every request is
// refused by an open breaker before any upstream work happens; if Release did
// not run, the pool would be permanently full after max_in_flight requests.
func TestLimiter_SlotIsReleasedOnEveryExitPath(t *testing.T) {
	be := newBackend(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	s := newServer(t, admissionConfig(noRetry, breakerCfg(1, time.Minute, 1),
		config.LimitConfig{MaxInFlight: 2}, be.URL))

	// Trip the breaker.
	get(t, s)

	// Far more requests than max_in_flight, every one of them exiting through
	// the "all breakers open" path.
	for i := 0; i < 50; i++ {
		if rec := get(t, s); rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("request %d: status %d, want 503", i, rec.Code)
		}
	}

	pi := s.pools["api"]
	if got := pi.limiter.InFlight(); got != 0 {
		t.Fatalf("limiter holds %d slots after every request finished, want 0", got)
	}
}

// --- metrics --------------------------------------------------------------

func TestMetrics_ShedAndLimitAndBreaker(t *testing.T) {
	be := newBlocker(t)
	m := observe.New("test")
	cfg := admissionConfig(noRetry, breakerCfg(1, time.Minute, 1),
		config.LimitConfig{MaxInFlight: 1}, be.URL)
	s, err := NewWithMetrics(cfg, m)
	if err != nil {
		t.Fatalf("NewWithMetrics: %v", err)
	}
	t.Cleanup(s.Close)

	poolLabels := map[string]string{"pool": "api"}
	upLabels := map[string]string{"pool": "api", "upstream": be.URL}

	// The limit gauge is published at startup, before any traffic, because an
	// operator asking "what bound is this process enforcing" must not have to
	// send a request to find out.
	fams := scrape(t, m)
	wantSeries(t, fams, "manifold_inflight_limit", poolLabels, 1)
	wantSeries(t, fams, "manifold_breaker_state", upLabels, float64(observe.BreakerClosed))
	wantSeries(t, fams, "manifold_requests_shed_total", poolLabels, 0)
	// Both reload outcomes exist from the first scrape even though nothing has
	// reloaded, so an alert can be written against the failure series before
	// the first failure happens.
	wantSeries(t, fams, "manifold_config_reloads_total", map[string]string{"result": "success"}, 0)
	wantSeries(t, fams, "manifold_config_reloads_total", map[string]string{"result": "failure"}, 0)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		get(t, s)
	}()
	be.waitEntered(t, 1)

	if rec := get(t, s); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected a shed, got %d", rec.Code)
	}
	be.releaseAll()
	wg.Wait()

	fams = scrape(t, m)
	wantSeries(t, fams, "manifold_requests_shed_total", poolLabels, 1)

	// The reload accessor belongs to this file even though the reload path
	// that calls it lives in cmd; exercise it here so the wiring is proven
	// before that path lands.
	m.ConfigReload(true)
	m.ConfigReload(false)
	m.ConfigReload(false)
	fams = scrape(t, m)
	wantSeries(t, fams, "manifold_config_reloads_total", map[string]string{"result": "success"}, 1)
	wantSeries(t, fams, "manifold_config_reloads_total", map[string]string{"result": "failure"}, 2)
}

// TestMetrics_BreakerStateAndTransitions. The gauge is read from the live
// breaker at scrape time, so it must move without the request path storing
// anything; the counter is what survives a breaker that trips and recovers
// between two scrapes.
func TestMetrics_BreakerStateAndTransitions(t *testing.T) {
	be := newBackend(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	m := observe.New("test")
	// open_for is 1ms so the half-open transition is reachable without the
	// test having to wait on anything an operator would recognise as a sleep.
	s, err := NewWithMetrics(admissionConfig(noRetry, breakerCfg(2, time.Millisecond, 1),
		config.LimitConfig{}, be.URL), m)
	if err != nil {
		t.Fatalf("NewWithMetrics: %v", err)
	}
	t.Cleanup(s.Close)

	upLabels := map[string]string{"pool": "api", "upstream": be.URL}
	toOpen := map[string]string{"pool": "api", "upstream": be.URL, "to": "open"}
	toHalf := map[string]string{"pool": "api", "upstream": be.URL, "to": "half_open"}

	get(t, s)
	fams := scrape(t, m)
	wantSeries(t, fams, "manifold_breaker_state", upLabels, float64(observe.BreakerClosed))
	wantSeries(t, fams, "manifold_breaker_transitions_total", toOpen, 0)

	get(t, s) // second consecutive failure trips it
	fams = scrape(t, m)
	wantSeries(t, fams, "manifold_breaker_state", upLabels, float64(observe.BreakerOpen))
	wantSeries(t, fams, "manifold_breaker_transitions_total", toOpen, 1)

	// Let open_for elapse, then send one request: it is admitted as the probe,
	// fails, and re-opens. Both edges must be counted.
	time.Sleep(10 * time.Millisecond)
	get(t, s)
	fams = scrape(t, m)
	wantSeries(t, fams, "manifold_breaker_transitions_total", toHalf, 1)
	wantSeries(t, fams, "manifold_breaker_transitions_total", toOpen, 2)
	wantSeries(t, fams, "manifold_breaker_state", upLabels, float64(observe.BreakerOpen))
}

// --- benchmarks -----------------------------------------------------------

// BenchmarkServeHTTPAdmission is BenchmarkServeHTTPInstrumented with both
// admission checks live, so the difference between the two is the whole
// per-request cost this change added to the data plane.
func BenchmarkServeHTTPAdmission(b *testing.B) {
	be := benchBackend(b)
	cfg := admissionConfig(noRetry, breakerCfg(5, 5*time.Second, 1),
		config.LimitConfig{MaxInFlight: 1 << 20}, be.URL)
	s, err := NewWithMetrics(cfg, observe.New("bench"))
	if err != nil {
		b.Fatalf("NewWithMetrics: %v", err)
	}
	defer s.Close()

	req := httptest.NewRequest(http.MethodGet, "http://bench.local/api/thing", nil)
	w := newNopWriter()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.reset()
		s.ServeHTTP(w, req)
	}
}
