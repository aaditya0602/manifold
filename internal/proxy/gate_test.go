package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaditya0602/manifold/internal/config"
)

// This file holds the Week 2 acceptance gate: "a backend killed under load is
// ejected and re-admitted automatically, proven by test".
//
// It is deliberately an end-to-end test through Server.ServeHTTP against real
// httptest origins over real TCP, not a test of Prober or Tracker in
// isolation. Those already have unit tests, and every one of them passed while
// nothing in the request path was wired to them at all -- which is precisely
// the failure this gate exists to catch. The only thing that proves the
// integration is traffic.
//
// Everything here is polled against a bounded deadline rather than slept
// through. A fixed sleep that happens to be long enough on the machine it was
// written on is the standard way an integration test becomes a coin flip in
// CI, and the numbers this test reports would be measuring its own sleeps.

const (
	// gateInterval is the active health check period. Short enough that the
	// whole gate runs in about a second, long enough to sit clear of timer
	// granularity on any host that runs this.
	gateInterval = 30 * time.Millisecond

	// gateProbeTimeout bounds one probe. Kept just under the interval, as
	// config validation requires, and generous relative to a loopback request
	// so that load on the machine cannot make a healthy backend look dead --
	// a spurious ejection of backend 1 or 3 would be a flake, and the test
	// asserts explicitly that it did not happen.
	gateProbeTimeout = 25 * time.Millisecond

	// gateDetectSlack is the allowance added to the plan's 2x-interval target
	// before the assertion fails.
	//
	// The target is unhealthy_threshold * interval, which is the time from the
	// first probe that can observe the failure. The measurement this test can
	// actually make starts at the wall-clock instant the backend was killed,
	// which lands at an arbitrary phase inside the probe cycle, and then pays
	// a probe round trip and this test's own polling granularity on top. The
	// slack covers that measurement overhead and OS timer jitter. It is far
	// too small to hide a real regression, which would look like an extra
	// whole interval -- a missed threshold, a serialised probe loop -- rather
	// than a few milliseconds.
	gateDetectSlack = 30 * time.Millisecond

	// gateWorkers is the concurrent load. Enough to keep every backend busy
	// and to guarantee requests are in flight at the instant of the kill,
	// without saturating the machine so hard that the prober is starved.
	gateWorkers = 4

	// gateSampleTarget is how many completed requests a phase's measurement
	// waits for. Sampling by count rather than by duration keeps the
	// assertions independent of how fast the host is.
	gateSampleTarget = 300

	gatePoolName = "api"
	gateHealth   = "/healthz"
)

// --- a backend with a kill switch ----------------------------------------

// gateBackend is an origin whose health can be flipped at will, in the two
// places that matter independently: its readiness endpoint and its data path.
//
// Both are flipped by the same switch here because the gate is about a backend
// that dies outright, but they are separate code paths on purpose -- the
// difference between "answers /healthz but fails real requests" and "fails
// both" is exactly the difference between passive and active health checking.
type gateBackend struct {
	*httptest.Server
	healthy atomic.Bool
	// served counts successful data-path responses only, so it answers "is
	// this backend receiving traffic" rather than "was it dialled".
	served atomic.Int64
}

func newGateBackend(t *testing.T) *gateBackend {
	t.Helper()
	g := &gateBackend{}
	g.healthy.Store(true)
	g.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		alive := g.healthy.Load()
		if r.URL.Path == gateHealth {
			if !alive {
				// A readiness endpoint that fails is the realistic shape of a
				// dying backend: the process is up enough to answer, and
				// honest enough to say no.
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		if !alive {
			// The data path refuses: ErrAbortHandler makes net/http drop the
			// connection without a response and without logging a panic, which
			// is what a crashed or connection-refusing origin looks like from
			// the proxy's side -- a transport-level error, retryable, and a
			// passive-health failure. Returning a 500 here instead would test
			// something weaker, because a 500 is not retryable and would let
			// the zero-error assertion below pass for the wrong reason.
			panic(http.ErrAbortHandler)
		}
		g.served.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(g.Close)
	return g
}

func (g *gateBackend) Served() int64 { return g.served.Load() }
func (g *gateBackend) resetServed()  { g.served.Store(0) }

// --- config ---------------------------------------------------------------

// gateConfig is one pool of round-robin backends with active health checking
// on a short interval and the thresholds the gate specifies.
//
// Passive health is off here. Both signals run through the same Server and the
// passive path has its own tests in health_test.go; leaving it enabled would
// mean two independent mechanisms racing to eject the same backend, and the
// measured time-to-ejection would no longer be attributable to either one.
// The gate measures the active checker, so the active checker is what runs.
func gateConfig(urls []string, retry config.RetryConfig) *config.Config {
	p := pool(gatePoolName, retry, urls...)
	p.Health.Active = config.ActiveHealthConfig{
		Enabled:            true,
		Path:               gateHealth,
		Method:             http.MethodGet,
		Interval:           config.Duration(gateInterval),
		Timeout:            config.Duration(gateProbeTimeout),
		HealthyThreshold:   2,
		UnhealthyThreshold: 2,
	}
	return &config.Config{
		Listen: ":0",
		Pools:  []config.PoolConfig{p},
		Routes: []config.RouteConfig{catchAll(gatePoolName)},
	}
}

// --- load driver ----------------------------------------------------------

// loadStats tallies what the client saw. Counters are atomic and resettable so
// each phase of the gate is measured on its own samples instead of on a
// cumulative total the detection window has already polluted.
type loadStats struct {
	total  atomic.Int64
	failed atomic.Int64
	// worst is the most recent non-2xx status since the last reset, kept so a
	// failure message can say what the client got rather than only how many.
	worst atomic.Int64
}

func (l *loadStats) record(status int) {
	l.total.Add(1)
	if status >= 200 && status < 300 {
		return
	}
	l.failed.Add(1)
	l.worst.Store(int64(status))
}

func (l *loadStats) reset() {
	l.total.Store(0)
	l.failed.Store(0)
	l.worst.Store(0)
}

// loadDriver runs continuous concurrent traffic through ServeHTTP.
type loadDriver struct {
	stats loadStats
	stop  context.CancelFunc
	wg    sync.WaitGroup
}

func startLoad(s *Server, workers int) *loadDriver {
	ctx, cancel := context.WithCancel(context.Background())
	d := &loadDriver{stop: cancel}
	for i := 0; i < workers; i++ {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			for ctx.Err() == nil {
				// A fresh recorder per request: ServeHTTP writes a real
				// response, and reusing one would let a later 200 overwrite an
				// earlier failure's status.
				rec := httptest.NewRecorder()
				s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
				d.stats.record(rec.Code)
			}
		}()
	}
	return d
}

// close stops the workers and waits for them, so no request is still in flight
// against an origin the test is about to shut down.
func (d *loadDriver) close() {
	d.stop()
	d.wg.Wait()
}

// --- polling --------------------------------------------------------------

// waitFor polls cond until it holds or the deadline passes, and reports how
// long that took. The tick is much finer than any interval the gate measures,
// so polling granularity is noise rather than a term in the result.
func waitFor(deadline time.Duration, cond func() bool) (time.Duration, bool) {
	const tick = 500 * time.Microsecond
	start := time.Now()
	for {
		if cond() {
			return time.Since(start), true
		}
		if time.Since(start) > deadline {
			return time.Since(start), false
		}
		time.Sleep(tick)
	}
}

// inCandidates reports whether the backend with the given ID is currently
// being offered to the balancing strategy.
//
// The gate asserts on the candidate set itself rather than on a proxy for it:
// "is this backend still being offered to the balancer" is the exact question
// the plan's gate asks, and inferring it from observed traffic would confound
// it with the retry policy.
func inCandidates(s *Server, poolName string, id int) bool {
	p, ok := s.reg.Pool(poolName)
	if !ok {
		return false
	}
	for _, c := range p.Candidates() {
		if c.ID == id {
			return true
		}
	}
	return false
}

// waitSamples blocks until the driver has completed n requests since the last
// reset, so a phase is measured in requests rather than in seconds.
func waitSamples(d *loadDriver, n int64, deadline time.Duration) bool {
	_, ok := waitFor(deadline, func() bool { return d.stats.total.Load() >= n })
	return ok
}

// --- the gate -------------------------------------------------------------

func TestGate_BackendKilledUnderLoadIsEjectedAndReadmitted(t *testing.T) {
	backends := []*gateBackend{newGateBackend(t), newGateBackend(t), newGateBackend(t)}
	urls := make([]string, len(backends))
	for i, b := range backends {
		urls[i] = b.URL
	}

	// max_attempts 2 over three backends: one retry is always enough to reach
	// a live origin while exactly one is dead, which is the property the
	// zero-client-error assertion depends on. A larger budget would make that
	// assertion pass more easily and prove less.
	s := newServer(t, gateConfig(urls, retryN(2, true)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	load := startLoad(s, gateWorkers)
	defer load.close()

	// --- 1. steady state -------------------------------------------------
	// Wait for traffic to actually be spread before touching anything, so the
	// kill lands mid-flight rather than during warm-up.
	if _, ok := waitFor(5*time.Second, func() bool {
		for _, b := range backends {
			if b.Served() == 0 {
				return false
			}
		}
		return true
	}); !ok {
		t.Fatalf("traffic never reached all three backends: %d/%d/%d",
			backends[0].Served(), backends[1].Served(), backends[2].Served())
	}

	// --- 2. kill backend 2 -----------------------------------------------
	load.stats.reset()
	killedAt := time.Now()
	backends[1].healthy.Store(false)

	// --- 3. it must leave the candidate set ------------------------------
	_, ejected := waitFor(2*time.Second, func() bool { return !inCandidates(s, gatePoolName, 1) })
	timeToEjection := time.Since(killedAt)
	if !ejected {
		t.Fatalf("backend 2 was still a candidate %s after it started failing", timeToEjection)
	}

	detectErrs, detectTotal := load.stats.failed.Load(), load.stats.total.Load()
	target := 2 * gateInterval
	t.Logf("time to ejection: %s (target %s, tolerated %s); %d/%d client errors during the detection window",
		timeToEjection.Round(100*time.Microsecond), target, target+gateDetectSlack, detectErrs, detectTotal)

	if timeToEjection > target+gateDetectSlack {
		t.Errorf("time to ejection %s exceeds 2x the health check interval (%s) plus %s of measurement slack",
			timeToEjection, target, gateDetectSlack)
	}

	// Collateral damage check. Ejecting the healthy backends too would satisfy
	// every other assertion in this test for entirely the wrong reason, and is
	// what a probe timeout tuned too tight actually looks like.
	for _, id := range []int{0, 2} {
		if !inCandidates(s, gatePoolName, id) {
			t.Fatalf("healthy backend %d was ejected as well", id+1)
		}
	}

	// --- 4. the client-visible error rate must return to zero ------------
	// Ejection and retry together are supposed to make a dead backend
	// invisible to callers. Errors during detection are expected and were
	// logged above; sustained errors after it are the failure.
	backends[1].resetServed()
	load.stats.reset()
	if !waitSamples(load, gateSampleTarget, 5*time.Second) {
		t.Fatalf("load stalled after ejection: only %d requests completed", load.stats.total.Load())
	}
	if failed := load.stats.failed.Load(); failed != 0 {
		t.Errorf("after ejection: %d/%d requests failed (last status %d), want 0",
			failed, load.stats.total.Load(), load.stats.worst.Load())
	}
	if served := backends[1].Served(); served != 0 {
		t.Errorf("ejected backend still served %d requests", served)
	}

	// --- 5. bring it back ------------------------------------------------
	revivedAt := time.Now()
	backends[1].healthy.Store(true)

	_, readmitted := waitFor(2*time.Second, func() bool { return inCandidates(s, gatePoolName, 1) })
	timeToReadmission := time.Since(revivedAt)
	if !readmitted {
		t.Fatalf("backend 2 was not readmitted %s after recovering", timeToReadmission)
	}
	t.Logf("time to readmission: %s (target %s, tolerated %s)",
		timeToReadmission.Round(100*time.Microsecond), target, target+gateDetectSlack)
	if timeToReadmission > target+gateDetectSlack {
		t.Errorf("time to readmission %s exceeds 2x the health check interval (%s) plus %s of measurement slack",
			timeToReadmission, target, gateDetectSlack)
	}

	// --- 6. traffic must actually redistribute ---------------------------
	// Rejoining the candidate set is not the same as receiving traffic: a
	// strategy caching a ring keyed on a generation that never moved would
	// pass every assertion above and still never route to the recovered
	// backend. Counting from zero is what proves the ring was rebuilt.
	for _, b := range backends {
		b.resetServed()
	}
	load.stats.reset()
	if _, ok := waitFor(5*time.Second, func() bool {
		for _, b := range backends {
			if b.Served() == 0 {
				return false
			}
		}
		return true
	}); !ok {
		t.Fatalf("traffic did not redistribute after readmission: %d/%d/%d",
			backends[0].Served(), backends[1].Served(), backends[2].Served())
	}
	if failed := load.stats.failed.Load(); failed != 0 {
		t.Errorf("after readmission: %d/%d requests failed (last status %d), want 0",
			failed, load.stats.total.Load(), load.stats.worst.Load())
	}
	t.Logf("post-readmission distribution: %d/%d/%d",
		backends[0].Served(), backends[1].Served(), backends[2].Served())
}

// TestGate_AllBackendsDownFailsFast is the other half of the gate: with the
// whole pool ejected, a request must fail fast rather than burn the retry
// budget against origins the proxy already knows are dead.
//
// The backends here accept connections, answer /healthz with a 500, and then
// hang forever on the data path. That shape is what makes the timing assertion
// sharp: if the candidate set were not empty, the request would sit on
// per_try_timeout for every attempt in the budget before answering, so broken
// wiring fails this test by nearly a second rather than by a millisecond.
func TestGate_AllBackendsDownFailsFast(t *testing.T) {
	const perTry = 300 * time.Millisecond
	const maxAttempts = 3

	// release unblocks the hung handlers. It is registered as a cleanup after
	// the servers, so it runs before their Close (cleanups are LIFO) --
	// otherwise httptest.Server.Close would block forever waiting on a handler
	// that is waiting on this channel.
	release := make(chan struct{})

	urls := make([]string, 3)
	for i := range urls {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == gateHealth {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			<-release
		}))
		t.Cleanup(srv.Close)
		urls[i] = srv.URL
	}
	t.Cleanup(func() { close(release) })

	retry := retryN(maxAttempts, true)
	retry.PerTryTimeout = config.Duration(perTry)
	s := newServer(t, gateConfig(urls, retry))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	p, ok := s.reg.Pool(gatePoolName)
	if !ok {
		t.Fatal("pool not found")
	}
	if _, ok := waitFor(2*time.Second, func() bool { return len(p.Candidates()) == 0 }); !ok {
		t.Fatalf("backends were never all ejected; %d still candidates", len(p.Candidates()))
	}

	rec := httptest.NewRecorder()
	start := time.Now()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	took := time.Since(start)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	// A tenth of one attempt's timeout, a thirtieth of the whole budget. Any
	// amount of real dialling or waiting blows through it.
	if limit := perTry / 10; took > limit {
		t.Errorf("503 took %s, want < %s (retry budget is %s)", took, limit, maxAttempts*perTry)
	}
	t.Logf("all backends down: 503 in %s (retry budget %s)", took.Round(time.Microsecond), maxAttempts*perTry)
}
