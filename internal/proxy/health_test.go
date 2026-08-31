package proxy

import (
	"context"
	"fmt"
	"github.com/aaditya0602/manifold/internal/observe"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaditya0602/manifold/internal/config"
)

// Tests for the health integration that the acceptance gate does not cover:
// the passive path (which the gate deliberately disables so its timings are
// attributable to the active prober), the Start/Close lifecycle, and the
// availability metric.

// passiveHealth is a passive config tuned for a test: a window long enough
// that nothing ages out mid-test, a minimum small enough to reach in a handful
// of requests, and an eject_for short enough that readmission can be observed
// without the test taking half a minute.
func passiveHealth(ejectFor time.Duration) config.PassiveHealthConfig {
	return config.PassiveHealthConfig{
		Enabled:     true,
		Window:      config.Duration(5 * time.Second),
		MinRequests: 3,
		ErrorRate:   0.5,
		EjectFor:    config.Duration(ejectFor),
	}
}

// passiveConfig is one pool with passive health on and active health off, so
// every transition observed is attributable to real traffic.
func passiveConfig(retry config.RetryConfig, ejectFor time.Duration, urls ...string) *config.Config {
	p := pool("api", retry, urls...)
	p.Health.Passive = passiveHealth(ejectFor)
	return &config.Config{
		Listen: ":0",
		Pools:  []config.PoolConfig{p},
		Routes: []config.RouteConfig{catchAll("api")},
	}
}

func available(t *testing.T, s *Server, id int) bool {
	t.Helper()
	p, ok := s.reg.Pool("api")
	if !ok {
		t.Fatal("pool api not found")
	}
	b := p.Backend(id)
	if b == nil {
		t.Fatalf("backend %d not found", id)
	}
	return b.Available()
}

// TestPassiveHealth_OutcomesAreRecordedPerAttempt is the test that pins down
// Part 2's requirement, and it is written so that a per-*request* recording
// cannot pass it.
//
// Backend 0 is dead; backend 1 is healthy; retries are on. Every request that
// starts at backend 0 fails there, is retried onto backend 1, and returns 200
// to the client. So the request-level outcome is success in every single case:
// an implementation that recorded one outcome per request would record nothing
// but successes and would never eject anything. Only per-attempt recording
// sees the failures at all.
func TestPassiveHealth_OutcomesAreRecordedPerAttempt(t *testing.T) {
	dead := deadURL(t)
	live := newBackend(t, nil)

	s := newServer(t, passiveConfig(retryN(2, true), time.Minute, dead, live.URL))
	// Started: the tracker only ejects when a sweeper exists to readmit it,
	// so a Server that is never started is a state production cannot reach.
	sCtx, sCancel := context.WithCancel(context.Background())
	defer sCancel()
	s.Start(sCtx)

	for i := 0; i < 20; i++ {
		if rec := do(s, http.MethodGet, "http://x/"); rec.Code != http.StatusOK {
			t.Fatalf("request %d: status %d, want 200 (retry should have masked the dead backend)", i, rec.Code)
		}
	}

	if available(t, s, 0) {
		t.Error("dead backend was never ejected: per-attempt outcomes are not reaching the tracker")
	}
	if !available(t, s, 1) {
		t.Error("healthy backend was ejected; a successful attempt must be recorded as a success")
	}
}

// TestPassiveHealth_ClientErrorsDoNotEject is the other direction, and it is
// the one whose absence is an outage.
//
// A backend answering 404 is working correctly and telling the client so. If
// the proxy classified 4xx as a failure, a scanner spraying 404s -- or one
// broken client -- would walk the pool and eject every healthy backend in it.
// This test is what stops health.Failed from being quietly reimplemented as
// `status >= 400` in the attempt loop.
func TestPassiveHealth_ClientErrorsDoNotEject(t *testing.T) {
	notFound := newBackend(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	})

	s := newServer(t, passiveConfig(noRetry, time.Minute, notFound.URL))
	// Started: the tracker only ejects when a sweeper exists to readmit it,
	// so a Server that is never started is a state production cannot reach.
	sCtx, sCancel := context.WithCancel(context.Background())
	defer sCancel()
	s.Start(sCtx)

	for i := 0; i < 40; i++ {
		if rec := do(s, http.MethodGet, "http://x/"); rec.Code != http.StatusNotFound {
			t.Fatalf("request %d: status %d, want 404", i, rec.Code)
		}
	}

	if !available(t, s, 0) {
		t.Fatal("a backend answering 404 was ejected; 4xx must not count as a passive health failure")
	}
}

// TestPassiveHealth_ServerErrorsEject is the complement: a 5xx is the backend
// failing, and it must count even though it is not a transport error and is
// never retried.
func TestPassiveHealth_ServerErrorsEject(t *testing.T) {
	broken := newBackend(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	live := newBackend(t, nil)

	s := newServer(t, passiveConfig(noRetry, time.Minute, broken.URL, live.URL))
	// Started: the tracker only ejects when a sweeper exists to readmit, so a
	// test that never starts the Server is testing a state production cannot
	// reach.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	for i := 0; i < 20; i++ {
		do(s, http.MethodGet, "http://x/")
	}

	if available(t, s, 0) {
		t.Error("a backend returning 500 to every request was not ejected")
	}
	if !available(t, s, 1) {
		t.Error("the healthy backend was ejected")
	}
}

// TestPassiveHealth_EjectionExpires proves the tracker's readmission sweeper
// is actually running, which is a property of Start and not of Record.
//
// Traffic stops before the wait: with the backend still dead, load would
// re-eject it within a few requests of readmission and the test would be
// racing to observe a window it created.
func TestPassiveHealth_EjectionExpires(t *testing.T) {
	const ejectFor = 100 * time.Millisecond

	dead := deadURL(t)
	live := newBackend(t, nil)

	s := newServer(t, passiveConfig(retryN(2, true), ejectFor, dead, live.URL))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	for i := 0; i < 20; i++ {
		do(s, http.MethodGet, "http://x/")
	}
	if available(t, s, 0) {
		t.Fatal("dead backend was not ejected")
	}

	// The sweeper ticks at eject_for/5, so readmission is late by up to one
	// tick by design. The deadline is generous because what is being asserted
	// is that it happens at all.
	took, ok := waitFor(2*time.Second, func() bool { return available(t, s, 0) })
	if !ok {
		t.Fatalf("ejection never expired: still ejected %s after eject_for=%s", took, ejectFor)
	}
	t.Logf("readmitted %s after ejection (eject_for %s)", took.Round(time.Millisecond), ejectFor)
}

// --- lifecycle ------------------------------------------------------------

// TestLifecycle_CloseWithoutStartAndTwice covers the two shapes a call site
// actually produces: a Server built and thrown away without ever running
// (`manifold -check`, a construction test), and a Server closed by both a
// `defer` and an explicit drain path.
func TestLifecycle_CloseWithoutStartAndTwice(t *testing.T) {
	be := newGateBackend(t)
	s, err := New(gateConfig([]string{be.URL}, noRetry))
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}

	s.Close()
	s.Close()

	// Start after Close must stay closed rather than resurrecting goroutines
	// that Close has already waited for.
	s.Start(context.Background())
	s.Close()
}

// TestLifecycle_CloseStopsProbing is the anti-flake test: it asserts that
// Close observes the prober goroutines' exit rather than merely signalling it.
//
// A prober that outlives its Server keeps mutating a Pool the next test
// believes it owns, which is how a suite acquires failures that only reproduce
// under -count=5 on somebody else's machine.
func TestLifecycle_CloseStopsProbing(t *testing.T) {
	var probes atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == gateHealth {
			probes.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	s, err := New(gateConfig([]string{srv.URL}, noRetry))
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	if _, ok := waitFor(2*time.Second, func() bool { return probes.Load() > 0 }); !ok {
		t.Fatal("prober never probed; Start did not start it")
	}

	s.Close()
	after := probes.Load()

	// Close returned, so every probe goroutine has exited. Any probe arriving
	// now came from a goroutine that outlived it.
	time.Sleep(4 * gateInterval)
	if got := probes.Load(); got != after {
		t.Errorf("%d probes arrived after Close returned; Close did not wait for the prober to exit", got-after)
	}
}

// --- metrics --------------------------------------------------------------

// TestMetrics_AvailabilityChanges asserts the transition counter, which exists
// because manifold_upstream_available is a gauge: a backend that flaps out and
// back between two scrapes is invisible in the gauge and is exactly the thing
// worth alerting on.
func TestMetrics_AvailabilityChanges(t *testing.T) {
	broken := newBackend(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	live := newBackend(t, nil)

	s, m := newInstrumentedServer(t, passiveConfig(noRetry, time.Minute, broken.URL, live.URL))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	for i := 0; i < 20; i++ {
		do(s, http.MethodGet, "http://x/")
	}
	if available(t, s, 0) {
		t.Fatal("broken backend was not ejected")
	}

	fams := scrape(t, m)
	const name = "manifold_upstream_availability_changes_total"

	wantSeries(t, fams, name,
		map[string]string{"pool": "api", "upstream": broken.URL, "state": "unavailable"}, 1)
	// Pre-resolved at startup, so the series exists at zero rather than being
	// absent -- which is what lets an alert on it be written before the first
	// outage ever happens.
	wantSeries(t, fams, name,
		map[string]string{"pool": "api", "upstream": broken.URL, "state": "available"}, 0)
	wantSeries(t, fams, name,
		map[string]string{"pool": "api", "upstream": live.URL, "state": "unavailable"}, 0)
}

// TestMetrics_SurviveGenerationTurnover reproduces what a hot reload does to
// the metrics registry.
//
// Each reload builds a replacement Server whose pools register the same
// scrape-time gauges under the same pool and upstream labels. Before the fix,
// the retired generation was never unregistered, so both were collected and a
// checked registry rejected the entire exposition: /metrics returned 500
// permanently from the first reload onward while the data plane stayed
// completely healthy. Found by running the failure-scenario suite, not by any
// unit test, because no test had ever built two generations against one
// registry.
func TestMetrics_SurviveGenerationTurnover(t *testing.T) {
	live := newBackend(t, nil)
	m := observe.New("test")

	scrapeOK := func(when string) {
		t.Helper()
		if _, err := m.Registry().Gather(); err != nil {
			t.Fatalf("%s: gathering metrics failed: %v", when, err)
		}
	}

	gen1, err := NewWithMetrics(simpleConfig(noRetry, live.URL), m)
	if err != nil {
		t.Fatalf("generation 1: %v", err)
	}
	scrapeOK("with one generation")

	// Ten turnovers, matching the reload gate. Each retires the previous
	// generation the way the supervisor does.
	prev := gen1
	for i := 2; i <= 11; i++ {
		next, err := NewWithMetrics(simpleConfig(noRetry, live.URL), m)
		if err != nil {
			t.Fatalf("generation %d: %v", i, err)
		}
		prev.Close()
		prev = next
		scrapeOK(fmt.Sprintf("after retiring generation %d", i-1))
	}
	prev.Close()
	scrapeOK("after retiring the last generation")
}
