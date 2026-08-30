package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/aaditya0602/manifold/internal/config"
	"github.com/aaditya0602/manifold/internal/observe"
)

// --- scrape helpers -------------------------------------------------------

// scrape performs a real HTTP GET against the metrics handler and parses the
// exposition format. Parsing, not substring matching: a substring assertion
// cannot tell a counter of 1 from a counter of 41, cannot check labels, and
// passes happily on output Prometheus would reject.
func scrape(t *testing.T, m *observe.Metrics) map[string]*dto.MetricFamily {
	t.Helper()

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape: status %d", rec.Code)
	}
	p := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := p.TextToMetricFamilies(rec.Body)
	if err != nil {
		t.Fatalf("scrape: parse exposition: %v", err)
	}
	return fams
}

func series(fams map[string]*dto.MetricFamily, name string, labels map[string]string) (float64, bool) {
	fam, ok := fams[name]
	if !ok {
		return 0, false
	}
next:
	for _, m := range fam.GetMetric() {
		if len(m.GetLabel()) != len(labels) {
			continue
		}
		for _, lp := range m.GetLabel() {
			if want, ok := labels[lp.GetName()]; !ok || want != lp.GetValue() {
				continue next
			}
		}
		switch {
		case m.Counter != nil:
			return m.Counter.GetValue(), true
		case m.Gauge != nil:
			return m.Gauge.GetValue(), true
		case m.Histogram != nil:
			return float64(m.Histogram.GetSampleCount()), true
		}
	}
	return 0, false
}

func wantSeries(t *testing.T, fams map[string]*dto.MetricFamily, name string, labels map[string]string, want float64) {
	t.Helper()
	got, ok := series(fams, name, labels)
	if !ok {
		t.Fatalf("no series %s%v", name, labels)
	}
	if got != want {
		t.Errorf("%s%v = %v, want %v", name, labels, got, want)
	}
}

// newInstrumentedServer is newServer with metrics attached.
func newInstrumentedServer(t *testing.T, cfg *config.Config) (*Server, *observe.Metrics) {
	t.Helper()
	m := observe.New("test")
	s, err := NewWithMetrics(cfg, m)
	if err != nil {
		t.Fatalf("proxy.NewWithMetrics: %v", err)
	}
	t.Cleanup(s.Close)
	return s, m
}

// do drives one request through the proxy and returns the recorder.
func do(s *Server, method, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec
}

// --- tests ----------------------------------------------------------------

// TestMetrics_RequestCountersAndLabels drives traffic through a real proxy and
// asserts the scrape reports it under the right labels.
func TestMetrics_RequestCountersAndLabels(t *testing.T) {
	be := newBackend(t, nil)
	s, m := newInstrumentedServer(t, simpleConfig(noRetry, be.URL))

	for i := 0; i < 3; i++ {
		if rec := do(s, http.MethodGet, "http://x/a"); rec.Code != http.StatusOK {
			t.Fatalf("request %d: status %d", i, rec.Code)
		}
	}
	do(s, http.MethodHead, "http://x/a")

	fams := scrape(t, m)

	wantSeries(t, fams, "manifold_requests_total",
		map[string]string{"pool": "api", "method": "GET", "status_class": "2xx"}, 3)
	wantSeries(t, fams, "manifold_requests_total",
		map[string]string{"pool": "api", "method": "HEAD", "status_class": "2xx"}, 1)
	wantSeries(t, fams, "manifold_upstream_requests_total",
		map[string]string{"pool": "api", "upstream": be.URL, "status_class": "2xx"}, 4)
	wantSeries(t, fams, "manifold_request_duration_seconds",
		map[string]string{"pool": "api"}, 4)
	wantSeries(t, fams, "manifold_retries_total", map[string]string{"pool": "api"}, 0)
	wantSeries(t, fams, "manifold_requests_no_route_total", nil, 0)
}

// TestMetrics_StatusClasses covers the requirement that a 500 from a backend
// lands in 5xx rather than being folded into "the request succeeded" — the
// proxy did return a response, so it is easy to get this wrong.
func TestMetrics_StatusClasses(t *testing.T) {
	be := newBackend(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/boom":
			w.WriteHeader(http.StatusInternalServerError)
		case "/gone":
			w.WriteHeader(http.StatusNotFound)
		case "/moved":
			w.WriteHeader(http.StatusMovedPermanently)
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	s, m := newInstrumentedServer(t, simpleConfig(noRetry, be.URL))

	do(s, http.MethodGet, "http://x/boom")
	do(s, http.MethodGet, "http://x/gone")
	do(s, http.MethodGet, "http://x/moved")
	do(s, http.MethodGet, "http://x/ok")

	fams := scrape(t, m)
	for class, want := range map[string]float64{"5xx": 1, "4xx": 1, "3xx": 1, "2xx": 1} {
		wantSeries(t, fams, "manifold_requests_total",
			map[string]string{"pool": "api", "method": "GET", "status_class": class}, want)
		wantSeries(t, fams, "manifold_upstream_requests_total",
			map[string]string{"pool": "api", "upstream": be.URL, "status_class": class}, want)
	}
}

// TestMetrics_NoRouteIsCounted checks the unrouted path, which deliberately
// skips every pool-labelled metric because there is no pool to label it with.
func TestMetrics_NoRouteIsCounted(t *testing.T) {
	be := newBackend(t, nil)
	cfg := &config.Config{
		Listen: ":0",
		Pools:  []config.PoolConfig{pool("api", noRetry, be.URL)},
		Routes: []config.RouteConfig{{
			Match: config.MatchConfig{PathPrefix: "/api"},
			Pool:  "api",
		}},
	}
	s, m := newInstrumentedServer(t, cfg)

	do(s, http.MethodGet, "http://x/api/thing")
	do(s, http.MethodGet, "http://x/nowhere")
	do(s, http.MethodGet, "http://x/also-nowhere")

	fams := scrape(t, m)
	wantSeries(t, fams, "manifold_requests_no_route_total", nil, 2)
	wantSeries(t, fams, "manifold_requests_total",
		map[string]string{"pool": "api", "method": "GET", "status_class": "2xx"}, 1)
	// The unrouted requests must not have inflated the pool histogram.
	wantSeries(t, fams, "manifold_request_duration_seconds", map[string]string{"pool": "api"}, 1)
}

// TestMetrics_RetryAndUpstreamFailure exercises the two counters that only
// move when something goes wrong: retries_total, and the "error" class of
// upstream_requests_total for an attempt that never got a status back.
func TestMetrics_RetryAndUpstreamFailure(t *testing.T) {
	dead := deadURL(t)
	live := newBackend(t, nil)

	// Two upstreams, two attempts: round-robin sends the first attempt to the
	// dead origin, the retry lands on the live one.
	cfg := simpleConfig(retryN(2, true), dead, live.URL)
	s, m := newInstrumentedServer(t, cfg)

	if rec := do(s, http.MethodGet, "http://x/a"); rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200 after retry", rec.Code)
	}

	fams := scrape(t, m)
	wantSeries(t, fams, "manifold_retries_total", map[string]string{"pool": "api"}, 1)
	wantSeries(t, fams, "manifold_upstream_requests_total",
		map[string]string{"pool": "api", "upstream": dead, "status_class": "error"}, 1)
	wantSeries(t, fams, "manifold_upstream_requests_total",
		map[string]string{"pool": "api", "upstream": live.URL, "status_class": "2xx"}, 1)
	// The client saw one successful request, not two.
	wantSeries(t, fams, "manifold_requests_total",
		map[string]string{"pool": "api", "method": "GET", "status_class": "2xx"}, 1)
}

// TestMetrics_AllBackendsDown checks the 502 path: every attempt fails at the
// transport, the client gets a 5xx, and both upstreams show an error.
func TestMetrics_AllBackendsDown(t *testing.T) {
	d1, d2 := deadURL(t), deadURL(t)
	s, m := newInstrumentedServer(t, simpleConfig(retryN(2, true), d1, d2))

	if rec := do(s, http.MethodGet, "http://x/a"); rec.Code != http.StatusBadGateway {
		t.Fatalf("status %d, want 502", rec.Code)
	}

	fams := scrape(t, m)
	wantSeries(t, fams, "manifold_requests_total",
		map[string]string{"pool": "api", "method": "GET", "status_class": "5xx"}, 1)
	total := 0.0
	for _, u := range []string{d1, d2} {
		v, _ := series(fams, "manifold_upstream_requests_total",
			map[string]string{"pool": "api", "upstream": u, "status_class": "error"})
		total += v
	}
	if total != 2 {
		t.Errorf("upstream error attempts = %v, want 2", total)
	}
}

// TestMetrics_InflightIsCollectedLive is the point of the custom collector: the
// gauge must report the request that is happening *right now*, read from the
// backend's own counter, not from a mirror the request path maintains.
func TestMetrics_InflightIsCollectedLive(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	be := newBackend(t, func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-release
		_, _ = w.Write([]byte("ok"))
	})
	s, m := newInstrumentedServer(t, simpleConfig(noRetry, be.URL))

	labels := map[string]string{"pool": "api", "upstream": be.URL}

	fams := scrape(t, m)
	wantSeries(t, fams, "manifold_upstream_inflight", labels, 0)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		do(s, http.MethodGet, "http://x/slow")
	}()

	<-entered
	fams = scrape(t, m)
	wantSeries(t, fams, "manifold_upstream_inflight", labels, 1)

	close(release)
	wg.Wait()

	fams = scrape(t, m)
	wantSeries(t, fams, "manifold_upstream_inflight", labels, 0)
}

// TestMetrics_UpstreamAvailable checks the availability gauge is present for
// every configured upstream.
//
// Until internal/upstream grows Backend.Available() this reports a constant 1
// (see backendAvailable in server.go); the assertion is deliberately "the
// series exists for each upstream, and is 0 or 1", so that it keeps passing —
// and keeps being meaningful — the moment real health state arrives.
func TestMetrics_UpstreamAvailable(t *testing.T) {
	b1, b2 := newBackend(t, nil), newBackend(t, nil)
	_, m := newInstrumentedServer(t, simpleConfig(noRetry, b1.URL, b2.URL))

	fams := scrape(t, m)
	for _, u := range []string{b1.URL, b2.URL} {
		v, ok := series(fams, "manifold_upstream_available", map[string]string{"pool": "api", "upstream": u})
		if !ok {
			t.Fatalf("no availability gauge for %s", u)
		}
		if v != 0 && v != 1 {
			t.Errorf("available{%s} = %v, want 0 or 1", u, v)
		}
	}
}

// TestNilMetrics_RequestPathIsSafe drives every branch of the request path
// through a Server built by the plain New — the constructor that takes no
// metrics — and asserts none of them panic and all still behave.
func TestNilMetrics_RequestPathIsSafe(t *testing.T) {
	live := newBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/boom" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	dead := deadURL(t)

	// A route that does not match everything, so the no-route branch runs too.
	cfg := &config.Config{
		Listen: ":0",
		Pools: []config.PoolConfig{
			pool("api", retryN(2, true), dead, live.URL),
		},
		Routes: []config.RouteConfig{{
			Match: config.MatchConfig{PathPrefix: "/api"},
			Pool:  "api",
		}},
	}

	// New, not NewWithMetrics: this is the uninstrumented constructor.
	s := newServer(t, cfg)

	for _, tc := range []struct {
		name, path string
	}{
		{"success", "/api/ok"},
		{"backend 500", "/api/boom"},
		{"retry onto live backend", "/api/ok"},
		{"no route", "/nope"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked with no metrics: %v", r)
				}
			}()
			rec := do(s, http.MethodGet, "http://x"+tc.path)
			if rec.Code == 0 {
				t.Fatal("no status written")
			}
		})
	}
}

// TestMetrics_NotReachableOnDataPlane is a security assertion, not a routing
// detail. The exposition output enumerates every pool name and every upstream
// address in the deployment; that is an internal network map, and it must not
// be obtainable by anyone who can merely reach the proxy.
//
// The test uses a catch-all route, which is the worst case: even when the
// route table forwards literally everything, a GET /metrics must be proxied to
// a backend like any other request rather than intercepted and answered with
// manifold's own metrics.
func TestMetrics_NotReachableOnDataPlane(t *testing.T) {
	be := newBackend(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("this is the backend"))
	})
	s, m := newInstrumentedServer(t, simpleConfig(noRetry, be.URL))

	for _, path := range []string{"/metrics", "/debug/pprof/", "/healthz"} {
		rec := do(s, http.MethodGet, "http://x"+path)
		body := rec.Body.String()
		if strings.Contains(body, "manifold_build_info") || strings.Contains(body, "# TYPE manifold_") {
			t.Fatalf("data plane served the metrics exposition at %s:\n%s", path, body)
		}
		if body != "this is the backend" {
			t.Errorf("%s: body = %q, want the request to have been proxied", path, body)
		}
	}

	// And the same names *are* reachable through the metrics handler, so the
	// assertion above is about where it is mounted rather than about the
	// metrics not existing.
	fams := scrape(t, m)
	if _, ok := fams["manifold_build_info"]; !ok {
		t.Fatal("metrics handler did not expose manifold_build_info")
	}
}
