package observe

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// scrape drives a real HTTP request through the metrics handler and parses the
// exposition format it returns.
//
// Parsing rather than substring-matching is the point. A substring assertion
// passes on a metric with the right name and the wrong labels, on a value of 0
// when the test meant 1, and on output that is not valid exposition format at
// all — which is the one thing a scrape endpoint must never emit, because
// Prometheus's response to a parse error is to drop the whole scrape.
func scrape(t *testing.T, h http.Handler) map[string]*dto.MetricFamily {
	t.Helper()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape: status %d, body %q", rec.Code, rec.Body.String())
	}

	// UTF8Validation explicitly: prometheus/common panics on an unset
	// validation scheme rather than defaulting, and the default is a global
	// this package has no business mutating.
	p := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := p.TextToMetricFamilies(rec.Body)
	if err != nil {
		t.Fatalf("scrape: parse exposition: %v", err)
	}
	return fams
}

// find returns the value of the single series named name carrying exactly the
// given labels.
func find(fams map[string]*dto.MetricFamily, name string, labels map[string]string) (float64, bool) {
	fam, ok := fams[name]
	if !ok {
		return 0, false
	}
metrics:
	for _, m := range fam.GetMetric() {
		if len(m.GetLabel()) != len(labels) {
			continue
		}
		for _, lp := range m.GetLabel() {
			if want, ok := labels[lp.GetName()]; !ok || want != lp.GetValue() {
				continue metrics
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

func mustFind(t *testing.T, fams map[string]*dto.MetricFamily, name string, labels map[string]string) float64 {
	t.Helper()
	v, ok := find(fams, name, labels)
	if !ok {
		t.Fatalf("no series %s%v in scrape", name, labels)
	}
	return v
}

// stubPool is a PoolStater whose live values the test can move between
// scrapes, which is the only way to prove the gauges are collected on demand
// rather than snapshotted at registration.
type stubPool struct {
	name      string
	keys      []string
	inflight  []atomic.Int64
	available []atomic.Bool
}

func newStubPool(name string, keys ...string) *stubPool {
	return &stubPool{
		name:      name,
		keys:      keys,
		inflight:  make([]atomic.Int64, len(keys)),
		available: make([]atomic.Bool, len(keys)),
	}
}

func (s *stubPool) Name() string { return s.name }

func (s *stubPool) Upstreams(yield func(string, int64, bool)) {
	for i, k := range s.keys {
		yield(k, s.inflight[i].Load(), s.available[i].Load())
	}
}

// allFamilies is every metric family manifold promises to expose.
var allFamilies = []string{
	"manifold_build_info",
	"manifold_requests_total",
	"manifold_request_duration_seconds",
	"manifold_upstream_requests_total",
	"manifold_upstream_inflight",
	"manifold_upstream_available",
	"manifold_retries_total",
	"manifold_requests_no_route_total",
	"manifold_requests_no_upstream_total",
}

// TestRegistration_NoDuplicatePanic covers the failure mode that only ever
// shows up at startup: MustRegister panics on a collector whose descriptors
// collide with one already in the registry. New does eight MustRegister calls,
// and Pool/Upstream resolve children on demand, so the panic can also be
// triggered later by resolving the same pool twice.
func TestRegistration_NoDuplicatePanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("registration panicked: %v", r)
		}
	}()

	m := New("test-version")

	// Two routes onto the same pool must share one PoolMetrics rather than
	// re-resolving (and re-registering) its children.
	pm1 := m.Pool("api")
	pm2 := m.Pool("api")
	if pm1 != pm2 {
		t.Fatal("Pool returned distinct PoolMetrics for the same pool name")
	}
	if um1, um2 := pm1.Upstream("http://a"), pm2.Upstream("http://a"); um1 != um2 {
		t.Fatal("Upstream returned distinct UpstreamMetrics for the same key")
	}

	m.Pool("web")
	m.RegisterPoolCollector(newStubPool("api", "http://a"))

	// A second, independent Metrics must also be constructible: the registry
	// is private, so nothing here can collide with the global one.
	_ = New("test-version")
}

// TestEveryFamilyIsExposed asserts the whole promised surface is actually
// reachable through a scrape once a pool has been wired up.
func TestEveryFamilyIsExposed(t *testing.T) {
	m := New("v1.2.3")
	pm := m.Pool("api")
	pm.Upstream("http://127.0.0.1:9001")
	m.RegisterPoolCollector(newStubPool("api", "http://127.0.0.1:9001"))

	// Touch the families that only materialise once something has happened.
	pm.Observe(http.MethodGet, 200, 0.004)
	pm.Retry()
	pm.NoUpstream()
	m.NoRoute().Inc()

	fams := scrape(t, m.Handler())
	for _, name := range allFamilies {
		if _, ok := fams[name]; !ok {
			t.Errorf("family %s missing from scrape", name)
		}
	}

	if got := mustFind(t, fams, "manifold_build_info", map[string]string{
		"version": "v1.2.3", "go_version": runtime.Version(),
	}); got != 1 {
		t.Errorf("build_info = %v, want 1", got)
	}
	if got := mustFind(t, fams, "manifold_retries_total", map[string]string{"pool": "api"}); got != 1 {
		t.Errorf("retries_total = %v, want 1", got)
	}
	if got := mustFind(t, fams, "manifold_requests_no_upstream_total", map[string]string{"pool": "api"}); got != 1 {
		t.Errorf("no_upstream_total = %v, want 1", got)
	}
	if got := mustFind(t, fams, "manifold_requests_no_route_total", nil); got != 1 {
		t.Errorf("no_route_total = %v, want 1", got)
	}
}

// TestStatusClassBucketing pins the mapping every dashboard depends on.
func TestStatusClassBucketing(t *testing.T) {
	cases := []struct {
		status int
		want   string
	}{
		{100, "1xx"}, {101, "1xx"},
		{200, "2xx"}, {204, "2xx"}, {299, "2xx"},
		{301, "3xx"}, {304, "3xx"},
		{400, "4xx"}, {404, "4xx"}, {429, "4xx"},
		{500, "5xx"}, {502, "5xx"}, {503, "5xx"}, {504, "5xx"}, {599, "5xx"},
		// 0 is what clientWriter records when the client vanished and the
		// proxy deliberately wrote nothing. Counting that as 5xx would turn
		// every browser navigate-away into a server error on the dashboard.
		{0, "other"},
		{99, "other"}, {600, "other"}, {-1, "other"},
	}
	for _, c := range cases {
		if got := classNames[classOf(c.status)]; got != c.want {
			t.Errorf("classOf(%d) = %s, want %s", c.status, got, c.want)
		}
	}
}

// TestStatusClassEndToEnd proves the class reaches the exposition output with
// the right label, not just that classOf is correct in isolation.
func TestStatusClassEndToEnd(t *testing.T) {
	m := New("t")
	pm := m.Pool("api")

	pm.Observe(http.MethodGet, 200, 0.001)
	pm.Observe(http.MethodGet, 500, 0.001)
	pm.Observe(http.MethodPost, 404, 0.001)

	fams := scrape(t, m.Handler())
	for _, c := range []struct {
		method, class string
		want          float64
	}{
		{"GET", "2xx", 1},
		{"GET", "5xx", 1},
		{"POST", "4xx", 1},
		{"POST", "2xx", 0},
	} {
		got := mustFind(t, fams, "manifold_requests_total", map[string]string{
			"pool": "api", "method": c.method, "status_class": c.class,
		})
		if got != c.want {
			t.Errorf("requests_total{method=%s,class=%s} = %v, want %v", c.method, c.class, got, c.want)
		}
	}
}

// TestUnknownMethodCollapses is the cardinality guard. A request method is a
// client-supplied token; if it reached the label unfiltered, a loop sending
// random methods would mint an unbounded number of series and take the process
// out on memory.
func TestUnknownMethodCollapses(t *testing.T) {
	m := New("t")
	pm := m.Pool("api")

	for _, method := range []string{"BREW", "\x00evil", "GETGETGET", "propfind"} {
		pm.Observe(method, 200, 0.001)
	}

	fams := scrape(t, m.Handler())
	got := mustFind(t, fams, "manifold_requests_total", map[string]string{
		"pool": "api", "method": "OTHER", "status_class": "2xx",
	})
	if got != 4 {
		t.Errorf("requests_total{method=OTHER} = %v, want 4", got)
	}

	// And nothing else appeared: the family is exactly the static grid.
	if n := len(fams["manifold_requests_total"].GetMetric()); n != numMethods*numClasses {
		t.Errorf("requests_total has %d series, want the static %d", n, numMethods*numClasses)
	}
}

// TestDurationBuckets checks the histogram is configured for a proxy rather
// than left on the Prometheus defaults, whose first boundary (5ms) is above
// manifold's p50.
func TestDurationBuckets(t *testing.T) {
	m := New("t")
	m.Pool("api").Observe(http.MethodGet, 200, 0.003)

	fams := scrape(t, m.Handler())
	fam, ok := fams["manifold_request_duration_seconds"]
	if !ok {
		t.Fatal("no duration histogram")
	}
	h := fam.GetMetric()[0].GetHistogram()

	var below5ms int
	var has3ms, has10ms bool
	for _, b := range h.GetBucket() {
		if b.GetUpperBound() < 0.005 {
			below5ms++
		}
		if b.GetUpperBound() == 0.003 {
			has3ms = true
		}
		if b.GetUpperBound() == 0.01 {
			has10ms = true
		}
	}
	if below5ms < 5 {
		t.Errorf("only %d boundaries below 5ms; the defaults have 0 and are useless for a proxy", below5ms)
	}
	if !has3ms || !has10ms {
		t.Errorf("want exact boundaries at the p50 (3ms) and p99 (10ms); has3ms=%v has10ms=%v", has3ms, has10ms)
	}
	if h.GetSampleCount() != 1 {
		t.Errorf("sample count = %d, want 1", h.GetSampleCount())
	}
}

// TestInflightCollectorIsLive is the whole reason manifold_upstream_inflight is
// a custom collector: the value must be read from the backend when Prometheus
// asks, so two scrapes of a changing system report two different numbers
// without the request path having touched a gauge.
func TestInflightCollectorIsLive(t *testing.T) {
	m := New("t")
	sp := newStubPool("api", "http://a", "http://b")
	sp.available[0].Store(true)
	m.RegisterPoolCollector(sp)

	labelsA := map[string]string{"pool": "api", "upstream": "http://a"}
	labelsB := map[string]string{"pool": "api", "upstream": "http://b"}

	fams := scrape(t, m.Handler())
	if got := mustFind(t, fams, "manifold_upstream_inflight", labelsA); got != 0 {
		t.Errorf("inflight a = %v, want 0", got)
	}
	if got := mustFind(t, fams, "manifold_upstream_available", labelsA); got != 1 {
		t.Errorf("available a = %v, want 1", got)
	}
	if got := mustFind(t, fams, "manifold_upstream_available", labelsB); got != 0 {
		t.Errorf("available b = %v, want 0", got)
	}

	sp.inflight[0].Store(7)
	sp.inflight[1].Store(3)
	sp.available[1].Store(true)
	sp.available[0].Store(false)

	fams = scrape(t, m.Handler())
	if got := mustFind(t, fams, "manifold_upstream_inflight", labelsA); got != 7 {
		t.Errorf("inflight a = %v, want 7 — the gauge was snapshotted, not collected", got)
	}
	if got := mustFind(t, fams, "manifold_upstream_inflight", labelsB); got != 3 {
		t.Errorf("inflight b = %v, want 3", got)
	}
	if got := mustFind(t, fams, "manifold_upstream_available", labelsA); got != 0 {
		t.Errorf("available a = %v, want 0", got)
	}
	if got := mustFind(t, fams, "manifold_upstream_available", labelsB); got != 1 {
		t.Errorf("available b = %v, want 1", got)
	}
}

// TestUpstreamRequestClasses covers the extra "error" class, which exists so a
// hard-down upstream is visible as a rising counter rather than as a counter
// that silently stops moving.
func TestUpstreamRequestClasses(t *testing.T) {
	m := New("t")
	um := m.Pool("api").Upstream("http://a")

	um.Response(200)
	um.Response(503)
	um.Failure()
	um.Failure()

	fams := scrape(t, m.Handler())
	for class, want := range map[string]float64{"2xx": 1, "5xx": 1, "error": 2, "4xx": 0} {
		got := mustFind(t, fams, "manifold_upstream_requests_total", map[string]string{
			"pool": "api", "upstream": "http://a", "status_class": class,
		})
		if got != want {
			t.Errorf("upstream_requests_total{class=%s} = %v, want %v", class, got, want)
		}
	}
}

// TestHotPathDoesNotAllocate is the performance contract stated as a test. If
// this ever fails, someone has put a WithLabelValues, a Sprintf, or an
// escaping closure back on the request path.
func TestHotPathDoesNotAllocate(t *testing.T) {
	m := New("t")
	pm := m.Pool("api")
	um := pm.Upstream("http://a")

	if n := testing.AllocsPerRun(1000, func() {
		pm.Observe(http.MethodGet, 200, 0.0031)
		pm.Retry()
		um.Response(200)
	}); n != 0 {
		t.Errorf("hot path allocated %v objects per request, want 0", n)
	}
}
