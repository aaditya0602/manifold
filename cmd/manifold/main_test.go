package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/aaditya0602/manifold/internal/config"
	"github.com/aaditya0602/manifold/internal/observe"
	"github.com/aaditya0602/manifold/internal/proxy"
)

// TestAdminHandler_ServesMetrics asserts /metrics is on the admin listener and
// emits parseable exposition format.
func TestAdminHandler_ServesMetrics(t *testing.T) {
	m := observe.New("test-version")
	h := adminHandler(m)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("admin /metrics: status %d", rec.Code)
	}

	p := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := p.TextToMetricFamilies(rec.Body)
	if err != nil {
		t.Fatalf("admin /metrics did not return parseable exposition: %v", err)
	}
	fam, ok := fams["manifold_build_info"]
	if !ok {
		t.Fatal("admin /metrics has no manifold_build_info")
	}
	var version string
	for _, lp := range fam.GetMetric()[0].GetLabel() {
		if lp.GetName() == "version" {
			version = lp.GetValue()
		}
	}
	if version != "test-version" {
		t.Errorf("build_info version = %q, want %q", version, "test-version")
	}

	// The rest of the admin surface still works.
	for _, path := range []string{"/healthz", "/version", "/debug/pprof/"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("admin %s: status %d", path, rec.Code)
		}
	}
}

// TestMetricsIsNotOnTheDataPlane is the security property the two-listener
// split exists to provide, asserted rather than assumed.
//
// The metrics exposition names every pool and every upstream address in the
// deployment. If it were reachable from wherever client traffic is reachable
// from, anyone who can hit the proxy could read the internal topology and, on
// a large deployment, force real work by scraping in a loop. The data plane's
// handler is the proxy itself, so the guarantee is structural — but "the
// handler happens not to have that route today" is exactly the kind of thing
// that quietly stops being true, which is why it is a test.
func TestMetricsIsNotOnTheDataPlane(t *testing.T) {
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("backend response"))
	}))
	defer be.Close()

	metrics := observe.New("test-version")
	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Admin:  "127.0.0.1:0",
		Pools: []config.PoolConfig{{
			Name:      "api",
			Strategy:  config.StrategyRoundRobin,
			Upstreams: []config.UpstreamConfig{{URL: be.URL, Weight: 1}},
			Retry:     config.RetryConfig{MaxAttempts: 1},
			Transport: config.TransportConfig{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 16,
				IdleConnTimeout:     config.Duration(30 * time.Second),
				DialTimeout:         config.Duration(2 * time.Second),
				DisableCompression:  true,
			},
		}},
		// A catch-all route: the worst case for this assertion, because the
		// data plane forwards literally everything.
		Routes: []config.RouteConfig{{
			Match: config.MatchConfig{PathPrefix: "/"},
			Pool:  "api",
		}},
	}

	px, err := proxy.NewWithMetrics(cfg, metrics)
	if err != nil {
		t.Fatalf("proxy.NewWithMetrics: %v", err)
	}
	defer px.Close()

	data := httptest.NewServer(px)
	defer data.Close()
	admin := httptest.NewServer(adminHandler(metrics))
	defer admin.Close()

	get := func(base, path string) (int, string) {
		t.Helper()
		res, err := http.Get(base + path)
		if err != nil {
			t.Fatalf("GET %s%s: %v", base, path, err)
		}
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		return res.StatusCode, string(body)
	}

	for _, path := range []string{"/metrics", "/debug/pprof/", "/healthz"} {
		_, body := get(data.URL, path)
		if strings.Contains(body, "manifold_build_info") || strings.Contains(body, "# TYPE manifold_") {
			t.Fatalf("data plane exposed metrics at %s:\n%s", path, body)
		}
		if body != "backend response" {
			t.Errorf("data plane %s: body = %q, want it proxied to the backend", path, body)
		}
	}

	code, body := get(admin.URL, "/metrics")
	if code != http.StatusOK || !strings.Contains(body, "manifold_build_info") {
		t.Fatalf("admin /metrics: status %d, body lacks manifold_build_info", code)
	}
}
