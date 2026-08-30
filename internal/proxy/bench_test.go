package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aaditya0602/manifold/internal/observe"
)

// nopWriter is a ResponseWriter that costs as close to nothing as possible.
//
// httptest.NewRecorder allocates a bytes.Buffer and a fresh Header map per
// call, which is 4-5 allocations that have nothing to do with the proxy. Since
// the claim under test is "instrumentation adds zero allocations", the
// measurement apparatus must not contribute any of its own.
type nopWriter struct{ hdr http.Header }

func newNopWriter() *nopWriter { return &nopWriter{hdr: make(http.Header, 8)} }

func (w *nopWriter) Header() http.Header         { return w.hdr }
func (w *nopWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *nopWriter) WriteHeader(int)             {}
func (w *nopWriter) Flush()                      {}

// reset clears the header map in place. clear() reuses the existing buckets,
// so no allocation is charged to the iteration.
func (w *nopWriter) reset() { clear(w.hdr) }

// benchBackend is a minimal origin: no logging, no header cloning, no atomic
// counters — the httptest helper in helpers_test.go does all three and would
// otherwise show up in the numbers.
func benchBackend(b *testing.B) *httptest.Server {
	b.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	b.Cleanup(srv.Close)
	return srv
}

// BenchmarkServeHTTPInstrumented measures the full instrumented request path:
// route match, balancing pick, forward over a keep-alive connection to a local
// backend, and record the metrics.
//
// The absolute ns/op here is dominated by the loopback round trip and is not a
// throughput figure. What it is good for is comparison against
// BenchmarkInstrumentationOverhead below, which isolates exactly the work this
// change added.
func BenchmarkServeHTTPInstrumented(b *testing.B) {
	be := benchBackend(b)
	s, err := NewWithMetrics(simpleConfig(noRetry, be.URL), observe.New("bench"))
	if err != nil {
		b.Fatalf("NewWithMetrics: %v", err)
	}
	defer s.Close()

	req := httptest.NewRequest(http.MethodGet, "http://bench.local/api/thing", nil)
	w := newNopWriter()

	// One warm request so the transport's keep-alive connection is already
	// established and the first iteration is not paying for a dial.
	s.ServeHTTP(w, req)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.reset()
		s.ServeHTTP(w, req)
	}
}

// BenchmarkInstrumentationOverhead isolates every operation the instrumentation
// added to the request path: the two clock reads that bracket the request, the
// requests_total increment, the duration observation, and the per-upstream
// counter increment.
//
// This is the number that matters for the performance budget. ServeHTTP's own
// benchmark is drowned in loopback TCP, so a regression of tens of nanoseconds
// in the metrics would be invisible there; here it is the entire measurement.
// allocs/op must be 0 — a nonzero value means a WithLabelValues, an
// fmt.Sprintf, or an escaping closure has been reintroduced on the hot path.
func BenchmarkInstrumentationOverhead(b *testing.B) {
	m := observe.New("bench")
	pm := m.Pool("api")
	um := pm.Upstream("http://127.0.0.1:9001")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		um.Response(200)
		pm.Observe(http.MethodGet, 200, time.Since(start).Seconds())
	}
}
