package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaditya0602/manifold/internal/config"
)

// ---------------------------------------------------------------------------
// Distribution
// ---------------------------------------------------------------------------

// TestRoundRobin_DistributesEvenly asserts an exact split, not an approximate
// one. Round-robin is deterministic by construction, so anything other than
// N/3 per backend is a bug and not variance.
func TestRoundRobin_DistributesEvenly(t *testing.T) {
	const n = 300

	b1, b2, b3 := newBackend(t, nil), newBackend(t, nil), newBackend(t, nil)
	s := newServer(t, simpleConfig(noRetry, b1.URL, b2.URL, b3.URL))

	counts := map[string]int{}
	for i := 0; i < n; i++ {
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, httptest.NewRequest("GET", "http://lb.test/", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status %d", i, rec.Code)
		}
		up := rec.Header().Get(UpstreamHeader)
		if up == "" {
			t.Fatalf("request %d: missing %s header", i, UpstreamHeader)
		}
		counts[up]++
	}

	if len(counts) != 3 {
		t.Fatalf("traffic reached %d distinct backends, want 3: %v", len(counts), counts)
	}
	for _, u := range []string{b1.URL, b2.URL, b3.URL} {
		if counts[u] != n/3 {
			t.Errorf("backend %s served %d requests, want %d (%v)", u, counts[u], n/3, counts)
		}
	}
}

// TestRoundRobin_DistributesEvenlyUnderConcurrency is the same assertion with
// every request issued in parallel.
//
// This is the deliberate stand-in for the race detector, which cannot run on
// the Windows dev host. A lost update in the round-robin counter, or a
// candidate slice shared across goroutines, would show up as an uneven split
// here — a torn increment means two requests take the same slot and some
// backend ends up short. An exact-count assertion catches that deterministically
// where a "roughly even" assertion would swallow it.
func TestRoundRobin_DistributesEvenlyUnderConcurrency(t *testing.T) {
	const n = 300

	b1, b2, b3 := newBackend(t, nil), newBackend(t, nil), newBackend(t, nil)
	s := newServer(t, simpleConfig(noRetry, b1.URL, b2.URL, b3.URL))

	var mu sync.Mutex
	counts := map[string]int{}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, httptest.NewRequest("GET", "http://lb.test/", nil))
			mu.Lock()
			counts[rec.Header().Get(UpstreamHeader)]++
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	for _, u := range []string{b1.URL, b2.URL, b3.URL} {
		if counts[u] != n/3 {
			t.Errorf("backend %s served %d requests, want exactly %d (%v)", u, counts[u], n/3, counts)
		}
	}
}

// ---------------------------------------------------------------------------
// Failure handling and retries
// ---------------------------------------------------------------------------

// TestAllBackendsDown_502 covers the terminal case: nothing is reachable.
func TestAllBackendsDown_502(t *testing.T) {
	s := newServer(t, simpleConfig(retryN(3, true), deadURL(t), deadURL(t), deadURL(t)))

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest("GET", "http://lb.test/", nil))

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 (body %q)", rec.Code, rec.Body.String())
	}
}

// TestRetries_AreBoundedByMaxAttempts is the guard against an accidental
// infinite retry loop. The sinks accept and immediately close, which is a
// connection-level failure (so every attempt is retryable) *and* is countable,
// unlike a refused dial.
func TestRetries_AreBoundedByMaxAttempts(t *testing.T) {
	for _, maxAttempts := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("max_attempts=%d", maxAttempts), func(t *testing.T) {
			s1, s2, s3 := newSink(t), newSink(t), newSink(t)
			s := newServer(t, simpleConfig(retryN(maxAttempts, true), s1.URL(), s2.URL(), s3.URL()))

			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, httptest.NewRequest("GET", "http://lb.test/", nil))

			if rec.Code != http.StatusBadGateway {
				t.Fatalf("status = %d, want 502", rec.Code)
			}
			total := s1.Accepts() + s2.Accepts() + s3.Accepts()
			if total != int64(maxAttempts) {
				t.Fatalf("upstream connection attempts = %d, want exactly %d", total, maxAttempts)
			}
		})
	}
}

// TestRetry_SucceedsOnSecondBackend is the happy path for retries: the first
// pick is unreachable, the second is fine, and the client never learns about
// the first.
func TestRetry_SucceedsOnSecondBackend(t *testing.T) {
	dead := deadURL(t)
	healthy := newBackend(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("served"))
	})

	// The dead origin is listed first, and a fresh round-robin picks index 0
	// for the first request, so attempt 1 is guaranteed to fail.
	s := newServer(t, simpleConfig(retryN(2, true), dead, healthy.URL))

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest("GET", "http://lb.test/", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "served" {
		t.Fatalf("body = %q, want %q", got, "served")
	}
	if got := rec.Header().Get(UpstreamHeader); got != healthy.URL {
		t.Fatalf("%s = %q, want %q", UpstreamHeader, got, healthy.URL)
	}
	if healthy.Hits() != 1 {
		t.Fatalf("healthy backend hits = %d, want 1", healthy.Hits())
	}
}

// TestRetry_NotAttemptedForPOST_WhenIdempotentOnly is the safety property that
// matters most: replaying a POST that may already have been applied is worse
// than failing it.
//
// The POST deliberately carries no body, which isolates the method gate. If it
// had a body the replay-safety gate would also block the retry and the test
// would pass for the wrong reason.
func TestRetry_NotAttemptedForPOST_WhenIdempotentOnly(t *testing.T) {
	dead := deadURL(t)
	second := newBackend(t, nil)

	s := newServer(t, simpleConfig(retryN(3, true), dead, second.URL))

	req := httptest.NewRequest("POST", "http://lb.test/orders", http.NoBody)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if second.Hits() != 0 {
		t.Fatalf("second backend saw %d requests; a POST must not be retried under idempotent_only", second.Hits())
	}
}

// TestRetry_AttemptedForPOST_WithIdempotencyKey is the escape hatch: a client
// that promises the origin de-duplicates opts back in.
func TestRetry_AttemptedForPOST_WithIdempotencyKey(t *testing.T) {
	dead := deadURL(t)
	second := newBackend(t, nil)

	s := newServer(t, simpleConfig(retryN(2, true), dead, second.URL))

	req := httptest.NewRequest("POST", "http://lb.test/orders", http.NoBody)
	req.Header.Set("Idempotency-Key", "8b1f-c0de")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if second.Hits() != 1 {
		t.Fatalf("second backend hits = %d, want 1", second.Hits())
	}
}

// TestRetry_NotAttemptedForRequestWithBody documents the Week 1 limitation in
// executable form: bodies are not buffered, so a request carrying one gets
// exactly one attempt even when its method is idempotent.
func TestRetry_NotAttemptedForRequestWithBody(t *testing.T) {
	dead := deadURL(t)
	second := newBackend(t, nil)

	s := newServer(t, simpleConfig(retryN(3, false), dead, second.URL))

	req := httptest.NewRequest("PUT", "http://lb.test/things/1", strings.NewReader("payload"))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if second.Hits() != 0 {
		t.Fatalf("second backend saw %d requests; an unbuffered body is not replayable", second.Hits())
	}
}

// TestNoRetryOn5xx is the outage-amplifier guard. A backend that answers with
// 500 has already done the work; re-dispatching turns one bad request into two
// units of load on a pool that is by hypothesis already unhappy.
func TestNoRetryOn5xx(t *testing.T) {
	first := newBackend(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	})
	second := newBackend(t, nil)

	s := newServer(t, simpleConfig(retryN(3, true), first.URL, second.URL))

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest("GET", "http://lb.test/", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 to reach the client unchanged", rec.Code)
	}
	if got := rec.Body.String(); got != "boom" {
		t.Fatalf("body = %q, want %q", got, "boom")
	}
	if first.Hits() != 1 {
		t.Fatalf("first backend hits = %d, want 1", first.Hits())
	}
	if second.Hits() != 0 {
		t.Fatalf("second backend hits = %d, want 0; a 5xx must never be retried", second.Hits())
	}
}

// TestUpstreamTimeout_504 checks the error mapping for a backend that is
// reachable but too slow, which must be distinguishable from one that is
// unreachable.
func TestUpstreamTimeout_504(t *testing.T) {
	slow := newBackend(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(5 * time.Second):
		case <-r.Context().Done():
		}
	})

	cfg := simpleConfig(config.RetryConfig{
		MaxAttempts:   1,
		PerTryTimeout: config.Duration(75 * time.Millisecond),
	}, slow.URL)
	s := newServer(t, cfg)

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest("GET", "http://lb.test/", nil))

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504 (body %q)", rec.Code, rec.Body.String())
	}
}

// TestStatusForError pins the error taxonomy, which is the part of the failure
// path a test cannot reach end-to-end for every case.
//
// Note what is deliberately absent: there is no "5xx" case, because a 5xx is
// not an error at this layer at all — it is a successful RoundTrip whose
// response passes straight through. TestNoRetryOn5xx covers that end to end.
//
// The 503 "no eligible backend" branch in ServeHTTP is genuinely unreachable
// in Week 1: NewPool rejects an empty pool and Candidates returns every
// backend, so the candidate set is never empty. It becomes reachable — and
// testable — when ejection lands in Week 2.
func TestStatusForError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"dial refused", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, http.StatusBadGateway},
		{"dial timeout is unreachability, not slowness", &net.OpError{Op: "dial", Err: os.ErrDeadlineExceeded}, http.StatusBadGateway},
		{"configured deadline", fmt.Errorf("proxying: %w", context.DeadlineExceeded), http.StatusGatewayTimeout},
		{"response header timeout", timeoutErr{}, http.StatusGatewayTimeout},
		{"connection reset", &net.OpError{Op: "read", Err: errors.New("reset by peer")}, http.StatusBadGateway},
		{"unclassified", errors.New("boom"), http.StatusBadGateway},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := statusForError(tc.err); got != tc.want {
				t.Fatalf("statusForError(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}

// timeoutErr is a net.Error that reports a timeout without being a dial error,
// which is the shape http.Transport's response-header timeout takes.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "timeout awaiting response headers" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

// TestRetryableError covers the classification directly, including the two
// cases that must never retry.
func TestRetryableError(t *testing.T) {
	if retryableError(nil) {
		t.Error("nil error must not be retryable")
	}
	if retryableError(fmt.Errorf("wrapped: %w", context.Canceled)) {
		t.Error("a cancelled client must not trigger a retry")
	}
	if !retryableError(&net.OpError{Op: "dial", Err: errors.New("refused")}) {
		t.Error("a dial failure must be retryable")
	}
}

// ---------------------------------------------------------------------------
// Forwarding fidelity
// ---------------------------------------------------------------------------

// TestXForwardedFor covers both halves of the trust decision. The default is
// the security-relevant one: an inbound X-Forwarded-For is attacker-controlled
// unless something upstream has already sanitised it, so manifold discards it
// and reports only the peer it actually observed. Preserving the chain is
// opt-in via server.trust_forwarded_for, for deployments behind an ALB or CDN
// where the peer address is just the edge and the chain is the only record of
// the real client.
func TestXForwardedFor(t *testing.T) {
	tests := []struct {
		name  string
		trust bool
		want  string
	}{{
		name:  "untrusted by default, forged chain is discarded",
		trust: false,
		want:  "198.51.100.7",
	}, {
		name:  "trusted edge, chain is preserved and appended to",
		trust: true,
		want:  "203.0.113.9, 198.51.100.7",
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newBackend(t, nil)
			cfg := simpleConfig(noRetry, b.URL)
			cfg.Server.TrustForwardedFor = tc.trust
			s := newServer(t, cfg)

			req := httptest.NewRequest("GET", "http://lb.test/", nil)
			// A client claiming to have been forwarded from 203.0.113.9.
			// Whether that claim survives is exactly what is under test.
			req.Header.Set("X-Forwarded-For", "203.0.113.9")
			req.RemoteAddr = "198.51.100.7:51234"

			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d", rec.Code)
			}

			hdr := b.lastHeader.Load()
			if hdr == nil {
				t.Fatal("backend recorded no headers")
			}
			if got := hdr.Get("X-Forwarded-For"); got != tc.want {
				t.Fatalf("X-Forwarded-For = %q, want %q", got, tc.want)
			}
			// X-Forwarded-Host is always derived from the inbound request,
			// never from a client-supplied value, in both modes.
			if h := hdr.Get("X-Forwarded-Host"); h != "lb.test" {
				t.Fatalf("X-Forwarded-Host = %q, want %q", h, "lb.test")
			}
		})
	}
}

// TestOriginalHostIsForwarded pins the deliberate choice to send the client's
// Host to the backend rather than the backend's own address.
func TestOriginalHostIsForwarded(t *testing.T) {
	b := newBackend(t, nil)
	s := newServer(t, simpleConfig(noRetry, b.URL))

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest("GET", "http://shop.example.com/cart", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}

	host := b.lastHost.Load()
	if host == nil || *host != "shop.example.com" {
		t.Fatalf("backend saw Host %v, want shop.example.com", host)
	}
}

// TestHopByHopHeadersAreStripped confirms we get the stdlib behaviour we
// declined to reimplement.
func TestHopByHopHeadersAreStripped(t *testing.T) {
	b := newBackend(t, nil)
	s := newServer(t, simpleConfig(noRetry, b.URL))

	req := httptest.NewRequest("GET", "http://lb.test/", nil)
	req.Header.Set("Connection", "X-Custom-Hop")
	req.Header.Set("X-Custom-Hop", "should-not-survive")
	req.Header.Set("X-Keep", "survives")

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	hdr := b.lastHeader.Load()
	if hdr == nil {
		t.Fatal("no headers recorded")
	}
	if v := hdr.Get("X-Custom-Hop"); v != "" {
		t.Errorf("hop-by-hop header survived: %q", v)
	}
	if v := hdr.Get("X-Keep"); v != "survives" {
		t.Errorf("end-to-end header lost: %q", v)
	}
}

// TestPassthrough_StatusHeadersAndStreamedBody runs through a real listener so
// the response is actually framed on the wire, which is the only way to assert
// that a streamed backend response stays chunked instead of being buffered.
func TestPassthrough_StatusHeadersAndStreamedBody(t *testing.T) {
	chunks := []string{"alpha\n", "beta\n", "gamma\n"}
	released := make(chan struct{})

	b := newBackend(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Custom", "kept")
		w.Header().Add("X-Multi", "one")
		w.Header().Add("X-Multi", "two")
		w.WriteHeader(http.StatusAccepted)
		fl, ok := w.(http.Flusher)
		if !ok {
			panic("backend ResponseWriter is not a Flusher")
		}
		for _, c := range chunks {
			_, _ = io.WriteString(w, c)
			fl.Flush()
		}
		<-released
	})

	s := newServer(t, simpleConfig(noRetry, b.URL))
	front := httptest.NewServer(s)
	defer front.Close()

	resp, err := front.Client().Get(front.URL + "/stream")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Custom"); got != "kept" {
		t.Fatalf("X-Custom = %q", got)
	}
	if got := resp.Header.Values("X-Multi"); len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("X-Multi = %v, want [one two]", got)
	}
	if got := resp.Header.Get(UpstreamHeader); got != b.URL {
		t.Fatalf("%s = %q, want %q", UpstreamHeader, got, b.URL)
	}
	if len(resp.TransferEncoding) == 0 || resp.TransferEncoding[0] != "chunked" {
		t.Fatalf("TransferEncoding = %v, want [chunked]", resp.TransferEncoding)
	}

	// Read the chunks as they arrive, before the backend handler returns. If
	// the proxy buffered the body this read would block until close(released),
	// which never happens before it.
	buf := make([]byte, 0, 64)
	tmp := make([]byte, 32)
	want := strings.Join(chunks, "")
	for len(buf) < len(want) {
		n, err := resp.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			t.Fatalf("read: %v (got %q)", err, buf)
		}
	}
	close(released)

	if string(buf) != want {
		t.Fatalf("streamed body = %q, want %q", buf, want)
	}
}

// TestPassthrough_RequestBodyAndQuery checks the request side of fidelity. The
// backend echoes what it saw in its response body rather than writing to test
// variables, so there is no cross-goroutine state for the race detector to
// object to on CI.
func TestPassthrough_RequestBodyAndQuery(t *testing.T) {
	b := newBackend(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, "%s|%s|%s|%s", r.Method, r.URL.Path, r.URL.RawQuery, body)
	})

	s := newServer(t, simpleConfig(noRetry, b.URL))
	front := httptest.NewServer(s)
	defer front.Close()

	resp, err := front.Client().Post(
		front.URL+"/v1/items?sort=asc&limit=10",
		"application/json",
		strings.NewReader(`{"name":"widget"}`),
	)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	const want = `POST|/v1/items|sort=asc&limit=10|{"name":"widget"}`
	if string(got) != want {
		t.Fatalf("backend observed %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

// TestInFlight_ReturnsToZero is the leak check for the Acquire/Release pairing.
// A missing Release shows up as a permanently non-zero counter, which would
// then permanently skew least_conn once it lands.
func TestInFlight_ReturnsToZero(t *testing.T) {
	const workers = 64
	const perWorker = 8

	mk := func() *backend {
		return newBackend(t, func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(time.Millisecond)
			_, _ = w.Write([]byte("ok"))
		})
	}
	b1, b2, b3 := mk(), mk(), mk()

	// Include an unreachable origin so the retry path — which acquires and
	// releases twice for one request — is exercised too.
	dead := deadURL(t)
	s := newServer(t, simpleConfig(retryN(2, true), b1.URL, b2.URL, b3.URL, dead))

	front := httptest.NewServer(s)
	defer front.Close()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				resp, err := front.Client().Get(front.URL + "/")
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()

	pl, ok := s.reg.Pool("api")
	if !ok {
		t.Fatal("pool api missing")
	}
	for _, b := range pl.Backends() {
		if n := b.InFlight(); n != 0 {
			t.Errorf("backend %s in-flight = %d after all requests completed, want 0", b.Key(), n)
		}
	}
}

// countingWriter reports whether the handler wrote anything at all, which
// httptest.ResponseRecorder cannot: its Code field is pre-seeded with 200, so
// "never wrote" and "wrote 200" look identical there.
type countingWriter struct {
	hdr          http.Header
	writeHeaders int
	writes       int
	lastStatus   int
}

func newCountingWriter() *countingWriter {
	return &countingWriter{hdr: make(http.Header)}
}

func (w *countingWriter) Header() http.Header { return w.hdr }

func (w *countingWriter) WriteHeader(code int) {
	w.writeHeaders++
	w.lastStatus = code
}

func (w *countingWriter) Write(b []byte) (int, error) {
	w.writes++
	return len(b), nil
}

// TestClientDisconnect_WritesNothing: once the client is gone, a status line
// is pointless, and counting it as a gateway error would make an ordinary
// navigate-away look like a backend outage in the metrics.
func TestClientDisconnect_WritesNothing(t *testing.T) {
	s := newServer(t, simpleConfig(retryN(3, true), deadURL(t), deadURL(t)))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := httptest.NewRequest("GET", "http://lb.test/", nil).WithContext(ctx)
	w := newCountingWriter()
	s.ServeHTTP(w, req)

	if w.writeHeaders != 0 || w.writes != 0 {
		t.Fatalf("wrote to a disconnected client: WriteHeader x%d, Write x%d (status %d)",
			w.writeHeaders, w.writes, w.lastStatus)
	}
}

// TestClientDisconnect_DoesNotRetry: a cancelled inbound context ends the
// request, it does not buy more attempts.
func TestClientDisconnect_DoesNotRetry(t *testing.T) {
	s1, s2 := newSink(t), newSink(t)
	s := newServer(t, simpleConfig(retryN(3, true), s1.URL(), s2.URL()))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := httptest.NewRequest("GET", "http://lb.test/", nil).WithContext(ctx)
	s.ServeHTTP(newCountingWriter(), req)

	if total := s1.Accepts() + s2.Accepts(); total > 1 {
		t.Fatalf("made %d upstream attempts for a cancelled request, want at most 1", total)
	}
}
