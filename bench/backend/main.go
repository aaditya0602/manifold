// Command backend is a configurable echo server used as the upstream-under-test
// for manifold's benchmarks and chaos tests. It exposes two listeners:
//
//   - a data-plane listener (-addr) that serves the traffic the load balancer
//     forwards, with dialable-in latency, jitter, error-rate, and body size;
//   - a separate admin listener (-admin) that serves control traffic.
//
// The two are kept on different ports on purpose: if control requests (health
// toggles, latency changes, stats polling) landed on the same listener as the
// benchmarked traffic, they would show up in the load balancer's own latency
// and throughput numbers and quietly pollute every benchmark run. Keeping
// control traffic on its own port means the data listener only ever sees the
// traffic under test.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// state holds the backend's runtime-mutable configuration and counters.
//
// latencyNanos and errorRateBits are both mutated concurrently by admin
// requests and read concurrently by every data-plane request, so both need
// atomic access. latencyNanos is a straightforward atomic.Int64 (nanoseconds
// is already an int64-shaped unit). errorRate is a float64, and Go has no
// atomic.Float64, so it is stored bit-cast into an atomic.Uint64 via
// math.Float64bits/Float64frombits rather than guarding it with a mutex --
// this keeps the hot read path (every request checks the error rate) lock-free,
// at the cost of a bit-cast on each read/write, which is cheaper than a mutex
// under contention at 5000 concurrent connections.
type state struct {
	backendID string
	bodySize  int
	healthPth string

	// pad is the precomputed filler for the "pad" JSON field, computed once
	// at startup by computePad and never touched again. -body-size is a
	// static flag (it never changes at runtime), so there is nothing to
	// gain by recomputing this per request: doing so would mean marshalling
	// the response twice and allocating a fresh pad string on every padded
	// request, which is wasted garbage on the hot path at 5000 concurrent
	// connections. Because pad length is computed against a representative
	// template (see computePad) rather than each request's actual
	// path/method/counters, the real response size can be a few bytes off
	// -body-size depending on how much the request deviates from that
	// template -- acceptable for a benchmark padding knob.
	pad string

	healthy       atomic.Bool
	latencyNanos  atomic.Int64
	errorRateBits atomic.Uint64

	inFlight atomic.Int64
	served   atomic.Uint64
}

func (s *state) latency() time.Duration {
	return time.Duration(s.latencyNanos.Load())
}

func (s *state) setLatency(d time.Duration) {
	s.latencyNanos.Store(int64(d))
}

func (s *state) errorRate() float64 {
	return math.Float64frombits(s.errorRateBits.Load())
}

func (s *state) setErrorRate(r float64) {
	s.errorRateBits.Store(math.Float64bits(r))
}

type echoResponse struct {
	BackendID string `json:"backend_id"`
	Path      string `json:"path"`
	Method    string `json:"method"`
	LatencyMs int64  `json:"latency_ms"`
	InFlight  int64  `json:"in_flight"`
	Served    uint64 `json:"served"`
	Pad       string `json:"pad,omitempty"`
}

// computePad returns the filler string to store in state.pad so that a
// typical response's marshalled size lands on bodySize. It is called once,
// at startup.
//
// It marshals a representative echoResponse (a root-path GET, zero-valued
// counters) to learn the natural body size, marshals it a second time with a
// 1-byte placeholder pad to learn the fixed overhead the "pad" field itself
// adds (its key, quotes, and the comma joining it to the previous field),
// then solves for the filler length that hits bodySize. The filler uses
// printable ASCII ('x') rather than zero bytes: encoding/json escapes each
// NUL byte as a six-character unicode escape sequence, which would inflate
// the body to roughly 6x the requested size.
func computePad(backendID string, bodySize int) string {
	if bodySize <= 0 {
		return ""
	}

	template := echoResponse{
		BackendID: backendID,
		Path:      "/",
		Method:    http.MethodGet,
	}
	base, err := json.Marshal(template)
	if err != nil || len(base) >= bodySize {
		// Natural body already meets/exceeds the target: nothing to pad.
		return ""
	}

	template.Pad = "x"
	withOnePad, err := json.Marshal(template)
	if err != nil {
		return ""
	}
	overhead := len(withOnePad) - len(base) - 1 // cost of the pad field, excluding its content

	padLen := bodySize - len(base) - overhead
	if padLen <= 0 {
		return ""
	}
	return strings.Repeat("x", padLen)
}

func main() {
	addr := flag.String("addr", ":9001", "data-plane listen address")
	admin := flag.String("admin", ":9101", "control listen address (separate port)")
	id := flag.String("id", "", "backend identity; empty -> derive from -addr")
	latency := flag.Duration("latency", 0, "artificial per-request delay")
	jitter := flag.Duration("jitter", 0, "uniform +/- added to latency")
	errorRate := flag.Float64("error-rate", 0, "fraction of requests answered 500, in [0,1]")
	bodySize := flag.Int("body-size", 0, "response body padding in bytes")
	healthPath := flag.String("health-path", "/healthz", "health check path")
	flag.Parse()

	if *errorRate < 0 || *errorRate > 1 {
		log.Fatalf("invalid -error-rate %v: must be in [0,1]", *errorRate)
	}
	if *bodySize < 0 {
		log.Fatalf("invalid -body-size %v: must be >= 0", *bodySize)
	}
	if !strings.HasPrefix(*healthPath, "/") {
		log.Fatalf("invalid -health-path %q: must start with /", *healthPath)
	}

	backendID := *id
	if backendID == "" {
		backendID = *addr
	}

	s := &state{
		backendID: backendID,
		bodySize:  *bodySize,
		healthPth: *healthPath,
		pad:       computePad(backendID, *bodySize),
	}
	s.healthy.Store(true)
	s.setLatency(*latency)
	s.setErrorRate(*errorRate)
	jitterNanos := int64(*jitter)

	dataSrv := newServer(*addr, dataHandler(s, jitterNanos))
	adminSrv := newServer(*admin, adminHandler(s))

	log.Printf(
		"backend %q starting: addr=%s admin=%s latency=%s jitter=%s error-rate=%v body-size=%d health-path=%s",
		s.backendID, *addr, *admin, *latency, *jitter, *errorRate, *bodySize, *healthPath,
	)

	errCh := make(chan error, 2)
	go func() { errCh <- runServer(dataSrv) }()
	go func() { errCh <- runServer(adminSrv) }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received, draining connections")
	case err := <-errCh:
		if err != nil {
			log.Fatalf("server error: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := dataSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("data server shutdown: %v", err)
	}
	if err := adminSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("admin server shutdown: %v", err)
	}
	log.Printf("shutdown complete: served=%d", s.served.Load())
}

func newServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func runServer(srv *http.Server) error {
	err := srv.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// dataHandler serves the benchmarked traffic: the health path returns the
// current health state, everything else is echoed back after the configured
// latency/jitter/error-rate treatment.
func dataHandler(s *state, jitterNanos int64) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc(s.healthPth, func(w http.ResponseWriter, r *http.Request) {
		if s.healthy.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("down"))
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.inFlight.Add(1)
		defer s.inFlight.Add(-1)

		delay := s.latency()
		if jitterNanos > 0 {
			// uniform in [-jitter, +jitter]
			offset := rand.Int64N(2*jitterNanos+1) - jitterNanos
			delay += time.Duration(offset)
			if delay < 0 {
				delay = 0
			}
		}
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}

		served := s.served.Add(1)

		if s.errorRate() > 0 && rand.Float64() < s.errorRate() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		resp := echoResponse{
			BackendID: s.backendID,
			Path:      r.URL.Path,
			Method:    r.Method,
			LatencyMs: delay.Milliseconds(),
			InFlight:  s.inFlight.Load(),
			Served:    served,
			Pad:       s.pad, // precomputed once at startup; see state.pad
		}
		body, err := json.Marshal(resp)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})

	return mux
}

// adminHandler serves control traffic on its own listener, kept separate
// from the data plane so toggling health/latency/error-rate never shows up
// as latency or errors in the benchmarked traffic.
func adminHandler(s *state) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/_control/health", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("state") {
		case "down":
			s.healthy.Store(false)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		case "up":
			s.healthy.Store(true)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		default:
			http.Error(w, "bad state: must be up or down", http.StatusBadRequest)
		}
	})

	mux.HandleFunc("/_control/latency", func(w http.ResponseWriter, r *http.Request) {
		raw := r.URL.Query().Get("d")
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 {
			http.Error(w, "bad duration: "+raw, http.StatusBadRequest)
			return
		}
		s.setLatency(d)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/_control/error-rate", func(w http.ResponseWriter, r *http.Request) {
		raw := r.URL.Query().Get("r")
		rate, err := strconv.ParseFloat(raw, 64)
		if err != nil || rate < 0 || rate > 1 {
			http.Error(w, "bad rate: must be a float in [0,1]", http.StatusBadRequest)
			return
		}
		s.setErrorRate(rate)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/_control/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := struct {
			BackendID  string  `json:"backend_id"`
			Healthy    bool    `json:"healthy"`
			LatencyMs  int64   `json:"latency_ms"`
			ErrorRate  float64 `json:"error_rate"`
			BodySize   int     `json:"body_size"`
			InFlight   int64   `json:"in_flight"`
			Served     uint64  `json:"served"`
			HealthPath string  `json:"health_path"`
		}{
			BackendID:  s.backendID,
			Healthy:    s.healthy.Load(),
			LatencyMs:  s.latency().Milliseconds(),
			ErrorRate:  s.errorRate(),
			BodySize:   s.bodySize,
			InFlight:   s.inFlight.Load(),
			Served:     s.served.Load(),
			HealthPath: s.healthPth,
		}
		body, err := json.Marshal(stats)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})

	return mux
}
