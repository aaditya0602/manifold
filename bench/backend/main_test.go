package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// startTestServer spins up a data listener and an admin listener on
// ephemeral ports (":0") and returns their base URLs plus a cleanup func.
func startTestServer(t *testing.T, s *state, jitterNanos int64) (dataURL, adminURL string, cleanup func()) {
	t.Helper()

	dataLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen data: %v", err)
	}
	adminLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen admin: %v", err)
	}

	dataSrv := newServer("", dataHandler(s, jitterNanos))
	adminSrv := newServer("", adminHandler(s))

	go dataSrv.Serve(dataLn)
	go adminSrv.Serve(adminLn)

	cleanup = func() {
		dataSrv.Close()
		adminSrv.Close()
	}

	return "http://" + dataLn.Addr().String(), "http://" + adminLn.Addr().String(), cleanup
}

func newTestState() *state {
	s := &state{backendID: "test-backend", healthPth: "/healthz"}
	s.healthy.Store(true)
	return s
}

func TestEchoResponseShape(t *testing.T) {
	s := newTestState()
	dataURL, _, cleanup := startTestServer(t, s, 0)
	defer cleanup()

	resp, err := http.Get(dataURL + "/foo/bar")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var got echoResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.BackendID != "test-backend" {
		t.Errorf("backend_id = %q, want %q", got.BackendID, "test-backend")
	}
	if got.Path != "/foo/bar" {
		t.Errorf("path = %q, want /foo/bar", got.Path)
	}
	if got.Method != http.MethodGet {
		t.Errorf("method = %q, want GET", got.Method)
	}
	if got.Served == 0 {
		t.Errorf("served = 0, want >= 1")
	}
}

func TestErrorRateOne(t *testing.T) {
	s := newTestState()
	s.setErrorRate(1)
	dataURL, _, cleanup := startTestServer(t, s, 0)
	defer cleanup()

	for i := 0; i < 10; i++ {
		resp, err := http.Get(dataURL + "/x")
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("iteration %d: status = %d, want 500", i, resp.StatusCode)
		}
	}
}

func TestHealthToggleViaAdmin(t *testing.T) {
	s := newTestState()
	dataURL, adminURL, cleanup := startTestServer(t, s, 0)
	defer cleanup()

	resp, err := http.Get(dataURL + "/healthz")
	if err != nil {
		t.Fatalf("GET healthz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initial health status = %d, want 200", resp.StatusCode)
	}

	ctrlResp, err := http.Get(adminURL + "/_control/health?state=down")
	if err != nil {
		t.Fatalf("GET control health down: %v", err)
	}
	ctrlResp.Body.Close()
	if ctrlResp.StatusCode != http.StatusOK {
		t.Fatalf("control health down status = %d, want 200", ctrlResp.StatusCode)
	}

	resp2, err := http.Get(dataURL + "/healthz")
	if err != nil {
		t.Fatalf("GET healthz after toggle: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("health status after toggle = %d, want 503", resp2.StatusCode)
	}
}

func TestLatencyFlagDelays(t *testing.T) {
	s := newTestState()
	s.setLatency(200 * time.Millisecond)
	dataURL, _, cleanup := startTestServer(t, s, 0)
	defer cleanup()

	start := time.Now()
	resp, err := http.Get(dataURL + "/slow")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	elapsed := time.Since(start)

	// generous tolerance: must be at least close to the configured delay,
	// well under double it even under CI scheduling jitter.
	if elapsed < 150*time.Millisecond {
		t.Errorf("elapsed = %v, want >= ~200ms", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Errorf("elapsed = %v, suspiciously long", elapsed)
	}
}

func TestControlEndpointRejectsBadValue(t *testing.T) {
	s := newTestState()
	_, adminURL, cleanup := startTestServer(t, s, 0)
	defer cleanup()

	cases := []string{
		"/_control/health?state=sideways",
		"/_control/latency?d=notaduration",
		"/_control/error-rate?r=2.5",
	}
	for _, path := range cases {
		resp, err := http.Get(adminURL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", path, resp.StatusCode)
		}
	}
}

func TestBodySizePadding(t *testing.T) {
	// Sizes comfortably larger than the natural (unpadded) body: the
	// precomputed pad should bring the marshalled response to within a
	// byte or two of -body-size. The tolerance exists because state.pad
	// is computed once at startup against a representative template
	// (root path, GET, zero-valued counters); requesting "/" here keeps
	// the real response's variable fields the same length as the
	// template's, so in practice these land exactly on target.
	for _, bodySize := range []int{150, 300, 1024} {
		t.Run(fmt.Sprintf("padded/%d", bodySize), func(t *testing.T) {
			s := newTestState()
			s.bodySize = bodySize
			s.pad = computePad(s.backendID, bodySize)

			dataURL, _, cleanup := startTestServer(t, s, 0)
			defer cleanup()

			resp, err := http.Get(dataURL + "/")
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatalf("read body: %v", err)
			}

			diff := len(body) - bodySize
			if diff < 0 {
				diff = -diff
			}
			if diff > 2 {
				t.Errorf("body-size=%d: len(body) = %d, want within 2 bytes", bodySize, len(body))
			}
		})
	}

	// A -body-size smaller than the natural body: padding must be
	// skipped entirely rather than truncating the response.
	t.Run("smaller than natural body", func(t *testing.T) {
		const bodySize = 10 // well under the natural ~90-byte echo body
		s := newTestState()
		s.bodySize = bodySize
		s.pad = computePad(s.backendID, bodySize)

		if s.pad != "" {
			t.Fatalf("computePad(%d) = %q, want empty (target smaller than natural body)", bodySize, s.pad)
		}

		dataURL, _, cleanup := startTestServer(t, s, 0)
		defer cleanup()

		resp, err := http.Get(dataURL + "/")
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("read body: %v", err)
		}

		var got echoResponse
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if got.Pad != "" {
			t.Errorf("pad = %q, want empty when -body-size < natural body", got.Pad)
		}
		if len(body) <= bodySize {
			t.Errorf("len(body) = %d, want > %d (natural body, unpadded)", len(body), bodySize)
		}
	})
}

func TestStatsEndpoint(t *testing.T) {
	s := newTestState()
	s.setLatency(5 * time.Millisecond)
	s.setErrorRate(0.25)
	dataURL, adminURL, cleanup := startTestServer(t, s, 0)
	defer cleanup()

	if _, err := http.Get(dataURL + "/warm"); err != nil {
		t.Fatalf("GET data: %v", err)
	}

	resp, err := http.Get(adminURL + "/_control/stats")
	if err != nil {
		t.Fatalf("GET stats: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stats status = %d, want 200", resp.StatusCode)
	}

	var stats struct {
		BackendID string  `json:"backend_id"`
		Healthy   bool    `json:"healthy"`
		LatencyMs int64   `json:"latency_ms"`
		ErrorRate float64 `json:"error_rate"`
		Served    uint64  `json:"served"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if stats.ErrorRate != 0.25 {
		t.Errorf("error_rate = %v, want 0.25", stats.ErrorRate)
	}
	if stats.Served == 0 {
		t.Errorf("served = 0, want >= 1")
	}
}
