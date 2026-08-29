package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aaditya0602/manifold/internal/config"
)

// TestRouting_TableDriven covers each MatchConfig field independently, their
// AND combination, case and port handling on Host, and — the case that
// actually encodes the routing contract — two routes that both match, where
// the first one declared must win.
func TestRouting_TableDriven(t *testing.T) {
	// One backend per pool, each announcing which pool served it. Asserting on
	// this rather than on X-Manifold-Upstream keeps the routing test
	// independent of the forwarding test.
	mk := func(name string) *backend {
		return newBackend(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Pool", name)
			_, _ = w.Write([]byte(name))
		})
	}
	specific, prefix, method, header, fallback := mk("specific"), mk("prefix"), mk("method"), mk("header"), mk("fallback")

	cfg := &config.Config{
		Listen: ":0",
		Pools: []config.PoolConfig{
			pool("specific", noRetry, specific.URL),
			pool("prefix", noRetry, prefix.URL),
			pool("method", noRetry, method.URL),
			pool("header", noRetry, header.URL),
			pool("fallback", noRetry, fallback.URL),
		},
		Routes: []config.RouteConfig{
			// Declared first, so it beats the /v1 prefix route below for
			// requests that satisfy both. This pair is the first-match-wins
			// case.
			{Match: config.MatchConfig{Host: "api.example.com", PathPrefix: "/v1"}, Pool: "specific"},
			{Match: config.MatchConfig{PathPrefix: "/v1"}, Pool: "prefix"},
			{Match: config.MatchConfig{Methods: []string{"POST", "PATCH"}}, Pool: "method"},
			{Match: config.MatchConfig{Headers: map[string]string{"X-Tenant": "acme"}}, Pool: "header"},
			{Match: config.MatchConfig{PathPrefix: "/"}, Pool: "fallback"},
		},
	}
	s := newServer(t, cfg)

	tests := []struct {
		name    string
		method  string
		target  string
		headers map[string]string
		want    string // X-Pool, or "" meaning expect 404
		status  int
	}{
		{
			name:   "host and prefix both match: first route wins",
			method: "GET", target: "http://api.example.com/v1/users",
			want: "specific", status: 200,
		},
		{
			name:   "prefix matches but host does not: second route wins",
			method: "GET", target: "http://other.example.com/v1/users",
			want: "prefix", status: 200,
		},
		{
			name:   "host match is case-insensitive",
			method: "GET", target: "http://API.Example.COM/v1/users",
			want: "specific", status: 200,
		},
		{
			name:   "host match ignores the port",
			method: "GET", target: "http://api.example.com:8443/v1/users",
			want: "specific", status: 200,
		},
		{
			name:   "method match",
			method: "POST", target: "http://other.example.com/anything",
			want: "method", status: 200,
		},
		{
			name:   "second method in the list also matches",
			method: "PATCH", target: "http://other.example.com/anything",
			want: "method", status: 200,
		},
		{
			name:   "method route does not capture other methods",
			method: "GET", target: "http://other.example.com/anything",
			want: "fallback", status: 200,
		},
		{
			name:   "header match",
			method: "GET", target: "http://other.example.com/anything",
			headers: map[string]string{"X-Tenant": "acme"},
			want:    "header", status: 200,
		},
		{
			name:   "header name matching is canonical, value matching is exact",
			method: "GET", target: "http://other.example.com/anything",
			headers: map[string]string{"x-tenant": "acme"},
			want:    "header", status: 200,
		},
		{
			name:   "wrong header value falls through",
			method: "GET", target: "http://other.example.com/anything",
			headers: map[string]string{"X-Tenant": "globex"},
			want:    "fallback", status: 200,
		},
		{
			name:   "prefix is a prefix, not a path segment match",
			method: "GET", target: "http://other.example.com/v1abc",
			want: "prefix", status: 200,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.target, nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, req)

			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.status, rec.Body.String())
			}
			if got := rec.Header().Get("X-Pool"); got != tc.want {
				t.Fatalf("served by pool %q, want %q", got, tc.want)
			}
		})
	}
}

// TestRouting_NoMatch_404 checks that an unmatched request is refused rather
// than falling through to an arbitrary pool.
func TestRouting_NoMatch_404(t *testing.T) {
	b := newBackend(t, nil)
	cfg := &config.Config{
		Listen: ":0",
		Pools:  []config.PoolConfig{pool("api", noRetry, b.URL)},
		Routes: []config.RouteConfig{
			{Match: config.MatchConfig{Host: "api.example.com", PathPrefix: "/v1"}, Pool: "api"},
		},
	}
	s := newServer(t, cfg)

	for _, target := range []string{
		"http://api.example.com/v2/users",  // right host, wrong prefix
		"http://nope.example.com/v1/users", // right prefix, wrong host
	} {
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, httptest.NewRequest("GET", target, nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s: status = %d, want 404", target, rec.Code)
		}
	}
	if b.Hits() != 0 {
		t.Fatalf("backend saw %d requests, want 0", b.Hits())
	}
}

// TestNew_UnknownPoolReference makes the misconfiguration a startup failure
// rather than a runtime 503 on a slice of traffic.
func TestNew_UnknownPoolReference(t *testing.T) {
	b := newBackend(t, nil)
	cfg := &config.Config{
		Listen: ":0",
		Pools:  []config.PoolConfig{pool("api", noRetry, b.URL)},
		Routes: []config.RouteConfig{catchAll("does-not-exist")},
	}
	if _, err := New(cfg); err == nil {
		t.Fatal("New succeeded with a route pointing at an unknown pool")
	}
}

func TestStripPort(t *testing.T) {
	tests := []struct{ in, want string }{
		{"", ""},
		{"example.com", "example.com"},
		{"example.com:8080", "example.com"},
		{"[::1]", "[::1]"},
		{"[::1]:8080", "[::1]"},
		{"[2001:db8::1]:443", "[2001:db8::1]"},
	}
	for _, tc := range tests {
		if got := stripPort(tc.in); got != tc.want {
			t.Errorf("stripPort(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
