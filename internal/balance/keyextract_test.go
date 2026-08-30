package balance

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewKeyExtractorEmpty(t *testing.T) {
	ex, err := NewKeyExtractor("")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	r := httptest.NewRequest(http.MethodGet, "/anything", nil)
	if got := ex(r); got != "" {
		t.Errorf("empty hashOn: got %q, want \"\"", got)
	}
}

func TestNewKeyExtractorClientIP(t *testing.T) {
	ex, err := NewKeyExtractor("client_ip")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "203.0.113.7:54321"
	if got := ex(r); got != "203.0.113.7" {
		t.Errorf("got %q, want %q", got, "203.0.113.7")
	}
}

func TestNewKeyExtractorClientIPWithoutPort(t *testing.T) {
	ex, err := NewKeyExtractor("client_ip")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "203.0.113.7" // no port: SplitHostPort fails, expect fallback
	if got := ex(r); got != "203.0.113.7" {
		t.Errorf("got %q, want %q", got, "203.0.113.7")
	}
}

func TestNewKeyExtractorPath(t *testing.T) {
	ex, err := NewKeyExtractor("path")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	r := httptest.NewRequest(http.MethodGet, "/orders/123", nil)
	if got := ex(r); got != "/orders/123" {
		t.Errorf("got %q, want %q", got, "/orders/123")
	}
}

func TestNewKeyExtractorHeader(t *testing.T) {
	ex, err := NewKeyExtractor("header:X-Session-ID")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("X-Session-ID", "abc123")
	if got := ex(r); got != "abc123" {
		t.Errorf("got %q, want %q", got, "abc123")
	}
}

func TestNewKeyExtractorHeaderMissing(t *testing.T) {
	ex, err := NewKeyExtractor("header:X-Session-ID")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if got := ex(r); got != "" {
		t.Errorf("missing header: got %q, want \"\"", got)
	}
}

func TestNewKeyExtractorCookie(t *testing.T) {
	ex, err := NewKeyExtractor("cookie:session")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.AddCookie(&http.Cookie{Name: "session", Value: "s3cr3t"})
	if got := ex(r); got != "s3cr3t" {
		t.Errorf("got %q, want %q", got, "s3cr3t")
	}
}

func TestNewKeyExtractorCookieMissing(t *testing.T) {
	ex, err := NewKeyExtractor("cookie:session")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if got := ex(r); got != "" {
		t.Errorf("missing cookie: got %q, want \"\"", got)
	}
}

func TestNewKeyExtractorUnknownForm(t *testing.T) {
	for _, bad := range []string{"query:id", "header:", "cookie:", "bogus"} {
		if _, err := NewKeyExtractor(bad); err == nil {
			t.Errorf("hashOn %q: want error, got nil", bad)
		}
	}
}
