package upstream

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aaditya0602/manifold/internal/balance"
	"github.com/aaditya0602/manifold/internal/config"
)

// keepAlivePeriod is the TCP keep-alive probe interval for upstream dials.
// It is deliberately not configurable: it tunes OS-level dead-peer detection,
// which is an implementation detail of dialing, not a policy an operator has a
// reason to reason about. Every knob that *is* policy lives in config.
const keepAlivePeriod = 30 * time.Second

// Pool is one named group of interchangeable backends plus the policy applied
// to them.
//
// A Pool is immutable after construction except for gen and the backends'
// in-flight counters. Hot reload builds a whole new Registry and swaps the
// pointer rather than mutating a live Pool, so the request path never takes a
// lock to read pool membership.
type Pool struct {
	cfg      config.PoolConfig
	strategy balance.Strategy
	backends []*Backend

	// transport is shared by every backend in the pool. One transport per
	// pool, not per backend: http.Transport keys its idle-connection pool by
	// host already, so a per-backend transport would only fragment the pool
	// and multiply the goroutines and idle sockets for no gain. Per-pool (not
	// global) because the transport tuning knobs — MaxConnsPerHost, timeouts —
	// are per-pool config.
	transport *http.Transport

	// gen is bumped whenever pool membership changes. Nothing increments it in
	// Week 1 (Candidates returns every backend), but the stateful strategies
	// cache derived structures keyed on it, so the plumbing has to exist from
	// the start: ejection and readmission in Week 2 become a single Add(1).
	gen atomic.Uint64
}

// NewPool builds a Pool from validated config. It returns an error rather than
// falling back to a default for anything it cannot honour — in particular a
// strategy that config accepts but balance has not implemented yet surfaces
// here, at startup, instead of silently degrading to round-robin at runtime.
func NewPool(cfg config.PoolConfig) (*Pool, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("upstream: pool has no name")
	}
	if len(cfg.Upstreams) == 0 {
		return nil, fmt.Errorf("upstream: pool %q has no upstreams", cfg.Name)
	}

	strategy, err := balance.New(string(cfg.Strategy), cfg.HashOn)
	if err != nil {
		return nil, fmt.Errorf("upstream: pool %q: %w", cfg.Name, err)
	}

	backends := make([]*Backend, 0, len(cfg.Upstreams))
	seen := make(map[string]int, len(cfg.Upstreams))
	for i, u := range cfg.Upstreams {
		parsed, err := url.Parse(u.URL)
		if err != nil {
			return nil, fmt.Errorf("upstream: pool %q upstream %d: invalid url %q: %w", cfg.Name, i, u.URL, err)
		}
		if parsed.Scheme == "" || parsed.Host == "" {
			return nil, fmt.Errorf("upstream: pool %q upstream %d: url %q must be scheme://host[:port]", cfg.Name, i, u.URL)
		}

		origin := &url.URL{
			Scheme: strings.ToLower(parsed.Scheme),
			Host:   strings.ToLower(parsed.Host),
		}
		key := origin.String()
		if prev, dup := seen[key]; dup {
			return nil, fmt.Errorf("upstream: pool %q lists %q twice (upstreams %d and %d)", cfg.Name, key, prev, i)
		}
		seen[key] = i

		w := u.Weight
		if w < 1 {
			w = 1
		}
		backends = append(backends, &Backend{id: i, url: origin, key: key, weight: w})
	}

	return &Pool{
		cfg:       cfg,
		strategy:  strategy,
		backends:  backends,
		transport: newTransport(cfg.Transport),
	}, nil
}

// newTransport builds the per-pool http.Transport.
func newTransport(t config.TransportConfig) *http.Transport {
	d := &net.Dialer{
		Timeout:   t.DialTimeout.D(),
		KeepAlive: keepAlivePeriod,
	}
	return &http.Transport{
		DialContext:           d.DialContext,
		MaxIdleConns:          t.MaxIdleConns,
		MaxIdleConnsPerHost:   t.MaxIdleConnsPerHost,
		MaxConnsPerHost:       t.MaxConnsPerHost,
		IdleConnTimeout:       t.IdleConnTimeout.D(),
		ResponseHeaderTimeout: t.ResponseHeaderTimeout.D(),
		DisableCompression:    t.DisableCompression,

		// manifold is deliberately HTTP/1.1-only to the upstream. HTTP/2
		// multiplexes every request for a host onto one connection, which
		// makes per-backend connection counts, MaxConnsPerHost, and
		// least-connections balancing meaningless — the load balancer would be
		// spreading streams it cannot see. Keeping one request per connection
		// is what makes the balancing decisions in this project observable and
		// testable. Setting this false (and never configuring TLSClientConfig
		// with h2 in NextProtos) is what actually disables the upgrade.
		ForceAttemptHTTP2: false,
	}
}

// Name is the pool's config name.
func (p *Pool) Name() string { return p.cfg.Name }

// Strategy is the pool's balancing algorithm.
func (p *Pool) Strategy() balance.Strategy { return p.strategy }

// Gen is the membership generation. Strategies use it to invalidate cached
// derived state (see balance.Strategy.Pick).
func (p *Pool) Gen() uint64 { return p.gen.Load() }

// Candidates returns the backends eligible to serve a request right now.
//
// Week 1 returns every backend: health checking, ejection, and circuit
// breaking are Week 2, and until they exist "eligible" and "configured" are
// the same set. The slice is freshly built on each call and handed to a
// strategy that may retain it, so it must not alias any Pool-owned storage.
func (p *Pool) Candidates() []balance.Candidate {
	out := make([]balance.Candidate, len(p.backends))
	for i, b := range p.backends {
		out[i] = balance.Candidate{
			ID:       b.id,
			Key:      b.key,
			Weight:   b.weight,
			InFlight: b.InFlight(),
		}
	}
	return out
}

// Backend maps a candidate ID back to the live backend. It returns nil for an
// unknown ID rather than panicking: IDs travel through strategy code, and a
// buggy strategy should degrade to a 503, not take the process down.
func (p *Pool) Backend(id int) *Backend {
	if id < 0 || id >= len(p.backends) {
		return nil
	}
	return p.backends[id]
}

// Backends returns the pool's backends in config order. The slice is fresh;
// the *Backend values are shared and live.
func (p *Pool) Backends() []*Backend {
	out := make([]*Backend, len(p.backends))
	copy(out, p.backends)
	return out
}

// Transport is the shared round tripper for this pool.
func (p *Pool) Transport() http.RoundTripper { return p.transport }

// Config returns the pool's configuration. PoolConfig contains slices and a
// map, so the copy is shallow; callers must treat it as read-only.
func (p *Pool) Config() config.PoolConfig { return p.cfg }

// CloseIdleConnections drops the pool's pooled sockets.
func (p *Pool) CloseIdleConnections() { p.transport.CloseIdleConnections() }
