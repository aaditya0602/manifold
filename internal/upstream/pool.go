package upstream

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
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

	// gen is bumped whenever the available set changes: a probe verdict, a
	// passive ejection, a readmission. The stateful strategies cache derived
	// structures keyed on it, and balance.Strategy.Pick documents the hard
	// obligation that it move on *any* change, including a same-length swap.
	// Every mutation lives in availability.go so that obligation is enforced
	// in one place rather than at each call site.
	gen atomic.Uint64

	// availMu serialises availability transitions so a backend's
	// read-modify-write and the matching gen bump are one atomic step.
	// Readers never take it: Candidates reads the state with plain atomic
	// loads.
	availMu sync.Mutex

	// hookMu serialises OnAvailabilityChange registration only; hooks itself
	// is copy-on-write so the notify path is lock-free.
	hookMu sync.Mutex
	hooks  atomic.Pointer[[]availabilityHook]
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
		b := &Backend{id: i, url: origin, key: key, weight: w}
		// Backends start available. An unreachable origin is discovered by the
		// prober within unhealthy_threshold intervals; starting them all
		// unhealthy would 503 every request for the first probe interval of
		// every boot and every config reload.
		b.avail.activeHealthy.Store(true)
		backends = append(backends, b)
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

// Candidates returns the backends eligible to serve a request right now:
// those an active prober has not marked unhealthy and passive health has not
// ejected.
//
// When nothing is available it returns an empty slice, the strategy reports
// ok=false, and the proxy answers 503. That is the intended behaviour: fail
// fast rather than hang or queue against origins we have concrete evidence
// are broken, and let the client retry or the edge fail over.
//
// The considered alternative is Envoy-style panic mode: once the healthy
// fraction drops below a threshold, ignore health entirely and load balance
// across all backends on the theory that mass unhealthiness usually means the
// health checker is wrong, not that every origin is down. It is deliberately
// not implemented here — it trades a crisp, explainable failure for traffic
// sent to backends we believe are dead, and manifold has no second signal
// with which to judge its own checker.
//
// The slice is freshly built on each call and handed to a strategy that may
// retain it, so it must not alias any Pool-owned storage.
func (p *Pool) Candidates() []balance.Candidate {
	out := make([]balance.Candidate, 0, len(p.backends))
	for _, b := range p.backends {
		if !b.Available() {
			continue
		}
		out = append(out, balance.Candidate{
			ID:       b.id,
			Key:      b.key,
			Weight:   b.weight,
			InFlight: b.InFlight(),
		})
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
