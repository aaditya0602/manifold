// Package health decides which backends are fit to serve traffic.
//
// It holds the two signals the project plan calls for and nothing else:
//
//   - Prober is active health checking. It calls a configured path on every
//     backend on a fixed interval, out of band, and reports a verdict after a
//     run of consecutive identical outcomes.
//   - Tracker is passive health checking. It watches the outcomes of real
//     proxied requests and ejects a backend whose error rate over a sliding
//     window crosses a threshold.
//
// Neither owns any state a request path reads. Both are pure producers of
// transitions, applied through upstream.Pool, which owns the availability
// state and the generation counter the balancing strategies key their caches
// on. That is why this package imports upstream and config and nothing else:
// metrics and logging subscribe to Pool.OnAvailabilityChange rather than being
// wired in here, which keeps health free of any dependency on observability.
//
// The two signals are deliberately independent. Active checking catches a
// backend that is down or has failed its own readiness check, including one
// receiving no traffic at all. Passive checking catches the backend that
// answers /healthz cheerfully while failing every real request — a different
// failure, and the more common one in production.
package health

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aaditya0602/manifold/internal/config"
	"github.com/aaditya0602/manifold/internal/upstream"
)

const (
	// probeMaxConnsPerHost caps concurrent probe connections to one origin.
	// A probe is one small request per interval per backend, so two is
	// already generous; the point of the cap is that a wedged origin cannot
	// accumulate probe sockets.
	probeMaxConnsPerHost = 2

	// probeIdleConnTimeout keeps one warm socket per origin between probes so
	// the steady-state check does not pay a TCP handshake every interval,
	// while still recycling it often enough that a silently dead peer is
	// re-dialed rather than probed over a zombie connection.
	probeIdleConnTimeout = 30 * time.Second

	// probeKeepAlive matches the data plane's dead-peer detection period.
	probeKeepAlive = 30 * time.Second

	// probeBodyLimit is how much of a probe response body is drained before
	// the connection is returned to the idle pool. A health endpoint answers
	// with a word or two; anything larger is a misconfiguration and is not
	// worth reading, so the connection is closed instead.
	probeBodyLimit = 4 << 10

	// thresholdClamp bounds the consecutive-outcome counters. Past the
	// configured threshold the exact count carries no information, and
	// clamping means a backend healthy for a year cannot overflow its own
	// success counter.
	thresholdClamp = 1 << 20
)

// Prober runs active health checks for one pool.
//
// One goroutine per backend, each with its own ticker and its own consecutive
// counters. That is the shape that makes a slow backend cost only its own
// probe: a single shared loop would let one origin sitting on its timeout
// delay every other origin's check by up to timeout, turning one sick backend
// into pool-wide detection lag.
type Prober struct {
	pool *upstream.Pool
	cfg  config.ActiveHealthConfig

	method string
	path   string

	// expect is the set of status codes counted as healthy. nil means "any
	// 2xx", which is what an empty expect_status configures.
	expect map[int]bool

	// client and transport are private to health checking and deliberately
	// NOT pool.Transport(). Two directions of interference are being avoided.
	//
	// Probes must not consume the data plane's pooled connections: sharing a
	// transport means every probe can take an idle keep-alive socket that a
	// real request was about to reuse, and under MaxConnsPerHost the probe
	// would compete for the same budget, evicting warm connections and adding
	// handshakes to user-visible latency.
	//
	// And a saturated data plane must not starve probes: with a shared
	// transport, a pool that has hit MaxConnsPerHost blocks the probe in the
	// connection queue until its timeout expires, so the checker declares a
	// backend unhealthy when the real problem was local queueing. Health
	// checking would then eject backends precisely when the system is most
	// loaded, which is the worst possible time to shrink the available set.
	client    *http.Client
	transport *http.Transport

	wg      sync.WaitGroup
	started atomic.Bool

	// probes counts completed probes. It exists for tests, which assert that
	// probing actually stops after cancellation.
	probes atomic.Int64
}

// NewProber builds the active checker for a pool. It returns an error rather
// than silently disabling itself, so a config that config validation somehow
// let through fails at startup instead of leaving a pool with no health
// checking and no sign of it.
//
// A pool with active checks disabled still yields a usable Prober whose Start
// is a no-op; callers do not have to special-case it, and every backend simply
// stays active-healthy.
func NewProber(pool *upstream.Pool) (*Prober, error) {
	if pool == nil {
		return nil, errors.New("health: nil pool")
	}
	cfg := pool.Config().Health.Active

	p := &Prober{pool: pool, cfg: cfg}
	if !cfg.Enabled {
		return p, nil
	}

	if cfg.Interval.D() <= 0 {
		return nil, fmt.Errorf("health: pool %q: active interval must be > 0, got %s", pool.Name(), cfg.Interval)
	}
	if cfg.Timeout.D() <= 0 {
		return nil, fmt.Errorf("health: pool %q: active timeout must be > 0, got %s", pool.Name(), cfg.Timeout)
	}
	if cfg.HealthyThreshold < 1 || cfg.UnhealthyThreshold < 1 {
		return nil, fmt.Errorf("health: pool %q: active thresholds must be >= 1, got healthy=%d unhealthy=%d",
			pool.Name(), cfg.HealthyThreshold, cfg.UnhealthyThreshold)
	}

	p.method = cfg.Method
	if p.method == "" {
		p.method = http.MethodGet
	}
	p.path = cfg.Path
	if p.path == "" {
		p.path = "/"
	}
	if len(cfg.ExpectStatus) > 0 {
		p.expect = make(map[int]bool, len(cfg.ExpectStatus))
		for _, code := range cfg.ExpectStatus {
			p.expect[code] = true
		}
	}

	p.transport = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   cfg.Timeout.D(),
			KeepAlive: probeKeepAlive,
		}).DialContext,
		MaxIdleConns:        probeMaxConnsPerHost * len(pool.Backends()),
		MaxIdleConnsPerHost: 1,
		MaxConnsPerHost:     probeMaxConnsPerHost,
		IdleConnTimeout:     probeIdleConnTimeout,
		DisableCompression:  true,
		// HTTP/1.1 only, matching the data plane: a probe that negotiated h2
		// would be testing a protocol path no proxied request uses.
		ForceAttemptHTTP2: false,
	}
	p.client = &http.Client{
		Transport: p.transport,
		// A redirect is not a healthy response. Following one would probe
		// whatever host the redirect names, so a backend could report on
		// another machine's health, and a redirect loop would burn the whole
		// probe timeout. The response is returned as-is and judged on its 3xx
		// status, which fails unless the operator listed it in expect_status.
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return p, nil
}

// Start launches one goroutine per backend and returns immediately. The
// goroutines exit when ctx is cancelled; Wait blocks until they have. Calling
// Start twice is a no-op after the first: a Prober owns its goroutines for the
// life of one context, and a second Start would double-probe every backend.
func (p *Prober) Start(ctx context.Context) {
	if !p.cfg.Enabled {
		return
	}
	if !p.started.CompareAndSwap(false, true) {
		return
	}

	for _, b := range p.pool.Backends() {
		p.wg.Add(1)
		go p.loop(ctx, b)
	}

	// Probe sockets are released on shutdown rather than left to idle out, so
	// a reload does not hold one connection per backend for
	// probeIdleConnTimeout after the old prober is gone.
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		<-ctx.Done()
		p.transport.CloseIdleConnections()
	}()
}

// Wait blocks until every goroutine started by Start has exited.
func (p *Prober) Wait() { p.wg.Wait() }

// loop is one backend's probe schedule.
func (p *Prober) loop(ctx context.Context, b *upstream.Backend) {
	defer p.wg.Done()

	interval := p.cfg.Interval.D()

	// Stagger the first probe by a random fraction of the interval. Without
	// it every backend is probed on the same tick, so the proxy fires a
	// synchronised burst at its own upstreams once per interval — a
	// thundering herd it inflicts on itself, and one that gets worse with
	// pool size exactly when the pool is large enough to matter. Randomising
	// the phase spreads the same probe rate evenly across the interval.
	//
	// Phase is randomised once, not per tick: a fixed offset keeps each
	// backend's probes evenly spaced, which is what the consecutive-outcome
	// thresholds assume when they translate a count into a detection time.
	if !sleepCtx(ctx, time.Duration(rand.Int64N(int64(interval)))) {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var st probeState
	for {
		p.probe(ctx, b, &st)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// probeState is one backend's consecutive-outcome bookkeeping. It is owned by
// a single goroutine, so it needs no synchronisation.
type probeState struct {
	failures  int
	successes int
}

// probe performs one health check and applies the threshold rules.
//
// A success resets the failure run and vice versa: the thresholds count
// *consecutive* outcomes, so a backend that fails twice, succeeds once, then
// fails twice more never reaches an unhealthy_threshold of 3. That is the
// intended reading of the config — the thresholds exist to distinguish a
// sustained outage from a blip, and an alternating pattern is a blip.
func (p *Prober) probe(ctx context.Context, b *upstream.Backend, st *probeState) {
	ok := p.probeOnce(ctx, b)
	p.probes.Add(1)

	if ok {
		st.failures = 0
		if st.successes < thresholdClamp {
			st.successes++
		}
		if st.successes >= p.cfg.HealthyThreshold {
			// Idempotent: SetActiveHealthy reports no change and bumps no
			// generation once the backend is already healthy.
			p.pool.SetActiveHealthy(b.ID(), true)
		}
		return
	}

	st.successes = 0
	if st.failures < thresholdClamp {
		st.failures++
	}
	if st.failures >= p.cfg.UnhealthyThreshold {
		p.pool.SetActiveHealthy(b.ID(), false)
	}
}

// probeOnce issues a single request and reports whether it counts as healthy.
// A failure is a transport error, a timeout, or a status outside the expected
// set — the three are not distinguished, because from the balancer's point of
// view "this origin did not correctly answer a trivial request" is the whole
// signal.
func (p *Prober) probeOnce(ctx context.Context, b *upstream.Backend) bool {
	// Each probe is bounded by its own timeout, which config validation keeps
	// strictly below the interval. Without the bound, a backend that accepts
	// connections and never answers would park a probe goroutine forever and
	// the checker would never form a verdict about it — the failure mode that
	// most needs detecting would be the one it cannot see.
	reqCtx, cancel := context.WithTimeout(ctx, p.cfg.Timeout.D())
	defer cancel()

	target := *b.URL()
	target.Path = p.path

	req, err := http.NewRequestWithContext(reqCtx, p.method, target.String(), http.NoBody)
	if err != nil {
		return false
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	// Drain a bounded amount so the connection is reusable. An unread body
	// forces the transport to close the socket, which would mean a fresh TCP
	// handshake for every probe of every backend forever.
	_, _ = io.CopyN(io.Discard, resp.Body, probeBodyLimit)

	return p.statusOK(resp.StatusCode)
}

// statusOK applies expect_status, defaulting to "any 2xx" when unset. 2xx and
// not 3xx: a redirect means the backend is not itself serving the health
// endpoint, and a 204 or 200 is what a readiness endpoint returns.
func (p *Prober) statusOK(code int) bool {
	if p.expect == nil {
		return code >= 200 && code < 300
	}
	return p.expect[code]
}

// sleepCtx waits for d or until ctx is done. It reports false when the
// context ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
