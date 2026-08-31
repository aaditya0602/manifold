// Package proxy is manifold's data plane: it matches an inbound request to a
// pool, picks a backend, forwards the request, and applies the retry policy.
//
// The package owns no balancing logic (that is package balance) and no backend
// state (that is package upstream). What it owns is the request lifecycle and,
// most importantly, the rules about when re-dispatching a request is safe.
package proxy

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/aaditya0602/manifold/internal/balance"
	"github.com/aaditya0602/manifold/internal/breaker"
	"github.com/aaditya0602/manifold/internal/config"
	"github.com/aaditya0602/manifold/internal/health"
	"github.com/aaditya0602/manifold/internal/limit"
	"github.com/aaditya0602/manifold/internal/observe"
	"github.com/aaditya0602/manifold/internal/upstream"
)

// UpstreamHeader names the backend that served the response. It is set on
// every proxied response.
//
// It exists for three reasons: an operator debugging a bad response needs to
// know which machine produced it without correlating logs; the distribution
// tests assert balancing fairness by tallying it; and the benchmark harness
// reads it to verify traffic actually spread. It is a debugging affordance, so
// it exposes internal topology — a deployment that treats backend identity as
// sensitive should strip it at the edge.
const UpstreamHeader = "X-Manifold-Upstream"

// attemptState carries per-attempt data through httputil.ReverseProxy, which
// has no other channel for it: Rewrite, ModifyResponse and ErrorHandler are
// pool-scoped closures, but the backend and the failure are request-scoped.
// Passing it by context value keeps a single ReverseProxy per pool instead of
// allocating one per attempt on the hot path.
type attemptState struct {
	target *url.URL
	key    string
	// err is written by ErrorHandler and read by the attempt loop. Both run on
	// the goroutine serving the request, so no synchronisation is needed.
	err error
}

type attemptKey struct{}

func attemptFrom(ctx context.Context) *attemptState {
	st, _ := ctx.Value(attemptKey{}).(*attemptState)
	return st
}

// Server is the HTTP handler for the data plane.
//
// Everything ServeHTTP reads is immutable after New. A config reload
// constructs a second Server and swaps it in; nothing on the request path is
// mutated under traffic, so ServeHTTP takes no locks.
//
// The health checkers are the one piece of mutable state, and they are kept
// strictly off the request path: Start owns them, Close stops them, and
// ServeHTTP only ever calls Tracker.Record, which is lock-free. The explicit
// Start/Close pair -- rather than starting goroutines in New -- exists so that
// constructing a Server is side-effect free. A `-check` run, a test that only
// exercises routing, and the second Server built by a future hot reload must
// all be able to exist without probing anybody's backends.
type Server struct {
	// metrics and collectors exist so Close can undo what New registered.
	metrics    *observe.Metrics
	collectors []observe.PoolStater

	reg    *upstream.Registry
	routes []route

	// probers and trackers are parallel to reg.Pools(): one of each per pool,
	// built in New and owned for the life of the Server.
	probers  []*health.Prober
	trackers []*health.Tracker

	// lifeMu guards the Start/Close state machine. It is never taken by
	// ServeHTTP.
	lifeMu  sync.Mutex
	cancel  context.CancelFunc
	started bool
	closed  bool

	// pools holds everything ServeHTTP needs for one pool behind a single map
	// lookup. It replaces what used to be a map to *httputil.ReverseProxy: the
	// lookup was already on the request path, so folding the pool's
	// pre-resolved metrics into the same value adds instrumentation for zero
	// additional lookups.
	pools map[string]*poolInstr

	// noRoute is resolved once at construction. It is unlabelled, so this is
	// the whole hot-path value, not a *Vec to be indexed per request.
	noRoute prometheus.Counter
}

// poolInstr is the per-pool request-path state: the shared reverse proxy plus
// every metric child already resolved.
type poolInstr struct {
	rp *httputil.ReverseProxy
	pm *observe.PoolMetrics

	// tracker is this pool's passive health checker. dispatch feeds it one
	// outcome per attempt. It is never nil: a pool with passive health
	// disabled still gets a Tracker whose Record returns on a single bool
	// load, which keeps the attempt loop free of a nil check that would be
	// taken in exactly the configurations nobody benchmarks.
	tracker *health.Tracker

	// limiter bounds this pool's concurrent in-flight requests. Like tracker
	// it is never nil: a pool configured with max_in_flight 0 gets a Limiter
	// whose Acquire returns on a single bool load, so the unlimited
	// configuration costs a branch rather than a nil check plus a branch.
	limiter *limit.Limiter

	// breakers is indexed by upstream.Backend.ID, in step with up. Per
	// upstream and not per pool: one sick replica out of four must cost the
	// pool a quarter of its capacity, not all of it, and a pool-wide breaker
	// would trip on the aggregate failure rate of a pool that is three
	// quarters healthy.
	breakers []*breaker.Breaker

	// up is indexed by upstream.Backend.ID, which is the backend's index
	// within its pool, so selecting an upstream's counters is an array index
	// and never a label lookup or a map hash.
	up []*observe.UpstreamMetrics
}

// breakerFor maps a candidate ID to its breaker, or nil for an ID outside the
// pool. The bounds check is not paranoia about our own code: IDs round-trip
// through balance.Strategy, and a strategy bug must degrade to a 503 rather
// than index off the end of a slice inside a request handler.
func (pi *poolInstr) breakerFor(id int) *breaker.Breaker {
	if id < 0 || id >= len(pi.breakers) {
		return nil
	}
	return pi.breakers[id]
}

// New builds a Server from validated config. It fails rather than starting
// degraded: an unimplemented balancing strategy, an unparseable upstream URL,
// or a route naming a pool that does not exist are all startup errors.
//
// It is the uninstrumented entry point, kept so existing callers and tests do
// not have to know about metrics. It delegates with a metrics value rather
// than a nil one — see NewWithMetrics for why.
func New(cfg *config.Config) (*Server, error) {
	return NewWithMetrics(cfg, nil)
}

// NewWithMetrics is New with Prometheus instrumentation attached.
//
// A nil m is legal and means "not observed". It is handled by constructing a
// throwaway Metrics over a private registry that nothing ever scrapes, rather
// than by storing nil and testing for it per request. That choice is
// deliberate: a nil test on the request path is a branch on every counter
// increment in the process, and — worse — it is a branch that is never taken
// in production and always taken in the benchmark, so it would make the
// benchmark measure a code path that never runs. Paying one wasted registry
// per unobserved Server at startup buys a request path with exactly one shape.
func NewWithMetrics(cfg *config.Config, m *observe.Metrics) (*Server, error) {
	if cfg == nil {
		return nil, errors.New("proxy: nil config")
	}
	if m == nil {
		m = observe.New("")
	}
	reg, err := upstream.NewRegistry(cfg)
	if err != nil {
		return nil, err
	}
	routes, err := compileRoutes(cfg, reg)
	if err != nil {
		reg.Close()
		return nil, err
	}

	pools := reg.Pools()
	s := &Server{
		reg:      reg,
		routes:   routes,
		metrics:  m,
		pools:    make(map[string]*poolInstr, len(cfg.Pools)),
		probers:  make([]*health.Prober, 0, len(pools)),
		trackers: make([]*health.Tracker, 0, len(pools)),
		noRoute:  m.NoRoute(),
	}
	for _, p := range pools {
		pm := m.Pool(p.Name())

		// Resolve one UpstreamMetrics per backend, indexed by backend ID.
		// Backend.ID is the config index within the pool, so the slice is
		// dense and every ID the balancer can hand back is in range.
		backends := p.Backends()
		up := make([]*observe.UpstreamMetrics, len(backends))
		// byKey is the same set of children indexed the other way. The
		// availability hook is handed a backend key, not an ID, and it runs
		// on a prober goroutine at transition frequency -- a handful of times
		// per outage -- so a map lookup there is free, and building it here
		// keeps the request path's dense array intact.
		byKey := make(map[string]*observe.UpstreamMetrics, len(backends))
		for _, b := range backends {
			um := pm.Upstream(b.Key())
			byKey[b.Key()] = um
			if id := b.ID(); id >= 0 && id < len(up) {
				up[id] = um
			}
		}

		// NewProber returns an error rather than disabling itself, so a pool
		// whose active health config is unusable fails at startup instead of
		// running forever with no checking and no sign of it.
		prober, err := health.NewProber(p)
		if err != nil {
			reg.Close()
			return nil, err
		}
		s.probers = append(s.probers, prober)

		tracker := health.NewTracker(p)
		s.trackers = append(s.trackers, tracker)

		// Subscribed here, in New, and not in Start: a transition can only be
		// caused by a checker this Server owns, and those cannot run until
		// Start. Subscribing at construction means there is no window in which
		// a prober is probing and nobody is watching, and it keeps the
		// subscription tied to the object's lifetime rather than to a call
		// that may never happen.
		p.OnAvailabilityChange(availabilityHook(p.Name(), byKey))

		// One breaker per backend, built here and owned for the life of the
		// Server, so the request path never constructs or looks one up.
		breakers := make([]*breaker.Breaker, len(backends))
		for _, b := range backends {
			id := b.ID()
			if id < 0 || id >= len(breakers) {
				continue
			}
			br := breaker.New(p.Config().Breaker)
			// Bound to the same UpstreamMetrics the request path uses, so a
			// transition is an array index and an atomic add. Registered here,
			// before the breaker is reachable from any request -- see
			// breaker.OnTransition on why that ordering is the whole
			// synchronisation story.
			if um := byKey[b.Key()]; um != nil {
				br.OnTransition(func(to breaker.State) { um.BreakerTransition(int(to)) })
			}
			breakers[id] = br
		}

		limiter := limit.New(p.Config().Limits)
		pm.SetInFlightLimit(limiter.Max())

		pi := &poolInstr{
			rp:       newReverseProxy(p, cfg.Server.TrustForwardedFor),
			pm:       pm,
			up:       up,
			tracker:  tracker,
			limiter:  limiter,
			breakers: breakers,
		}
		s.pools[p.Name()] = pi
		// Kept so Close can unregister it. A retired generation whose pools
		// stay registered is collected alongside the generation that replaced
		// it, and identical series from both make every scrape fail.
		st := poolStats{p: p, pi: pi}
		s.collectors = append(s.collectors, st)
		m.RegisterPoolCollector(st)
	}
	return s, nil
}

// poolStats adapts an upstream.Pool to observe.PoolStater, so that
// manifold_upstream_inflight and manifold_upstream_available are read from the
// live backends when Prometheus scrapes instead of being mirrored on the
// request path.
//
// The adapter lives here rather than in observe because observe must not
// import the data plane, and it lives here rather than in upstream because
// upstream must not know that Prometheus exists.
type poolStats struct {
	p  *upstream.Pool
	pi *poolInstr
}

func (ps poolStats) Name() string { return ps.p.Name() }

func (ps poolStats) Upstreams(yield func(key string, inflight int64, available bool, breakerState int)) {
	for _, b := range ps.p.Backends() {
		state := observe.BreakerClosed
		if br := ps.pi.breakerFor(b.ID()); br != nil {
			state = int(br.State())
		}
		yield(b.Key(), b.InFlight(), backendAvailable(b), state)
	}
}

// backendAvailable reports whether a backend is currently eligible to serve.
// It is read at scrape time by the manifold_upstream_available collector, never
// on the request path -- the request path gets an already-filtered candidate
// set from Pool.Candidates.
func backendAvailable(b *upstream.Backend) bool { return b.Available() }

// newReverseProxy builds the single ReverseProxy shared by every request to
// one pool.
func newReverseProxy(p *upstream.Pool, trustForwardedFor bool) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Transport: p.Transport(),

		// Rewrite, not Director. Director is the legacy API: it hands you the
		// outbound request only, so it cannot see the inbound one, which is
		// why X-Forwarded-* handling with Director is a well-known source of
		// header-spoofing bugs: a client-supplied X-Forwarded-For is passed
		// straight through unless you remember to reset it. Rewrite sees both
		// requests and strips the client's forwarding headers before it runs,
		// so the forwarding policy below is an explicit choice rather than an
		// accident of what the client happened to send.
		Rewrite: func(pr *httputil.ProxyRequest) {
			st := attemptFrom(pr.In.Context())
			pr.SetURL(st.target)

			// Forward the client's original Host to the backend, matching
			// nginx's `proxy_set_header Host $host`. The opposite choice —
			// letting SetURL rewrite Host to the backend's own address, which
			// is Go's default — is equally defensible and is what you want
			// when the backend is a shared host doing its own vhost routing on
			// a different name. We forward because manifold's backends are
			// interchangeable replicas of one service: a backend that
			// generates absolute URLs, sets cookie domains, or issues
			// redirects must see the name the client used, or it will hand the
			// client back a link to 127.0.0.1:9001.
			pr.Out.Host = pr.In.Host

			// ReverseProxy deletes the client's Forwarded / X-Forwarded-*
			// headers from Out before calling Rewrite, and SetXForwarded reads
			// Out — so on its own it *replaces* the chain with the immediate
			// peer's IP. That is the paranoid default, and it is the right one
			// for a proxy exposed directly to the internet, where a client can
			// forge any chain it likes.
			//
			// That paranoid default is what manifold ships with. Only when the
			// operator sets server.trust_forwarded_for do we restore the
			// inbound chain so SetXForwarded appends to it instead of
			// replacing it — the behaviour you want behind a trusted edge (an
			// ALB, a CDN, another nginx), where discarding the chain would
			// destroy the only record of the real client IP.
			//
			// Defaulting this to true would have been a security bug: with
			// manifold as the first hop from untrusted clients, a caller can
			// prepend arbitrary addresses to X-Forwarded-For, and anything
			// downstream reading the left-most entry — rate limiting, geo-IP,
			// audit logs, abuse blocking — can be lied to. Trust has to be
			// configured, never assumed.
			if trustForwardedFor {
				if prior := pr.In.Header.Values("X-Forwarded-For"); len(prior) > 0 {
					pr.Out.Header["X-Forwarded-For"] = append([]string(nil), prior...)
				}
			}

			// Appends the client IP to the chain above and sets
			// X-Forwarded-Host / X-Forwarded-Proto from the inbound request
			// (never from client-supplied values).
			pr.SetXForwarded()

			// Hop-by-hop headers (Connection, Keep-Alive, Transfer-Encoding,
			// TE, Trailer, Upgrade, Proxy-*) are stripped by ReverseProxy
			// itself, including the ones a client names in its own Connection
			// header. Re-implementing that here would be duplicated stdlib
			// work that silently rots when the list changes.
		},

		ModifyResponse: func(res *http.Response) error {
			if st := attemptFrom(res.Request.Context()); st != nil {
				res.Header.Set(UpstreamHeader, st.key)
			}
			return nil
		},

		// Capture the failure instead of letting ReverseProxy write its
		// default 502. The retry loop decides whether this is terminal, and
		// writing anything here would set clientWriter.wrote and forfeit the
		// retry.
		ErrorHandler: func(_ http.ResponseWriter, req *http.Request, err error) {
			if st := attemptFrom(req.Context()); st != nil {
				st.err = err
			}
		},

		// Silenced deliberately: an upstream failure is an expected event that
		// the Server maps to a status code, not a line on stderr. Structured
		// request logging and metrics are a separate concern and a separate
		// package.
		ErrorLog: log.New(io.Discard, "", 0),
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rt := s.lookup(r)
	if rt == nil {
		s.noRoute.Inc()
		http.Error(w, "no route matched", http.StatusNotFound)
		return
	}

	pi := s.pools[rt.pool.Name()]
	cw := &clientWriter{ResponseWriter: w}

	// Started after routing so an unrouted request pays nothing, and so the
	// histogram measures the work manifold actually did on behalf of a pool.
	start := time.Now()

	// Admission control comes first, before routing has cost anything beyond
	// a match and before a single byte goes upstream. That ordering is the
	// point of the mechanism: shedding is only cheap if it happens before the
	// expensive part. A limiter consulted after the dial has already been
	// paid for is a latency amplifier wearing a limiter's clothes.
	if !pi.limiter.Acquire(r.Context()) {
		pi.pm.Shed()
		shed(cw)
		pi.pm.Observe(r.Method, cw.status, time.Since(start).Seconds())
		return
	}
	// Deferred, not called on each exit path. dispatch can leave through half
	// a dozen returns and ReverseProxy can panic with http.ErrAbortHandler on
	// a client that vanished mid-body; a slot leaked on any one of those paths
	// lowers max_in_flight permanently, and the symptom -- a proxy that sheds
	// at a fraction of its configured concurrency after a few days of uptime
	// -- points at everything except the missing Release.
	defer pi.limiter.Release()

	s.dispatch(cw, r, rt.pool, pi)

	// One observation point for the whole request. dispatch has half a dozen
	// exits — 503 with no candidate, 503 with no pick, success, terminal
	// failure, client gone — and threading a metric increment through each of
	// them is how a status class ends up uncounted on the one path nobody
	// tested. cw.status is what the client actually saw, and is 0 when the
	// client vanished before anything was written, which classOf maps to
	// "other" rather than pretending it was a server error.
	pi.pm.Observe(r.Method, cw.status, time.Since(start).Seconds())
}

// shed writes the response for a request rejected by the limiter.
//
// 503 and not 429: 429 says "you, the client, are sending too much", which
// blames a caller that may be sending one request a minute into a pool that is
// saturated by somebody else entirely. 503 says "the service cannot take this
// right now", which is what actually happened.
//
// Retry-After: 1 is a deliberate, honest guess rather than a computed backoff.
// manifold has no queue depth to extrapolate from -- it refused instead of
// queueing, which is the whole design -- so any number here is a hint. One
// second is short enough that a client retrying immediately is no worse off
// and long enough to be worth obeying. It must be set before http.Error,
// because http.Error commits the header.
func shed(cw *clientWriter) {
	cw.Header().Set("Retry-After", "1")
	http.Error(cw, "overloaded", http.StatusServiceUnavailable)
}

// dispatch runs the attempt/retry loop for one request against one pool.
func (s *Server) dispatch(cw *clientWriter, r *http.Request, pool *upstream.Pool, pi *poolInstr) {
	rp := pi.rp
	retryCfg := pool.Config().Retry

	maxAttempts := retryCfg.MaxAttempts
	if maxAttempts < 1 {
		// Defaulting guarantees >= 1; belt and braces so a hand-built Config
		// in a test cannot produce a zero-attempt request that hangs.
		maxAttempts = 1
	}

	// tried is a small slice, not a map: max_attempts is single digits in any
	// sane config, and a linear scan over three ints beats a map allocation on
	// every request.
	tried := make([]int, 0, maxAttempts)
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		candidates := pool.Candidates()
		if len(candidates) == 0 {
			// Week 1 this only happens for an empty pool, which NewPool
			// rejects. Week 2 it is the real "everything is ejected" case.
			pi.pm.NoUpstream()
			http.Error(cw, "no available upstream", http.StatusServiceUnavailable)
			return
		}

		// The strategy always sees the full candidate set, never a
		// retry-filtered one. Feeding it a shrinking slice would invalidate
		// the weight table it caches per generation and quietly skew the
		// distribution of ordinary traffic to pay for the exceptional path.
		c, ok := pool.Strategy().Pick(pool.Gen(), candidates, balance.Request{
			// hash_on extraction lands with consistent hashing in Week 2;
			// round_robin ignores this field entirely.
			HashKey: "",
		})
		if !ok {
			pi.pm.NoUpstream()
			http.Error(cw, "no available upstream", http.StatusServiceUnavailable)
			return
		}
		if contains(tried, c.ID) {
			// A retry must land somewhere new. Round-robin's counter usually
			// arranges that on its own; this is the fallback for strategies
			// that are sticky by design (consistent hashing) or for a
			// collision. If every candidate has been tried, keep the
			// strategy's pick: with fewer backends than attempts, one more try
			// against a backend that may have recovered beats giving up early.
			if alt, found := firstUntried(candidates, tried); found {
				c = alt
			}
		}

		// Circuit breaking is applied to the strategy's pick rather than by
		// filtering the candidate slice the strategy sees. Handing a strategy
		// a pre-filtered set would invalidate the weight table it caches per
		// generation -- the same reason the retry path does not filter it --
		// and would make every request pay a scan of every breaker instead of
		// the one it is about to use.
		br := pi.breakerFor(c.ID)
		if br == nil || !br.Allow() {
			alt, altBr, found := admit(candidates, tried, pi)
			if !found {
				// Every candidate's breaker is open. Returning 503 here is the
				// entire value of the feature: the alternative is to pick one
				// anyway and pay a dial timeout per attempt against backends
				// we have concrete, recent evidence are down -- which converts
				// a fast failure into a slow one and keeps the sick backends
				// under load while they try to recover.
				pi.pm.NoUpstream()
				http.Error(cw, "no available upstream", http.StatusServiceUnavailable)
				return
			}
			c, br = alt, altBr
		}

		b := pool.Backend(c.ID)
		if b == nil {
			// A strategy returned an ID outside the candidate set. Degrade to
			// 503 rather than panicking the whole process.
			//
			// The breaker has already admitted this attempt, so give the probe
			// budget back before bailing out; otherwise a half-open breaker
			// leaks a probe it will never get an outcome for and can never
			// close again.
			br.Failure()
			pi.pm.NoUpstream()
			http.Error(cw, "no available upstream", http.StatusServiceUnavailable)
			return
		}

		st := &attemptState{target: b.URL(), key: b.Key()}
		lastErr = s.attempt(cw, r, rp, b, br, st, retryCfg)

		// Passive health, recorded per *attempt* rather than per request.
		// That distinction is the entire point of the signal: a request that
		// fails against A and succeeds on B is evidence against A and for B,
		// and folding it into one per-request outcome would either exonerate
		// A (because the client got a 200) or convict B (because the request
		// had failed once). The attempt loop is the only place that can see
		// both.
		//
		// The classification is health.Failed and is deliberately not
		// reimplemented here. It counts transport errors and 5xx, and
		// explicitly not 4xx -- a client spraying 404s must not be able to
		// eject the pool one backend at a time.
		//
		// cw.status is safe to read for the failure case even though it may
		// hold a status from nothing at all: when lastErr is non-nil,
		// health.Failed short-circuits on the error and never looks at it.
		pi.tracker.Record(c.ID, health.Failed(cw.status, lastErr))

		// Per-upstream accounting, by array index rather than label lookup.
		// A transport failure is recorded as its own class: the upstream
		// produced no status, and an upstream that is hard-down would
		// otherwise just stop appearing in this counter, which reads
		// identically to receiving no traffic.
		um := pi.up[c.ID]
		if lastErr == nil {
			um.Response(cw.status)
			return
		}
		um.Failure()

		tried = append(tried, c.ID)

		if !canRetry(cw, r, lastErr, retryCfg, attempt, maxAttempts) {
			break
		}
		pi.pm.Retry()
	}

	writeFailure(cw, r, lastErr)
}

// attempt runs one forwarding attempt and returns its transport-level error,
// or nil on success.
func (s *Server) attempt(
	cw *clientWriter,
	r *http.Request,
	rp *httputil.ReverseProxy,
	b *upstream.Backend,
	br *breaker.Breaker,
	st *attemptState,
	retryCfg config.RetryConfig,
) error {
	ctx := context.WithValue(r.Context(), attemptKey{}, st)
	var cancel context.CancelFunc
	if d := retryCfg.PerTryTimeout.D(); d > 0 {
		ctx, cancel = context.WithTimeout(ctx, d)
	}

	// The closure exists so that Release and cancel are deferred to the end of
	// *this attempt* rather than the end of the request: a two-attempt request
	// must not hold an in-flight slot on the first backend while it talks to
	// the second. Deferring also means a panic inside ReverseProxy — including
	// the http.ErrAbortHandler it raises when a streamed body dies mid-copy —
	// cannot strand the counter.
	func() {
		if cancel != nil {
			// Safe to cancel here: ReverseProxy copies the response body
			// synchronously inside ServeHTTP, so by the time this returns the
			// client has the whole body. per_try_timeout therefore bounds the
			// entire attempt including body transfer, which is the intent.
			defer cancel()
		}
		b.Acquire()
		defer b.Release()
		// Recorded from a defer for the same reason Release is. Allow has
		// already consumed a half-open probe from a budget of, by default,
		// exactly one; if the outcome is dropped -- and ReverseProxy really
		// does panic, with http.ErrAbortHandler, whenever a client disappears
		// mid-body -- that breaker sits half-open with no budget left and
		// rejects every request to a backend that may be perfectly healthy,
		// forever. This is the one place in the request path where losing an
		// update is not a lost statistic but a permanent outage.
		defer recordBreaker(br, cw, st)
		rp.ServeHTTP(cw, r.WithContext(ctx))
	}()

	return st.err
}

// admit walks the candidate set for one whose breaker will take an attempt,
// preferring a backend this request has not already tried.
//
// It returns the breaker whose Allow *already returned true*, and the caller
// must not call Allow on it again: in the half-open state a true return
// consumes one probe from the budget, so a second call would either burn a
// second probe or, with the default budget of one, refuse the attempt this
// function just authorised.
func admit(candidates []balance.Candidate, tried []int, pi *poolInstr) (balance.Candidate, *breaker.Breaker, bool) {
	if c, br, ok := admitWhere(candidates, tried, pi, false); ok {
		return c, br, true
	}
	// Nothing untried will take it. Falling back to an already-tried backend
	// is not pointless: this request's earlier attempt against it failed at
	// the transport level, but its breaker is still closed, which means the
	// pool's recent history says it works. One more try there beats a 503.
	return admitWhere(candidates, tried, pi, true)
}

func admitWhere(candidates []balance.Candidate, tried []int, pi *poolInstr, wantTried bool) (balance.Candidate, *breaker.Breaker, bool) {
	for _, c := range candidates {
		if contains(tried, c.ID) != wantTried {
			continue
		}
		if br := pi.breakerFor(c.ID); br != nil && br.Allow() {
			return c, br, true
		}
	}
	return balance.Candidate{}, nil, false
}

// recordBreaker feeds one attempt outcome to the upstream's breaker.
//
// The classification is health.Failed, exactly as the passive tracker uses,
// and deliberately not a second implementation of the same idea. It counts
// transport errors and 5xx and explicitly not 4xx: a scanner spraying 404s
// must not be able to open every breaker in the pool one backend at a time,
// which is the same self-inflicted outage the tracker's comment describes and
// the same reason the predicate lives in one place.
func recordBreaker(br *breaker.Breaker, cw *clientWriter, st *attemptState) {
	if health.Failed(cw.status, st.err) {
		br.Failure()
		return
	}
	br.Success()
}

// canRetry is the full retry gate. Every condition must hold.
func canRetry(
	cw *clientWriter,
	r *http.Request,
	err error,
	retryCfg config.RetryConfig,
	attempt, maxAttempts int,
) bool {
	// 1. Budget remains (which also means max_attempts > 1 at all).
	if attempt+1 >= maxAttempts {
		return false
	}
	// 2. Nothing has reached the client. Checked early because it is the
	//    condition whose violation corrupts the response rather than merely
	//    wasting work.
	if cw.wrote {
		return false
	}
	// 3. The client is still there.
	if r.Context().Err() != nil {
		return false
	}
	// 4. The failure is connection-level, never a 5xx the backend produced.
	if !retryableError(err) {
		return false
	}
	// 5. Replaying the method is safe under the configured policy.
	if !methodRetryable(r, retryCfg) {
		return false
	}
	// 6. The body can actually be sent again.
	return bodyReplayable(r)
}

// writeFailure turns a terminal upstream error into a client response.
func writeFailure(cw *clientWriter, r *http.Request, err error) {
	if err == nil || cw.wrote {
		// Either the request succeeded, or the response is already committed
		// and there is nothing left to say.
		return
	}
	if r.Context().Err() != nil || errors.Is(err, context.Canceled) {
		// The client hung up. Writing a status to a dead connection is
		// pointless, and counting it as a gateway error would make an ordinary
		// browser navigate-away look like a backend outage. Say nothing.
		return
	}
	code := statusForError(err)
	http.Error(cw, http.StatusText(code), code)
}

func contains(ids []int, id int) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

func firstUntried(candidates []balance.Candidate, tried []int) (balance.Candidate, bool) {
	for _, c := range candidates {
		if !contains(tried, c.ID) {
			return c, true
		}
	}
	return balance.Candidate{}, false
}

// Compile-time assertion that Server is a handler.
var _ http.Handler = (*Server)(nil)
