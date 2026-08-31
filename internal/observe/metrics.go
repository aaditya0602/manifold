// Package observe owns manifold's Prometheus instrumentation.
//
// The design constraint that shapes every type here is that manifold is a
// proxy whose request path is already the bottleneck: it delivers 13.7k rps
// against nginx's 19.2k. Instrumentation that costs a map hash, a mutex, or an
// allocation per request would show up directly in that number.
//
// So the package is split in two along a temporal seam:
//
//   - Construction time resolves every labelled child metric exactly once.
//     Pool, PoolMetrics.Upstream and the constructors below all do label
//     lookups, allocate, and take locks. They are called from proxy.New.
//   - Request time touches only the already-resolved prometheus.Counter and
//     prometheus.Observer values stored on PoolMetrics / UpstreamMetrics. No
//     WithLabelValues, no fmt, no map, no allocation.
//
// Cardinality is bounded by construction, not by convention. The only label
// values that exist are pool names and upstream keys (both from config, both
// fixed at startup), a closed set of HTTP methods, and a closed set of status
// classes. Nothing client-controlled — no path, no raw status code, no header
// value — is ever used as a label, because an unbounded label is both a memory
// exhaustion vector and a way to make a scrape slower than the traffic it
// describes.
package observe

import (
	"net/http"
	"runtime"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// durationBuckets are the histogram boundaries for request duration, in
// seconds.
//
// Prometheus's DefBuckets start at 5ms and put only three boundaries below
// 100ms. For manifold, whose p50 is ~3ms and p99 ~10ms, that means the median
// and the tail land in the *same* bucket: every quantile you would actually
// want to read is pure interpolation across one 5ms-to-10ms span, and a
// regression that doubles p50 from 3ms to 6ms is invisible. The defaults are
// tuned for web applications, not for a proxy that measures itself in
// microseconds of added latency.
//
// These boundaries instead:
//
//   - Start at 100µs. Below that manifold is not the thing being measured —
//     that is a same-host loopback backend answering instantly, and one bucket
//     is enough to see it.
//   - Place exact boundaries at 3ms and 10ms, the current p50 and p99.
//     histogram_quantile interpolates *within* a bucket, so a quantile that
//     sits on a boundary is reported exactly rather than estimated; putting
//     the two numbers the project is judged on directly on boundaries makes
//     the SLO readable without error bars.
//   - Keep six boundaries between 1ms and 15ms, the band where all normal
//     traffic lives and where a regression must be detectable.
//   - Thin out above 25ms, where the only question is "how bad" rather than
//     "how much worse", and run out to 5s so a hung upstream hitting a
//     per-try timeout still lands in a real bucket instead of +Inf.
//
// 18 boundaries (19 series with +Inf) per pool. A histogram bucket is a plain
// atomic add on the observation path regardless of count, so the width costs
// scrape bytes and TSDB series, not request latency.
var durationBuckets = []float64{
	0.0001, 0.00025, 0.0005, // sub-millisecond: is the proxy the cost at all
	0.001, 0.002, 0.003, 0.005, 0.0075, 0.01, 0.015, // the working band; 0.003 = p50, 0.01 = p99
	0.025, 0.05, 0.1, 0.25, 0.5, // degraded but serving
	1, 2.5, 5, // timeouts and pathology
}

// Status classes. The index of a class is its position in classNames, so the
// request path resolves a class with an integer, never a string.
const (
	class1xx = iota
	class2xx
	class3xx
	class4xx
	class5xx
	// classOther covers a status outside 100..599, including the zero value
	// recorded when the proxy wrote nothing at all because the client hung up
	// mid-request. Folding those into 5xx would report a client disconnect as
	// a server error.
	classOther
	// classError is only ever used for upstream requests: it is a
	// transport-level failure (refused dial, reset, per-try timeout) where the
	// upstream never produced a status at all. Without it an upstream that is
	// hard-down is invisible in manifold_upstream_requests_total — it would
	// simply stop counting, which is indistinguishable from receiving no
	// traffic.
	classError
	numClasses
)

var classNames = [numClasses]string{"1xx", "2xx", "3xx", "4xx", "5xx", "other", "error"}

// classOf maps an HTTP status code to a class index. Integer division, no
// allocation, no branching on strings.
func classOf(status int) int {
	switch {
	case status >= 200 && status < 300:
		return class2xx
	case status >= 300 && status < 400:
		return class3xx
	case status >= 400 && status < 500:
		return class4xx
	case status >= 500 && status < 600:
		return class5xx
	case status >= 100 && status < 200:
		return class1xx
	default:
		return classOther
	}
}

// Method indices. HTTP methods are client-controlled — a request may carry any
// token as its method, so labelling with r.Method directly is an unbounded
// label and therefore a memory-exhaustion vector. Everything outside the
// registered set collapses into "OTHER".
const (
	methodGet = iota
	methodHead
	methodPost
	methodPut
	methodPatch
	methodDelete
	methodOptions
	methodConnect
	methodTrace
	methodOther
	numMethods
)

var methodNames = [numMethods]string{
	http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut,
	http.MethodPatch, http.MethodDelete, http.MethodOptions,
	http.MethodConnect, http.MethodTrace, "OTHER",
}

// methodIndex maps a request method to its index.
//
// A switch, not a map: the compiler turns a string switch into a length check
// plus a short comparison chain with no hashing and no allocation, whereas a
// map lookup hashes the string. This runs once per request, so the difference
// is small but it is free to take.
func methodIndex(m string) int {
	switch m {
	case http.MethodGet:
		return methodGet
	case http.MethodHead:
		return methodHead
	case http.MethodPost:
		return methodPost
	case http.MethodPut:
		return methodPut
	case http.MethodPatch:
		return methodPatch
	case http.MethodDelete:
		return methodDelete
	case http.MethodOptions:
		return methodOptions
	case http.MethodConnect:
		return methodConnect
	case http.MethodTrace:
		return methodTrace
	default:
		return methodOther
	}
}

// Metrics is the whole instrumentation surface: one private registry, the
// metric families registered in it, and the per-pool children resolved from
// them.
//
// It is safe for concurrent use. The mutex guards only the startup-time
// resolution caches; nothing on the request path touches it.
type Metrics struct {
	reg *prometheus.Registry

	requests         *prometheus.CounterVec
	duration         *prometheus.HistogramVec
	upstreamRequests *prometheus.CounterVec
	retries          *prometheus.CounterVec
	noUpstream       *prometheus.CounterVec
	availability     *prometheus.CounterVec

	breakerTransitions *prometheus.CounterVec
	shed               *prometheus.CounterVec
	inflightLimit      *prometheus.GaugeVec

	// configReloads is resolved into configReload at construction. Nothing in
	// this package or in the data plane calls it: it is wired up by the hot
	// reload path in cmd/manifold, which increments it once per reload signal.
	// It is declared here rather than there because every manifold metric
	// family lives in this file, and a reload counter registered from cmd
	// would be the one series that is not.
	configReloads *prometheus.CounterVec
	configReload  [numReloadResults]prometheus.Counter

	noRoute   prometheus.Counter
	collector *poolCollector

	mu    sync.Mutex
	pools map[string]*PoolMetrics
}

// New builds a Metrics over a private registry.
//
// The registry is explicitly constructed rather than prometheus.DefaultRegisterer
// for two reasons. It stops any dependency that calls MustRegister in an init
// function from silently publishing metrics on manifold's admin port, and it
// makes the exposition output a deterministic function of this package, which
// is what lets the tests parse it and assert on exact families.
func New(version string) *Metrics {
	m := &Metrics{
		reg:   prometheus.NewRegistry(),
		pools: make(map[string]*PoolMetrics),
	}

	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "manifold_build_info",
		Help: "Build information. Always 1; the value carries nothing, the labels are the point.",
	}, []string{"version", "go_version"})
	buildInfo.WithLabelValues(version, runtime.Version()).Set(1)

	m.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "manifold_requests_total",
		Help: "Client requests completed, by pool, method and response status class.",
	}, []string{"pool", "method", "status_class"})

	m.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "manifold_request_duration_seconds",
		Help:    "End-to-end client request duration in seconds, including retries.",
		Buckets: durationBuckets,
	}, []string{"pool"})

	m.upstreamRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "manifold_upstream_requests_total",
		Help: "Forwarding attempts to an upstream, by response status class. The 'error' class is a transport failure with no response.",
	}, []string{"pool", "upstream", "status_class"})

	m.retries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "manifold_retries_total",
		Help: "Retried forwarding attempts, i.e. attempts after the first.",
	}, []string{"pool"})

	m.noUpstream = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "manifold_requests_no_upstream_total",
		Help: "Requests failed with 503 because the matched pool had no usable upstream.",
	}, []string{"pool"})

	// Availability changes are edges, not levels. manifold_upstream_available
	// already reports the current state at scrape time, but a gauge sampled
	// every 15s cannot show a backend that flapped out and back between two
	// scrapes -- and flapping is the single most useful thing to alert on,
	// because it means the health thresholds are mistuned rather than that a
	// machine died. A monotonic counter of transitions survives sampling;
	// rate() over it is a flap rate.
	//
	// state is a closed two-value set, so this adds exactly 2 series per
	// upstream. It is incremented from an availability hook, never from the
	// request path.
	m.availability = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "manifold_upstream_availability_changes_total",
		Help: "Transitions of an upstream into or out of the available set, by the state it moved to.",
	}, []string{"pool", "upstream", "state"})

	// Breaker transitions are edges, exactly as availability changes are, and
	// for the same reason: manifold_breaker_state is a gauge read at scrape
	// time and cannot show a breaker that tripped and recovered between two
	// scrapes. A breaker that flaps is the signal that failure_threshold is
	// mistuned, and it is invisible without this.
	m.breakerTransitions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "manifold_breaker_transitions_total",
		Help: "Circuit breaker state transitions, by the state moved to.",
	}, []string{"pool", "upstream", "to"})

	// Shedding is deliberately its own family rather than a status class of
	// manifold_requests_total. A shed request is manifold working correctly
	// under overload; an upstream 503 is a backend failing. Both reach the
	// client as a 503, so without a separate counter the single most important
	// operational distinction in this system -- "are we protecting ourselves
	// or are we broken" -- is unanswerable from the metrics.
	m.shed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "manifold_requests_shed_total",
		Help: "Requests rejected with 503 because the pool was at max_in_flight.",
	}, []string{"pool"})

	// A gauge rather than a number an operator reads out of the config file,
	// because the bound that matters during an incident is the one the running
	// process is enforcing. Set once at startup; 0 means unlimited.
	m.inflightLimit = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "manifold_inflight_limit",
		Help: "Configured max_in_flight for the pool. 0 means unlimited.",
	}, []string{"pool"})

	m.configReloads = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "manifold_config_reloads_total",
		Help: "Configuration reload attempts, by outcome.",
	}, []string{"result"})
	// Both children are resolved now so ConfigReload is an array index and an
	// atomic add, and -- more usefully -- so that both series exist from the
	// first scrape. A failure counter that only appears after the first
	// failure cannot be alerted on with rate(), because there is no series to
	// write the alert against until the thing you wanted to catch has already
	// happened.
	for i, name := range reloadResultNames {
		m.configReload[i] = m.configReloads.WithLabelValues(name)
	}

	m.noRoute = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "manifold_requests_no_route_total",
		Help: "Requests rejected with 404 because no route matched.",
	})

	m.collector = newPoolCollector()

	// MustRegister is correct here and not a latent panic: every collector is
	// created immediately above, in a registry created immediately above, so a
	// duplicate registration would be a bug in this function that must fail
	// loudly at startup rather than leave a metric silently missing.
	m.reg.MustRegister(
		buildInfo,
		m.requests,
		m.duration,
		m.upstreamRequests,
		m.retries,
		m.noUpstream,
		m.availability,
		m.breakerTransitions,
		m.shed,
		m.inflightLimit,
		m.configReloads,
		m.noRoute,
		m.collector,
	)

	return m
}

// Registry is the private registry every manifold metric lives in.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// Handler serves the exposition format. It is mounted on the admin listener
// only — see cmd/manifold.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{
		// A collector error must not be swallowed: a scrape that silently
		// returns a partial view is worse than a scrape that fails, because
		// the alert that should fire on the missing series never does.
		ErrorHandling: promhttp.HTTPErrorOnError,
		// Bounded: without this a slow scrape from a second Prometheus can
		// stack collections against the same live pools.
		MaxRequestsInFlight: 4,
	})
}

// Config reload outcomes.
const (
	reloadSuccess = iota
	reloadFailure
	numReloadResults
)

var reloadResultNames = [numReloadResults]string{"success", "failure"}

// ConfigReload records one configuration reload attempt.
//
// Nothing calls this yet: it is the accessor the hot reload path in
// cmd/manifold uses when it lands. It lives here because this file owns every
// metric family manifold exposes, and splitting registration across two
// packages is how a family ends up registered twice in one registry and
// panicking at startup.
//
// A failed reload is the quietest dangerous event a proxy has: the process
// keeps serving happily on the *old* config, so nothing pages and nothing
// looks broken, while the change the operator believes they shipped is not
// running. This counter is what makes that visible.
func (m *Metrics) ConfigReload(success bool) {
	if success {
		m.configReload[reloadSuccess].Inc()
		return
	}
	m.configReload[reloadFailure].Inc()
}

// NoRoute is the counter for requests that matched no route. Resolve it once
// at startup and keep the returned Counter; it has no labels, so this is the
// whole hot-path value.
func (m *Metrics) NoRoute() prometheus.Counter { return m.noRoute }

// Pool resolves every labelled child a pool's request path needs, and caches
// the result so two routes onto the same pool share one PoolMetrics.
//
// Call this at startup. It does len(methods) * len(classes) = 70 label lookups
// per pool, each one a map hash and a mutex acquire inside client_golang —
// which is exactly the work the request path must never do, done once here so
// that it never has to.
func (m *Metrics) Pool(name string) *PoolMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()

	if pm, ok := m.pools[name]; ok {
		return pm
	}

	pm := &PoolMetrics{
		duration:   m.duration.WithLabelValues(name),
		retries:    m.retries.WithLabelValues(name),
		noUpstream: m.noUpstream.WithLabelValues(name),
		shed:       m.shed.WithLabelValues(name),
		limit:      m.inflightLimit.WithLabelValues(name),
		name:       name,
		parent:     m,
		upstreams:  make(map[string]*UpstreamMetrics),
	}
	for mi := 0; mi < numMethods; mi++ {
		for ci := 0; ci < numClasses; ci++ {
			pm.requests[mi][ci] = m.requests.WithLabelValues(name, methodNames[mi], classNames[ci])
		}
	}
	m.pools[name] = pm
	return pm
}

// RegisterPoolCollector adds a pool to the scrape-time collector behind
// manifold_upstream_inflight and manifold_upstream_available.
//
// Those two are gauges of live state, so they are read from the pool when
// Prometheus asks rather than mirrored by the request path. Mirroring would
// mean two extra atomic increments per request to reproduce a number the
// backend already maintains for the balancer, and it would be wrong the moment
// a panic skipped a decrement.
func (m *Metrics) RegisterPoolCollector(p PoolStater) { m.collector.add(p) }

// UnregisterPoolCollector drops a pool registered by RegisterPoolCollector. A
// Server that is being retired must call this, or its pools keep being
// scraped alongside the generation that replaced them and every scrape fails
// as a duplicate. See poolCollector.remove.
func (m *Metrics) UnregisterPoolCollector(p PoolStater) { m.collector.remove(p) }

// PoolMetrics is one pool's pre-resolved children. Every field is a concrete
// child metric; there is no *Vec here on purpose, because reaching a *Vec at
// request time is exactly the label lookup this type exists to avoid.
type PoolMetrics struct {
	// requests is indexed [method][statusClass]. A 10x7 array of interface
	// values is 1120 bytes per pool — trivially small, and it turns the
	// hottest metric in the process into two integer index operations.
	requests [numMethods][numClasses]prometheus.Counter

	duration   prometheus.Observer
	retries    prometheus.Counter
	noUpstream prometheus.Counter
	shed       prometheus.Counter
	limit      prometheus.Gauge

	name   string
	parent *Metrics

	// mu guards upstreams, which is only written at startup.
	mu        sync.Mutex
	upstreams map[string]*UpstreamMetrics
}

// Observe records one completed client request. This is the hot path: two
// array indexes, an atomic add, and a histogram observation. No allocation.
func (pm *PoolMetrics) Observe(method string, status int, seconds float64) {
	pm.requests[methodIndex(method)][classOf(status)].Inc()
	pm.duration.Observe(seconds)
}

// Retry records one forwarding attempt after the first.
func (pm *PoolMetrics) Retry() { pm.retries.Inc() }

// NoUpstream records a request that matched this pool but found no usable
// backend.
func (pm *PoolMetrics) NoUpstream() { pm.noUpstream.Inc() }

// Shed records one request rejected because the pool was already at
// max_in_flight. One atomic add: it runs on the overload path, which is the
// worst possible moment to be doing anything more expensive.
func (pm *PoolMetrics) Shed() { pm.shed.Inc() }

// SetInFlightLimit publishes the pool's configured bound. Startup only.
func (pm *PoolMetrics) SetInFlightLimit(n int64) { pm.limit.Set(float64(n)) }

// Name is the pool's config name.
func (pm *PoolMetrics) Name() string { return pm.name }

// Upstream resolves the per-upstream counters for one backend key. Call it at
// startup, once per backend, and hold the result.
func (pm *PoolMetrics) Upstream(key string) *UpstreamMetrics {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if um, ok := pm.upstreams[key]; ok {
		return um
	}
	um := &UpstreamMetrics{}
	for ci := 0; ci < numClasses; ci++ {
		um.requests[ci] = pm.parent.upstreamRequests.WithLabelValues(pm.name, key, classNames[ci])
	}
	for si, state := range availabilityStateNames {
		um.availability[si] = pm.parent.availability.WithLabelValues(pm.name, key, state)
	}
	for bi, state := range BreakerStateNames {
		um.breaker[bi] = pm.parent.breakerTransitions.WithLabelValues(pm.name, key, state)
	}
	pm.upstreams[key] = um
	return um
}

// Availability state indices. Both children are resolved at startup, in the
// same pre-resolution pass as the request counters, so recording a transition
// is one array index and one atomic add -- no WithLabelValues on a path that
// runs while a prober goroutine is blocked waiting for it.
const (
	stateUnavailable = iota
	stateAvailable
	numAvailabilityStates
)

var availabilityStateNames = [numAvailabilityStates]string{"unavailable", "available"}

// UpstreamMetrics is one backend's pre-resolved counters within one pool.
type UpstreamMetrics struct {
	requests [numClasses]prometheus.Counter

	// availability is indexed by the stateAvailable/stateUnavailable
	// constants, so a bool selects a counter without a branch into a map.
	availability [numAvailabilityStates]prometheus.Counter

	// breaker is indexed by BreakerClosed/BreakerOpen/BreakerHalfOpen, which
	// are numerically identical to breaker.State. Taking an int rather than
	// the real type is what keeps this package free of any import from the
	// data plane -- observe is imported *by* proxy, never the other way round.
	breaker [NumBreakerStates]prometheus.Counter
}

// Circuit breaker states, as observe sees them.
//
// These mirror breaker.State's values exactly and must stay in step with it.
// The duplication is deliberate: importing internal/breaker here would make
// the instrumentation package depend on the data plane, which is the same
// inversion PoolStater exists to avoid.
const (
	BreakerClosed = iota
	BreakerOpen
	BreakerHalfOpen
	NumBreakerStates
)

// BreakerStateNames are the label values for a breaker state, in index order.
// They are also what manifold_breaker_state's numeric values decode to.
var BreakerStateNames = [NumBreakerStates]string{"closed", "open", "half_open"}

// BreakerTransition records one circuit breaker state change for this
// upstream. state is a BreakerClosed/BreakerOpen/BreakerHalfOpen index; an
// out-of-range value is dropped rather than panicking, because this is called
// from a hook running on a goroutine that is serving a request.
func (um *UpstreamMetrics) BreakerTransition(state int) {
	if state < 0 || state >= NumBreakerStates {
		return
	}
	um.breaker[state].Inc()
}

// AvailabilityChange records one transition of this upstream into or out of
// the available set. It is called from an upstream.Pool availability hook,
// which runs on whichever goroutine caused the transition -- a prober, the
// passive tracker's sweeper, or a request recording a passive outcome -- so it
// must not block. An atomic add does not.
func (um *UpstreamMetrics) AvailabilityChange(available bool) {
	if available {
		um.availability[stateAvailable].Inc()
		return
	}
	um.availability[stateUnavailable].Inc()
}

// Response records an attempt that produced an upstream response.
func (um *UpstreamMetrics) Response(status int) { um.requests[classOf(status)].Inc() }

// Failure records an attempt that failed below HTTP: the upstream was never
// reached, or never answered, so there is no status to classify.
func (um *UpstreamMetrics) Failure() { um.requests[classError].Inc() }
