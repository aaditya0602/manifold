package observe

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// PoolStater is the scrape-time view of one pool's live backend state.
//
// It is an interface rather than a dependency on internal/upstream so that
// observe stays free of the data plane's types: this package must be
// importable by the proxy without dragging pool construction, transports and
// balancing into its test binary, and the proxy is the only thing that knows
// how to read a Backend.
type PoolStater interface {
	// Name is the pool label value.
	Name() string

	// Upstreams reports each backend's live state, calling yield once per
	// backend. It is called during a Prometheus scrape, on a goroutine that is
	// not serving traffic, and must not retain yield.
	Upstreams(yield func(key string, inflight int64, available bool))
}

// poolCollector implements prometheus.Collector for the two gauges that must
// reflect live state at scrape time rather than a mirrored copy:
// manifold_upstream_inflight and manifold_upstream_available.
//
// The distinction matters. A mirrored gauge is a second source of truth that
// the request path has to pay to maintain and that drifts from the real
// counter the first time an increment and its decrement do not pair — which is
// precisely what happens when ReverseProxy panics with http.ErrAbortHandler on
// a client that vanished mid-body. Reading Backend.InFlight() when Prometheus
// asks costs the request path nothing and cannot drift, because it *is* the
// number the balancer makes its decisions on.
type poolCollector struct {
	inflight  *prometheus.Desc
	available *prometheus.Desc

	// mu guards pools. Registration happens at startup and collection happens
	// on scrape, so this is an uncontended read lock in practice; it exists
	// because nothing forbids a future hot reload from registering a pool
	// while a scrape is in flight.
	mu    sync.RWMutex
	pools []PoolStater
}

func newPoolCollector() *poolCollector {
	return &poolCollector{
		inflight: prometheus.NewDesc(
			"manifold_upstream_inflight",
			"Requests currently outstanding to an upstream, read live at scrape time.",
			[]string{"pool", "upstream"}, nil,
		),
		available: prometheus.NewDesc(
			"manifold_upstream_available",
			"1 if the upstream is currently eligible to receive traffic, 0 if it is not.",
			[]string{"pool", "upstream"}, nil,
		),
	}
}

func (c *poolCollector) add(p PoolStater) {
	if p == nil {
		return
	}
	c.mu.Lock()
	c.pools = append(c.pools, p)
	c.mu.Unlock()
}

// Describe sends both descriptors, making this a checked collector: the
// registry will reject a Collect that emits anything else, which is what turns
// a label typo into a failing scrape instead of a silently duplicated series.
func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.inflight
	ch <- c.available
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	pools := c.pools
	c.mu.RUnlock()

	for _, p := range pools {
		name := p.Name()
		p.Upstreams(func(key string, inflight int64, available bool) {
			ch <- prometheus.MustNewConstMetric(
				c.inflight, prometheus.GaugeValue, float64(inflight), name, key,
			)
			var up float64
			if available {
				up = 1
			}
			ch <- prometheus.MustNewConstMetric(
				c.available, prometheus.GaugeValue, up, name, key,
			)
		})
	}
}

var _ prometheus.Collector = (*poolCollector)(nil)
