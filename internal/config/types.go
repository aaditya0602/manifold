// Package config defines manifold's YAML configuration schema, loading, and
// validation.
//
// The schema is deliberately explicit: every timeout, threshold, and window is
// nameable in YAML rather than hidden behind a constant.
//
// Defaulting is presence-aware: a key the operator omits gets its default, and
// a key the operator writes wins even when the value written is the zero value.
// That distinction matters — "max_in_flight: 0" means unlimited and must not be
// silently replaced by the default, and "enabled: true" must be the default for
// health checking without making "enabled: false" impossible to express. The
// mechanism is in defaults.go: decoding starts from a fully populated struct
// rather than a zero one, so yaml.v3 only overwrites the keys actually present
// in the document. Every default documented in config.example.yaml is therefore
// the real default, and validation rules like "interval must be > 0" stay
// reachable instead of being papered over by defaulting.
package config

import (
	"fmt"
	"time"
)

// Duration wraps time.Duration so YAML can carry human strings ("500ms", "2s").
// gopkg.in/yaml.v3 has no built-in decoder for time.Duration, so we supply one.
type Duration time.Duration

// D returns the underlying time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

func (d Duration) String() string { return time.Duration(d).String() }

// UnmarshalYAML decodes a duration string. yaml.v3 calls the func-style
// unmarshaler when it is present on the type.
func (d *Duration) UnmarshalYAML(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return fmt.Errorf("duration must be a string like \"250ms\": %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// MarshalYAML renders the duration back as a human string.
func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

// Strategy names a load balancing algorithm.
type Strategy string

// Supported balancing strategies.
const (
	StrategyRoundRobin     Strategy = "round_robin"
	StrategyLeastConn      Strategy = "least_conn"
	StrategyConsistentHash Strategy = "consistent_hash"
)

// Config is the root of the YAML document. A *Config, once validated, is
// treated as immutable: hot reload swaps a whole new pointer rather than
// mutating fields in place, so the request path never takes a lock.
type Config struct {
	// Listen is the address the proxy serves client traffic on, e.g. ":8080".
	Listen string `yaml:"listen"`
	// Admin serves /metrics, /healthz and pprof. Separate port so operational
	// endpoints are never reachable from the data-plane listener.
	Admin string `yaml:"admin"`

	// Server holds listener-side timeouts for connections from clients.
	Server ServerConfig `yaml:"server"`

	Pools  []PoolConfig  `yaml:"pools"`
	Routes []RouteConfig `yaml:"routes"`
}

// ServerConfig bounds how long a misbehaving client can hold a connection.
type ServerConfig struct {
	ReadHeaderTimeout Duration `yaml:"read_header_timeout"`
	ReadTimeout       Duration `yaml:"read_timeout"`
	WriteTimeout      Duration `yaml:"write_timeout"`
	IdleTimeout       Duration `yaml:"idle_timeout"`
	// DrainTimeout bounds graceful shutdown: after it elapses, remaining
	// in-flight requests are cut off rather than waited on forever.
	DrainTimeout Duration `yaml:"drain_timeout"`

	// TrustForwardedFor decides whether an inbound X-Forwarded-For chain is
	// preserved and appended to, or discarded and replaced with the immediate
	// peer's address.
	//
	// It defaults to false, the safe value, because any client can forge this
	// header. When manifold is the first hop from untrusted clients, trusting
	// the chain lets a caller prepend arbitrary addresses and lie to anything
	// downstream that reads the left-most entry: rate limiting, geo-IP,
	// audit logs, abuse blocking.
	//
	// Set it to true only when manifold sits behind a trusted edge — an ALB,
	// a CDN, another proxy — that has already sanitised the header. There,
	// discarding the chain would destroy the only record of the real client
	// IP, since the peer address is just the edge.
	//
	// The general form of this setting is a list of trusted proxy CIDRs, so
	// the chain is honoured only when the peer is itself trusted. That is the
	// right eventual design; this boolean is the honest subset of it.
	TrustForwardedFor bool `yaml:"trust_forwarded_for"`
}

// PoolConfig is one group of interchangeable backends plus the policy applied
// to them. Health, breaker, limit and retry policy are per-pool, not global.
type PoolConfig struct {
	Name     string   `yaml:"name"`
	Strategy Strategy `yaml:"strategy"`
	// HashOn selects the request attribute keyed on when Strategy is
	// consistent_hash. Forms: "client_ip", "header:X-Session-Id", "cookie:sid",
	// "path".
	HashOn string `yaml:"hash_on"`

	Upstreams []UpstreamConfig `yaml:"upstreams"`
	Health    HealthConfig     `yaml:"health"`
	Breaker   BreakerConfig    `yaml:"breaker"`
	Limits    LimitConfig      `yaml:"limits"`
	Retry     RetryConfig      `yaml:"retry"`
	Transport TransportConfig  `yaml:"transport"`
}

// UpstreamConfig is a single backend origin.
type UpstreamConfig struct {
	// URL is scheme://host:port. Path components are rejected: routing decides
	// the path, an upstream entry only names an origin.
	URL string `yaml:"url"`
	// Weight biases selection and is always >= 1 after decoding. An omitted or
	// zero weight becomes 1; a negative weight is a validation error. See the
	// note in defaults.go for why zero is not distinguishable from absent.
	Weight int `yaml:"weight"`
}

// HealthConfig groups the two independent health signals.
type HealthConfig struct {
	Active  ActiveHealthConfig  `yaml:"active"`
	Passive PassiveHealthConfig `yaml:"passive"`
}

// ActiveHealthConfig probes backends out-of-band on a fixed interval.
type ActiveHealthConfig struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`
	// Method defaults to GET.
	Method   string   `yaml:"method"`
	Interval Duration `yaml:"interval"`
	Timeout  Duration `yaml:"timeout"`
	// ExpectStatus is the set of status codes counted as healthy. Empty means
	// any 2xx.
	ExpectStatus []int `yaml:"expect_status"`
	// HealthyThreshold consecutive successes readmit an ejected backend.
	HealthyThreshold int `yaml:"healthy_threshold"`
	// UnhealthyThreshold consecutive failures eject a backend.
	UnhealthyThreshold int `yaml:"unhealthy_threshold"`
}

// PassiveHealthConfig ejects backends based on real traffic outcomes, so a
// backend that passes /healthz but fails real requests is still caught.
type PassiveHealthConfig struct {
	Enabled bool `yaml:"enabled"`
	// Window is the sliding period over which the error rate is computed.
	Window Duration `yaml:"window"`
	// MinRequests guards against ejecting on a tiny sample.
	MinRequests int `yaml:"min_requests"`
	// ErrorRate in (0,1]; exceeding it within Window ejects the backend. Zero
	// is rejected rather than treated as "eject on any error", because an
	// operator who writes 0 much more likely meant to disable passive health.
	ErrorRate float64 `yaml:"error_rate"`
	// EjectFor is how long an ejected backend stays out before retrial.
	EjectFor Duration `yaml:"eject_for"`
}

// BreakerConfig is a per-upstream circuit breaker: closed -> open -> half-open.
type BreakerConfig struct {
	Enabled bool `yaml:"enabled"`
	// FailureThreshold consecutive failures trip the breaker open.
	FailureThreshold int `yaml:"failure_threshold"`
	// OpenFor is how long the breaker stays open before allowing probes.
	OpenFor Duration `yaml:"open_for"`
	// HalfOpenProbes requests are admitted while half-open; all must succeed
	// to close the breaker.
	HalfOpenProbes int `yaml:"half_open_probes"`
}

// LimitConfig bounds concurrent work so overload sheds rather than queues.
type LimitConfig struct {
	// MaxInFlight caps concurrent proxied requests for the pool. Omitting the
	// key applies the default; writing 0 explicitly means unlimited.
	MaxInFlight int `yaml:"max_in_flight"`
	// QueueTimeout is how long a request may wait for an in-flight slot before
	// being shed with 503. 0 = shed immediately.
	QueueTimeout Duration `yaml:"queue_timeout"`
}

// RetryConfig governs re-dispatch to a different backend.
type RetryConfig struct {
	// MaxAttempts counts the first try. 1 disables retries.
	MaxAttempts int `yaml:"max_attempts"`
	// IdempotentOnly restricts retries to GET/HEAD/PUT/DELETE/OPTIONS/TRACE
	// and requests carrying an Idempotency-Key header. Retrying a POST that
	// already reached the backend can double-apply a side effect.
	IdempotentOnly bool `yaml:"idempotent_only"`
	// PerTryTimeout bounds each individual attempt. 0 = no per-try bound.
	PerTryTimeout Duration `yaml:"per_try_timeout"`
}

// TransportConfig tunes Go's http.Transport. Connection pooling and keep-alive
// are provided by the standard library; manifold only sizes and bounds them.
type TransportConfig struct {
	MaxIdleConns          int      `yaml:"max_idle_conns"`
	MaxIdleConnsPerHost   int      `yaml:"max_idle_conns_per_host"`
	MaxConnsPerHost       int      `yaml:"max_conns_per_host"`
	IdleConnTimeout       Duration `yaml:"idle_conn_timeout"`
	DialTimeout           Duration `yaml:"dial_timeout"`
	ResponseHeaderTimeout Duration `yaml:"response_header_timeout"`
	DisableCompression    bool     `yaml:"disable_compression"`
}

// RouteConfig maps matching requests to a pool. Routes are evaluated in the
// order given; the first match wins.
type RouteConfig struct {
	Match MatchConfig `yaml:"match"`
	Pool  string      `yaml:"pool"`
}

// MatchConfig is an AND over the non-empty fields.
type MatchConfig struct {
	// Host matches the request Host header exactly (case-insensitive). Empty
	// matches any host.
	Host string `yaml:"host"`
	// PathPrefix matches the request path. Empty is treated as "/".
	PathPrefix string `yaml:"path_prefix"`
	// Methods restricts the route to these HTTP methods. Empty matches any.
	Methods []string `yaml:"methods"`
	// Headers requires each named header to equal the given value.
	Headers map[string]string `yaml:"headers"`
}
