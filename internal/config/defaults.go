package config

import (
	"time"

	"gopkg.in/yaml.v3"
)

// Defaults are applied by *seeding the destination before decoding* rather
// than by patching zero values afterwards. yaml.v3 writes only the fields a
// document actually contains, so a seeded destination gives us presence-aware
// defaulting for free: an omitted key keeps its default, and a key written as
// 0 or false wins. A post-decode pass cannot do this, because at that point
// "absent" and "written as the zero value" are the same bits — which would
// make `max_in_flight: 0` (unlimited) and `enabled: false` unexpressible.
//
// Every default lives in one of the three functions below. No component
// downstream may invent a fallback of its own.

const (
	defaultAdmin = ":9090"

	defaultReadHeaderTimeout = 5 * time.Second
	defaultReadTimeout       = 30 * time.Second
	defaultWriteTimeout      = 30 * time.Second
	defaultIdleTimeout       = 90 * time.Second
	defaultDrainTimeout      = 15 * time.Second
)

// defaultConfig is the root seed. Listen has no default: it is required.
func defaultConfig() Config {
	return Config{
		Admin: defaultAdmin,
		Server: ServerConfig{
			ReadHeaderTimeout: Duration(defaultReadHeaderTimeout),
			ReadTimeout:       Duration(defaultReadTimeout),
			WriteTimeout:      Duration(defaultWriteTimeout),
			IdleTimeout:       Duration(defaultIdleTimeout),
			DrainTimeout:      Duration(defaultDrainTimeout),
		},
	}
}

// defaultPool seeds one pool. Nested value structs (Health, Breaker, Limits,
// Retry, Transport) are filled here and need no unmarshaler of their own:
// decoding a struct field writes into the existing value, so present keys
// overwrite and absent ones keep what this function set.
func defaultPool() PoolConfig {
	return PoolConfig{
		Strategy: StrategyRoundRobin,
		Health: HealthConfig{
			Active: ActiveHealthConfig{
				Enabled:            true,
				Path:               "/healthz",
				Method:             "GET",
				Interval:           Duration(2 * time.Second),
				Timeout:            Duration(500 * time.Millisecond),
				HealthyThreshold:   2,
				UnhealthyThreshold: 3,
			},
			Passive: PassiveHealthConfig{
				Enabled:     true,
				Window:      Duration(10 * time.Second),
				MinRequests: 20,
				ErrorRate:   0.5,
				EjectFor:    Duration(30 * time.Second),
			},
		},
		Breaker: BreakerConfig{
			Enabled:          true,
			FailureThreshold: 5,
			OpenFor:          Duration(5 * time.Second),
			HalfOpenProbes:   1,
		},
		Limits: LimitConfig{
			// 0 written explicitly means unlimited, so the default must be a
			// real number rather than a sentinel.
			MaxInFlight:  1024,
			QueueTimeout: 0,
		},
		Retry: RetryConfig{
			MaxAttempts:    2,
			IdempotentOnly: true,
			PerTryTimeout:  0,
		},
		Transport: TransportConfig{
			MaxIdleConns:          2048,
			MaxIdleConnsPerHost:   256,
			MaxConnsPerHost:       0,
			IdleConnTimeout:       Duration(90 * time.Second),
			DialTimeout:           Duration(2 * time.Second),
			ResponseHeaderTimeout: Duration(10 * time.Second),
			DisableCompression:    true,
		},
	}
}

// defaultRoute seeds one route.
func defaultRoute() RouteConfig {
	return RouteConfig{Match: MatchConfig{PathPrefix: "/"}}
}

// UnmarshalYAML seeds pool defaults before decoding. Slice elements need this:
// yaml.v3 rebuilds a slice with reflect.MakeSlice, so every element starts
// zeroed no matter what the destination held, and a seeded root is not enough.
//
// The local `plain` type sheds this method, which is what stops Decode from
// recursing into it forever.
func (p *PoolConfig) UnmarshalYAML(value *yaml.Node) error {
	type plain PoolConfig
	seeded := plain(defaultPool())
	if err := value.Decode(&seeded); err != nil {
		return err
	}
	*p = PoolConfig(seeded)

	// Weight is the one field where an explicit 0 is still normalised to 1,
	// and the reason is a genuine limitation rather than a preference.
	// Upstreams is a slice, so every element starts zeroed no matter what the
	// enclosing pool was seeded with: "weight omitted" and "weight: 0" are
	// indistinguishable here. Distinguishing them would mean giving
	// UpstreamConfig its own UnmarshalYAML, which puts a third type behind
	// Node.Decode's strictness hole and forces a hand-mirrored strictUpstream
	// to keep rejecting typos inside upstream entries. That is a lot of
	// duplication to buy an error message for a value nobody writes on
	// purpose. Negative weights are still rejected by Validate, so the only
	// unreported case is a deliberate 0, which behaves as 1.
	//
	// If zero-weight ever needs to mean "drain: keep existing connections,
	// accept no new requests", this is the decision to revisit.
	for i := range p.Upstreams {
		if p.Upstreams[i].Weight == 0 {
			p.Upstreams[i].Weight = 1
		}
	}
	return nil
}

// UnmarshalYAML seeds route defaults before decoding, for the same reason
// PoolConfig does.
func (r *RouteConfig) UnmarshalYAML(value *yaml.Node) error {
	type plain RouteConfig
	seeded := plain(defaultRoute())
	if err := value.Decode(&seeded); err != nil {
		return err
	}
	*r = RouteConfig(seeded)
	return nil
}
