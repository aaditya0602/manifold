package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// problems accumulates validation failures so that one Validate call reports
// every mistake in a config file, not just the first. Fixing configuration one
// error per restart is a bad operator experience, especially during an
// incident.
type problems []error

func (p *problems) addf(format string, args ...any) {
	*p = append(*p, fmt.Errorf(format, args...))
}

// Validate checks the whole document and returns every problem found, joined
// into a single error. Each message is prefixed with the YAML path of the
// offending field, e.g.
//
//	pools[1].upstreams[0].url: scheme must be http or https, got "tcp"
//
// Validate assumes applyDefaults has already run.
func (c *Config) Validate() error {
	var p problems

	c.validateListeners(&p)
	c.validatePools(&p)
	c.validateRoutes(&p)

	return errors.Join(p...)
}

func (c *Config) validateListeners(p *problems) {
	if c.Listen == "" {
		p.addf("listen: required")
	} else if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		p.addf("listen: not a host:port address: %q", c.Listen)
	}
	// Admin is defaulted, so an empty value here means it was set to "".
	if c.Admin == "" {
		p.addf("admin: required")
	} else if _, _, err := net.SplitHostPort(c.Admin); err != nil {
		p.addf("admin: not a host:port address: %q", c.Admin)
	}
}

func (c *Config) validatePools(p *problems) {
	if len(c.Pools) == 0 {
		p.addf("pools: at least one pool is required")
	}

	seenName := make(map[string]int, len(c.Pools))
	for i := range c.Pools {
		pool := &c.Pools[i]
		at := fmt.Sprintf("pools[%d]", i)

		if pool.Name == "" {
			p.addf("%s.name: required", at)
		} else if first, dup := seenName[pool.Name]; dup {
			p.addf("%s.name: duplicate pool name %q, already declared at pools[%d]", at, pool.Name, first)
		} else {
			seenName[pool.Name] = i
		}

		validateStrategy(p, at, pool)
		validateUpstreams(p, at, pool.Upstreams)
		validateActiveHealth(p, at+".health.active", &pool.Health.Active)
		validatePassiveHealth(p, at+".health.passive", &pool.Health.Passive)
		validateBreaker(p, at+".breaker", &pool.Breaker)
		validateLimits(p, at+".limits", &pool.Limits)
		validateRetry(p, at+".retry", &pool.Retry)
		validateTransport(p, at+".transport", &pool.Transport)
	}
}

func validateStrategy(p *problems, at string, pool *PoolConfig) {
	switch pool.Strategy {
	case StrategyRoundRobin, StrategyLeastConn, StrategyConsistentHash:
	default:
		p.addf("%s.strategy: must be one of %q, %q, %q, got %q",
			at, StrategyRoundRobin, StrategyLeastConn, StrategyConsistentHash, pool.Strategy)
	}

	if pool.Strategy == StrategyConsistentHash {
		switch {
		case pool.HashOn == "":
			p.addf("%s.hash_on: required when strategy is %q", at, StrategyConsistentHash)
		case !validHashOn(pool.HashOn):
			p.addf("%s.hash_on: must be client_ip, path, header:NAME or cookie:NAME, got %q", at, pool.HashOn)
		}
		return
	}

	// A hash_on on a non-hashing strategy would be silently ignored, which
	// hides a real misconfiguration behind working-looking traffic.
	if pool.HashOn != "" {
		p.addf("%s.hash_on: only valid when strategy is %q, but strategy is %q",
			at, StrategyConsistentHash, pool.Strategy)
	}
}

func validHashOn(s string) bool {
	switch s {
	case "client_ip", "path":
		return true
	}
	if name, ok := strings.CutPrefix(s, "header:"); ok {
		return name != ""
	}
	if name, ok := strings.CutPrefix(s, "cookie:"); ok {
		return name != ""
	}
	return false
}

func validateUpstreams(p *problems, at string, ups []UpstreamConfig) {
	if len(ups) == 0 {
		p.addf("%s.upstreams: at least one upstream is required", at)
		return
	}

	seenURL := make(map[string]int, len(ups))
	for i := range ups {
		u := &ups[i]
		uat := fmt.Sprintf("%s.upstreams[%d]", at, i)

		if u.Weight < 0 {
			p.addf("%s.weight: must not be negative, got %d", uat, u.Weight)
		}

		if u.URL == "" {
			p.addf("%s.url: required", uat)
			continue
		}
		parsed, err := url.Parse(u.URL)
		if err != nil {
			p.addf("%s.url: not a valid URL %q: %v", uat, u.URL, err)
			continue
		}
		switch parsed.Scheme {
		case "http", "https":
		default:
			p.addf("%s.url: scheme must be http or https, got %q", uat, parsed.Scheme)
		}
		if parsed.Host == "" {
			p.addf("%s.url: missing host in %q", uat, u.URL)
		}
		// An upstream names an origin; routing decides the path. Accepting a
		// path here would make it ambiguous whose path wins at dispatch.
		if parsed.Path != "" {
			p.addf("%s.url: must not contain a path, got %q", uat, parsed.Path)
		}
		if parsed.RawQuery != "" {
			p.addf("%s.url: must not contain a query, got %q", uat, parsed.RawQuery)
		}
		if parsed.Fragment != "" {
			p.addf("%s.url: must not contain a fragment, got %q", uat, parsed.Fragment)
		}

		// Duplicate detection runs on the canonical origin, not the literal
		// string, and must match how upstream.NewPool canonicalises: scheme and
		// host lower-cased. Comparing raw strings here let
		// "http://x:9001" and "HTTP://x:9001" validate clean and then fail at
		// pool construction, so `manifold -check` reported ok for a config that
		// could not start -- exactly the failure -check exists to prevent.
		//
		// The two normalisations are duplicated rather than shared because
		// upstream imports config; the reverse would be an import cycle. If one
		// changes, the other must.
		canon := strings.ToLower(parsed.Scheme) + "://" + strings.ToLower(parsed.Host)
		if first, dup := seenURL[canon]; dup {
			p.addf("%s.url: duplicate upstream %q, already declared at %s.upstreams[%d] (compared as %q)", uat, u.URL, at, first, canon)
		} else {
			seenURL[canon] = i
		}
	}
}

func validateActiveHealth(p *problems, at string, a *ActiveHealthConfig) {
	if !a.Enabled {
		return
	}
	if !strings.HasPrefix(a.Path, "/") {
		p.addf("%s.path: must start with a slash, got %q", at, a.Path)
	}
	// A probe is a liveness check, not a request. Anything that can carry a
	// body would mutate the backend on every interval tick.
	if !probeMethods[a.Method] {
		p.addf("%s.method: must be GET, HEAD or OPTIONS, got %q", at, a.Method)
	}
	if a.Interval <= 0 {
		p.addf("%s.interval: must be > 0, got %s", at, a.Interval)
	}
	if a.Timeout <= 0 {
		p.addf("%s.timeout: must be > 0, got %s", at, a.Timeout)
	} else if a.Interval > 0 && a.Timeout >= a.Interval {
		// A probe that can outlive its own period stacks up on a slow backend.
		p.addf("%s.timeout: must be less than interval (%s), got %s", at, a.Interval, a.Timeout)
	}
	if a.HealthyThreshold < 1 {
		p.addf("%s.healthy_threshold: must be >= 1, got %d", at, a.HealthyThreshold)
	}
	if a.UnhealthyThreshold < 1 {
		p.addf("%s.unhealthy_threshold: must be >= 1, got %d", at, a.UnhealthyThreshold)
	}
	for i, code := range a.ExpectStatus {
		if code < 100 || code > 599 {
			p.addf("%s.expect_status[%d]: must be a status code in [100,599], got %d", at, i, code)
		}
	}
}

func validatePassiveHealth(p *problems, at string, pa *PassiveHealthConfig) {
	if !pa.Enabled {
		return
	}
	if pa.Window <= 0 {
		p.addf("%s.window: must be > 0, got %s", at, pa.Window)
	}
	if pa.MinRequests < 1 {
		p.addf("%s.min_requests: must be >= 1, got %d", at, pa.MinRequests)
	}
	if pa.ErrorRate <= 0 || pa.ErrorRate > 1 {
		p.addf("%s.error_rate: must be in (0,1], got %v", at, pa.ErrorRate)
	}
	if pa.EjectFor <= 0 {
		p.addf("%s.eject_for: must be > 0, got %s", at, pa.EjectFor)
	}
}

func validateBreaker(p *problems, at string, b *BreakerConfig) {
	if !b.Enabled {
		return
	}
	if b.FailureThreshold < 1 {
		p.addf("%s.failure_threshold: must be >= 1, got %d", at, b.FailureThreshold)
	}
	if b.OpenFor <= 0 {
		p.addf("%s.open_for: must be > 0, got %s", at, b.OpenFor)
	}
	if b.HalfOpenProbes < 1 {
		p.addf("%s.half_open_probes: must be >= 1, got %d", at, b.HalfOpenProbes)
	}
}

// probeMethods are the methods an active health check may use.
var probeMethods = map[string]bool{"GET": true, "HEAD": true, "OPTIONS": true}

// validateLimits allows 0: written explicitly it means unlimited, and
// presence-aware defaulting keeps that distinct from an omitted key.
func validateLimits(p *problems, at string, l *LimitConfig) {
	if l.MaxInFlight < 0 {
		p.addf("%s.max_in_flight: must be >= 0, got %d", at, l.MaxInFlight)
	}
	if l.QueueTimeout < 0 {
		p.addf("%s.queue_timeout: must be >= 0, got %s", at, l.QueueTimeout)
	}
}

func validateRetry(p *problems, at string, r *RetryConfig) {
	if r.MaxAttempts < 1 || r.MaxAttempts > 5 {
		p.addf("%s.max_attempts: must be in [1,5], got %d", at, r.MaxAttempts)
	}
	if r.PerTryTimeout < 0 {
		p.addf("%s.per_try_timeout: must be >= 0, got %s", at, r.PerTryTimeout)
	}
}

func validateTransport(p *problems, at string, t *TransportConfig) {
	if t.MaxIdleConns < 0 {
		p.addf("%s.max_idle_conns: must be >= 0, got %d", at, t.MaxIdleConns)
	}
	if t.MaxIdleConnsPerHost < 0 {
		p.addf("%s.max_idle_conns_per_host: must be >= 0, got %d", at, t.MaxIdleConnsPerHost)
	}
	if t.MaxConnsPerHost < 0 {
		p.addf("%s.max_conns_per_host: must be >= 0, got %d", at, t.MaxConnsPerHost)
	}
	if t.IdleConnTimeout <= 0 {
		p.addf("%s.idle_conn_timeout: must be > 0, got %s", at, t.IdleConnTimeout)
	}
	if t.DialTimeout <= 0 {
		p.addf("%s.dial_timeout: must be > 0, got %s", at, t.DialTimeout)
	}
	if t.ResponseHeaderTimeout < 0 {
		p.addf("%s.response_header_timeout: must be >= 0, got %s", at, t.ResponseHeaderTimeout)
	}
}

// httpMethods is the set of methods a route may name. Matching is
// case-sensitive: HTTP method tokens are uppercase, and accepting "get" here
// would mean the router silently never matched a real request.
var httpMethods = map[string]bool{
	"GET": true, "HEAD": true, "POST": true, "PUT": true, "PATCH": true,
	"DELETE": true, "CONNECT": true, "OPTIONS": true, "TRACE": true,
}

func (c *Config) validateRoutes(p *problems) {
	if len(c.Routes) == 0 {
		p.addf("routes: at least one route is required")
	}

	declared := make(map[string]bool, len(c.Pools))
	for i := range c.Pools {
		if c.Pools[i].Name != "" {
			declared[c.Pools[i].Name] = true
		}
	}

	referenced := make(map[string]bool, len(c.Pools))
	for i := range c.Routes {
		r := &c.Routes[i]
		at := fmt.Sprintf("routes[%d]", i)

		switch {
		case r.Pool == "":
			p.addf("%s.pool: required", at)
		case !declared[r.Pool]:
			p.addf("%s.pool: no pool named %q is declared", at, r.Pool)
		default:
			referenced[r.Pool] = true
		}

		if !strings.HasPrefix(r.Match.PathPrefix, "/") {
			p.addf("%s.match.path_prefix: must start with a slash, got %q", at, r.Match.PathPrefix)
		}
		for j, m := range r.Match.Methods {
			if !httpMethods[m] {
				p.addf("%s.match.methods[%d]: not a valid uppercase HTTP method, got %q", at, j, m)
			}
		}
	}

	// A pool no route can reach is dead config: it looks like capacity but
	// never receives a request.
	for i := range c.Pools {
		name := c.Pools[i].Name
		if name == "" || referenced[name] {
			continue
		}
		p.addf("pools[%d].name: pool %q is declared but no route references it", i, name)
	}
}
