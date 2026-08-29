package proxy

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/aaditya0602/manifold/internal/config"
	"github.com/aaditya0602/manifold/internal/upstream"
)

// route is a compiled config.RouteConfig. Everything that can be normalised
// once — lowercasing the host, uppercasing methods, canonicalising header
// names — is done at construction, so matching a request is pure comparison
// with no allocation.
type route struct {
	pool *upstream.Pool

	// host is lowercase and port-free; "" matches any host.
	host string
	// pathPrefix is "" only when it matches everything.
	pathPrefix string
	// methods are uppercase; nil matches any method.
	methods []string
	// headerNames and headerValues are parallel slices rather than a map: a
	// route rarely constrains more than one or two headers, and a linear walk
	// over a two-element slice beats hashing the key.
	headerNames  []string
	headerValues []string
}

// compileRoutes resolves each route's pool reference and normalises its match
// criteria. An unknown pool name is a startup error: a route pointing at
// nothing would 404 (or 503) a slice of production traffic while the process
// reported itself healthy.
func compileRoutes(cfg *config.Config, reg *upstream.Registry) ([]route, error) {
	routes := make([]route, 0, len(cfg.Routes))
	for i := range cfg.Routes {
		rc := &cfg.Routes[i]
		p, ok := reg.Pool(rc.Pool)
		if !ok {
			return nil, fmt.Errorf("proxy: route %d references unknown pool %q", i, rc.Pool)
		}

		r := route{
			pool: p,
			host: strings.ToLower(stripPort(rc.Match.Host)),
		}
		// "/" constrains nothing, so drop it and skip the HasPrefix call per
		// request. Defaulting already rewrites an omitted prefix to "/".
		if pp := rc.Match.PathPrefix; pp != "" && pp != "/" {
			r.pathPrefix = pp
		}
		for _, m := range rc.Match.Methods {
			r.methods = append(r.methods, strings.ToUpper(strings.TrimSpace(m)))
		}
		for name, val := range rc.Match.Headers {
			r.headerNames = append(r.headerNames, http.CanonicalHeaderKey(name))
			r.headerValues = append(r.headerValues, val)
		}
		routes = append(routes, r)
	}
	return routes, nil
}

// match reports whether req satisfies every set field of the route. An unset
// field is not a wildcard that matches "anything including absent" — it is
// simply not tested, which is the AND-over-set-fields semantics MatchConfig
// documents.
func (r *route) match(req *http.Request, host string) bool {
	if r.host != "" && r.host != host {
		return false
	}
	if r.pathPrefix != "" && !strings.HasPrefix(req.URL.Path, r.pathPrefix) {
		return false
	}
	if len(r.methods) > 0 {
		found := false
		for _, m := range r.methods {
			if m == req.Method {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	for i, name := range r.headerNames {
		// Header.Get returns the first value; a repeated header with a
		// matching second value does not match. Exact equality on the first
		// value is what MatchConfig specifies and is the predictable choice.
		if req.Header.Get(name) != r.headerValues[i] {
			return false
		}
	}
	return true
}

// lookup returns the first matching route. Order is config order and the first
// match wins, so a specific route must be declared above a general one — the
// same rule as an nginx location block list, and the reason routes are a slice
// and not a map.
func (s *Server) lookup(req *http.Request) *route {
	host := strings.ToLower(stripPort(req.Host))
	for i := range s.routes {
		if s.routes[i].match(req, host) {
			return &s.routes[i]
		}
	}
	return nil
}

// stripPort removes a trailing :port from a Host header value, leaving IPv6
// literals intact. net.SplitHostPort is not used: it errors on the common
// "example.com" (no port) case, and this runs on every request.
func stripPort(host string) string {
	if host == "" {
		return ""
	}
	if strings.HasPrefix(host, "[") {
		// IPv6 literal: "[::1]:8080" -> "[::1]".
		if end := strings.LastIndex(host, "]"); end >= 0 {
			return host[:end+1]
		}
		return host
	}
	i := strings.LastIndex(host, ":")
	if i < 0 {
		return host
	}
	return host[:i]
}
