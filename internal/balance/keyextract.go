package balance

import (
	"fmt"
	"net"
	"net/http"
	"strings"
)

// KeyExtractor pulls the value ConsistentHash keys on out of an inbound
// request. The proxy builds one per pool from its configured hash_on and
// calls it once per request to fill Request.HashKey.
type KeyExtractor func(*http.Request) string

// NewKeyExtractor builds the extractor for hashOn, which must already be one
// of the forms config validation accepts (see internal/config/validate.go's
// validHashOn): "client_ip", "path", "header:NAME" or "cookie:NAME". An empty
// hashOn is valid too — it is what every non-hashing pool has — and yields an
// extractor that always returns "".
func NewKeyExtractor(hashOn string) (KeyExtractor, error) {
	switch {
	case hashOn == "":
		return func(*http.Request) string { return "" }, nil

	case hashOn == "client_ip":
		return clientIPKey, nil

	case hashOn == "path":
		return func(r *http.Request) string { return r.URL.Path }, nil

	case strings.HasPrefix(hashOn, "header:"):
		name := strings.TrimPrefix(hashOn, "header:")
		if name == "" {
			return nil, fmt.Errorf("balance: invalid hash_on %q", hashOn)
		}
		return func(r *http.Request) string { return r.Header.Get(name) }, nil

	case strings.HasPrefix(hashOn, "cookie:"):
		name := strings.TrimPrefix(hashOn, "cookie:")
		if name == "" {
			return nil, fmt.Errorf("balance: invalid hash_on %q", hashOn)
		}
		return func(r *http.Request) string {
			ck, err := r.Cookie(name)
			if err != nil {
				// No such cookie on this request: yield "" rather than
				// erroring, same as a missing header. ConsistentHash.Pick
				// documents what happens to a "" key.
				return ""
			}
			return ck.Value
		}, nil

	default:
		return nil, fmt.Errorf("balance: unknown hash_on form %q", hashOn)
	}
}

// clientIPKey extracts the host portion of r.RemoteAddr.
//
// Known gap: when server.trust_forwarded_for is enabled, the correct source
// for the real client IP is the left-most entry of X-Forwarded-For, not
// RemoteAddr (which is the last hop — e.g. a load balancer or proxy in front
// of us). We deliberately do not read that header here: X-Forwarded-For is
// client-supplied and unauthenticated, so honouring it for hashing would let
// any client forge a chosen session-affinity key by setting the header
// themselves. Fixing this properly needs the same trusted-hop validation the
// proxy already applies to TrustForwardedFor, threaded into extractor
// construction — until then, client_ip hashing behind a trusted proxy will
// hash the proxy's address rather than the real client's.
func clientIPKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		// RemoteAddr without a port (uncommon, but not impossible for some
		// test/dial setups) — fall back to using it verbatim.
		return r.RemoteAddr
	}
	return host
}
