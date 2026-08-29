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

	"github.com/aaditya0602/manifold/internal/balance"
	"github.com/aaditya0602/manifold/internal/config"
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
// It is immutable after New. A config reload constructs a second Server and
// swaps it in; nothing here is mutated under traffic, so ServeHTTP takes no
// locks.
type Server struct {
	reg     *upstream.Registry
	routes  []route
	proxies map[string]*httputil.ReverseProxy
}

// New builds a Server from validated config. It fails rather than starting
// degraded: an unimplemented balancing strategy, an unparseable upstream URL,
// or a route naming a pool that does not exist are all startup errors.
func New(cfg *config.Config) (*Server, error) {
	if cfg == nil {
		return nil, errors.New("proxy: nil config")
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

	s := &Server{
		reg:     reg,
		routes:  routes,
		proxies: make(map[string]*httputil.ReverseProxy, len(cfg.Pools)),
	}
	for _, p := range reg.Pools() {
		s.proxies[p.Name()] = newReverseProxy(p, cfg.Server.TrustForwardedFor)
	}
	return s, nil
}

// Close releases the pools' idle connections.
func (s *Server) Close() { s.reg.Close() }

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
		http.Error(w, "no route matched", http.StatusNotFound)
		return
	}

	pool := rt.pool
	rp := s.proxies[pool.Name()]
	retryCfg := pool.Config().Retry

	maxAttempts := retryCfg.MaxAttempts
	if maxAttempts < 1 {
		// Defaulting guarantees >= 1; belt and braces so a hand-built Config
		// in a test cannot produce a zero-attempt request that hangs.
		maxAttempts = 1
	}

	cw := &clientWriter{ResponseWriter: w}

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

		b := pool.Backend(c.ID)
		if b == nil {
			// A strategy returned an ID outside the candidate set. Degrade to
			// 503 rather than panicking the whole process.
			http.Error(cw, "no available upstream", http.StatusServiceUnavailable)
			return
		}

		st := &attemptState{target: b.URL(), key: b.Key()}
		lastErr = s.attempt(cw, r, rp, b, st, retryCfg)
		if lastErr == nil {
			return
		}
		tried = append(tried, c.ID)

		if !canRetry(cw, r, lastErr, retryCfg, attempt, maxAttempts) {
			break
		}
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
		rp.ServeHTTP(cw, r.WithContext(ctx))
	}()

	return st.err
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
