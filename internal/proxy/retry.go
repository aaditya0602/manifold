package proxy

import (
	"context"
	"errors"
	"net"
	"net/http"

	"github.com/aaditya0602/manifold/internal/config"
)

// idempotentMethods are the methods RFC 9110 defines as idempotent: repeating
// them has the same effect as issuing them once. POST and PATCH are absent by
// design.
var idempotentMethods = map[string]bool{
	http.MethodGet:     true,
	http.MethodHead:    true,
	http.MethodPut:     true,
	http.MethodDelete:  true,
	http.MethodOptions: true,
	http.MethodTrace:   true,
}

// idempotencyKeyHeader lets a client opt a non-idempotent request into
// retries by promising the origin will de-duplicate on the key. This mirrors
// the Stripe/IETF `Idempotency-Key` convention.
const idempotencyKeyHeader = "Idempotency-Key"

// methodRetryable applies the idempotent_only policy.
func methodRetryable(r *http.Request, cfg config.RetryConfig) bool {
	if !cfg.IdempotentOnly {
		return true
	}
	if idempotentMethods[r.Method] {
		return true
	}
	return r.Header.Get(idempotencyKeyHeader) != ""
}

// bodyReplayable reports whether the request body can be sent a second time.
//
// Week 1 limitation, stated honestly: manifold only retries requests with no
// body. A request body is an io.ReadCloser that has already been consumed by
// the first attempt; replaying it requires buffering it in memory (or
// spilling to disk) up to some cap, plus a policy for what to do when a
// request exceeds that cap. That is real work with real memory-exhaustion
// consequences under load, and it is deliberately out of scope here rather
// than half-done. A POST with a body therefore gets exactly one attempt even
// when it carries an Idempotency-Key.
//
// http.NoBody and ContentLength == 0 are both checked: a client that sends no
// body at all yields ContentLength 0 with a non-nil sentinel body, and Go's
// own client uses http.NoBody for the same thing. ContentLength == -1
// (unknown length, chunked) is explicitly not replayable.
func bodyReplayable(r *http.Request) bool {
	if r.Body == nil || r.Body == http.NoBody {
		return true
	}
	return r.ContentLength == 0
}

// retryableError reports whether err is a connection-level failure, the only
// class of failure manifold retries.
//
// The critical exclusion is not visible here, because it does not need to be:
// a 5xx from a reachable backend is not an error at all at this layer, it is a
// successful RoundTrip that returned a response. That is exactly right. The
// backend accepted the request and did the work; re-sending it to a second
// backend converts one failed request into two units of load, and a pool that
// is 5xx-ing because it is overloaded gets an extra multiple of traffic at the
// precise moment it can least absorb it. Retrying 5xx is an outage amplifier,
// so manifold never does it.
//
// A cancelled inbound context is likewise not retryable: the client is gone.
func retryableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	// Everything that reaches here came out of Transport.RoundTrip, which
	// fails only before a response header is parsed: dial refused, dial
	// timeout, connection reset, TLS failure, or a response-header timeout.
	// In every one of those cases the request either never arrived or the
	// backend never got far enough to answer, so a different backend is a
	// legitimate second try.
	return true
}

// statusForError maps a terminal upstream failure to a client-facing status.
//
// The ordering matters and encodes a judgement call: an explicit deadline
// configured by the operator (per_try_timeout, response_header_timeout) is a
// 504, because the backend was reachable and simply too slow. A dial failure —
// including a dial that timed out against transport.dial_timeout — is a 502,
// because the backend was never reached at all and "gateway timeout" would
// mislead an operator into looking at backend latency instead of backend
// reachability.
func statusForError(err error) int {
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout
	}
	var oe *net.OpError
	if errors.As(err, &oe) && oe.Op == "dial" {
		return http.StatusBadGateway
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return http.StatusGatewayTimeout
	}
	return http.StatusBadGateway
}
