// Package balance holds the upstream selection algorithms.
//
// Design decision: strategies are pure functions over a candidate snapshot.
// The proxy is responsible for deciding which upstreams are eligible (health,
// circuit breaker, ejection) and passes only those in. A strategy therefore
// never needs to know what "healthy" means, which keeps every algorithm
// independently testable with a plain slice and no live backends.
package balance

// Candidate is one eligible upstream, as seen by a strategy.
type Candidate struct {
	// ID is the upstream's stable index within its pool. The proxy maps it
	// back to a real backend; strategies only ever pass it through.
	ID int
	// Key is a stable identity string (the upstream URL). Consistent hashing
	// places ring points from it, so it must not change across reloads for an
	// upstream that is logically the same backend.
	Key string
	// Weight biases selection; always >= 1 after config defaulting.
	Weight int
	// InFlight is the number of requests currently outstanding to this
	// upstream. Only least_conn reads it. It is a sampled value, not a lock-
	// held one: a slightly stale count is acceptable and cheaper than
	// serialising every pick.
	InFlight int64
}

// Request carries the per-request inputs a strategy may need. Round-robin and
// least-connections ignore it entirely.
type Request struct {
	// HashKey is the value consistent hashing keys on, already extracted by
	// the proxy according to the pool's hash_on setting (client IP, a header,
	// a cookie, or the path). Empty when the pool does not hash.
	HashKey string
}

// Strategy picks one candidate per request.
//
// gen is a generation counter that the proxy increments whenever pool
// membership changes (config reload, ejection, readmission). Stateful
// strategies use it to cache derived structures — consistent hashing rebuilds
// its ring only when gen differs from the one it last saw — without having to
// diff the candidate slice on every request.
//
// The caller carries a hard obligation here: gen MUST change on *any* change
// to the candidate set, including one that leaves the length identical. If
// backend A is ejected and backend B readmitted in the same instant, the slice
// is still the same length, and a strategy that cached by generation will keep
// routing to A. Length is a cheap sanity check that some implementations also
// apply, but it is not a safe discriminator and must not be relied on as one.
// Bump the counter on every membership mutation, not only on resize.
//
// Implementations must be safe for concurrent use by many goroutines.
// Pick returns ok=false when candidates is empty; the proxy turns that into a
// 503 rather than picking a known-bad backend.
type Strategy interface {
	Name() string
	Pick(gen uint64, candidates []Candidate, req Request) (c Candidate, ok bool)
}
