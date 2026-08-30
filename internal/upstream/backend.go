// Package upstream owns the runtime representation of configured backends:
// the per-pool connection transport, the stable identity of each origin, and
// the in-flight accounting the balancing strategies read.
//
// The split against package balance is deliberate. balance decides *which*
// candidate to use given a snapshot; upstream owns the mutable state that
// snapshot is taken from. Nothing here knows about HTTP routing, and nothing
// in balance knows about sockets.
package upstream

import (
	"net/url"
	"sync/atomic"
)

// Backend is one configured origin.
//
// It carries an atomic counter, so a Backend must never be copied — pools hand
// out *Backend everywhere and the zero value is not useful on its own.
type Backend struct {
	id     int
	url    *url.URL
	key    string
	weight int

	// inFlight is the number of requests currently outstanding to this
	// origin. Read by least_conn (Week 2) and by tests asserting that the
	// proxy's Acquire/Release pairing is leak-free.
	inFlight atomic.Int64

	// avail is the health/ejection state read by Available. It is mutated
	// only through Pool, which owns the generation counter — see
	// availability.go.
	avail availabilityState
}

// ID is the backend's stable index within its pool. balance.Candidate.ID
// round-trips through the strategies, and Pool.Backend maps it back.
func (b *Backend) ID() int { return b.id }

// URL returns the origin URL. The returned pointer is shared and must be
// treated as read-only: it is handed to every request that targets this
// backend, so mutating it would race.
func (b *Backend) URL() *url.URL { return b.url }

// Key is the canonical URL string for this origin. It is the identity used by
// consistent hashing ring placement and by the X-Manifold-Upstream response
// header, so it must stay stable across config reloads for a backend that is
// logically the same machine. Canonicalisation is scheme+host lowercasing
// only; anything more (default-port elision, DNS resolution) would make the
// key depend on state outside the config file.
func (b *Backend) Key() string { return b.key }

// InFlight reports the current outstanding request count. It is a sampled
// value: by the time a caller acts on it another goroutine may have changed
// it. That is intentional — serialising the count would put a mutex on the
// hottest path in the proxy for information that is advisory anyway.
func (b *Backend) InFlight() int64 { return b.inFlight.Load() }

// Acquire records the start of a request to this backend.
func (b *Backend) Acquire() { b.inFlight.Add(1) }

// Release records its completion. Callers must pair it with Acquire in a
// defer so that a panic in the proxy cannot strand the count.
func (b *Backend) Release() { b.inFlight.Add(-1) }
