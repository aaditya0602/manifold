// Package limit bounds the number of requests manifold has in flight for one
// pool, so that overload sheds rather than queues.
//
// The distinction is the entire reason the package exists. A proxy with no
// bound does not refuse work under overload; it accepts it and buffers it, in
// goroutines, in connection pools, in kernel socket buffers. Latency climbs
// until every client has timed out, the backend is still being fed requests
// nobody is waiting for any more, and the proxy dies of memory rather than of
// load. Shedding at a known concurrency turns that into a crisp, survivable
// failure: a bounded number of requests are served at normal latency and the
// rest get an immediate 503 with a Retry-After, which is a response a client
// or an edge can actually act on.
//
// Two mechanisms, chosen by configuration, because they have genuinely
// different costs:
//
//   - queue_timeout == 0 (the default) is pure shedding. It is an atomic
//     counter and nothing else: no channel, no timer, no goroutine parking.
//     This is the hot path and it is the one that must be fastest.
//   - queue_timeout > 0 admits a bounded wait, which needs a real blocking
//     primitive. A buffered channel is the whole implementation: its buffer
//     length *is* the in-flight count, its send blocks exactly when the pool
//     is full, and select gives the timeout and context cancellation for free.
//
// max_in_flight == 0 means unlimited, and unlimited must be free -- not "one
// cheap atomic", actually free. An operator who turns the feature off should
// not be able to measure that it was ever there.
package limit

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/aaditya0602/manifold/internal/config"
)

// Limiter bounds concurrent in-flight requests for one pool. It is safe for
// concurrent use and must not be copied after first use.
type Limiter struct {
	// unlimited is a plain bool checked first in every method. Not an atomic,
	// not a nil-pointer test on the Limiter itself: the caller holds a
	// non-nil *Limiter unconditionally so the request path has one shape, and
	// this field is what makes the unlimited configuration cost a single
	// predictable-branch load.
	unlimited bool

	max int64

	// queueTimeout is how long Acquire may wait for a slot. Zero means shed
	// immediately, and in that configuration queue is nil.
	queueTimeout time.Duration

	// queue is the semaphore for the waiting configuration: capacity
	// max_in_flight, one token per in-flight request. It is nil when
	// queue_timeout is 0, and that nil-ness is what selects the fast path.
	queue chan struct{}

	// inFlight is the counter for the shed-immediately configuration. It is
	// unused when queue is non-nil, because len(queue) is already an exact,
	// free answer to the same question.
	inFlight atomic.Int64
}

// New builds a Limiter from a pool's validated limit configuration.
func New(cfg config.LimitConfig) *Limiter {
	if cfg.MaxInFlight <= 0 {
		// 0 is "unlimited", written explicitly by the operator; config's
		// presence-aware defaulting keeps it distinct from an omitted key.
		// Negative is rejected by validation and treated the same here.
		return &Limiter{unlimited: true}
	}

	l := &Limiter{
		max:          int64(cfg.MaxInFlight),
		queueTimeout: cfg.QueueTimeout.D(),
	}
	if l.queueTimeout > 0 {
		l.queue = make(chan struct{}, cfg.MaxInFlight)
	}
	return l
}

// Acquire takes an in-flight slot, reporting false when the request must be
// shed. A true return must be paired with exactly one Release, which is why
// every caller does it in a defer.
//
// ctx is the inbound request's context. It matters only in the waiting
// configuration, and there it matters a great deal: a client that has already
// hung up must not keep occupying a queue slot that a live client could use.
// Waiting on a dead request is worse than useless -- it converts a client
// disconnect into reduced capacity for everybody else.
func (l *Limiter) Acquire(ctx context.Context) bool {
	if l.unlimited {
		return true
	}
	if l.queue == nil {
		return l.acquireOrShed()
	}
	return l.acquireOrWait(ctx)
}

// acquireOrShed is the default path: a load and a CAS, no allocation.
//
// The obvious cheaper alternative is a single fetch-and-add with a rollback
// when it overshoots -- one atomic instead of two, and it never spins. It is
// measurably faster: 9.0 ns/op serial and 78 ns/op with eight cores on one
// counter, against 12.3 and 173 for the loop below.
//
// It is still not what is used here, because it makes the counter transiently
// exceed max_in_flight. The set of requests actually admitted is identical
// either way -- the overshooters roll back and shed -- but the counter is the
// number InFlight reports and the number the bound is supposed to guarantee,
// and a limiter whose own accounting briefly reads higher than the configured
// maximum cannot be tested for the property it exists to provide. "Wrong only
// by the number of racing goroutines" is not a bound.
//
// The price is affordable with room to spare: 173 ns/op is the figure for
// eight cores doing nothing whatsoever except contending on this one cache
// line, which is about 5.8M admissions per second against a proxy that serves
// 13.7k requests per second. Real traffic spends microseconds between Acquire
// and Release, so the line is not contended anywhere near continuously.
func (l *Limiter) acquireOrShed() bool {
	for {
		cur := l.inFlight.Load()
		if cur >= l.max {
			return false
		}
		if l.inFlight.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// acquireOrWait is the queue_timeout > 0 path.
func (l *Limiter) acquireOrWait(ctx context.Context) bool {
	// Try without blocking first. When the pool is not saturated -- which is
	// the normal condition even for a limiter that is configured to wait --
	// this returns without constructing a timer, and a timer is an allocation
	// and a runtime heap operation per request.
	select {
	case l.queue <- struct{}{}:
		return true
	default:
	}

	// Saturated. Check the client is still there before parking: a request
	// whose context is already dead should shed now and free the slot it would
	// otherwise have been handed.
	if ctx.Err() != nil {
		return false
	}

	t := time.NewTimer(l.queueTimeout)
	defer t.Stop()

	select {
	case l.queue <- struct{}{}:
		return true
	case <-t.C:
		return false
	case <-ctx.Done():
		return false
	}
}

// Release returns a slot taken by a successful Acquire. It is safe in a defer,
// including when the deferred call runs because ReverseProxy panicked with
// http.ErrAbortHandler on a client that vanished mid-body.
//
// Calling Release without a matching successful Acquire is a caller bug. It is
// clamped rather than trusted: the counter is what the bound is made of, and
// letting it drift negative would silently raise max_in_flight for the rest of
// the process's life -- a bug that only manifests as an outage months later,
// under the exact load the limiter was installed to survive.
func (l *Limiter) Release() {
	if l.unlimited {
		return
	}
	if l.queue != nil {
		select {
		case <-l.queue:
		default:
			// Unpaired Release. Draining nothing is the safe response;
			// blocking here would deadlock the request that made the mistake.
		}
		return
	}
	if l.inFlight.Add(-1) < 0 {
		l.inFlight.Add(1)
	}
}

// InFlight reports the slots currently held.
//
// In the waiting configuration this is len(queue), which is exact and costs
// nothing extra -- the channel already maintains it, so mirroring it in a
// second counter would be two more atomics per request to reproduce a number
// that is already correct. In the shedding configuration it is the counter
// itself.
//
// Like upstream.Backend.InFlight this is a sampled value: another goroutine
// may change it before the caller acts. It is for metrics and tests, never for
// an admission decision -- Acquire is the only thing allowed to make one.
func (l *Limiter) InFlight() int64 {
	if l.unlimited {
		return 0
	}
	if l.queue != nil {
		return int64(len(l.queue))
	}
	return l.inFlight.Load()
}

// Max is the configured bound, or 0 for unlimited. It is read once at startup
// to publish manifold_inflight_limit.
func (l *Limiter) Max() int64 {
	if l.unlimited {
		return 0
	}
	return l.max
}
