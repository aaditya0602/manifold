package upstream

import (
	"sync/atomic"
	"time"
)

// Availability is split across two independent signals, both owned here.
//
//   - activeHealthy is the out-of-band prober's verdict (internal/health).
//   - ejectedUntil is the passive tracker's verdict, driven by real traffic.
//
// They are deliberately separate rather than one tri-state: a backend that a
// prober has already marked unhealthy can still be ejected by passive traffic
// outcomes, and when the prober later declares it healthy the ejection must
// still hold until its deadline. Collapsing both into one flag would let
// whichever signal fired last silently overwrite the other.
//
// Every mutation goes through Pool, never through Backend, because Pool owns
// the generation counter and balance.Strategy requires gen to move on any
// change to the available set (see the contract on balance.Strategy.Pick).

// availabilityState is embedded in Backend. It is atomic so that Available()
// stays a lock-free read on the request hot path: Candidates calls it once per
// backend per request, and taking a mutex there would serialise every pick
// behind the prober's interval tick.
type availabilityState struct {
	// activeHealthy starts true. A backend is assumed good until a prober
	// says otherwise, so a boot against an unreachable target loses that
	// backend after unhealthy_threshold intervals rather than blackholing all
	// traffic from the first millisecond.
	activeHealthy atomic.Bool

	// ejectedUntilNanos is the passive ejection deadline as Unix nanoseconds,
	// or 0 for "not ejected". The deadline is stored but NOT enforced by
	// reads: Ejected does not compare against the clock. Expiry is an explicit
	// Readmit call from the tracker's sweeper, because an implicit time-based
	// expiry would change the available set without bumping gen, which is
	// exactly the stale-ring bug balance.Strategy warns about.
	ejectedUntilNanos atomic.Int64
}

// Available reports whether this backend may serve traffic right now.
func (b *Backend) Available() bool { return b.ActiveHealthy() && !b.Ejected() }

// ActiveHealthy is the active prober's verdict. It is true until a prober
// marks the backend down, so a pool with active health checks disabled is
// always active-healthy.
func (b *Backend) ActiveHealthy() bool { return b.avail.activeHealthy.Load() }

// Ejected reports whether passive health has taken this backend out of
// rotation. See availabilityState for why this is state, not a clock read.
func (b *Backend) Ejected() bool { return b.avail.ejectedUntilNanos.Load() != 0 }

// SetActiveHealthy records the active prober's verdict for one backend.
//
// It reports whether the stored flag changed, which is not the same question
// as whether availability changed: marking an already-ejected backend
// unhealthy changes the flag but leaves it unavailable either way. gen is
// bumped and subscribers are notified only on a genuine availability
// transition, so a steady stream of identical probe results does not
// invalidate every strategy's cached ring once per interval.
func (p *Pool) SetActiveHealthy(id int, healthy bool) bool {
	b := p.Backend(id)
	if b == nil {
		return false
	}

	p.availMu.Lock()
	if b.avail.activeHealthy.Load() == healthy {
		p.availMu.Unlock()
		return false
	}
	before := b.Available()
	b.avail.activeHealthy.Store(healthy)
	after := b.Available()
	if before != after {
		p.gen.Add(1)
	}
	p.availMu.Unlock()

	if before != after {
		p.notifyAvailability(b.key, after)
	}
	return true
}

// Eject takes a backend out of rotation until Readmit is called. until is
// recorded for the sweeper that will readmit it and for observability; it is
// never consulted by the read path.
//
// It reports whether the backend was newly ejected. Ejecting an
// already-ejected backend overwrites the deadline and returns false: there is
// no availability transition to announce, but the newer deadline should win so
// a backend that keeps failing is not readmitted on the original schedule.
func (p *Pool) Eject(id int, until time.Time) bool {
	b := p.Backend(id)
	if b == nil {
		return false
	}
	// 0 is the "not ejected" sentinel, so a deadline landing exactly on the
	// Unix epoch is nudged by a nanosecond rather than read back as a
	// readmission. Nothing real passes that value; the guard exists so a
	// zero-valued deadline cannot silently un-eject a backend.
	nanos := until.UnixNano()
	if nanos == 0 {
		nanos = 1
	}

	p.availMu.Lock()
	if b.avail.ejectedUntilNanos.Load() != 0 {
		b.avail.ejectedUntilNanos.Store(nanos)
		p.availMu.Unlock()
		return false
	}
	before := b.Available()
	b.avail.ejectedUntilNanos.Store(nanos)
	after := b.Available()
	if before != after {
		p.gen.Add(1)
	}
	p.availMu.Unlock()

	if before != after {
		p.notifyAvailability(b.key, after)
	}
	return true
}

// Readmit clears an ejection. It reports whether the backend was ejected. The
// backend only becomes available again if the active prober also considers it
// healthy.
func (p *Pool) Readmit(id int) bool {
	b := p.Backend(id)
	if b == nil {
		return false
	}

	p.availMu.Lock()
	if b.avail.ejectedUntilNanos.Load() == 0 {
		p.availMu.Unlock()
		return false
	}
	before := b.Available()
	b.avail.ejectedUntilNanos.Store(0)
	after := b.Available()
	if before != after {
		p.gen.Add(1)
	}
	p.availMu.Unlock()

	if before != after {
		p.notifyAvailability(b.key, after)
	}
	return true
}

// EjectedUntil is the deadline set by the last Eject, or the zero time when
// the backend is not ejected.
func (p *Pool) EjectedUntil(id int) time.Time {
	b := p.Backend(id)
	if b == nil {
		return time.Time{}
	}
	nanos := b.avail.ejectedUntilNanos.Load()
	if nanos == 0 {
		return time.Time{}
	}
	return time.Unix(0, nanos)
}

// availabilityHook is the subscriber signature for OnAvailabilityChange.
type availabilityHook = func(backendKey string, available bool)

// OnAvailabilityChange registers a callback invoked after a backend enters or
// leaves the available set. Multiple subscribers are supported and are called
// in registration order.
//
// This inversion exists so that internal/health imports nothing but upstream:
// health mutates state through Pool, and whoever wants to count the
// transitions subscribes here. Callbacks run synchronously on the goroutine
// that caused the transition but outside every Pool lock, so a callback may
// call back into the Pool without deadlocking. They must not block: a slow
// subscriber stalls a prober goroutine, or a request that was recording a
// passive outcome.
//
// Delivery is not serialised across concurrent transitions, so two backends
// flipping at the same instant may be reported in either order. Subscribers
// should treat each call as an edge for one backend key, not as a snapshot of
// the whole pool.
func (p *Pool) OnAvailabilityChange(fn func(backendKey string, available bool)) {
	if fn == nil {
		return
	}
	p.hookMu.Lock()
	defer p.hookMu.Unlock()

	// Copy-on-write: the notify path loads the slice with one atomic load and
	// never takes hookMu, so a callback is free to register another subscriber
	// without deadlocking against the notification it is handling.
	var next []availabilityHook
	if cur := p.hooks.Load(); cur != nil {
		next = make([]availabilityHook, 0, len(*cur)+1)
		next = append(next, *cur...)
	}
	next = append(next, fn)
	p.hooks.Store(&next)
}

// notifyAvailability fans a transition out to subscribers. It must only be
// called with no Pool lock held.
func (p *Pool) notifyAvailability(key string, available bool) {
	hooks := p.hooks.Load()
	if hooks == nil {
		return
	}
	for _, fn := range *hooks {
		fn(key, available)
	}
}
