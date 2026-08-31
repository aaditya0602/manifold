// Package breaker is manifold's per-upstream circuit breaker.
//
// The breaker exists to stop manifold paying for a backend's failure twice.
// Passive health (internal/health) already ejects a backend whose error rate
// crosses a threshold, but ejection is a pool-level, window-based judgement
// that takes min_requests samples to form. The breaker is the fast, local
// complement: a handful of consecutive failures and this upstream stops being
// dialled at all, immediately, with no window to fill and no sweeper to wait
// for.
//
// The point that decides the whole design is that an open breaker must reject
// *without touching the network*. A "breaker" that still pays a dial timeout
// before giving up has saved nothing -- it has merely moved where the two
// seconds are spent. So Allow is a pure in-process decision: one plain bool
// load when the breaker is disabled, one atomic load when it is closed.
//
// # Everything is one 64-bit word
//
// State, both counters, and the open deadline are packed into a single
// atomic.Uint64 and moved with CAS. That is not micro-optimisation for its own
// sake; it is what makes the state machine correct under concurrency.
//
// The alternative -- a state word plus a separate atomic deadline -- has a real
// race with no clean fix. Opening the breaker has to publish two facts, "the
// state is now open" and "it stays open until T". Whichever is published first
// leaves a window: publish the deadline first and a stale writer from an
// earlier open cycle can clobber the newer one; publish the state first and a
// concurrent Allow reads Open together with the *previous* cycle's expired
// deadline and immediately promotes to half-open -- re-hammering a backend that
// has just been declared sick, which is the one outcome this package exists to
// prevent. Packing both into one word makes the transition a single atomic
// publication, so neither window exists.
//
// The deadline doubles as the generation counter the CAS loops would otherwise
// need: it is monotonic in real time, so an open -> half-open -> open cycle
// never reproduces an earlier word.
package breaker

import (
	"sync/atomic"
	"time"

	"github.com/aaditya0602/manifold/internal/config"
)

// State is the breaker's position in the closed -> open -> half-open cycle.
//
// The numeric values are part of the contract: they are exported as
// manifold_breaker_state and are the index into observe's pre-resolved
// transition counters, so they must not be reordered.
type State int32

// Breaker states.
const (
	// Closed is the healthy state: every request is allowed through and
	// consecutive failures are counted.
	Closed State = iota
	// Open rejects everything until the deadline set when it was entered.
	Open
	// HalfOpen admits a bounded number of concurrent probes. All must succeed
	// to close; any failure re-opens.
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// Word layout, low bits first:
//
//	 1..0  state
//	12..2  counter a -- consecutive failures (closed) or admitted probes (half-open)
//	23..13 counter b -- succeeded probes (half-open)
//	63..24 deadline -- milliseconds since the breaker's epoch
//
// Eleven bits per counter caps both at 2047. failure_threshold and
// half_open_probes are single-digit in any configuration that makes sense
// (the defaults are 5 and 1), and New clamps anything larger, so the cap is
// reached only by a config that was already nonsense.
const (
	stateBits    = 2
	countBits    = 11
	deadlineBits = 40

	aShift        = stateBits
	bShift        = stateBits + countBits
	deadlineShift = stateBits + 2*countBits

	stateMask    = 1<<stateBits - 1
	countMask    = 1<<countBits - 1
	deadlineMask = 1<<deadlineBits - 1

	// maxCount saturates either counter.
	maxCount = countMask
)

func pack(state, a, b, deadline uint64) uint64 {
	return deadline<<deadlineShift | b<<bShift | a<<aShift | state
}

func unpack(v uint64) (state, a, b, deadline uint64) {
	return v & stateMask,
		v >> aShift & countMask,
		v >> bShift & countMask,
		v >> deadlineShift & deadlineMask
}

// Breaker is one upstream's circuit breaker. It is safe for concurrent use and
// must not be copied after first use.
type Breaker struct {
	// word holds the entire state machine. See the layout comment above.
	word atomic.Uint64

	// enabled is a plain bool, not an atomic: a disabled breaker must cost a
	// single non-atomic load on the request path, so that turning the feature
	// off really does turn its cost off.
	enabled bool

	failureThreshold uint64
	halfOpenProbes   uint64
	openFor          time.Duration

	// now and epoch are the clock. The deadline field is milliseconds since
	// epoch rather than an absolute Unix time because 40 bits of absolute
	// nanoseconds would cover about 18 minutes.
	now   func() time.Time
	epoch time.Time

	// onTransition is notified after each state change. It is registered at
	// construction, before the breaker takes traffic, and is deliberately not
	// synchronised -- see OnTransition.
	onTransition func(State)
}

// New builds a breaker on the real clock.
func New(cfg config.BreakerConfig) *Breaker { return NewWithClock(cfg, time.Now) }

// NewWithClock is New with an injectable clock.
//
// State transitions here are timed, and a test that asserts on them by
// sleeping is a test that fails on a loaded CI machine and passes on the
// author's laptop. Injecting the clock makes "open_for has elapsed" an
// assertion about a value the test set, not a race against the scheduler.
func NewWithClock(cfg config.BreakerConfig, now func() time.Time) *Breaker {
	if now == nil {
		now = time.Now
	}
	b := &Breaker{now: now}
	if !cfg.Enabled {
		// Nothing else is initialised: a disabled breaker never reads any of
		// it, and leaving it zero makes that obvious.
		return b
	}

	// These clamps are defensive; config validation already rejects every one
	// of them. They exist because a hand-built config.BreakerConfig in a test
	// or in a future caller must not be able to produce a breaker that trips
	// on zero failures or admits an unbounded number of probes.
	ft := cfg.FailureThreshold
	if ft < 1 {
		ft = 1
	}
	if ft > maxCount {
		ft = maxCount
	}
	hp := cfg.HalfOpenProbes
	if hp < 1 {
		hp = 1
	}
	if hp > maxCount {
		hp = maxCount
	}
	of := cfg.OpenFor.D()
	if of <= 0 {
		of = time.Millisecond
	}

	b.enabled = true
	b.failureThreshold = uint64(ft)
	b.halfOpenProbes = uint64(hp)
	b.openFor = of
	b.epoch = now()
	return b
}

// Enabled reports whether this breaker does anything at all.
func (b *Breaker) Enabled() bool { return b.enabled }

// OnTransition registers a callback invoked after every state change, with the
// state moved to.
//
// It must be called during construction, before the breaker is reachable from
// any request. It is a plain field write with no synchronisation, because the
// alternative -- an atomic.Pointer loaded on every transition -- buys nothing:
// a hook registered after traffic has started would be a lifecycle bug either
// way, and transitions are rare enough that the load would be pure ceremony.
//
// The callback runs on whichever goroutine caused the transition, which on the
// Allow path is a goroutine serving a request. It must not block.
func (b *Breaker) OnTransition(fn func(State)) { b.onTransition = fn }

// Allow reports whether a request may be forwarded to this upstream. It is on
// the request hot path and is called once per attempt.
//
// In the half-open state a true return *consumes* one probe from the budget,
// so every true must be paired with exactly one Success or Failure. A caller
// that drops the outcome permanently shrinks the probe budget and can strand
// the breaker half-open -- which is why the proxy records the outcome from a
// defer rather than from the normal return path.
func (b *Breaker) Allow() bool {
	if !b.enabled {
		return true
	}
	cur := b.word.Load()
	if cur&stateMask == uint64(Closed) {
		return true
	}
	return b.allowSlow(cur)
}

// allowSlow handles the open and half-open states. It is split out so the
// closed path stays a load and a compare with no loop for the compiler to
// reason about.
func (b *Breaker) allowSlow(cur uint64) bool {
	for {
		state, admitted, succeeded, deadline := unpack(cur)
		switch State(state) {
		case Closed:
			return true

		case Open:
			if b.millis(b.now()) < deadline {
				return false
			}
			// open_for has elapsed. The goroutine that wins this CAS performs
			// the transition *and* takes the first probe in the same step.
			// Transitioning first and admitting afterwards would open a window
			// in which every one of the thousands of goroutines queued behind
			// a sick backend sees "half-open, budget free" -- which is exactly
			// the stampede the half-open state exists to prevent.
			if b.word.CompareAndSwap(cur, pack(uint64(HalfOpen), 1, 0, deadline)) {
				b.transitioned(HalfOpen)
				return true
			}

		default: // HalfOpen
			if admitted >= b.halfOpenProbes {
				return false
			}
			if b.word.CompareAndSwap(cur, pack(uint64(HalfOpen), admitted+1, succeeded, deadline)) {
				return true
			}
		}
		cur = b.word.Load()
	}
}

// Success records one successful outcome against this upstream.
func (b *Breaker) Success() {
	if !b.enabled {
		return
	}
	cur := b.word.Load()
	for {
		state, admitted, succeeded, deadline := unpack(cur)
		switch State(state) {
		case Closed:
			if admitted == 0 {
				// The overwhelmingly common case: a healthy breaker with no
				// failures to forget. One load, no write, so concurrent
				// successes never contend on the word at all.
				return
			}
			// A success resets the consecutive-failure run. "Consecutive" is
			// the whole semantic: five failures spread across a thousand
			// successful requests is a backend that is fine, and a breaker
			// that tripped on it would remove healthy capacity.
			if b.word.CompareAndSwap(cur, pack(uint64(Closed), 0, 0, deadline)) {
				return
			}

		case Open:
			// A late outcome from a request that was admitted before the
			// breaker opened. The open deadline is already running; folding
			// this in would either shorten it or corrupt the counters.
			return

		default: // HalfOpen
			succeeded++
			if succeeded >= b.halfOpenProbes {
				// Every probe came back good. Close, and clear the counters so
				// the backend starts its second life with a full
				// failure_threshold of credit rather than being one failure
				// from re-opening.
				if b.word.CompareAndSwap(cur, pack(uint64(Closed), 0, 0, deadline)) {
					b.transitioned(Closed)
					return
				}
				break
			}
			if b.word.CompareAndSwap(cur, pack(uint64(HalfOpen), admitted, succeeded, deadline)) {
				return
			}
		}
		cur = b.word.Load()
	}
}

// Failure records one failed outcome against this upstream. The classification
// of "failed" belongs to the caller; the proxy uses health.Failed, the same
// predicate the passive tracker uses, so a 4xx can never trip a breaker.
func (b *Breaker) Failure() {
	if !b.enabled {
		return
	}
	cur := b.word.Load()
	for {
		state, failures, _, deadline := unpack(cur)
		switch State(state) {
		case Closed:
			if failures+1 < b.failureThreshold {
				// The stale deadline is carried rather than zeroed. It is
				// never read while closed, but keeping it makes every word the
				// breaker has ever held distinct across open cycles, which is
				// what rules out a preempted goroutine's CAS succeeding
				// against a word that has since gone all the way around the
				// state machine and back.
				if b.word.CompareAndSwap(cur, pack(uint64(Closed), failures+1, 0, deadline)) {
					return
				}
				break
			}
			if b.open(cur) {
				return
			}

		case Open:
			// Already open; the deadline stands. Extending it on every late
			// outcome would let a burst of requests that were admitted before
			// the trip push recovery arbitrarily far out.
			return

		default: // HalfOpen
			// One bad probe is enough. The backend was given a chance and took
			// it badly, so the full open_for restarts from now -- not from
			// when the breaker first opened.
			if b.open(cur) {
				return
			}
		}
		cur = b.word.Load()
	}
}

// open transitions to Open with a fresh deadline, reporting whether the CAS
// won.
func (b *Breaker) open(cur uint64) bool {
	if !b.word.CompareAndSwap(cur, pack(uint64(Open), 0, 0, b.deadline(b.now()))) {
		return false
	}
	b.transitioned(Open)
	return true
}

// State reports the breaker's current state. It is read at scrape time for
// manifold_breaker_state and by tests; it is never consulted on the request
// path, where Allow is the only entry point.
//
// A breaker whose open_for has elapsed still reports Open until a request
// actually promotes it. That is deliberate and honest: nothing has probed the
// backend yet, so "open" is the true description of what the next observer
// will find.
func (b *Breaker) State() State {
	if !b.enabled {
		return Closed
	}
	return State(b.word.Load() & stateMask)
}

func (b *Breaker) transitioned(to State) {
	if b.onTransition != nil {
		b.onTransition(to)
	}
}

// millis is the current time in the deadline field's units.
//
// The field is 40 bits, so it wraps after about 35 years of process uptime.
// The consequence of a wrap is that a stored deadline compares as already
// elapsed and the breaker promotes to half-open one cycle early -- it degrades
// towards probing a backend, never towards refusing traffic forever, which is
// the right direction for the failure to fall.
func (b *Breaker) millis(t time.Time) uint64 {
	d := t.Sub(b.epoch)
	if d < 0 {
		// An injected clock that runs backwards, or a monotonic reading taken
		// before the epoch. Treat it as the epoch rather than wrapping into a
		// huge positive.
		return 0
	}
	return uint64(d/time.Millisecond) & deadlineMask
}

// deadline is now+open_for in the field's units, rounded *up*.
//
// Rounding up rather than truncating is what keeps a sub-millisecond open_for
// meaningful: truncation would produce a deadline equal to the current
// millisecond, so the breaker would open and be eligible for half-open in the
// same instant, and the open state would never actually reject anything.
func (b *Breaker) deadline(t time.Time) uint64 {
	d := t.Sub(b.epoch) + b.openFor
	if d < 0 {
		d = 0
	}
	ms := (d + time.Millisecond - 1) / time.Millisecond
	return uint64(ms) & deadlineMask
}
