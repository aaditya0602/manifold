package breaker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaditya0602/manifold/internal/config"
)

// clock is a deterministic time source.
//
// Every assertion in this file is about a state transition that depends on
// elapsed time, and the alternative -- time.Sleep -- would make every one of
// them a race against the scheduler that passes locally and flakes on a loaded
// CI box. It stores nanoseconds in an atomic because the concurrency tests
// read it from hundreds of goroutines at once.
type clock struct{ nanos atomic.Int64 }

// base is a whole number of milliseconds so that the breaker's millisecond
// quantisation is exact in these tests and the assertions can be about the
// state machine rather than about rounding.
var base = time.Unix(1700000000, 0)

func newClock() *clock {
	c := &clock{}
	c.nanos.Store(base.UnixNano())
	return c
}

func (c *clock) now() time.Time           { return time.Unix(0, c.nanos.Load()) }
func (c *clock) advance(d time.Duration)  { c.nanos.Add(int64(d)) }
func (c *clock) set(offset time.Duration) { c.nanos.Store(base.Add(offset).UnixNano()) }
func (c *clock) at(offset time.Duration)  { c.set(offset) }

const openFor = 100 * time.Millisecond

func cfg(threshold, probes int) config.BreakerConfig {
	return config.BreakerConfig{
		Enabled:          true,
		FailureThreshold: threshold,
		OpenFor:          config.Duration(openFor),
		HalfOpenProbes:   probes,
	}
}

func newTestBreaker(threshold, probes int) (*Breaker, *clock) {
	c := newClock()
	return NewWithClock(cfg(threshold, probes), c.now), c
}

func mustState(t *testing.T, b *Breaker, want State) {
	t.Helper()
	if got := b.State(); got != want {
		t.Fatalf("state = %v, want %v", got, want)
	}
}

func mustAllow(t *testing.T, b *Breaker, want bool) {
	t.Helper()
	if got := b.Allow(); got != want {
		t.Fatalf("Allow() = %v, want %v (state %v)", got, want, b.State())
	}
}

// TestClosed_TripsOnExactThreshold is the boundary that decides how much
// evidence the breaker demands before it removes capacity. One failure short
// must leave the backend fully in service: an off-by-one here either makes the
// breaker hair-trigger (removing a healthy replica on a transient blip) or
// makes it one failure slower than configured, which is the difference between
// catching a bad deploy and not.
func TestClosed_TripsOnExactThreshold(t *testing.T) {
	const threshold = 5
	b, _ := newTestBreaker(threshold, 1)

	for i := 0; i < threshold-1; i++ {
		b.Failure()
		mustState(t, b, Closed)
		mustAllow(t, b, true)
	}

	b.Failure()
	mustState(t, b, Open)
	mustAllow(t, b, false)
}

// TestClosed_SuccessResetsRun encodes the word "consecutive". Five failures
// scattered through a thousand successes describe a backend that is fine; a
// breaker that summed them would eventually trip on every backend in a pool
// serving a normal error rate and take the whole service down by itself.
func TestClosed_SuccessResetsRun(t *testing.T) {
	const threshold = 3
	b, _ := newTestBreaker(threshold, 1)

	b.Failure()
	b.Failure()
	b.Success()

	// The run restarted, so the next two failures must not be enough.
	b.Failure()
	b.Failure()
	mustState(t, b, Closed)

	b.Failure()
	mustState(t, b, Open)
}

// TestOpen_RejectsUntilDeadline pins the one property that makes the breaker
// worth having: while open it refuses, and it refuses for exactly as long as
// it was told to.
func TestOpen_RejectsUntilDeadline(t *testing.T) {
	b, c := newTestBreaker(1, 1)

	b.Failure()
	mustState(t, b, Open)

	c.at(openFor - time.Millisecond)
	mustAllow(t, b, false)
	mustState(t, b, Open)

	c.at(openFor)
	mustAllow(t, b, true)
	mustState(t, b, HalfOpen)
}

// TestHalfOpen_ProbeBudgetIsExact covers the sequential case; the concurrent
// one below is the test that actually matters.
func TestHalfOpen_ProbeBudgetIsExact(t *testing.T) {
	const probes = 3
	b, c := newTestBreaker(1, probes)

	b.Failure()
	c.at(openFor)

	for i := 0; i < probes; i++ {
		mustAllow(t, b, true)
	}
	// The budget is spent. Nothing more gets through until the outcomes of the
	// probes already in flight resolve the state one way or the other.
	for i := 0; i < 10; i++ {
		mustAllow(t, b, false)
	}
	mustState(t, b, HalfOpen)
}

// TestHalfOpen_ConcurrentProbeBudget is the load-bearing concurrency test.
//
// In production the open -> half-open moment is not one goroutine noticing the
// deadline: it is every request that has been piling up against a sick backend
// discovering simultaneously that the door is open again. If the budget is
// enforced with a check followed by an increment, all of them read "0 admitted"
// before any of them writes "1 admitted", and the backend that just told us it
// was sick is hit by the entire backlog at once -- the exact stampede half-open
// exists to prevent, at the exact moment it can least survive it.
//
// So the assertion is an exact count, not a bound-ish one. Anything other than
// halfOpenProbes is a bug: more means the CAS is not enforcing the budget,
// fewer means a goroutine that should have been admitted lost its slot.
func TestHalfOpen_ConcurrentProbeBudget(t *testing.T) {
	const (
		goroutines = 500
		probes     = 4
	)

	// Repeated, because a lost CAS is a timing bug and one run of a timing bug
	// proves very little.
	for round := 0; round < 20; round++ {
		b, c := newTestBreaker(1, probes)
		b.Failure()
		c.at(openFor)

		var admitted atomic.Int64
		var transitions atomic.Int64
		b.OnTransition(func(to State) {
			if to == HalfOpen {
				transitions.Add(1)
			}
		})

		// A start barrier, not just a WaitGroup: without it the goroutines
		// launch over several microseconds and the first one is likely to
		// finish before the last one starts, which is precisely the race the
		// test is trying to create.
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if b.Allow() {
					admitted.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()

		if got := admitted.Load(); got != probes {
			t.Fatalf("round %d: admitted %d probes, want exactly %d", round, got, probes)
		}
		// The transition itself must also happen exactly once, or the
		// transitions counter would report one flap per racing goroutine.
		if got := transitions.Load(); got != 1 {
			t.Fatalf("round %d: %d half_open transitions, want exactly 1", round, got)
		}
		mustState(t, b, HalfOpen)
	}
}

// TestHalfOpen_FailureReopensAndRestartsTimer. Restarting matters: a breaker
// that re-opened on the *original* deadline would already be expired, so a
// backend that failed its probe would be probed again immediately and forever,
// which is a busy loop against a dead machine rather than a backoff.
func TestHalfOpen_FailureReopensAndRestartsTimer(t *testing.T) {
	b, c := newTestBreaker(1, 2)

	b.Failure()
	c.at(openFor)
	mustAllow(t, b, true)
	mustState(t, b, HalfOpen)

	b.Failure()
	mustState(t, b, Open)

	// The second open window runs from the failure, not from the first trip.
	c.at(2*openFor - time.Millisecond)
	mustAllow(t, b, false)

	c.at(2 * openFor)
	mustAllow(t, b, true)
	mustState(t, b, HalfOpen)
}

// TestHalfOpen_AllProbesSucceedCloses, and the closed breaker starts with a
// full failure budget rather than one failure from re-opening.
func TestHalfOpen_AllProbesSucceedCloses(t *testing.T) {
	const (
		threshold = 3
		probes    = 2
	)
	b, c := newTestBreaker(threshold, probes)

	b.Failure()
	b.Failure()
	b.Failure()
	mustState(t, b, Open)

	c.at(openFor)
	mustAllow(t, b, true)
	mustAllow(t, b, true)
	mustAllow(t, b, false) // budget spent

	b.Success()
	mustState(t, b, HalfOpen) // one probe still outstanding
	b.Success()
	mustState(t, b, Closed)

	// Full credit again.
	b.Failure()
	b.Failure()
	mustState(t, b, Closed)
	b.Failure()
	mustState(t, b, Open)
}

// TestDisabled_NeverBlocks. A disabled breaker must be inert in both
// directions: it never refuses, and nothing recorded into it can change that.
func TestDisabled_NeverBlocks(t *testing.T) {
	b := New(config.BreakerConfig{Enabled: false, FailureThreshold: 1, HalfOpenProbes: 1})

	if b.Enabled() {
		t.Fatal("Enabled() true for a disabled breaker")
	}
	for i := 0; i < 1000; i++ {
		b.Failure()
		mustAllow(t, b, true)
		mustState(t, b, Closed)
	}
	b.Success()
	mustAllow(t, b, true)
}

// TestTransitions_ReportedOnce checks the sequence the metrics see. A
// transition counter that double-counts turns rate() into fiction, and one
// that misses the close makes a recovered breaker look permanently open.
func TestTransitions_ReportedOnce(t *testing.T) {
	b, c := newTestBreaker(2, 1)

	var got []State
	b.OnTransition(func(to State) { got = append(got, to) })

	b.Failure()
	b.Failure() // -> Open
	c.at(openFor)
	b.Allow() // -> HalfOpen
	b.Success()
	// -> Closed

	want := []State{Open, HalfOpen, Closed}
	if len(got) != len(want) {
		t.Fatalf("transitions %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("transitions %v, want %v", got, want)
		}
	}
}

// TestOpen_LateOutcomesDoNotDisturbDeadline. Requests admitted just before the
// trip are still in flight when it happens, and their outcomes arrive against
// an already-open breaker. Neither a straggling success nor a straggling
// failure may move the recovery deadline: successes must not shorten it
// (the backend has not been probed) and failures must not extend it (a burst
// of a hundred in-flight requests would otherwise push recovery a hundred
// windows out).
func TestOpen_LateOutcomesDoNotDisturbDeadline(t *testing.T) {
	b, c := newTestBreaker(1, 1)

	b.Failure()
	mustState(t, b, Open)

	for i := 0; i < 50; i++ {
		b.Failure()
		b.Success()
	}
	mustState(t, b, Open)

	c.at(openFor - time.Millisecond)
	mustAllow(t, b, false)
	c.at(openFor)
	mustAllow(t, b, true)
}

// TestConcurrent_MixedTrafficStaysConsistent runs the whole state machine under
// contention and asserts the invariant that must hold at every instant: the
// state is always one of the three legal values and the breaker never admits
// more than half_open_probes while half-open. It is the test the race detector
// runs against in CI, where -race actually works.
func TestConcurrent_MixedTrafficStaysConsistent(t *testing.T) {
	b, c := newTestBreaker(3, 2)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				if !b.Allow() {
					continue
				}
				if (id+n)%3 == 0 {
					b.Failure()
				} else {
					b.Success()
				}
			}
		}(i)
	}

	// Drive the clock forward so open windows really do expire during the run.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			c.advance(openFor / 4)
		}
	}()

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()

	switch b.State() {
	case Closed, Open, HalfOpen:
	default:
		t.Fatalf("breaker ended in illegal state %d", b.State())
	}
}

// --- benchmarks -----------------------------------------------------------
//
// Allow runs once per forwarding attempt on every request, so its cost is
// added to manifold's per-request budget in full. The numbers that matter are
// the closed and disabled cases; open and half-open only happen while
// something is already broken.

func BenchmarkAllow_Closed(b *testing.B) {
	br := New(cfg(5, 1))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !br.Allow() {
			b.Fatal("closed breaker refused")
		}
	}
}

func BenchmarkAllow_Disabled(b *testing.B) {
	br := New(config.BreakerConfig{Enabled: false})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !br.Allow() {
			b.Fatal("disabled breaker refused")
		}
	}
}

// BenchmarkAllow_Open is the rejection path, which costs a clock read on top
// of the load. It is measured because "an open breaker is cheap" is a claim
// the feature depends on -- rejecting must be far cheaper than dialling.
func BenchmarkAllow_Open(b *testing.B) {
	br := New(cfg(1, 1))
	br.Failure()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if br.Allow() {
			b.Fatal("open breaker admitted")
		}
	}
}

// BenchmarkAllow_ClosedParallel is the shape that matters in production: every
// core hitting the same breaker at once. A closed Allow is a pure load with no
// store, so the cache line stays shared and this should not degrade with core
// count -- which is exactly what a check-then-store design would not do.
func BenchmarkAllow_ClosedParallel(b *testing.B) {
	br := New(cfg(5, 1))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if !br.Allow() {
				b.Fatal("closed breaker refused")
			}
		}
	})
}

// BenchmarkRecord_Success is the other half of the per-attempt cost: the proxy
// calls Success or Failure exactly once for every Allow that returned true.
func BenchmarkRecord_Success(b *testing.B) {
	br := New(cfg(5, 1))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		br.Success()
	}
}
