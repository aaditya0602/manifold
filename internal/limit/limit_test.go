package limit

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaditya0602/manifold/internal/config"
)

func shedding(max int) config.LimitConfig {
	return config.LimitConfig{MaxInFlight: max}
}

func queueing(max int, wait time.Duration) config.LimitConfig {
	return config.LimitConfig{MaxInFlight: max, QueueTimeout: config.Duration(wait)}
}

// TestShed_AtCapacity is the default configuration: fill it, and the next
// request is refused rather than parked.
func TestShed_AtCapacity(t *testing.T) {
	const max = 3
	l := New(shedding(max))

	for i := 0; i < max; i++ {
		if !l.Acquire(context.Background()) {
			t.Fatalf("Acquire %d refused below capacity", i)
		}
	}
	if got := l.InFlight(); got != max {
		t.Fatalf("InFlight() = %d, want %d", got, max)
	}

	// Refusal must be immediate, not "immediate-ish". A shed that blocks is
	// not a shed.
	start := time.Now()
	if l.Acquire(context.Background()) {
		t.Fatal("Acquire succeeded above capacity")
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("shed took %s; queue_timeout 0 must not wait", d)
	}

	// A freed slot is immediately reusable.
	l.Release()
	if !l.Acquire(context.Background()) {
		t.Fatal("Acquire refused after a slot was released")
	}
}

// TestUnlimited_NeverSheds. max_in_flight 0 is written explicitly by an
// operator who wants the feature off, and config's presence-aware defaulting
// keeps that distinct from an omitted key -- so it must really be off, not
// "off, but still counting".
func TestUnlimited_NeverSheds(t *testing.T) {
	l := New(shedding(0))

	for i := 0; i < 10000; i++ {
		if !l.Acquire(context.Background()) {
			t.Fatalf("unlimited limiter shed at %d", i)
		}
	}
	if got := l.InFlight(); got != 0 {
		t.Fatalf("InFlight() = %d, want 0 for an unlimited limiter", got)
	}
	if got := l.Max(); got != 0 {
		t.Fatalf("Max() = %d, want 0", got)
	}
	// Release on an unlimited limiter must be a no-op, not an underflow.
	for i := 0; i < 100; i++ {
		l.Release()
	}
	if !l.Acquire(context.Background()) {
		t.Fatal("unlimited limiter shed after unpaired Releases")
	}
}

// TestQueue_WaitsForSlot covers the queue_timeout > 0 configuration in both
// directions: a slot that frees inside the window is taken, and one that does
// not is shed.
func TestQueue_WaitsForSlot(t *testing.T) {
	t.Run("slot frees in time", func(t *testing.T) {
		l := New(queueing(1, 2*time.Second))
		if !l.Acquire(context.Background()) {
			t.Fatal("first Acquire refused")
		}

		// Handed back from another goroutine after a delay the waiter must
		// tolerate. The delay is short and the timeout is long, so the test
		// asserts "waited and won" without being a race.
		go func() {
			time.Sleep(30 * time.Millisecond)
			l.Release()
		}()

		start := time.Now()
		if !l.Acquire(context.Background()) {
			t.Fatal("Acquire shed despite a slot freeing inside queue_timeout")
		}
		if d := time.Since(start); d < 20*time.Millisecond {
			t.Fatalf("Acquire returned after %s; it cannot have waited for the slot", d)
		}
	})

	t.Run("timeout expires", func(t *testing.T) {
		const wait = 60 * time.Millisecond
		l := New(queueing(1, wait))
		if !l.Acquire(context.Background()) {
			t.Fatal("first Acquire refused")
		}

		start := time.Now()
		if l.Acquire(context.Background()) {
			t.Fatal("Acquire succeeded with no slot available")
		}
		d := time.Since(start)
		if d < wait {
			t.Fatalf("gave up after %s, before queue_timeout %s", d, wait)
		}
		if d > wait+2*time.Second {
			t.Fatalf("waited %s, far beyond queue_timeout %s", d, wait)
		}
		// The failed waiter must not have taken the slot on its way out.
		if got := l.InFlight(); got != 1 {
			t.Fatalf("InFlight() = %d after a shed wait, want 1", got)
		}
	})
}

// TestQueue_CancelledContextReleasesSlotPromptly.
//
// A client that has hung up still occupies a queue slot until somebody notices,
// and under overload -- the only time the queue is used at all -- that is
// exactly when disconnects are most common. A limiter that waits out the full
// queue_timeout on dead requests converts client impatience into reduced
// capacity for the clients who are still there.
func TestQueue_CancelledContextReleasesSlotPromptly(t *testing.T) {
	const wait = 10 * time.Second
	l := New(queueing(1, wait))
	if !l.Acquire(context.Background()) {
		t.Fatal("first Acquire refused")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		ok := l.Acquire(ctx)
		if ok {
			l.Release()
			done <- -1
			return
		}
		done <- time.Since(start)
	}()

	// Let the waiter park, then hang up.
	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case d := <-done:
		if d < 0 {
			t.Fatal("Acquire succeeded although no slot was ever freed")
		}
		if d > time.Second {
			t.Fatalf("cancelled waiter took %s to give up", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled waiter did not return; it is holding a queue slot for a dead client")
	}

	// An already-cancelled context sheds without parking at all.
	start := time.Now()
	if l.Acquire(ctx) {
		t.Fatal("Acquire succeeded for an already-cancelled context")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("pre-cancelled Acquire waited %s", d)
	}
}

// TestConcurrent_NeverExceedsBound is the property the whole package exists to
// provide, asserted the only way that means anything: a live counter sampled
// by every goroutine that holds a slot, checked against the bound, and required
// to land back on exactly zero when the storm is over.
//
// Zero at the end is not a formality. A limiter that leaks one slot per
// thousand requests looks perfect in every functional test and sheds at a
// fraction of its configured concurrency after a day of production traffic.
func TestConcurrent_NeverExceedsBound(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  config.LimitConfig
	}{
		{"shed", shedding(8)},
		{"queue", queueing(8, 250*time.Millisecond)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const (
				goroutines = 200
				perG       = 50
				max        = 8
			)
			l := New(tc.cfg)

			var held atomic.Int64
			var peak atomic.Int64
			var admitted, shedded atomic.Int64

			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := 0; i < goroutines; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					for n := 0; n < perG; n++ {
						if !l.Acquire(context.Background()) {
							shedded.Add(1)
							continue
						}
						admitted.Add(1)
						cur := held.Add(1)
						for {
							old := peak.Load()
							if cur <= old || peak.CompareAndSwap(old, cur) {
								break
							}
						}
						if cur > max {
							t.Errorf("%d slots held at once, bound is %d", cur, max)
						}
						if inf := l.InFlight(); inf > max {
							t.Errorf("InFlight() = %d, bound is %d", inf, max)
						}
						// Yield while holding the slot. Acquire-to-Release is
						// otherwise ~20ns, and on a loaded machine 10,000
						// acquisitions can complete without two goroutines ever
						// being inside the window at once -- which makes the
						// contention this test exists to create a matter of
						// scheduling luck.
						runtime.Gosched()
						held.Add(-1)
						l.Release()
					}
				}()
			}
			close(start)
			wg.Wait()

			if got := l.InFlight(); got != 0 {
				t.Fatalf("InFlight() = %d after every request finished, want exactly 0", got)
			}
			if got := held.Load(); got != 0 {
				t.Fatalf("held = %d after every request finished, want 0", got)
			}
			if admitted.Load()+shedded.Load() != goroutines*perG {
				t.Fatalf("accounted for %d+%d of %d requests",
					admitted.Load(), shedded.Load(), goroutines*perG)
			}
			// Report the contention actually achieved. This is deliberately not
			// a failure: the bound itself is asserted deterministically by
			// TestShed_AtCapacity, which fills to capacity and checks that the
			// next acquire is refused, with no scheduling dependence at all.
			// What this test uniquely covers -- that the bound holds under
			// concurrency, that every request is accounted for, and that the
			// counter returns to exactly zero -- is asserted above regardless
			// of how much overlap the scheduler produced.
			if p := peak.Load(); p < 2 {
				t.Logf("peak concurrency was %d: the scheduler serialised this run, "+
					"so only the accounting invariants were exercised here", p)
			} else {
				t.Logf("peak concurrency %d of a bound of %d", p, max)
			}
		})
	}
}

// TestRelease_NeverGoesNegative. Release without a matching Acquire is a
// caller bug, but the consequence of trusting the caller is a counter that
// drifts below zero and silently raises max_in_flight for the life of the
// process -- a fault that surfaces months later as an outage under exactly the
// load the limiter was installed to survive.
func TestRelease_NeverGoesNegative(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  config.LimitConfig
	}{
		{"shed", shedding(2)},
		{"queue", queueing(2, 10*time.Millisecond)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := New(tc.cfg)
			for i := 0; i < 100; i++ {
				l.Release()
			}
			if got := l.InFlight(); got != 0 {
				t.Fatalf("InFlight() = %d after unpaired Releases, want 0", got)
			}
			// The bound must still be exactly 2, not 102.
			for i := 0; i < 2; i++ {
				if !l.Acquire(context.Background()) {
					t.Fatalf("Acquire %d refused below capacity", i)
				}
			}
			if l.Acquire(context.Background()) {
				t.Fatal("unpaired Releases raised the effective bound")
			}
		})
	}
}

// --- benchmarks -----------------------------------------------------------
//
// Acquire and Release bracket every single request, so their combined cost is
// pure overhead added to manifold's per-request budget.

func BenchmarkAcquireRelease_Shed(b *testing.B) {
	l := New(shedding(1 << 20))
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !l.Acquire(ctx) {
			b.Fatal("shed below capacity")
		}
		l.Release()
	}
}

func BenchmarkAcquireRelease_Unlimited(b *testing.B) {
	l := New(shedding(0))
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !l.Acquire(ctx) {
			b.Fatal("unlimited limiter shed")
		}
		l.Release()
	}
}

// BenchmarkAcquireRelease_Queue measures the waiting configuration on the path
// it actually takes when the pool is *not* saturated -- the non-blocking send.
// If that path allocated a timer it would be a per-request allocation for a
// wait that never happens.
func BenchmarkAcquireRelease_Queue(b *testing.B) {
	l := New(queueing(1<<16, time.Second))
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !l.Acquire(ctx) {
			b.Fatal("shed below capacity")
		}
		l.Release()
	}
}

// BenchmarkAcquireRelease_ShedParallel is the contended shape: every core
// hammering the one counter that bounds the pool.
func BenchmarkAcquireRelease_ShedParallel(b *testing.B) {
	l := New(shedding(1 << 20))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			if !l.Acquire(ctx) {
				b.Fatal("shed below capacity")
			}
			l.Release()
		}
	})
}
