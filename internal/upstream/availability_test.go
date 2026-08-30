package upstream

import (
	"sync"
	"testing"
	"time"

	"github.com/aaditya0602/manifold/internal/balance"
)

func availPool(t *testing.T, urls ...string) *Pool {
	t.Helper()
	p, err := NewPool(poolCfg("avail", urls...))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	return p
}

func TestBackendsStartAvailable(t *testing.T) {
	p := availPool(t, "http://127.0.0.1:9001", "http://127.0.0.1:9002")
	for _, b := range p.Backends() {
		if !b.ActiveHealthy() {
			t.Errorf("%s: ActiveHealthy = false at construction", b.Key())
		}
		if b.Ejected() {
			t.Errorf("%s: Ejected = true at construction", b.Key())
		}
		if !b.Available() {
			t.Errorf("%s: Available = false at construction", b.Key())
		}
	}
	if got := len(p.Candidates()); got != 2 {
		t.Errorf("Candidates = %d, want 2", got)
	}
	if p.Gen() != 0 {
		t.Errorf("Gen = %d, want 0 before any transition", p.Gen())
	}
}

func TestSetActiveHealthy_ChangedAndGen(t *testing.T) {
	p := availPool(t, "http://127.0.0.1:9001")

	if !p.SetActiveHealthy(0, false) {
		t.Fatal("SetActiveHealthy(false) = false, want a state change")
	}
	if p.Gen() != 1 {
		t.Fatalf("Gen = %d after going unhealthy, want 1", p.Gen())
	}
	if p.Backend(0).Available() {
		t.Error("Available = true after being marked unhealthy")
	}

	// A repeated identical verdict must be a no-op. Every probe interval
	// reports the same result in steady state; bumping gen there would
	// invalidate every strategy's cached ring forever.
	if p.SetActiveHealthy(0, false) {
		t.Error("repeated SetActiveHealthy(false) reported a change")
	}
	if p.Gen() != 1 {
		t.Errorf("Gen = %d after a no-op verdict, want 1", p.Gen())
	}

	if !p.SetActiveHealthy(0, true) {
		t.Fatal("SetActiveHealthy(true) = false, want a state change")
	}
	if p.Gen() != 2 {
		t.Errorf("Gen = %d after recovery, want 2", p.Gen())
	}
	if !p.Backend(0).Available() {
		t.Error("Available = false after recovery")
	}
}

func TestEjectReadmit(t *testing.T) {
	p := availPool(t, "http://127.0.0.1:9001")
	until := time.Now().Add(time.Minute)

	if !p.Eject(0, until) {
		t.Fatal("Eject = false on a live backend")
	}
	if !p.Backend(0).Ejected() || p.Backend(0).Available() {
		t.Fatal("backend still available after Eject")
	}
	if got := p.EjectedUntil(0); !got.Equal(until) {
		t.Errorf("EjectedUntil = %v, want %v", got, until)
	}
	if p.Gen() != 1 {
		t.Errorf("Gen = %d after ejection, want 1", p.Gen())
	}

	// Re-ejecting an ejected backend extends the deadline but is not a
	// transition.
	later := until.Add(time.Minute)
	if p.Eject(0, later) {
		t.Error("Eject on an already-ejected backend reported a transition")
	}
	if got := p.EjectedUntil(0); !got.Equal(later) {
		t.Errorf("EjectedUntil = %v after re-eject, want the later deadline %v", got, later)
	}
	if p.Gen() != 1 {
		t.Errorf("Gen = %d after a re-eject, want 1", p.Gen())
	}

	if !p.Readmit(0) {
		t.Fatal("Readmit = false on an ejected backend")
	}
	if !p.Backend(0).Available() {
		t.Error("backend not available after Readmit")
	}
	if !p.EjectedUntil(0).IsZero() {
		t.Errorf("EjectedUntil = %v after Readmit, want zero", p.EjectedUntil(0))
	}
	if p.Gen() != 2 {
		t.Errorf("Gen = %d after readmission, want 2", p.Gen())
	}
	if p.Readmit(0) {
		t.Error("Readmit on a live backend reported a transition")
	}
}

// An ejection and a probe verdict are independent signals: an unhealthy
// backend that is also ejected must not become available on readmission alone.
func TestEjectionAndProbeVerdictAreIndependent(t *testing.T) {
	p := availPool(t, "http://127.0.0.1:9001")

	p.Eject(0, time.Now().Add(time.Minute))
	genAfterEject := p.Gen()

	// Already unavailable, so the flag changes but the available set does not.
	if !p.SetActiveHealthy(0, false) {
		t.Fatal("SetActiveHealthy reported no change to the stored flag")
	}
	if p.Gen() != genAfterEject {
		t.Errorf("Gen moved on a transition that did not change the available set")
	}

	p.Readmit(0)
	if p.Backend(0).Available() {
		t.Error("Available = true after readmission while still probe-unhealthy")
	}
	p.SetActiveHealthy(0, true)
	if !p.Backend(0).Available() {
		t.Error("Available = false after both signals cleared")
	}
}

// The generation must move even when the available set keeps its length.
// balance.Strategy.Pick documents this as a hard obligation: a strategy that
// caches a ring by generation would keep routing to the ejected backend.
func TestGenChangesOnSameLengthSwap(t *testing.T) {
	p := availPool(t, "http://127.0.0.1:9001", "http://127.0.0.1:9002")

	// Start with backend 1 out, so the swap keeps exactly one candidate.
	p.Eject(1, time.Now().Add(time.Minute))
	before := p.Gen()
	if got := len(p.Candidates()); got != 1 {
		t.Fatalf("Candidates = %d, want 1 before the swap", got)
	}

	// Eject 0 and readmit 1 in the same instant: same count, different set.
	p.Eject(0, time.Now().Add(time.Minute))
	p.Readmit(1)

	after := p.Gen()
	if after == before {
		t.Fatalf("Gen = %d unchanged across a same-length swap; strategies would reuse a stale ring", after)
	}
	cands := p.Candidates()
	if len(cands) != 1 {
		t.Fatalf("Candidates = %d, want 1 after the swap", len(cands))
	}
	if cands[0].ID != 1 {
		t.Errorf("candidate ID = %d, want the readmitted backend 1", cands[0].ID)
	}
}

func TestCandidates_ShrinksAndGrows(t *testing.T) {
	p := availPool(t, "http://127.0.0.1:9001", "http://127.0.0.1:9002", "http://127.0.0.1:9003")

	if got := len(p.Candidates()); got != 3 {
		t.Fatalf("Candidates = %d, want 3", got)
	}
	p.SetActiveHealthy(1, false)
	cands := p.Candidates()
	if len(cands) != 2 {
		t.Fatalf("Candidates = %d after one backend went unhealthy, want 2", len(cands))
	}
	for _, c := range cands {
		if c.ID == 1 {
			t.Error("unhealthy backend still present in Candidates")
		}
	}
	p.SetActiveHealthy(1, true)
	if got := len(p.Candidates()); got != 3 {
		t.Errorf("Candidates = %d after recovery, want 3", got)
	}
}

// With nothing available the pool returns an empty candidate set; the strategy
// reports ok=false and the proxy answers 503 rather than queueing against
// origins it has evidence are broken.
func TestCandidates_EmptyWhenAllUnavailable(t *testing.T) {
	p := availPool(t, "http://127.0.0.1:9001", "http://127.0.0.1:9002")

	p.SetActiveHealthy(0, false)
	p.Eject(1, time.Now().Add(time.Minute))

	cands := p.Candidates()
	if len(cands) != 0 {
		t.Fatalf("Candidates = %v, want empty", cands)
	}
	if _, ok := p.Strategy().Pick(p.Gen(), cands, balance.Request{}); ok {
		t.Error("strategy picked a candidate from an empty set")
	}
}

func TestOnAvailabilityChange(t *testing.T) {
	p := availPool(t, "http://127.0.0.1:9001", "http://127.0.0.1:9002")

	var mu sync.Mutex
	var first, second []string
	record := func(dst *[]string) func(string, bool) {
		return func(key string, available bool) {
			mu.Lock()
			defer mu.Unlock()
			state := "up"
			if !available {
				state = "down"
			}
			*dst = append(*dst, key+" "+state)
		}
	}
	p.OnAvailabilityChange(record(&first))
	p.OnAvailabilityChange(record(&second))

	p.SetActiveHealthy(0, false)
	p.SetActiveHealthy(0, false) // no-op, must not notify
	p.SetActiveHealthy(0, true)
	p.Eject(1, time.Now().Add(time.Minute))

	want := []string{
		"http://127.0.0.1:9001 down",
		"http://127.0.0.1:9001 up",
		"http://127.0.0.1:9002 down",
	}
	mu.Lock()
	defer mu.Unlock()
	for name, got := range map[string][]string{"first": first, "second": second} {
		if len(got) != len(want) {
			t.Fatalf("%s subscriber got %v, want %v", name, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s subscriber event %d = %q, want %q", name, i, got[i], want[i])
			}
		}
	}
}

// A subscriber must be able to call back into the Pool. Notifying while
// holding availMu would deadlock here.
func TestOnAvailabilityChange_CallbackMayReenter(t *testing.T) {
	p := availPool(t, "http://127.0.0.1:9001", "http://127.0.0.1:9002")

	done := make(chan struct{})
	var once sync.Once
	p.OnAvailabilityChange(func(key string, available bool) {
		// Read pool state and register another subscriber from inside the
		// callback; both take Pool locks.
		_ = p.Candidates()
		_ = p.EjectedUntil(0)
		p.OnAvailabilityChange(func(string, bool) {})
		if key == "http://127.0.0.1:9001" {
			p.SetActiveHealthy(1, false)
		}
		once.Do(func() { close(done) })
	})

	go func() { p.SetActiveHealthy(0, false) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("callback deadlocked: notification is holding a Pool lock")
	}
}
