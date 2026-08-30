package balance

import (
	"sync"
	"testing"
)

func TestLeastConnEmpty(t *testing.T) {
	l := NewLeastConn()
	if _, ok := l.Pick(1, nil, Request{}); ok {
		t.Fatal("Pick on an empty candidate set must report ok=false")
	}
}

func TestLeastConnSingle(t *testing.T) {
	l := NewLeastConn()
	cs := []Candidate{{ID: 0, Weight: 1, InFlight: 7}}
	got, ok := l.Pick(1, cs, Request{})
	if !ok || got.ID != 0 {
		t.Fatalf("got (%+v, %v), want ID 0", got, ok)
	}
}

func TestLeastConnPicksMinimum(t *testing.T) {
	l := NewLeastConn()
	cs := []Candidate{
		{ID: 0, Weight: 1, InFlight: 5},
		{ID: 1, Weight: 1, InFlight: 2},
		{ID: 2, Weight: 1, InFlight: 9},
	}
	for i := 0; i < 20; i++ {
		got, ok := l.Pick(1, cs, Request{})
		if !ok || got.ID != 1 {
			t.Fatalf("got (%+v, %v), want ID 1 (fewest in-flight)", got, ok)
		}
	}
}

// TestLeastConnWeightedTie checks the InFlight/Weight ratio, not raw
// InFlight: a backend with weight 2 and 2 in-flight (ratio 1) ties a backend
// with weight 1 and 1 in-flight (ratio 1), and both should be eligible.
func TestLeastConnWeightedTie(t *testing.T) {
	l := NewLeastConn()
	cs := []Candidate{
		{ID: 0, Weight: 1, InFlight: 1},
		{ID: 1, Weight: 2, InFlight: 2},
	}
	seen := map[int]bool{}
	for i := 0; i < 500; i++ {
		got, ok := l.Pick(1, cs, Request{})
		if !ok {
			t.Fatal("unexpected ok=false")
		}
		if got.ID != 0 && got.ID != 1 {
			t.Fatalf("unexpected candidate ID %d", got.ID)
		}
		seen[got.ID] = true
	}
	if !seen[0] || !seen[1] {
		t.Errorf("tie between weighted-equal candidates should let both win, saw %v", seen)
	}
}

// TestLeastConnWeightBiasesOverTie confirms a heavier candidate is NOT tied
// with a lighter one at the same raw InFlight count: weight 2 should still
// look less loaded than weight 1 at equal InFlight.
func TestLeastConnWeightBiasesOverTie(t *testing.T) {
	l := NewLeastConn()
	cs := []Candidate{
		{ID: 0, Weight: 1, InFlight: 4},
		{ID: 1, Weight: 2, InFlight: 4},
	}
	for i := 0; i < 20; i++ {
		got, ok := l.Pick(1, cs, Request{})
		if !ok || got.ID != 1 {
			t.Fatalf("got (%+v, %v), want ID 1 (double weight, same in-flight)", got, ok)
		}
	}
}

func TestLeastConnIdlePoolSpreads(t *testing.T) {
	l := NewLeastConn()
	cs := candidates(1, 1, 1, 1)
	seen := make(map[int]bool)
	const picks = 2000
	for i := 0; i < picks; i++ {
		got, ok := l.Pick(1, cs, Request{})
		if !ok {
			t.Fatal("unexpected ok=false")
		}
		seen[got.ID] = true
	}
	for _, c := range cs {
		if !seen[c.ID] {
			t.Errorf("candidate %d was never picked from an idle pool", c.ID)
		}
	}
}

func TestLeastConnConcurrent(t *testing.T) {
	l := NewLeastConn()
	cs := []Candidate{
		{ID: 0, Weight: 1, InFlight: 3},
		{ID: 1, Weight: 1, InFlight: 1},
		{ID: 2, Weight: 1, InFlight: 8},
	}

	const goroutines = 8
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				got, ok := l.Pick(1, cs, Request{})
				if !ok {
					t.Error("unexpected ok=false")
					return
				}
				// InFlight is static in this test (no proxy is actually
				// mutating it), so the minimum is deterministic even though
				// Pick is called concurrently.
				if got.ID != 1 {
					t.Errorf("got ID %d, want ID 1", got.ID)
					return
				}
			}
		}()
	}
	wg.Wait()
}
