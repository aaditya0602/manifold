package balance

import (
	"fmt"
	"sync"
	"testing"
)

// hashCandidates builds n uniform-weight candidates with stable, distinct
// Keys — consistent hashing places ring points from Key, so ID reshuffles on
// membership change must not matter, only Key does.
func hashCandidates(n int) []Candidate {
	cs := make([]Candidate, n)
	for i := 0; i < n; i++ {
		cs[i] = Candidate{ID: i, Key: fmt.Sprintf("backend-%d", i), Weight: 1}
	}
	return cs
}

func TestConsistentHashEmpty(t *testing.T) {
	c := NewConsistentHash()
	if _, ok := c.Pick(1, nil, Request{HashKey: "x"}); ok {
		t.Fatal("Pick on an empty candidate set must report ok=false")
	}
}

func TestConsistentHashSameKeySameBackend(t *testing.T) {
	c := NewConsistentHash()
	cs := hashCandidates(10)

	first, ok := c.Pick(1, cs, Request{HashKey: "user-42"})
	if !ok {
		t.Fatal("unexpected ok=false")
	}
	for i := 0; i < 100; i++ {
		got, ok := c.Pick(1, cs, Request{HashKey: "user-42"})
		if !ok || got.Key != first.Key {
			t.Fatalf("pick %d: got %q, want %q (same key must map to same backend)", i, got.Key, first.Key)
		}
	}
}

func TestConsistentHashEmptyKeyIsDeterministic(t *testing.T) {
	c := NewConsistentHash()
	cs := hashCandidates(10)

	first, ok := c.Pick(1, cs, Request{})
	if !ok {
		t.Fatal("unexpected ok=false")
	}
	for i := 0; i < 50; i++ {
		got, ok := c.Pick(1, cs, Request{})
		if !ok || got.Key != first.Key {
			t.Fatalf("empty HashKey must deterministically map to one backend, got %q then %q", first.Key, got.Key)
		}
	}
}

// TestConsistentHashStability is the entire reason to use consistent hashing
// over hash(key) % n: removing one backend out of ten must remap well under
// 20% of keys, not close to 90% as plain modulo hashing would.
func TestConsistentHashStability(t *testing.T) {
	const numBackends = 10
	const numKeys = 10000

	c := NewConsistentHash()
	before := hashCandidates(numBackends)

	assignment := make(map[string]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		got, ok := c.Pick(1, before, Request{HashKey: key})
		if !ok {
			t.Fatal("unexpected ok=false")
		}
		assignment[key] = got.Key
	}

	// Remove one backend (keep Keys stable for the survivors) and rebuild
	// under a new generation, per the gen contract in strategy.go.
	after := hashCandidates(numBackends)[1:]

	remapped := 0
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		got, ok := c.Pick(2, after, Request{HashKey: key})
		if !ok {
			t.Fatal("unexpected ok=false")
		}
		if got.Key != assignment[key] {
			remapped++
		}
	}

	pct := float64(remapped) / float64(numKeys) * 100
	t.Logf("removing 1 of %d backends remapped %d/%d keys (%.2f%%)", numBackends, remapped, numKeys, pct)
	if pct >= 20 {
		t.Errorf("remap percentage %.2f%% is too high, want well under 20%%", pct)
	}
}

func TestConsistentHashDistributionIsRoughlyEven(t *testing.T) {
	const numBackends = 10
	const numKeys = 10000

	c := NewConsistentHash()
	cs := hashCandidates(numBackends)

	counts := make(map[string]int, numBackends)
	for i := 0; i < numKeys; i++ {
		got, ok := c.Pick(1, cs, Request{HashKey: fmt.Sprintf("key-%d", i)})
		if !ok {
			t.Fatal("unexpected ok=false")
		}
		counts[got.Key]++
	}

	mean := float64(numKeys) / float64(numBackends)
	low, high := mean/2, mean*2
	for _, cand := range cs {
		n := counts[cand.Key]
		if float64(n) < low || float64(n) > high {
			t.Errorf("backend %s got %d keys, want between %.0f and %.0f (mean %.0f)", cand.Key, n, low, high, mean)
		}
	}
}

func TestConsistentHashRebuildsOnGenerationChange(t *testing.T) {
	c := NewConsistentHash()
	five := hashCandidates(5)
	if _, ok := c.Pick(1, five, Request{HashKey: "k"}); !ok {
		t.Fatal("unexpected ok=false")
	}

	three := hashCandidates(3)
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key-%d", i)
		got, ok := c.Pick(2, three, Request{HashKey: key})
		if !ok {
			t.Fatal("unexpected ok=false")
		}
		valid := false
		for _, cand := range three {
			if cand.Key == got.Key {
				valid = true
				break
			}
		}
		if !valid {
			t.Fatalf("stale ring: returned %q which is outside the current 3-candidate set", got.Key)
		}
	}
}

func TestConsistentHashConcurrent(t *testing.T) {
	c := NewConsistentHash()
	cs := hashCandidates(8)

	const goroutines = 8
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := fmt.Sprintf("key-%d", i%50)
				got1, ok1 := c.Pick(1, cs, Request{HashKey: key})
				got2, ok2 := c.Pick(1, cs, Request{HashKey: key})
				if !ok1 || !ok2 || got1.Key != got2.Key {
					t.Errorf("same key must map consistently even under concurrent Pick, got %v/%v vs %v/%v", got1, ok1, got2, ok2)
					return
				}
			}
		}()
	}
	wg.Wait()
}
