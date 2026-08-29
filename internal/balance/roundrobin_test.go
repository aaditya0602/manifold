package balance

import (
	"sync"
	"testing"
)

func candidates(weights ...int) []Candidate {
	cs := make([]Candidate, len(weights))
	for i, w := range weights {
		cs[i] = Candidate{ID: i, Key: string(rune('a' + i)), Weight: w}
	}
	return cs
}

func TestRoundRobinEmpty(t *testing.T) {
	rr := NewRoundRobin()
	if _, ok := rr.Pick(1, nil, Request{}); ok {
		t.Fatal("Pick on an empty candidate set must report ok=false")
	}
}

func TestRoundRobinSingle(t *testing.T) {
	rr := NewRoundRobin()
	cs := candidates(1)
	for i := 0; i < 10; i++ {
		got, ok := rr.Pick(1, cs, Request{})
		if !ok || got.ID != 0 {
			t.Fatalf("pick %d: got (%+v, %v), want ID 0", i, got, ok)
		}
	}
}

func TestRoundRobinUniformIsExactlyEven(t *testing.T) {
	rr := NewRoundRobin()
	cs := candidates(1, 1, 1)
	counts := make(map[int]int)
	const picks = 999
	for i := 0; i < picks; i++ {
		c, ok := rr.Pick(1, cs, Request{})
		if !ok {
			t.Fatal("unexpected ok=false")
		}
		counts[c.ID]++
	}
	for id, want := 0, picks/len(cs); id < len(cs); id++ {
		if counts[id] != want {
			t.Errorf("candidate %d served %d requests, want exactly %d", id, counts[id], want)
		}
	}
}

func TestRoundRobinRespectsWeights(t *testing.T) {
	rr := NewRoundRobin()
	cs := candidates(3, 1)
	counts := make(map[int]int)
	const picks = 4000 // a whole number of 4-request cycles
	for i := 0; i < picks; i++ {
		c, _ := rr.Pick(1, cs, Request{})
		counts[c.ID]++
	}
	if counts[0] != 3000 || counts[1] != 1000 {
		t.Errorf("weights 3:1 gave %d:%d over %d picks, want 3000:1000", counts[0], counts[1], picks)
	}
}

func TestRoundRobinZeroWeightTreatedAsOne(t *testing.T) {
	rr := NewRoundRobin()
	cs := []Candidate{{ID: 0, Weight: 0}, {ID: 1, Weight: 0}}
	counts := make(map[int]int)
	for i := 0; i < 100; i++ {
		c, _ := rr.Pick(1, cs, Request{})
		counts[c.ID]++
	}
	if counts[0] != 50 || counts[1] != 50 {
		t.Errorf("got %d:%d, want 50:50", counts[0], counts[1])
	}
}

func TestRoundRobinRebuildsOnGenerationChange(t *testing.T) {
	rr := NewRoundRobin()
	three := candidates(1, 1, 1)
	if _, ok := rr.Pick(1, three, Request{}); !ok {
		t.Fatal("unexpected ok=false")
	}

	// Membership shrinks under a new generation. The cached weight table must
	// not be reused, or Pick could index past the new slice.
	two := candidates(5, 1)
	counts := make(map[int]int)
	const picks = 600
	for i := 0; i < picks; i++ {
		c, ok := rr.Pick(2, two, Request{})
		if !ok {
			t.Fatal("unexpected ok=false")
		}
		if c.ID > 1 {
			t.Fatalf("stale table: returned ID %d for a 2-candidate set", c.ID)
		}
		counts[c.ID]++
	}
	if counts[0] != 500 || counts[1] != 100 {
		t.Errorf("after regeneration got %d:%d, want 500:100", counts[0], counts[1])
	}
}

// TestRoundRobinConcurrent is the reason Pick is lock-free: run it with -race.
func TestRoundRobinConcurrent(t *testing.T) {
	rr := NewRoundRobin()
	cs := candidates(1, 1, 1, 1)

	const (
		goroutines    = 8
		picksPerG     = 1000
		totalExpected = goroutines * picksPerG
	)

	var mu sync.Mutex
	counts := make(map[int]int)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make(map[int]int)
			for i := 0; i < picksPerG; i++ {
				c, ok := rr.Pick(1, cs, Request{})
				if !ok {
					t.Error("unexpected ok=false")
					return
				}
				local[c.ID]++
			}
			mu.Lock()
			for k, v := range local {
				counts[k] += v
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	// The atomic counter guarantees every increment is distinct, so the totals
	// must be exactly even even though the interleaving is not deterministic.
	want := totalExpected / len(cs)
	for id := range cs {
		if counts[id] != want {
			t.Errorf("candidate %d served %d, want exactly %d", id, counts[id], want)
		}
	}
}

func TestFactory(t *testing.T) {
	s, err := New(StrategyRoundRobin, "")
	if err != nil {
		t.Fatalf("round_robin: unexpected error %v", err)
	}
	if s.Name() != StrategyRoundRobin {
		t.Errorf("Name() = %q, want %q", s.Name(), StrategyRoundRobin)
	}

	for _, name := range []string{StrategyLeastConn, StrategyConsistentHash} {
		if _, err := New(name, "client_ip"); err == nil {
			t.Errorf("%s: want ErrNotImplemented, got nil", name)
		} else if _, ok := err.(*ErrNotImplemented); !ok {
			t.Errorf("%s: want *ErrNotImplemented, got %T", name, err)
		}
	}

	if _, err := New("nonsense", ""); err == nil {
		t.Error("unknown strategy: want error, got nil")
	}
}

func BenchmarkRoundRobinPickUniform(b *testing.B) {
	rr := NewRoundRobin()
	cs := candidates(1, 1, 1, 1, 1, 1, 1, 1)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rr.Pick(1, cs, Request{})
		}
	})
}

func BenchmarkRoundRobinPickWeighted(b *testing.B) {
	rr := NewRoundRobin()
	cs := candidates(5, 3, 1, 1, 2, 4, 1, 1)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rr.Pick(1, cs, Request{})
		}
	})
}
