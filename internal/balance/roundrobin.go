package balance

import (
	"sort"
	"sync/atomic"
)

// RoundRobin walks the candidate list in order, honouring weights.
//
// Design decision: the pick path is lock-free. A single atomic counter is
// incremented per request and reduced modulo the total weight. The alternative,
// smooth weighted round-robin (nginx's algorithm), interleaves heavier
// backends more evenly but requires mutating per-candidate state under a lock
// on every pick — a lock on the hottest path in the proxy. We take the
// clumping instead: over any window of total-weight requests both algorithms
// hand each backend exactly its share, they differ only in ordering within the
// window.
type RoundRobin struct {
	next  atomic.Uint64
	table atomic.Pointer[weightTable]
}

// weightTable is the cumulative weight prefix sum for one generation of the
// candidate set. Recomputing it per request would be O(n) on the hot path, so
// it is cached and rebuilt only when the proxy reports a new generation.
type weightTable struct {
	gen uint64
	// uniform is true when every weight is 1, which is the common case and
	// lets Pick skip the search entirely.
	uniform bool
	total   int
	// cum[i] is the sum of weights[0..i], so a position in [0,total) maps to a
	// candidate by binary search.
	cum []int
}

// NewRoundRobin returns a round-robin strategy. The zero value is also usable.
func NewRoundRobin() *RoundRobin { return &RoundRobin{} }

// Name implements Strategy.
func (rr *RoundRobin) Name() string { return StrategyRoundRobin }

// Pick implements Strategy. It ignores req: round-robin is stateless with
// respect to the request.
func (rr *RoundRobin) Pick(gen uint64, candidates []Candidate, _ Request) (Candidate, bool) {
	if len(candidates) == 0 {
		return Candidate{}, false
	}

	t := rr.table.Load()
	if t == nil || t.gen != gen || len(t.cum) != len(candidates) {
		t = buildWeightTable(gen, candidates)
		// A concurrent rebuild is possible and harmless: two goroutines seeing
		// the same generation build byte-identical tables, so whichever store
		// lands last is still correct.
		rr.table.Store(t)
	}

	n := rr.next.Add(1) - 1

	if t.uniform {
		return candidates[n%uint64(len(candidates))], true
	}

	pos := int(n % uint64(t.total))
	// First index whose cumulative weight exceeds pos.
	i := sort.SearchInts(t.cum, pos+1)
	return candidates[i], true
}

func buildWeightTable(gen uint64, candidates []Candidate) *weightTable {
	t := &weightTable{gen: gen, uniform: true, cum: make([]int, len(candidates))}
	sum := 0
	for i, c := range candidates {
		w := c.Weight
		if w < 1 {
			// Config validation normalises weights, but a strategy must not
			// divide by zero if it is ever handed an unvalidated set.
			w = 1
		}
		if w != 1 {
			t.uniform = false
		}
		sum += w
		t.cum[i] = sum
	}
	t.total = sum
	return t
}
