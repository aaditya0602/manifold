package balance

import "math/rand/v2"

// LeastConn picks the candidate with the fewest in-flight requests per unit
// of weight — a backend with twice the weight should carry twice the
// connections before it is considered equally loaded.
//
// Design decision: an exact O(n) scan over the candidate slice, not
// power-of-two-choices (P2C). P2C exists to avoid reading global load state
// across a large fleet; here the candidate slice is already in hand (the
// proxy built it from its own pool state) and is small, so a full scan is
// both simpler and strictly more accurate than sampling two of them.
type LeastConn struct{}

// NewLeastConn returns a least-connections strategy. The zero value is also
// usable.
func NewLeastConn() *LeastConn { return &LeastConn{} }

// Name implements Strategy.
func (l *LeastConn) Name() string { return StrategyLeastConn }

// Pick implements Strategy. It ignores req: least-conn only reads InFlight.
//
// InFlight is sampled, not consistent: two goroutines can both read the same
// candidate as the least-loaded one and both pick it before either's
// increment is visible. That race is acceptable — the alternative is
// serialising every pick behind a lock across the whole candidate set, which
// would make this the contention point for the entire proxy.
func (l *LeastConn) Pick(_ uint64, candidates []Candidate, _ Request) (Candidate, bool) {
	if len(candidates) == 0 {
		return Candidate{}, false
	}

	best := 0
	// ties counts how many candidates seen so far (including best) are tied
	// for lowest load, so each tied candidate gets an equal 1/ties chance of
	// replacing best (reservoir sampling of size 1).
	ties := 1

	for i := 1; i < len(candidates); i++ {
		c := &candidates[i]
		b := &candidates[best]

		bw, cw := weightOf(b), weightOf(c)
		// Compare InFlight/Weight for b and c without floating point:
		// InFlight_c/Weight_c < InFlight_b/Weight_b
		//   <=> InFlight_c*Weight_b < InFlight_b*Weight_c
		lhs := c.InFlight * int64(bw)
		rhs := b.InFlight * int64(cw)

		switch {
		case lhs < rhs:
			best = i
			ties = 1
		case lhs == rhs:
			ties++
			// Break ties randomly rather than always keeping the earlier
			// index: with an idle pool every candidate has InFlight == 0,
			// and always favouring the first would send every request to
			// one backend.
			if rand.IntN(ties) == 0 {
				best = i
			}
		}
	}

	return candidates[best], true
}

// weightOf returns c.Weight, defensively normalised to at least 1. Config
// validation guarantees this already, but Pick must not divide by (or
// multiply against) zero if ever handed an unvalidated candidate.
func weightOf(c *Candidate) int {
	if c.Weight < 1 {
		return 1
	}
	return c.Weight
}
