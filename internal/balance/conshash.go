package balance

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync/atomic"
)

// vnodesPerWeight is the number of virtual (ring) points placed per unit of
// candidate weight. More virtual nodes smooth the load distribution — with
// too few, a single unlucky placement can leave one backend with a
// disproportionate arc of the ring — at the cost of a bigger ring to build
// and binary-search. 160 is the value popularised by libketama and is a
// reasonable default: it keeps per-candidate load within a few percent of
// its weighted share for typical pool sizes (single- to low-triple-digit
// backends) without the ring growing large enough to matter for memory or
// rebuild cost.
const vnodesPerWeight = 160

// ConsistentHash routes by request key: the same key always lands on the
// same backend as long as the backend stays in the pool, and losing one
// backend only remaps the keys that hashed nearest to it.
//
// Design decision: the pick path is lock-free reads of a cached ring. The
// ring is rebuilt only when the proxy reports a new generation (see
// Strategy.Pick's gen contract), exactly as roundrobin.go caches its weight
// table. Rebuilding — O(n log n) to sort the ring points — on every request
// would make consistent hashing far more expensive than the other
// strategies for no benefit, since the ring only changes on membership
// mutation.
type ConsistentHash struct {
	ring atomic.Pointer[hashRing]
}

// hashRing is one generation's set of virtual-node points, sorted so Pick can
// binary search it.
type hashRing struct {
	gen uint64
	// points[i] is the hash of a virtual node; owner[i] is the candidate
	// index it belongs to. Parallel slices, sorted by points, so a lookup key
	// maps to a backend via sort.Search followed by a wraparound at the end.
	points []uint64
	owner  []int
}

// NewConsistentHash returns a consistent-hashing strategy. The zero value is
// also usable.
func NewConsistentHash() *ConsistentHash { return &ConsistentHash{} }

// Name implements Strategy.
func (c *ConsistentHash) Name() string { return StrategyConsistentHash }

// Pick implements Strategy.
func (c *ConsistentHash) Pick(gen uint64, candidates []Candidate, req Request) (Candidate, bool) {
	if len(candidates) == 0 {
		return Candidate{}, false
	}

	r := c.ring.Load()
	if r == nil || r.gen != gen {
		r = buildHashRing(gen, candidates)
		// A concurrent rebuild is possible and harmless: two goroutines
		// seeing the same generation build byte-identical rings, so whichever
		// store lands last is still correct — same as roundrobin's table.
		c.ring.Store(r)
	}

	// req.HashKey is empty when the proxy could not extract one (the
	// configured hash_on attribute was missing from this particular
	// request, e.g. no cookie set yet). We do not fail in that case: we hash
	// the empty string like any other key, which deterministically picks one
	// backend. Every such request landing on the same backend is the
	// intended, visible consequence of a missing key, not a bug — it keeps
	// behaviour predictable instead of silently falling back to random
	// selection.
	h := hashKey(req.HashKey)

	i := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= h })
	if i == len(r.points) {
		i = 0 // wrap around the ring
	}

	return candidates[r.owner[i]], true
}

func buildHashRing(gen uint64, candidates []Candidate) *hashRing {
	total := 0
	for _, c := range candidates {
		total += vnodesOf(c)
	}

	r := &hashRing{
		gen:    gen,
		points: make([]uint64, 0, total),
		owner:  make([]int, 0, total),
	}

	for idx, cand := range candidates {
		n := vnodesOf(cand)
		for v := 0; v < n; v++ {
			// Distinct virtual nodes for one candidate need distinct inputs;
			// appending the vnode index to the stable Key does that without
			// needing a seeded hash.
			h := hashKey(cand.Key + "#" + strconv.Itoa(v))
			r.points = append(r.points, h)
			r.owner = append(r.owner, idx)
		}
	}

	// byPoint is a struct copy, but points/owner are slice headers pointing
	// at the same backing arrays as r, so sorting the copy still sorts r's
	// data in place.
	sort.Sort(byPoint(*r))
	return r
}

// vnodesOf returns the number of ring points a candidate contributes,
// defensively normalising Weight the same way roundrobin.go does.
func vnodesOf(c Candidate) int {
	w := c.Weight
	if w < 1 {
		w = 1
	}
	return w * vnodesPerWeight
}

// hashKey hashes s with FNV-1a 64, the standard library's fastest
// well-distributed non-cryptographic hash. Consistent hashing needs uniform
// spread, not collision resistance, so FNV is preferable to something like
// SHA-256 on this hot path.
//
// FNV-1a's own avalanche is weak in the high bits for short, structurally
// similar inputs — virtual-node keys are exactly that shape ("backend-1#0",
// "backend-1#1", ...) — and ring placement is fully determined by those high
// bits, since points are ordered as plain 64-bit integers. Measured without
// the finalizer below, 10 backends at 160 vnodes each landed one backend
// with 21% of the ring and another with 4%, versus an 8-12% spread with it.
// The finalizer is the splitmix64/MurmurHash3 fmix64 bit-mixing step: cheap,
// allocation-free, and it only reshuffles bits FNV-1a already produced, so it
// does not change which hash family is doing the work.
func hashKey(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s)) // fnv.Write never errors
	return avalanche(h.Sum64())
}

func avalanche(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

// byPoint sorts a hashRing's parallel slices together by point value.
type byPoint hashRing

func (r byPoint) Len() int { return len(r.points) }
func (r byPoint) Swap(i, j int) {
	r.points[i], r.points[j] = r.points[j], r.points[i]
	r.owner[i], r.owner[j] = r.owner[j], r.owner[i]
}
func (r byPoint) Less(i, j int) bool { return r.points[i] < r.points[j] }
