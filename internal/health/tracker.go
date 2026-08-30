package health

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aaditya0602/manifold/internal/config"
	"github.com/aaditya0602/manifold/internal/upstream"
)

const (
	// numBuckets is the resolution of the sliding window. Ten buckets means
	// the window edge moves in tenth-of-a-window steps, so at most 10% of the
	// window's samples age out at once. The alternative — an exact per-sample
	// timestamp list — is unbounded in memory and allocates on the request
	// hot path, which is exactly what a load balancer cannot afford.
	numBuckets = 10

	// A bucket packs its epoch and both counters into one 64-bit word so a
	// sample is a single CAS: no per-backend mutex on the request path, and no
	// window where a rotating goroutine has zeroed the counters while another
	// goroutine is mid-increment. The split is 24/20/20.
	failBits  = 20
	totalBits = 20
	epochBits = 24

	countMask = 1<<totalBits - 1
	epochMask = 1<<epochBits - 1

	// maxBucketCount saturates each counter. At the default one-second bucket
	// that is a million requests per second to a single backend before the
	// count stops rising; the ratio the tracker actually cares about is barely
	// affected, and saturating is far better than wrapping into a nonsense
	// value that could eject a healthy backend.
	maxBucketCount = countMask

	// readmitSweepDivisor and its clamps set how often the sweeper looks for
	// expired ejections. Readmission is therefore late by up to one tick,
	// which is a deliberate trade: a timer per ejection would be exact but
	// would spawn goroutines proportional to the failure rate at the moment
	// the system is least healthy.
	readmitSweepDivisor = 5
	minReadmitSweep     = time.Millisecond
	maxReadmitSweep     = 250 * time.Millisecond
)

// Failed classifies a proxied request outcome for passive health.
//
// A failure is a connection-level error or a 5xx. A 4xx is NOT a failure: it
// is the backend correctly telling a client that the client is wrong. Counting
// 4xx here would mean a scanner spraying 404s, or one broken client sending
// malformed requests, could eject every healthy backend in the pool one after
// another and take the service down — a self-inflicted outage caused by the
// safety mechanism. This is a real and common failure mode of naive outlier
// detection, so the classification lives here, next to the window that acts on
// it, rather than being left to each call site to get right.
func Failed(status int, err error) bool {
	return err != nil || status >= 500
}

// bucket is one time slot of one backend's window.
type bucket struct {
	// v packs epoch<<40 | total<<20 | fail. Reads and writes go through
	// pack/unpack; nothing else may touch it.
	v atomic.Uint64
}

// window is one backend's ring of buckets.
type window struct {
	buckets [numBuckets]bucket
}

// Tracker is passive health checking for one pool: it ejects a backend whose
// real traffic is failing, and readmits it after eject_for so a recovered
// backend returns to service without operator action.
//
// Passive checking exists because active checking is not evidence about real
// requests. A backend can serve /healthz from a goroutine that is fine while
// its database pool is exhausted, and only actual traffic reveals that.
type Tracker struct {
	pool *upstream.Pool
	cfg  config.PassiveHealthConfig

	bucketDur time.Duration
	ejectFor  time.Duration

	// windows is indexed by backend ID and fixed at construction, so Record
	// needs no map lookup, no lock, and no allocation.
	windows []*window

	wg      sync.WaitGroup
	started atomic.Bool
}

// NewTracker builds the passive checker for a pool. Unlike NewProber it cannot
// fail: it has no client to construct, and a pool with passive checks disabled
// yields a Tracker whose Record and Start are no-ops.
func NewTracker(pool *upstream.Pool) *Tracker {
	t := &Tracker{}
	if pool == nil {
		return t
	}
	t.pool = pool
	t.cfg = pool.Config().Health.Passive
	if !t.cfg.Enabled {
		return t
	}

	windowDur := t.cfg.Window.D()
	if windowDur <= 0 {
		// Defensive: config validation rejects this. Disabling is the safe
		// degradation — a zero-length window would make every bucket the
		// current one and eject on the first failure.
		t.cfg.Enabled = false
		return t
	}
	t.bucketDur = windowDur / numBuckets
	if t.bucketDur <= 0 {
		t.bucketDur = time.Nanosecond
	}
	t.ejectFor = t.cfg.EjectFor.D()
	if t.cfg.MinRequests < 1 {
		// Defensive, as above: validation enforces >= 1. A zero minimum would
		// make the very first failed request an ejection.
		t.cfg.MinRequests = 1
	}

	backends := pool.Backends()
	t.windows = make([]*window, len(backends))
	for i := range t.windows {
		t.windows[i] = &window{}
	}
	return t
}

// Record notes the outcome of one proxied request. It is called on the request
// hot path by the proxy, once per attempt, so it does exactly one atomic CAS
// plus — only when the backend is still in rotation — numBuckets atomic loads
// to re-evaluate. It allocates nothing and is safe for concurrent callers.
//
// Evaluation happens here rather than on a timer because the useful moment to
// eject is the request that crossed the threshold, not up to a tick later; by
// then the balancer has sent more traffic to a backend it already had the
// evidence to remove.
func (t *Tracker) Record(backendID int, failed bool) {
	if !t.cfg.Enabled {
		return
	}
	if backendID < 0 || backendID >= len(t.windows) {
		return
	}

	now := time.Now()
	slot := now.UnixNano() / int64(t.bucketDur)
	t.add(t.windows[backendID], slot, failed)

	b := t.pool.Backend(backendID)
	if b == nil || b.Ejected() {
		// Already out of rotation. Samples keep being recorded (in-flight and
		// retried requests can still land here) but there is nothing to
		// decide: readmission is the sweeper's job and is time-based, not
		// outcome-based.
		return
	}
	t.evaluate(backendID, slot, now)
}

// add folds one sample into the bucket for slot.
func (t *Tracker) add(w *window, slot int64, failed bool) {
	epoch := uint64(slot) & epochMask
	b := &w.buckets[slot%numBuckets]

	for {
		cur := b.v.Load()
		curEpoch, total, fail := unpack(cur)
		if curEpoch != epoch {
			// The bucket holds a slot from a previous lap of the ring; the
			// sample that reclaims it also resets it. Reclaiming inside the
			// CAS is what makes rotation free of a race: a concurrent
			// increment either wins the CAS with the old epoch and loses to
			// this one, or sees the reclaimed value and adds to it.
			total, fail = 0, 0
		}
		if total < maxBucketCount {
			total++
			if failed && fail < maxBucketCount {
				fail++
			}
		}
		if b.v.CompareAndSwap(cur, pack(epoch, total, fail)) {
			return
		}
	}
}

// evaluate ejects the backend when the window justifies it.
func (t *Tracker) evaluate(backendID int, slot int64, now time.Time) {
	// Never eject unless a sweeper is running to readmit. Ejection happens
	// here on the request path with no goroutine required, but readmission is
	// time-based and only the sweeper started by Start performs it. A Tracker
	// that was used without being started would therefore eject backends
	// permanently -- a silent, unrecoverable capacity loss. Recording samples
	// without Start stays harmless; acting on them does not.
	if !t.started.Load() {
		return
	}

	total, fail := t.counts(t.windows[backendID], slot)

	// min_requests first: an error rate over three requests is noise, and
	// ejecting on it would make small pools flap under trivial load. Note
	// this also means a backend receiving almost no traffic is never ejected
	// passively — that gap is exactly what active checking covers.
	if total < uint64(t.cfg.MinRequests) {
		return
	}
	// Strictly greater: an error_rate of 1.0 must mean "every request failed",
	// not "all but one".
	if float64(fail)/float64(total) <= t.cfg.ErrorRate {
		return
	}

	if t.pool.Eject(backendID, now.Add(t.ejectFor)) {
		// Reset the window on ejection. Without it, a backend readmitted after
		// eject_for is judged on samples collected before it was removed, so
		// the first request after readmission re-ejects it instantly and the
		// backend never gets a real second chance. Readmission must start from
		// an empty window.
		t.reset(t.windows[backendID])
	}
}

// counts sums the window ending at slot. Buckets whose epoch does not match
// one of the last numBuckets slots have aged out and are skipped, which is how
// the window slides without anyone having to clear them.
func (t *Tracker) counts(w *window, slot int64) (total, fail uint64) {
	for i := int64(0); i < numBuckets; i++ {
		s := slot - i
		b := &w.buckets[((s%numBuckets)+numBuckets)%numBuckets]
		epoch, bt, bf := unpack(b.v.Load())
		if epoch != uint64(s)&epochMask {
			continue
		}
		total += bt
		fail += bf
	}
	return total, fail
}

// reset clears every bucket in a window.
func (t *Tracker) reset(w *window) {
	for i := range w.buckets {
		w.buckets[i].v.Store(0)
	}
}

// Start launches the readmission sweeper and returns immediately. It exits
// when ctx is cancelled; Wait blocks until it has.
func (t *Tracker) Start(ctx context.Context) {
	if !t.cfg.Enabled {
		return
	}
	if !t.started.CompareAndSwap(false, true) {
		return
	}

	t.wg.Add(1)
	go t.sweep(ctx)
}

// Wait blocks until every goroutine started by Start has exited.
func (t *Tracker) Wait() { t.wg.Wait() }

// sweep readmits backends whose ejection has expired.
//
// A single sweeper for the whole pool, rather than a timer per ejection, keeps
// the goroutine count constant no matter how badly the pool is failing.
func (t *Tracker) sweep(ctx context.Context) {
	defer t.wg.Done()

	ticker := time.NewTicker(t.sweepInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.readmitExpired(time.Now())
		}
	}
}

// readmitExpired returns every backend whose ejection deadline has passed.
func (t *Tracker) readmitExpired(now time.Time) {
	for id := range t.windows {
		b := t.pool.Backend(id)
		if b == nil || !b.Ejected() {
			continue
		}
		if now.Before(t.pool.EjectedUntil(id)) {
			continue
		}
		if t.pool.Readmit(id) {
			// Clean slate again: requests that landed on this backend while
			// it was ejected (in flight at ejection time, or retries) left
			// samples behind, and judging the readmitted backend on them
			// would re-eject it before it has served anything new.
			t.reset(t.windows[id])
		}
	}
}

// sweepInterval bounds how late a readmission can be.
func (t *Tracker) sweepInterval() time.Duration {
	d := t.ejectFor / readmitSweepDivisor
	if d < minReadmitSweep {
		d = minReadmitSweep
	}
	if d > maxReadmitSweep {
		d = maxReadmitSweep
	}
	return d
}

// The epoch is masked to 24 bits, so a bucket left untouched for exactly 2^24
// slots — 194 days at the default one-second bucket — could be misread as
// current. That requires a backend to receive no traffic for that long and
// then a request to land on the same ring index in the same phase; the next
// sample corrects it, and the cost is bounded by one stale bucket.
func pack(epoch, total, fail uint64) uint64 {
	return epoch<<(totalBits+failBits) | total<<failBits | fail
}

func unpack(v uint64) (epoch, total, fail uint64) {
	return v >> (totalBits + failBits) & epochMask, v >> failBits & countMask, v & countMask
}
