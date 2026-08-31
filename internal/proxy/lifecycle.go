package proxy

import (
	"context"
	"log/slog"

	"github.com/aaditya0602/manifold/internal/observe"
)

// Start begins health checking for every pool and returns immediately.
//
// It is separate from New on purpose. New must be free of side effects: it is
// called by `manifold -check`, by tests that only exercise routing, and by the
// hot reload path that builds a second Server before deciding whether to swap
// it in. None of those should put probe traffic on somebody's backends, and a
// Server that starts goroutines in its constructor has no way to avoid it.
//
// The seam matters for reload specifically. Reload builds the new Server,
// Starts it, swaps the handler pointer, then Closes the old one -- so the new
// pools have already formed a health opinion before they take traffic, and the
// old pools' probers are shut down deterministically rather than left to a
// finaliser that does not exist.
//
// ctx bounds the checkers' lifetime; so does Close, whichever comes first.
// Calling Start twice is a no-op after the first, and calling it after Close
// does nothing at all -- a closed Server stays closed rather than resurrecting
// goroutines that Close has already waited for.
func (s *Server) Start(ctx context.Context) {
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()

	if s.started || s.closed {
		return
	}
	s.started = true

	// Derived so Close can stop the checkers without the caller's context
	// having to be cancellable -- Close must work for a Server started with
	// context.Background().
	ctx, s.cancel = context.WithCancel(ctx)

	for _, p := range s.probers {
		p.Start(ctx)
	}
	for _, t := range s.trackers {
		t.Start(ctx)
	}
}

// Close stops health checking, waits for every checker goroutine to exit, and
// releases the pools' idle connections.
//
// The wait is not a nicety. A prober goroutine outliving its Server keeps
// probing origins that the test which created them has already shut down, and
// -- worse in a test binary -- it keeps mutating a Pool that the next test
// believes it owns exclusively. That is how a suite acquires failures that
// only reproduce under -count=5 and only on the machine you do not have. The
// goroutine's exit has to be observed, not assumed.
//
// Close is safe without Start (there is nothing to cancel and the WaitGroups
// are empty) and safe to call twice: it is idempotent rather than
// panic-on-second-call, because the natural shape at a call site is a
// `defer srv.Close()` plus an explicit Close on the drain path, and neither
// should have to know about the other.
func (s *Server) Close() {
	s.lifeMu.Lock()
	if s.closed {
		s.lifeMu.Unlock()
		return
	}
	s.closed = true
	cancel := s.cancel
	s.cancel = nil
	s.lifeMu.Unlock()

	// The lock is released before waiting. Holding it across Wait would mean a
	// second Close -- or a Start racing a shutdown -- blocks for as long as the
	// slowest in-flight probe, and a lock held across an arbitrary wait is a
	// deadlock looking for a reason.
	if cancel != nil {
		cancel()
	}
	for _, p := range s.probers {
		p.Wait()
	}
	for _, t := range s.trackers {
		t.Wait()
	}

	// Unregister this generation's scrape-time collectors before releasing the
	// pools they read from. A hot reload builds a replacement Server whose
	// pools register under the same names; if the retired generation stays
	// registered, both emit manifold_upstream_inflight with identical labels
	// and the registry rejects the whole exposition. /metrics then returns 500
	// permanently from the first reload onward while the data plane stays
	// perfectly healthy -- an observability outage that only appears in
	// production, and only after someone changes the config.
	if s.metrics != nil {
		for _, c := range s.collectors {
			s.metrics.UnregisterPoolCollector(c)
		}
	}

	s.reg.Close()
}

// availabilityHook builds the subscriber registered on each pool.
//
// Two things happen on every transition, and both exist because the other one
// is not enough. The counter survives sampling -- a gauge scraped every 15s
// cannot show a backend that flapped out and back in between two scrapes -- but
// it is only visible to someone already looking at Prometheus. The log line is
// what an operator reading stderr during an incident actually sees, and a
// backend silently leaving rotation with no log line is the difference between
// "we found it in a minute" and "we found it in an hour".
//
// upstreams is keyed by backend key because that is what the hook is handed;
// the map is fixed at construction, so it is read-only here and needs no lock.
//
// Pool documents that the hook runs outside every Pool lock and may be
// re-entrant, so this deliberately does not call back into the Pool: nothing
// here reads Candidates, Backend, or any availability state. Everything it
// reports comes from its arguments. It also must not block, since it runs on a
// prober goroutine or on a request recording a passive outcome -- an slog
// handler write and an atomic add are the whole budget.
func availabilityHook(poolName string, upstreams map[string]*observe.UpstreamMetrics) func(string, bool) {
	return func(backendKey string, available bool) {
		if um := upstreams[backendKey]; um != nil {
			um.AvailabilityChange(available)
		}

		// Warn for leaving rotation, Info for returning. The asymmetry is the
		// point: losing capacity is the event worth paging on, regaining it is
		// the event worth correlating against afterwards. Both carry the same
		// keys so they can be grepped as one stream.
		if available {
			slog.Info("upstream available",
				"pool", poolName, "upstream", backendKey, "state", "available")
			return
		}
		slog.Warn("upstream unavailable",
			"pool", poolName, "upstream", backendKey, "state", "unavailable")
	}
}
