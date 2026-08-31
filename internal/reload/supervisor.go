// Package reload implements manifold's hot configuration reload: a new
// *proxy.Server is built from the new file, started, swapped in atomically,
// and only then is the previous one drained and closed.
//
// The invariant the whole package exists to protect is that a reload is
// invisible to a client. That means three things, and each one dictates a
// piece of the design below.
//
//  1. The request path never locks. The live server is held in an
//     atomic.Pointer and loaded once per request. A RWMutex would work and
//     would be wrong: the reader side is every request in the process, and the
//     writer side is a rebuild that takes milliseconds, so the one moment a
//     reload happens is the one moment every request in flight queues behind a
//     writer.
//
//  2. A bad config never takes down a running proxy. Loading, validating and
//     building all happen before anything is swapped, and any failure leaves
//     the previous generation serving. A typo in a config file must produce a
//     log line, not an outage.
//
//  3. The old server is closed only after it is idle. Requests that were
//     already dispatched still hold the old *proxy.Server; closing it out from
//     under them releases its pools' connections mid-response. So the swap
//     comes first, the drain second, bounded by server.drain_timeout.
package reload

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aaditya0602/manifold/internal/config"
	"github.com/aaditya0602/manifold/internal/proxy"
)

// Results reported to Options.OnResult, matching the label values of
// manifold_config_reloads_total{result}.
const (
	ResultSuccess = "success"
	ResultFailure = "failure"
)

// ErrClosed is returned by Reload after the Supervisor has been closed.
var ErrClosed = errors.New("reload: supervisor is closed")

// fallbackDrainTimeout bounds a drain when the config carries no positive
// server.drain_timeout. A validated config always does; this covers the
// hand-built Config a test can pass, where an unbounded wait would turn a bug
// into a hung suite rather than a failure.
const fallbackDrainTimeout = 15 * time.Second

// Builder constructs a Server from a validated config. It exists so the
// process can inject proxy.NewWithMetrics (which needs the registry that only
// main owns) and so tests can inject a builder that fails on demand.
type Builder func(*config.Config) (*proxy.Server, error)

// Options configures a Supervisor. The zero value is usable.
type Options struct {
	// Build defaults to proxy.New.
	Build Builder
	// Logger defaults to slog.Default().
	Logger *slog.Logger
	// OnResult is called once per reload attempt with ResultSuccess or
	// ResultFailure. It is the seam for manifold_config_reloads_total{result},
	// kept as a callback so this package does not import observe and so a
	// Supervisor in a test is not obliged to own a metrics registry.
	OnResult func(result string)
}

// generation is one config epoch: the server built from it, plus the number of
// requests currently inside that server.
//
// The in-flight counter lives here rather than in proxy.Server because
// proxy.Server has no such counter and this package may not add one -- but it
// belongs here regardless. The count that decides when an old server may be
// closed is "requests that loaded this pointer and have not yet returned",
// which is a property of the handover, not of the proxy. A counter inside
// proxy.Server could not see the window that actually matters: the instant
// between a request loading the old pointer and entering its ServeHTTP.
// Counting on this side of the delegation closes that window (see acquire),
// and it keeps proxy.Server free of state only the supervisor's lifecycle
// reads.
type generation struct {
	srv *proxy.Server
	cfg *config.Config
	num uint64

	inflight atomic.Int64
	// retiring is set once the generation has been swapped out and is being
	// drained. Together with inflight it forms the handoff that lets the last
	// request out of the door signal idle, rather than making the drain poll
	// for a zero it would notice late.
	retiring  atomic.Bool
	idle      chan struct{}
	closeIdle sync.Once
}

func (g *generation) release() {
	// The store to retiring and this decrement race by construction, and Go's
	// memory model gives sync/atomic operations sequentially consistent
	// semantics, so exactly one of two orderings is observable: either this
	// goroutine sees retiring and signals, or retire sees this decrement in
	// its own inflight load and signals itself. Both cannot miss it, so a
	// drain cannot hang on a request that has already finished.
	if g.inflight.Add(-1) == 0 && g.retiring.Load() {
		g.signalIdle()
	}
}

func (g *generation) signalIdle() { g.closeIdle.Do(func() { close(g.idle) }) }

// retire marks the generation as draining and returns a channel closed when
// its last in-flight request has returned.
func (g *generation) retire() <-chan struct{} {
	g.retiring.Store(true)
	if g.inflight.Load() == 0 {
		g.signalIdle()
	}
	return g.idle
}

// Supervisor is the process-lifetime http.Handler. It is what
// http.Server.Handler is set to, once, and never swapped: mutating the fields
// of a running http.Server races its accept loop, and rebuilding the listener
// would drop every connection it is holding open -- precisely the failure this
// feature exists to avoid.
type Supervisor struct {
	path     string
	build    Builder
	log      *slog.Logger
	onResult func(string)

	current atomic.Pointer[generation]

	// mu serialises reloads against each other and against Close, so a SIGHUP
	// arriving while the file watcher is mid-reload queues behind it instead
	// of building a second server concurrently. It is never taken by
	// ServeHTTP.
	mu     sync.Mutex
	ctx    context.Context
	num    uint64
	closed bool
}

// New returns a Supervisor already serving srv, which must have been built
// from cfg and need not have been started. path is the file consulted by
// SIGHUP and by the watcher.
func New(path string, cfg *config.Config, srv *proxy.Server, opts Options) *Supervisor {
	s := &Supervisor{
		path:     path,
		build:    opts.Build,
		log:      opts.Logger,
		onResult: opts.OnResult,
		ctx:      context.Background(),
		num:      1,
	}
	if s.build == nil {
		s.build = proxy.New
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	s.current.Store(&generation{srv: srv, cfg: cfg, num: 1, idle: make(chan struct{})})
	return s
}

// Start starts the current server's health checkers and records ctx as the
// lifetime of every server a later reload builds, so cancelling it stops
// health checking for whichever generation is live at the time.
func (s *Supervisor) Start(ctx context.Context) {
	s.mu.Lock()
	s.ctx = ctx
	s.mu.Unlock()
	s.current.Load().srv.Start(ctx)
}

// ServeHTTP delegates to the live server. On top of the proxy's own work it
// costs two atomic loads and two atomic adds, and no locks.
func (s *Supervisor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g := s.acquire()
	defer g.release()
	g.srv.ServeHTTP(w, r)
}

// acquire pins the live generation for the duration of one request.
//
// The re-check is the whole point and is not paranoia. Load, increment, load
// again: if the second load still sees the same generation then the swap that
// retires it has not happened yet, and since a drain only ever reads inflight
// *after* storing the new pointer, that drain is guaranteed to see this
// increment. Without the re-check there is a real window -- load the pointer,
// get descheduled, have a whole reload swap and drain to zero, then wake up
// and hand a request to a server that has already been closed.
func (s *Supervisor) acquire() *generation {
	for {
		g := s.current.Load()
		g.inflight.Add(1)
		if s.current.Load() == g {
			return g
		}
		// Lost the race with a swap: back out and take the new generation.
		// The decrement goes through release, not a bare Add, because the
		// generation being backed out of may already be draining and this may
		// be its last reference.
		g.release()
	}
}

// Config returns the config the live generation was built from.
func (s *Supervisor) Config() *config.Config { return s.current.Load().cfg }

// Reload loads path, builds a new server from it, swaps it in, and drains the
// previous one. On any failure it logs, reports ResultFailure, and leaves the
// running proxy exactly as it was.
func (s *Supervisor) Reload(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}

	start := time.Now()
	old := s.current.Load()

	// 1. Parse and validate. A file that does not parse, or that fails
	//    validation, ends the reload here -- before anything has been built,
	//    let alone swapped.
	cfg, err := config.Load(path)
	if err != nil {
		return s.fail("config is invalid, keeping the running configuration", path, err)
	}

	// 2. Build. This is the other half of what can be wrong with a config, and
	//    the half the schema cannot catch: an upstream URL that does not
	//    resolve, a strategy with no implementation, a route naming a pool
	//    that is not there. Same rule as step 1 -- the running proxy is
	//    untouched until this succeeds.
	srv, err := s.build(cfg)
	if err != nil {
		return s.fail("new configuration could not be built, keeping the running configuration", path, err)
	}

	// 3. Start health checking before the swap, so the new pools have already
	//    formed an opinion about their backends by the time they take their
	//    first request.
	srv.Start(s.ctx)

	s.warnUnappliable(old.cfg, cfg)

	// 4. The swap. Every request that loads the pointer from here on serves
	//    from the new configuration.
	s.num++
	s.current.Store(&generation{srv: srv, cfg: cfg, num: s.num, idle: make(chan struct{})})

	// 5. Only now is the old server drained. Requests dispatched before the
	//    swap are still executing inside it.
	drained := s.drain(old, cfg.Server.DrainTimeout.D())

	s.log.Info("config reloaded",
		"path", path,
		"generation", s.num,
		"pools", len(cfg.Pools),
		"routes", len(cfg.Routes),
		"drained", drained,
		"took", time.Since(start),
	)
	s.report(ResultSuccess)
	return nil
}

// drain waits for a retired generation to go idle, then closes it. It reports
// whether the drain completed within the budget.
//
// The close is unconditional: past the deadline the old server's health
// checkers and pooled connections have to be released, or a process reloaded
// every minute with one wedged request per reload leaks a prober set an hour.
// The requests still inside it are the ones drain_timeout declares forfeit,
// which is the same bargain the process-wide shutdown drain makes.
func (s *Supervisor) drain(g *generation, timeout time.Duration) bool {
	if timeout <= 0 {
		timeout = fallbackDrainTimeout
	}
	idle := g.retire()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-idle:
		g.srv.Close()
		return true
	case <-timer.C:
		s.log.Warn("previous configuration did not drain in time, closing anyway",
			"generation", g.num, "timeout", timeout, "in_flight", g.inflight.Load())
		g.srv.Close()
		return false
	}
}

// Close drains and closes the live generation. It does not stop serving:
// callers shut their listeners down first, so by the time Close runs there is
// nothing left to serve. It is idempotent.
func (s *Supervisor) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	g := s.current.Load()
	s.drain(g, g.cfg.Server.DrainTimeout.D())
}

// HandleSignals reloads once per signal received on ch, until ch is closed or
// ctx is cancelled. It is a plain function over a channel rather than a
// signal.Notify call inside, so a test can deliver a SIGHUP without signalling
// the test binary's own process.
func (s *Supervisor) HandleSignals(ctx context.Context, ch <-chan os.Signal) {
	for {
		select {
		case <-ctx.Done():
			return
		case sig, ok := <-ch:
			if !ok {
				return
			}
			s.log.Info("reload requested", "signal", sig.String(), "path", s.path)
			if err := s.Reload(s.path); err != nil {
				// Already logged, with its reason, by Reload. A failed reload
				// is not a reason to stop listening for the next one: the
				// operator's next move is to fix the file and signal again.
				continue
			}
		}
	}
}

// Watch reloads whenever the config file changes on disk, until ctx is
// cancelled. debounce <= 0 uses config.DefaultDebounce.
func (s *Supervisor) Watch(ctx context.Context, debounce time.Duration) error {
	return config.Watch(ctx, s.path, debounce, func() {
		s.log.Info("config file changed", "path", s.path)
		if err := s.Reload(s.path); err != nil {
			return
		}
	})
}

// fail logs a failed reload, reports it, and returns the error.
func (s *Supervisor) fail(msg, path string, err error) error {
	s.log.Error("config reload failed: "+msg, "path", path, "err", err)
	s.report(ResultFailure)
	return fmt.Errorf("reload %s: %w", path, err)
}

func (s *Supervisor) report(result string) {
	if s.onResult != nil {
		s.onResult(result)
	}
}

// warnUnappliable logs the settings a reload cannot apply.
//
// Listener addresses and the server-side timeouts are properties of the
// running http.Server and its bound sockets, not of the handler. Applying them
// would mean rebuilding the listener, which closes every connection currently
// held open by keep-alive -- a reload that drops connections in order to
// change a timeout is worse than one that does not change it. They are
// ignored; this makes them loudly ignored, which is the difference between
// "manifold is broken" and "that field needs a restart".
func (s *Supervisor) warnUnappliable(old, next *config.Config) {
	if old.Listen != next.Listen {
		s.log.Warn("listen address change needs a restart, ignoring",
			"running", old.Listen, "in_file", next.Listen)
	}
	if old.Admin != next.Admin {
		s.log.Warn("admin address change needs a restart, ignoring",
			"running", old.Admin, "in_file", next.Admin)
	}
	if old.Server != next.Server {
		s.log.Warn("server timeout changes need a restart, ignoring")
	}
}

// Compile-time assertion that the Supervisor is what http.Server.Handler gets.
var _ http.Handler = (*Supervisor)(nil)
