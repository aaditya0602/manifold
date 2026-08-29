// Command manifold is an L7 HTTP reverse proxy and load balancer.
//
// It runs two listeners: a data plane carrying client traffic, and an admin
// plane carrying operational endpoints. Keeping them apart means /metrics and
// pprof are never reachable from whatever can reach the proxy, and admin
// traffic never shows up in the benchmark numbers.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/aaditya0602/manifold/internal/config"
	"github.com/aaditya0602/manifold/internal/proxy"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		configPath  = flag.String("config", "config.yaml", "path to the YAML configuration file")
		checkOnly   = flag.Bool("check", false, "validate the configuration and exit")
		logLevel    = flag.String("log-level", "info", "debug, info, warn, or error")
		showVersion = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Println(versionString())
		return nil
	}

	logger, err := newLogger(*logLevel)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config %s: %w", *configPath, err)
	}

	// -check exists so a deploy can validate a config file before restarting
	// anything. Hot reload (Week 3) will use the same code path.
	if *checkOnly {
		fmt.Printf("%s: ok\n", *configPath)
		return nil
	}

	px, err := proxy.New(cfg)
	if err != nil {
		return fmt.Errorf("build proxy: %w", err)
	}
	defer px.Close()

	dataSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           px,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout.D(),
		ReadTimeout:       cfg.Server.ReadTimeout.D(),
		WriteTimeout:      cfg.Server.WriteTimeout.D(),
		IdleTimeout:       cfg.Server.IdleTimeout.D(),
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}

	adminSrv := &http.Server{
		Addr:              cfg.Admin,
		Handler:           adminHandler(),
		ReadHeaderTimeout: 5 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}

	// A server that fails to bind must take the process down rather than
	// leaving a half-running proxy that looks healthy.
	errCh := make(chan error, 2)
	go func() { errCh <- serve("data", dataSrv) }()
	go func() { errCh <- serve("admin", adminSrv) }()

	slog.Info("manifold started",
		"version", versionString(),
		"listen", cfg.Listen,
		"admin", cfg.Admin,
		"pools", len(cfg.Pools),
		"routes", len(cfg.Routes),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		// Stop trapping signals so a second Ctrl-C aborts a stuck drain
		// instead of being swallowed.
		stop()
	}

	return shutdown(dataSrv, adminSrv, cfg.Server.DrainTimeout.D())
}

func serve(name string, srv *http.Server) error {
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s listener on %s: %w", name, srv.Addr, err)
	}
	return nil
}

// shutdown drains both listeners: they stop accepting immediately, in-flight
// requests are given up to drainTimeout to finish, and anything still running
// after that is cut off. Draining both concurrently means a slow data-plane
// request cannot hold the admin listener open past the deadline.
func shutdown(data, admin *http.Server, drainTimeout time.Duration) error {
	slog.Info("draining", "timeout", drainTimeout)
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()

	errs := make(chan error, 2)
	for _, srv := range []*http.Server{data, admin} {
		go func(s *http.Server) { errs <- s.Shutdown(ctx) }(srv)
	}

	var err error
	for i := 0; i < 2; i++ {
		err = errors.Join(err, <-errs)
	}

	if errors.Is(err, context.DeadlineExceeded) {
		slog.Warn("drain timed out, cutting off remaining requests", "after", time.Since(start))
	} else {
		slog.Info("drained cleanly", "after", time.Since(start))
	}
	return err
}

// adminHandler serves the operational endpoints. Prometheus /metrics is added
// in Week 2; the handler exists now so the port contract is stable.
func adminHandler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})

	mux.HandleFunc("GET /version", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = fmt.Fprintln(w, versionString())
	})

	// Registered explicitly rather than via the net/http/pprof package's
	// init-time DefaultServeMux side effect, so profiling endpoints can only
	// ever appear on the admin listener.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return mux
}

func newLogger(level string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid -log-level %q: %w", level, err)
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})), nil
}

// versionString reports the VCS revision stamped in by the Go toolchain, so a
// benchmark result can be tied back to the exact commit that produced it.
func versionString() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "manifold (unknown build)"
	}
	rev, dirty := "unknown", ""
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
			if len(rev) > 12 {
				rev = rev[:12]
			}
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "-dirty"
			}
		}
	}
	return fmt.Sprintf("manifold %s%s (%s)", rev, dirty, info.GoVersion)
}
