package config

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

// DefaultDebounce is the quiet period Watch waits for after the last event
// that touched the config file before it reports a change.
//
// One save is not one event. An editor that writes in place emits CREATE
// (temp) + WRITE + WRITE + RENAME + CHMOD within a few milliseconds; a
// kubectl-driven ConfigMap update swaps a whole directory of symlinks. Acting
// on each event would rebuild the proxy several times per save, and — worse —
// the intermediate states are frequently unparseable, so an undebounced
// watcher reports failures for configs that were never wrong.
//
// 250ms is long enough to coalesce a save and short enough that an operator
// does not notice the delay.
const DefaultDebounce = 250 * time.Millisecond

// Watch calls onChange whenever the file at path is created, written,
// renamed over, or replaced, and blocks until ctx is cancelled. It returns
// nil on cancellation and an error only if the watch could not be
// established or was lost irrecoverably.
//
// It watches the file's *parent directory*, not the file. Watching the file
// works only for the one write pattern nobody uses: an in-place, truncating
// write. Editors, `kubectl`, Helm, and every config-management tool worth the
// name write a temporary file and rename it over the target, because that is
// the only way to make the replacement atomic for readers. A rename replaces
// the inode, and an inotify/ReadDirectoryChangesW watch is bound to the
// object, not the name — so a file watch sees the change at most once and
// then goes permanently deaf, watching an unlinked inode that nobody will
// ever write to again. A directory watch sees the rename as an event on the
// directory and keeps working forever. Kubernetes is the extreme case: the
// mounted path is a symlink into a timestamped directory, and the file the
// symlink pointed at is never modified at all.
//
// Events for other files in the directory are filtered out by name, so a busy
// directory costs a string comparison per event and nothing else.
//
// debounce <= 0 uses DefaultDebounce.
func Watch(ctx context.Context, path string, debounce time.Duration, onChange func()) error {
	if debounce <= 0 {
		debounce = DefaultDebounce
	}

	abs, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("config: watch %s: %w", path, err)
	}
	dir := filepath.Dir(abs)

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("config: watch %s: %w", path, err)
	}
	defer func() { _ = w.Close() }()

	if err := w.Add(dir); err != nil {
		return fmt.Errorf("config: watch %s: %w", dir, err)
	}

	// Stopped timer as the debounce clock. It is created already drained so
	// the first event starts the period rather than firing one immediately.
	timer := time.NewTimer(debounce)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	pending := false

	for {
		select {
		case <-ctx.Done():
			return nil

		case ev, ok := <-w.Events:
			if !ok {
				return nil
			}
			if !affects(ev, abs, dir) {
				continue
			}
			// Re-arm rather than accumulate: the period is measured from the
			// last event of a burst, so a multi-write save fires once.
			if pending && !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(debounce)
			pending = true

		case <-timer.C:
			pending = false
			// Re-establish the directory watch if the directory itself was
			// replaced. Watching a directory survives any rename *inside* it,
			// but not a rename *of* it — which is exactly what a Kubernetes
			// ConfigMap update does to the ..data directory. Re-adding a
			// directory already watched is a no-op in fsnotify, so this costs
			// one syscall per debounced change and removes a whole class of
			// "reload worked once and then stopped" bug.
			_ = w.Add(dir)
			onChange()

		case err, ok := <-w.Errors:
			if !ok {
				return nil
			}
			return fmt.Errorf("config: watch %s: %w", path, err)
		}
	}
}

// affects reports whether an event concerns the watched file.
//
// It matches the file itself and also the directory, because a directory
// event means the container of the file changed under us (the ConfigMap
// symlink-swap case) and the file must be re-read even though its own name
// never appeared in an event.
func affects(ev fsnotify.Event, abs, dir string) bool {
	// Chmod alone is not a content change; ignoring it keeps a `chmod` or an
	// atime update from rebuilding the proxy. Every writer that changes bytes
	// also emits Write, Create, Rename or Remove.
	if ev.Op == fsnotify.Chmod {
		return false
	}
	name := filepath.Clean(ev.Name)
	return samePath(name, abs) || samePath(name, dir)
}

// samePath compares two cleaned absolute paths using the filesystem's own
// case rules. Windows and macOS are case-insensitive by default, and an
// event whose name differs only in case from the configured path is still an
// event about that file.
func samePath(a, b string) bool {
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		return strings.EqualFold(a, b)
	}
	return a == b
}
