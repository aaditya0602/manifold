package config

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// watchDebounce is short enough to keep the tests quick and long enough to
// coalesce a burst of writes on a loaded CI machine.
const watchDebounce = 60 * time.Millisecond

// startWatch runs Watch in the background and returns a channel that receives
// one value per reported change, plus a counter of the same events.
//
// Establishing an OS-level watch is asynchronous with respect to the goroutine
// that asked for it, so every test here either retries its write until the
// first change lands (see waitLive) or performs its writes only after that.
// Sleeping a fixed amount instead is how a filesystem test becomes flaky on
// somebody else's machine.
func startWatch(t *testing.T, path string) (<-chan struct{}, *atomic.Int64) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	changes := make(chan struct{}, 64)
	var count atomic.Int64
	done := make(chan error, 1)

	go func() {
		done <- Watch(ctx, path, watchDebounce, func() {
			count.Add(1)
			select {
			case changes <- struct{}{}:
			default:
			}
		})
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Watch returned %v, want nil on cancellation", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Watch did not return after its context was cancelled")
		}
	})

	return changes, &count
}

// waitLive writes to path until a change is reported, which proves the watch
// is established, then waits out the quiet period so the counter can be reset
// to a known zero.
func waitLive(t *testing.T, path string, changes <-chan struct{}, count *atomic.Int64) {
	t.Helper()

	deadline := time.After(10 * time.Second)
	for {
		if err := os.WriteFile(path, []byte("listen: \":1\"\n"), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		select {
		case <-changes:
			// Let any trailing events from this burst expire, then drain.
			time.Sleep(4 * watchDebounce)
			for {
				select {
				case <-changes:
					continue
				default:
				}
				break
			}
			count.Store(0)
			return
		case <-time.After(200 * time.Millisecond):
		case <-deadline:
			t.Fatal("watch never reported a change; it was not established")
		}
	}
}

func atomicReplace(t *testing.T, path, content string) {
	t.Helper()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	// The rename, not the write, is the event a real config update produces.
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename over %s: %v", path, err)
	}
}

// TestWatch_DetectsAtomicReplace is the case a naive fsnotify.Write watch on
// the file itself misses entirely.
//
// Editors, kubectl, Helm and every deployment tool worth using write a
// temporary file and rename it over the target, because that is the only way
// to make the replacement atomic for a concurrent reader. The rename swaps the
// inode; a watch registered on the old inode sees the change at most once and
// is then permanently deaf. Hence the second replacement below: detecting one
// rename proves nothing, because the naive implementation frequently manages
// the first one too.
func TestWatch_DetectsAtomicReplace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("listen: \":1\"\n"), 0o600); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	changes, count := startWatch(t, path)
	waitLive(t, path, changes, count)

	for i, content := range []string{"listen: \":2\"\n", "listen: \":3\"\n"} {
		atomicReplace(t, path, content)
		select {
		case <-changes:
		case <-time.After(5 * time.Second):
			t.Fatalf("atomic replace %d was not detected; the watch went deaf after a rename", i+1)
		}
	}
}

// TestWatch_DebouncesBurst asserts that one save is one reload.
//
// A single editor save emits several inotify events, and a rebuild per event
// means the proxy is reconstructed three times for one change — with the
// intermediate reads frequently landing on a half-written file, which reports
// failures for a config that was never wrong.
func TestWatch_DebouncesBurst(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("listen: \":1\"\n"), 0o600); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	changes, count := startWatch(t, path)
	waitLive(t, path, changes, count)

	// A burst: writes, a rename, more writes, all well inside one debounce
	// period.
	for i := 0; i < 4; i++ {
		if err := os.WriteFile(path, []byte("listen: \":9\"\n"), 0o600); err != nil {
			t.Fatalf("burst write: %v", err)
		}
	}
	atomicReplace(t, path, "listen: \":10\"\n")

	select {
	case <-changes:
	case <-time.After(5 * time.Second):
		t.Fatal("burst produced no change at all")
	}

	// Well past the debounce window: anything the burst was going to produce
	// has produced it by now.
	time.Sleep(6 * watchDebounce)
	if got := count.Load(); got != 1 {
		t.Errorf("burst of 5 writes produced %d reloads, want exactly 1", got)
	}
}

// TestWatch_IgnoresOtherFiles keeps the directory watch from turning every
// neighbouring file into a proxy rebuild. Config files routinely live in a
// directory with other things in it.
func TestWatch_IgnoresOtherFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("listen: \":1\"\n"), 0o600); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	changes, count := startWatch(t, path)
	waitLive(t, path, changes, count)

	for i := 0; i < 3; i++ {
		other := filepath.Join(dir, "unrelated.yaml")
		if err := os.WriteFile(other, []byte("noise\n"), 0o600); err != nil {
			t.Fatalf("write neighbour: %v", err)
		}
	}

	time.Sleep(6 * watchDebounce)
	if got := count.Load(); got != 0 {
		t.Errorf("writes to a neighbouring file produced %d reloads, want 0", got)
	}
}
