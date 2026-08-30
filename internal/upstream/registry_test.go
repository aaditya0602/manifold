package upstream

import (
	"testing"

	"github.com/aaditya0602/manifold/internal/config"
)

func TestNewRegistry(t *testing.T) {
	cfg := &config.Config{
		Pools: []config.PoolConfig{
			poolCfg("api", "http://127.0.0.1:9001", "http://127.0.0.1:9002"),
			poolCfg("static", "http://127.0.0.1:9101"),
		},
	}

	r, err := NewRegistry(cfg)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer r.Close()

	if _, ok := r.Pool("api"); !ok {
		t.Error("pool api missing")
	}
	if _, ok := r.Pool("static"); !ok {
		t.Error("pool static missing")
	}
	if _, ok := r.Pool("nope"); ok {
		t.Error("Pool returned ok for an unknown name")
	}

	pools := r.Pools()
	if len(pools) != 2 {
		t.Fatalf("Pools() length = %d, want 2", len(pools))
	}
	// Config order, so startup logs and admin output are stable.
	if pools[0].Name() != "api" || pools[1].Name() != "static" {
		t.Fatalf("Pools() order = %q, %q; want config order", pools[0].Name(), pools[1].Name())
	}

	// The returned slice must not alias the registry's own storage.
	pools[0] = nil
	if r.Pools()[0] == nil {
		t.Fatal("Pools() returned aliased storage")
	}
}

func TestNewRegistry_Rejects(t *testing.T) {
	t.Run("nil config", func(t *testing.T) {
		if _, err := NewRegistry(nil); err == nil {
			t.Fatal("want an error")
		}
	})

	t.Run("duplicate pool name", func(t *testing.T) {
		cfg := &config.Config{Pools: []config.PoolConfig{
			poolCfg("api", "http://127.0.0.1:9001"),
			poolCfg("api", "http://127.0.0.1:9002"),
		}}
		if _, err := NewRegistry(cfg); err == nil {
			t.Fatal("want an error for a duplicate pool name")
		}
	})

	t.Run("bad pool fails the whole registry", func(t *testing.T) {
		// Any pool that cannot be built will do; a URL with no scheme is
		// the cheapest. (This used to use least_conn, which was
		// unimplemented and is not any more.)
		bad := poolCfg("broken", "127.0.0.1:9002")

		cfg := &config.Config{Pools: []config.PoolConfig{
			poolCfg("api", "http://127.0.0.1:9001"),
			bad,
		}}
		r, err := NewRegistry(cfg)
		if err == nil {
			t.Fatal("want an error; a partially built registry would 503 a slice of traffic silently")
		}
		if r != nil {
			t.Fatal("a failed NewRegistry must return a nil registry")
		}
	})
}

// TestRegistry_CloseIsSafe: Close runs on the reload path after a drain, and a
// second Close (or a Close on an empty registry) must not panic.
func TestRegistry_CloseIsSafe(t *testing.T) {
	r, err := NewRegistry(&config.Config{Pools: []config.PoolConfig{
		poolCfg("api", "http://127.0.0.1:9001"),
	}})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	r.Close()
	r.Close()

	empty, err := NewRegistry(&config.Config{})
	if err != nil {
		t.Fatalf("NewRegistry (no pools): %v", err)
	}
	empty.Close()
	if len(empty.Pools()) != 0 {
		t.Fatal("expected no pools")
	}
}
