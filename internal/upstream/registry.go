package upstream

import (
	"fmt"

	"github.com/aaditya0602/manifold/internal/config"
)

// Registry is the set of pools built from one Config.
//
// It is immutable once constructed. A config reload builds a second Registry
// from the new file, swaps the proxy's pointer to it, drains, and only then
// calls Close on the old one — so an in-flight request is never looking at a
// half-rebuilt pool and never has its transport pulled out from under it.
type Registry struct {
	byName map[string]*Pool
	// order preserves config order so Pools() is deterministic, which keeps
	// startup logs and admin output stable across restarts.
	order []*Pool
}

// NewRegistry builds every pool in cfg. It fails on the first pool that cannot
// be built; a partially constructed registry is never returned, because a
// proxy serving with a missing pool would 503 a subset of traffic silently
// instead of refusing to start.
func NewRegistry(cfg *config.Config) (*Registry, error) {
	if cfg == nil {
		return nil, fmt.Errorf("upstream: nil config")
	}

	r := &Registry{
		byName: make(map[string]*Pool, len(cfg.Pools)),
		order:  make([]*Pool, 0, len(cfg.Pools)),
	}
	for _, pc := range cfg.Pools {
		if _, dup := r.byName[pc.Name]; dup {
			// Config validation should already reject this; failing here too
			// means the invariant holds even for a Config built in code.
			r.Close()
			return nil, fmt.Errorf("upstream: duplicate pool name %q", pc.Name)
		}
		p, err := NewPool(pc)
		if err != nil {
			r.Close()
			return nil, err
		}
		r.byName[pc.Name] = p
		r.order = append(r.order, p)
	}
	return r, nil
}

// Pool looks a pool up by name.
func (r *Registry) Pool(name string) (*Pool, bool) {
	p, ok := r.byName[name]
	return p, ok
}

// Pools returns every pool in config order.
func (r *Registry) Pools() []*Pool {
	out := make([]*Pool, len(r.order))
	copy(out, r.order)
	return out
}

// Close releases every pool's idle sockets. Without it a reload would leak one
// set of keep-alive connections per generation: the old transports would stay
// referenced by their idle conns' read loops until IdleConnTimeout expired.
func (r *Registry) Close() {
	for _, p := range r.order {
		p.CloseIdleConnections()
	}
}
