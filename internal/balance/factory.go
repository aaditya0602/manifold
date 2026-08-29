package balance

import "fmt"

// Strategy names, mirroring the values accepted in YAML. They are duplicated
// here rather than imported from the config package on purpose: balance has no
// internal dependencies, so a strategy can be tested and reasoned about
// without dragging configuration parsing along with it. The proxy converts.
const (
	StrategyRoundRobin     = "round_robin"
	StrategyLeastConn      = "least_conn"
	StrategyConsistentHash = "consistent_hash"
)

// ErrNotImplemented reports a strategy that is a valid config value but has no
// implementation yet. Config validation accepts all three names from day one
// so the schema does not churn; the factory is where the gap is honest.
type ErrNotImplemented struct{ Strategy string }

func (e *ErrNotImplemented) Error() string {
	return fmt.Sprintf("balance: strategy %q is not implemented yet", e.Strategy)
}

// New builds the strategy named by s. hashOn is the request attribute to key
// on and is only meaningful for consistent hashing.
func New(s, hashOn string) (Strategy, error) {
	switch s {
	case StrategyRoundRobin:
		return NewRoundRobin(), nil
	case StrategyLeastConn, StrategyConsistentHash:
		_ = hashOn
		return nil, &ErrNotImplemented{Strategy: s}
	default:
		return nil, fmt.Errorf("balance: unknown strategy %q", s)
	}
}
