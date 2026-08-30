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

// New builds the strategy named by s. hashOn is the request attribute to key
// on and is only meaningful for consistent hashing.
func New(s, hashOn string) (Strategy, error) {
	switch s {
	case StrategyRoundRobin:
		return NewRoundRobin(), nil
	case StrategyLeastConn:
		_ = hashOn
		return NewLeastConn(), nil
	case StrategyConsistentHash:
		// hashOn is not validated here: config validation already requires
		// it to be set (and well-formed) whenever strategy is
		// consistent_hash, so an empty hashOn reaching this branch would
		// mean validation was skipped, not that this strategy should fail.
		return NewConsistentHash(), nil
	default:
		return nil, fmt.Errorf("balance: unknown strategy %q", s)
	}
}
