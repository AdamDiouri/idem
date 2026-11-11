package fakeshop

import "fmt"

// Fault modes (M2 design §5.4).
const (
	ModeDown            = "down"              // the connection is closed with no answer (a crashed shop)
	ModeSlow            = "slow"              // answers 200 after Options.Slow
	Mode5xx             = "5xx"               // 500, nothing recorded
	ModeAckThen5xx      = "ack_then_5xx"      // records the event, then answers 500
	ModeAckAfterTimeout = "ack_after_timeout" // records the event, answers 200 after Options.LateAck
)

// Modes lists every fault mode.
var Modes = []string{ModeDown, ModeSlow, Mode5xx, ModeAckThen5xx, ModeAckAfterTimeout}

// Faults configures fault injection. The first matching rule wins.
type Faults struct {
	Seed  int64  `json:"seed"`
	Rules []Rule `json:"rules"`
}

// Rule injects Mode on deliveries. Probability 0 means always; Times 0 means
// unlimited.
type Rule struct {
	Mode        string  `json:"mode"`
	Probability float64 `json:"probability"`
	Times       int     `json:"times"`
}

// Validate rejects rules that would otherwise be silently ignored.
func (f Faults) Validate() error {
	valid := map[string]bool{}
	for _, m := range Modes {
		valid[m] = true
	}
	for i, r := range f.Rules {
		switch {
		case !valid[r.Mode]:
			return fmt.Errorf("rule %d: unknown mode %q", i, r.Mode)
		case r.Probability < 0 || r.Probability > 1:
			return fmt.Errorf("rule %d: probability %v outside [0,1]", i, r.Probability)
		case r.Times < 0:
			return fmt.Errorf("rule %d: times must not be negative", i)
		}
	}
	return nil
}
