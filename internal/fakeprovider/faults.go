package fakeprovider

import "fmt"

// Fault modes. "before" modes never move money; "after" modes move money and
// store the success response, then fail the current request anyway — the
// ambiguity that makes idempotency keys necessary.
const (
	ModeTimeoutBeforeEffect = "timeout_before_effect"
	ModeTimeoutAfterEffect  = "timeout_after_effect"
	ModeErrorBeforeEffect   = "error_before_effect"
	ModeErrorAfterEffect    = "error_after_effect"
	ModeDecline             = "decline"
	ModeRateLimit           = "rate_limit"  // 429 + Retry-After: 1, no effect
	ModePayoutFail          = "payout_fail" // payouts only: accepted (202), then settles failed with account_closed
)

// Faults configures fault injection. The first matching rule wins.
type Faults struct {
	Seed  int64  `json:"seed"`
	Rules []Rule `json:"rules"`
}

// Rule injects Mode on requests to Endpoint (charges, payouts or refunds).
// Probability 0 means always; Times 0 means unlimited.
type Rule struct {
	Endpoint    string  `json:"endpoint"`
	Mode        string  `json:"mode"`
	Probability float64 `json:"probability"`
	Times       int     `json:"times"`
}

var validModes = map[string]bool{
	ModeTimeoutBeforeEffect: true,
	ModeTimeoutAfterEffect:  true,
	ModeErrorBeforeEffect:   true,
	ModeErrorAfterEffect:    true,
	ModeDecline:             true,
	ModeRateLimit:           true,
	ModePayoutFail:          true,
}

var validEndpoints = map[string]bool{"charges": true, "payouts": true, "refunds": true}

// Validate rejects rules that would otherwise be silently ignored, so a typo
// can never make a fault-injection run pass without injecting anything.
func (f Faults) Validate() error {
	for i, r := range f.Rules {
		switch {
		case !validEndpoints[r.Endpoint]:
			return fmt.Errorf("rule %d: unknown endpoint %q (want charges, payouts or refunds)", i, r.Endpoint)
		case !validModes[r.Mode]:
			return fmt.Errorf("rule %d: unknown mode %q", i, r.Mode)
		case r.Mode == ModePayoutFail && r.Endpoint != "payouts":
			return fmt.Errorf("rule %d: mode payout_fail is only valid on payouts", i)
		case r.Probability < 0 || r.Probability > 1:
			return fmt.Errorf("rule %d: probability %v outside [0,1]", i, r.Probability)
		case r.Times < 0:
			return fmt.Errorf("rule %d: times must not be negative", i)
		}
	}
	return nil
}

// pickFault returns the mode to apply to one request, or "" for none.
// Callers must hold p.mu.
func (p *Provider) pickFault(endpoint string) string {
	for i, r := range p.faults.Rules {
		if r.Endpoint != endpoint {
			continue
		}
		if r.Times > 0 && p.ruleUses[i] >= r.Times {
			continue
		}
		if r.Probability > 0 && p.rng.Float64() >= r.Probability {
			continue
		}
		p.ruleUses[i]++
		p.hits[r.Endpoint+"/"+r.Mode]++
		return r.Mode
	}
	return ""
}
