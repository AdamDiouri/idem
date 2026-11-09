package fakeprovider

// Fault modes. "before" modes never move money; "after" modes move money and
// store the success response, then fail the current request anyway — the
// ambiguity that makes idempotency keys necessary.
const (
	ModeTimeoutBeforeEffect = "timeout_before_effect"
	ModeTimeoutAfterEffect  = "timeout_after_effect"
	ModeErrorBeforeEffect   = "error_before_effect"
	ModeErrorAfterEffect    = "error_after_effect"
	ModeDecline             = "decline"
)

// Faults configures fault injection. The first matching rule wins.
type Faults struct {
	Seed  int64  `json:"seed"`
	Rules []Rule `json:"rules"`
}

// Rule injects Mode on requests to Endpoint ("charges").
// Probability 0 means always; Times 0 means unlimited.
type Rule struct {
	Endpoint    string  `json:"endpoint"`
	Mode        string  `json:"mode"`
	Probability float64 `json:"probability"`
	Times       int     `json:"times"`
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
		return r.Mode
	}
	return ""
}
