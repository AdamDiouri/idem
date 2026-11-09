package failpoint

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"
)

// EnableFromEnv enables failpoints from a spec such as
// "after_provider_call=exit@0.05,before_commit=sleep:2s".
//
// Each entry is name=action[@probability]. Actions: exit (process exits with
// code 137, like SIGKILL), panic (panics with ErrCrash), sleep:<duration>.
// Probability is in [0,1] and defaults to 1. An empty spec is a no-op.
// The spec is validated completely before anything is enabled.
func EnableFromEnv(spec string) error {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil
	}

	parsed := map[string]Action{}
	for entry := range strings.SplitSeq(spec, ",") {
		name, a, err := parseEntry(strings.TrimSpace(entry))
		if err != nil {
			return fmt.Errorf("failpoint spec entry %q: %w", entry, err)
		}
		parsed[name] = a
	}
	for name, a := range parsed {
		Enable(name, a)
	}
	return nil
}

func parseEntry(entry string) (string, Action, error) {
	name, rest, ok := strings.Cut(entry, "=")
	if !ok || name == "" || rest == "" {
		return "", nil, fmt.Errorf("want name=action[@probability]")
	}

	actionSpec, probSpec, hasProb := strings.Cut(rest, "@")
	prob := 1.0
	if hasProb {
		p, err := strconv.ParseFloat(probSpec, 64)
		if err != nil || p < 0 || p > 1 {
			return "", nil, fmt.Errorf("probability must be a number in [0,1]")
		}
		prob = p
	}

	var a Action
	switch {
	case actionSpec == "exit":
		a = func() { exitFunc(137) }
	case actionSpec == "panic":
		a = func() { panic(ErrCrash) }
	case strings.HasPrefix(actionSpec, "sleep:"):
		d, err := time.ParseDuration(strings.TrimPrefix(actionSpec, "sleep:"))
		if err != nil || d < 0 {
			return "", nil, fmt.Errorf("invalid sleep duration")
		}
		a = func() { time.Sleep(d) }
	default:
		return "", nil, fmt.Errorf("unknown action %q (want exit, panic or sleep:<duration>)", actionSpec)
	}

	if prob < 1 {
		inner := a
		a = func() {
			if rand.Float64() < prob { //nolint:gosec // fault injection, not security
				inner()
			}
		}
	}
	return name, a, nil
}
