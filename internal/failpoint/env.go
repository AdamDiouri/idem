package failpoint

import (
	"fmt"
	"math/rand/v2"
	"slices"
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
	if !compiledIn {
		return fmt.Errorf("failpoints requested (%q) but this binary was built with -tags nofailpoint", spec)
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

// ValidateSpec reports whether spec is valid for EnableFromEnv, without
// enabling anything. When known names are given, any other name is rejected,
// so a misspelt failpoint cannot silently never fire.
func ValidateSpec(spec string, known ...string) error {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil
	}
	for entry := range strings.SplitSeq(spec, ",") {
		name, _, err := parseEntry(strings.TrimSpace(entry))
		if err != nil {
			return fmt.Errorf("failpoint spec entry %q: %w", entry, err)
		}
		if len(known) > 0 && !slices.Contains(known, name) {
			return fmt.Errorf("failpoint spec entry %q: unknown failpoint %q (known: %s)",
				entry, name, strings.Join(known, ", "))
		}
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
		a = func() {
			_, _ = fmt.Fprintf(stderr, "failpoint %s fired: exit\n", name)
			exitFunc(137)
		}
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
