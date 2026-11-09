package main

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func run(t *testing.T, args ...string) (int, string) {
	t.Helper()
	var out bytes.Buffer
	code := runIdem(args, &out, &out)
	return code, out.String()
}

// Deferred minor 6: usage mistakes are recognised by type, not by error text.
func TestExitCode_UsageMistakesExitTwo(t *testing.T) {
	for name, args := range map[string][]string{
		"unknown command":       {"bogus"},
		"unknown admin command": {"admin", "bogus"}, // used to print help and exit 0
		"unknown flag":          {"admin", "list", "--bogus"},
		"bad flag value":        {"admin", "retry-step", "x", "--expect-version", "nope"},
		"missing argument":      {"admin", "retry-step", "--reason", "r", "--expect-version", "1"},
		"extra argument":        {"admin", "list", "extra"},
		"missing required flag": {"admin", "retry-step", "0190f1d2-0000-7000-8000-000000000000"},
	} {
		t.Run(name, func(t *testing.T) {
			code, out := run(t, args...)
			require.Equal(t, 2, code, out)
		})
	}
}

// An operational error must exit 1 whatever its text says.
func TestExitCode_ErrorTextDoesNotDecide(t *testing.T) {
	require.Equal(t, 1, exitCode(errors.New(`provider said: unknown command "refund"; accepts 2 args; required flag`)))
	require.Equal(t, 0, exitCode(nil))
}

func TestExitCode_HelpExitsZero(t *testing.T) {
	code, out := run(t, "admin", "--help")
	require.Equal(t, 0, code)
	require.Contains(t, out, "retry-step")
	code, _ = run(t, "admin")
	require.Equal(t, 0, code, "a bare group command shows its help")
}
