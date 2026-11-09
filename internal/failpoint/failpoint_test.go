//go:build !nofailpoint

package failpoint

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func catch(f func()) (r any) {
	defer func() { r = recover() }()
	f()
	return nil
}

func TestHit_NoopWhenDisabled(t *testing.T) {
	t.Cleanup(Reset)
	require.Nil(t, catch(func() { Hit("x") }))
}

func TestEnable_RunsAction(t *testing.T) {
	t.Cleanup(Reset)
	n := 0
	Enable("x", func() { n++ })
	Hit("x")
	Hit("x")
	Hit("other")
	require.Equal(t, 2, n)

	Disable("x")
	Hit("x")
	require.Equal(t, 2, n)
}

func TestCrashOnce(t *testing.T) {
	t.Cleanup(Reset)
	Enable("x", CrashOnce())

	r := catch(func() { Hit("x") })
	require.True(t, IsCrash(r), "got %v", r)
	require.Nil(t, catch(func() { Hit("x") }), "second hit must not crash")

	require.False(t, IsCrash("something else"))
	require.False(t, IsCrash(nil))
}

func TestBlockOnce(t *testing.T) {
	t.Cleanup(Reset)
	action, entered, release := BlockOnce()
	Enable("x", action)

	done := make(chan struct{})
	go func() {
		Hit("x")
		close(done)
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("blocked action never entered")
	}
	select {
	case <-done:
		t.Fatal("Hit returned before release")
	case <-time.After(50 * time.Millisecond):
	}

	release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Hit did not return after release")
	}

	start := time.Now()
	Hit("x")
	require.Less(t, time.Since(start), 50*time.Millisecond, "second hit must not block")
	release() // idempotent
}

func TestEnableFromEnv(t *testing.T) {
	t.Cleanup(Reset)

	require.NoError(t, EnableFromEnv(""))

	require.NoError(t, EnableFromEnv("a=sleep:20ms,b=panic,d=panic@0"))
	start := time.Now()
	Hit("a")
	require.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)

	require.True(t, IsCrash(catch(func() { Hit("b") })))

	for range 100 {
		require.Nil(t, catch(func() { Hit("d") }))
	}

	var code int
	orig := exitFunc
	exitFunc = func(c int) { code = c }
	t.Cleanup(func() { exitFunc = orig })
	require.NoError(t, EnableFromEnv("c=exit"))
	Hit("c")
	require.Equal(t, 137, code)
}

// Review finding: a misspelt failpoint name must be rejected, not silently never fire.
func TestValidateSpec_KnownNames(t *testing.T) {
	known := []string{"after_provider_call", "before_commit"}
	require.NoError(t, ValidateSpec("after_provider_call=exit,before_commit=panic@0.5", known...))
	err := ValidateSpec("after_provider_cal=exit", known...)
	require.Error(t, err)
	require.Contains(t, err.Error(), "after_provider_cal")
	require.NoError(t, ValidateSpec("anything=exit"), "no known names: names are not checked")
	require.Error(t, ValidateSpec("a=fly"))
}

func TestEnableFromEnv_Errors(t *testing.T) {
	t.Cleanup(Reset)
	for _, spec := range []string{"noequals", "a=fly", "a=sleep:abc", "a=panic@1.5", "a=panic@x", "=panic"} {
		err := EnableFromEnv(spec)
		require.Error(t, err, spec)
		entry := spec
		require.Contains(t, err.Error(), entry, "error must name the bad entry")
	}
}
