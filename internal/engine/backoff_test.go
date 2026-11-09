package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func approx(t *testing.T, want, got time.Duration) {
	t.Helper()
	require.InDelta(t, float64(want), float64(got), float64(time.Millisecond), "want ≈%s got %s", want, got)
}

func TestBackoff_UpperBoundDoublesThenCaps(t *testing.T) {
	b := Backoff{Base: 2 * time.Second, Cap: 5 * time.Minute, Rand: func() float64 { return 0.999999 }}
	approx(t, 2*time.Second, b.Delay(1))
	approx(t, 4*time.Second, b.Delay(2))
	approx(t, 8*time.Second, b.Delay(3))
	approx(t, 16*time.Second, b.Delay(4))
	approx(t, 5*time.Minute, b.Delay(9)) // 512s capped
	require.NotPanics(t, func() { approx(t, 5*time.Minute, b.Delay(1000)) })
	approx(t, 2*time.Second, b.Delay(0)) // defensive: attempt < 1 treated as 1
}

func TestBackoff_FullJitterLowerBound(t *testing.T) {
	b := Backoff{Base: 2 * time.Second, Cap: 5 * time.Minute, Rand: func() float64 { return 0 }}
	for attempt := int32(1); attempt < 20; attempt++ {
		require.Zero(t, b.Delay(attempt))
	}
}

func TestBackoff_ZeroBaseMeansNoDelay(t *testing.T) {
	b := Backoff{Base: 0, Cap: 5 * time.Minute, Rand: func() float64 { return 0.5 }}
	require.Zero(t, b.Delay(5))
}

func TestBackoff_DefaultRandWithinBounds(t *testing.T) {
	b := Backoff{Base: 2 * time.Second, Cap: 5 * time.Minute}
	for range 1000 {
		d := b.Delay(3)
		require.GreaterOrEqual(t, d, time.Duration(0))
		require.LessOrEqual(t, d, 8*time.Second)
	}
}
