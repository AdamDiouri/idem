package engine

import (
	"math/rand/v2"
	"time"
)

// Backoff computes retry delays with exponential growth and full jitter:
// rand(0, min(Cap, Base·2^(attempt−1))). Full jitter spreads retries from many
// steps that failed together, so they don't hit the provider in lockstep.
type Backoff struct {
	Base time.Duration
	Cap  time.Duration
	Rand func() float64 // in [0,1); nil uses math/rand/v2
}

// Delay returns the delay before retrying after the given 1-based attempt.
func (b Backoff) Delay(attempt int32) time.Duration {
	return time.Duration(b.rand() * float64(b.ceiling(attempt)))
}

// ceiling is min(Cap, Base·2^(attempt−1)), 0 when Base is unset.
func (b Backoff) ceiling(attempt int32) time.Duration {
	if b.Base <= 0 {
		return 0
	}
	if attempt < 1 {
		attempt = 1
	}
	// Double Base up to Cap without overflowing for large attempts.
	d := b.Base
	for i := int32(1); i < attempt && d < b.Cap; i++ {
		d *= 2
	}
	return min(d, b.Cap)
}

func (b Backoff) rand() float64 {
	if b.Rand != nil {
		return b.Rand()
	}
	return rand.Float64() //nolint:gosec // jitter, not security
}

// EqualJitter returns a delay in [ceiling/2, ceiling], where ceiling is
// min(Cap, Base·2^(attempt−1)): still spread out, but a run of retries has a
// guaranteed minimum total wait. Used while the provider reports the same
// key still in flight (409), so a busy provider can't burn the attempt
// budget in a second.
func (b Backoff) EqualJitter(attempt int32) time.Duration {
	c := b.ceiling(attempt)
	return c/2 + time.Duration(b.rand()*float64(c/2))
}
