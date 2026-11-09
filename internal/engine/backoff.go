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
	upper := min(d, b.Cap)

	r := b.Rand
	if r == nil {
		r = rand.Float64 //nolint:gosec // jitter, not security
	}
	return time.Duration(r() * float64(upper))
}
