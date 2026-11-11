package fakeprovider

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Slice 2.5b: only a queued delivery is ever taken; a delivered or given-up
// one is final, and one being sent is not sent twice at once.
func TestTakeDue_OnlyQueued(t *testing.T) {
	p := &Provider{}
	q, s, d, g := &delivery{state: queued}, &delivery{state: sending}, &delivery{state: delivered}, &delivery{state: gaveUp}
	p.webhooks = []*delivery{q, s, d, g}
	due := p.takeDueLocked(func(*delivery) bool { return true })
	require.Equal(t, []*delivery{q}, due)
	require.Equal(t, sending, q.state)
	require.Empty(t, p.takeDueLocked(func(*delivery) bool { return true }), "nothing left to take")
}
