package money_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/money"
)

func TestFee(t *testing.T) {
	cases := []struct{ amount, want int64 }{
		{10000, 175},             // 150 + 25
		{100, 27},                // 1.5 → 2 (half-even rounds up: 1 is odd)
		{300, 29},                // 4.5 → 4 (half-even rounds down: 4 is even)
		{500, 33},                // 7.5 → 8
		{33, 25},                 // 0.495 → 0
		{34, 26},                 // 0.51 → 1
		{100_000_000, 1_500_025}, // max amount
	}
	for _, c := range cases {
		require.Equal(t, c.want, money.Fee(c.amount, 150, 25), "amount=%d", c.amount)
	}
}

func TestFee_ZeroRateIsFixedOnly(t *testing.T) {
	require.Equal(t, int64(25), money.Fee(10000, 0, 25))
}
