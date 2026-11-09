// Package money holds integer-cent arithmetic. Floats are never used for money.
package money

const bpsDenominator = 10_000

// Fee returns the platform fee in cents for amountCents:
// round_half_even(amountCents × rateBps / 10000) + fixedCents.
//
// Callers must keep amountCents × rateBps within int64; with the API limits
// (amount ≤ 100,000,000 cents, rate ≤ 10,000 bps) the product is ≤ 1e12.
func Fee(amountCents, rateBps, fixedCents int64) int64 {
	product := amountCents * rateBps
	q, r := product/bpsDenominator, product%bpsDenominator

	const half = bpsDenominator / 2
	if r > half || (r == half && q%2 == 1) {
		q++
	}
	return q + fixedCents
}
