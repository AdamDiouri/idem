package webhook_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/webhook"
)

var (
	secret = []byte("whsec_current")
	old    = []byte("whsec_previous")
	body   = []byte(`{"id":"evt_1","type":"payout.paid"}`)
	now    = time.Unix(1_800_000_000, 0)
)

const tolerance = 5 * time.Minute

func TestSign_VerifyRoundTrip(t *testing.T) {
	require.NoError(t, webhook.Verify(webhook.Sign(secret, now, body), body, [][]byte{secret}, now, tolerance))
}

func TestVerify_RejectsTamperedBody(t *testing.T) {
	err := webhook.Verify(webhook.Sign(secret, now, body), []byte(`{"id":"evt_2"}`), [][]byte{secret}, now, tolerance)
	require.ErrorIs(t, err, webhook.ErrBadSignature)
}

func TestVerify_RejectsWrongSecret(t *testing.T) {
	err := webhook.Verify(webhook.Sign([]byte("nope"), now, body), body, [][]byte{secret}, now, tolerance)
	require.ErrorIs(t, err, webhook.ErrBadSignature)
}

// Rotation: both the current and the previous secret are accepted.
func TestVerify_AcceptsAnyConfiguredSecret(t *testing.T) {
	require.NoError(t, webhook.Verify(webhook.Sign(old, now, body), body, [][]byte{secret, old}, now, tolerance))
}

// Replay protection: a timestamp outside the tolerance is refused, either way.
func TestVerify_StaleAndFutureTimestamps(t *testing.T) {
	for name, at := range map[string]time.Time{"6m old": now.Add(-6 * time.Minute), "6m ahead": now.Add(6 * time.Minute)} {
		t.Run(name, func(t *testing.T) {
			err := webhook.Verify(webhook.Sign(secret, at, body), body, [][]byte{secret}, now, tolerance)
			require.ErrorIs(t, err, webhook.ErrStaleSignature)
		})
	}
	require.NoError(t, webhook.Verify(webhook.Sign(secret, now.Add(-4*time.Minute), body), body, [][]byte{secret}, now, tolerance))
}

func TestVerify_Malformed(t *testing.T) {
	for _, h := range []string{"", "v1=ab", "t=x,v1=ab", "t=1800000000,v1=zz", "t=1800000000"} {
		require.ErrorIs(t, webhook.Verify(h, body, [][]byte{secret}, now, tolerance), webhook.ErrMalformedSignature, h)
	}
}

func TestVerify_NoSecretsRefuses(t *testing.T) {
	require.ErrorIs(t, webhook.Verify(webhook.Sign(secret, now, body), body, nil, now, tolerance), webhook.ErrBadSignature)
}
