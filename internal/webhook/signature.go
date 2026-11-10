// Package webhook signs and verifies webhooks (design §9.2, M2 §4.3) and
// stores received provider events in the inbox. Inbound provider webhooks and,
// later, idem's own outbound webhooks to shops use the same scheme as Stripe:
// a header "t=<unix>,v1=<hex HMAC-SHA256(secret, "<t>.<raw body>")>".
package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"
)

// ProviderSignatureHeader carries the provider's signature on its webhooks.
const ProviderSignatureHeader = "Idem-Provider-Signature"

// Verification failures. Nothing about a refused webhook may be stored.
var (
	ErrMalformedSignature = errors.New("malformed signature header")
	ErrStaleSignature     = errors.New("signature timestamp outside the tolerance (replay protection)")
	ErrBadSignature       = errors.New("signature does not match")
)

// Sign returns the signature header value for body, signed at t.
func Sign(secret []byte, t time.Time, body []byte) string {
	return "t=" + strconv.FormatInt(t.Unix(), 10) + ",v1=" + hex.EncodeToString(mac(secret, t.Unix(), body))
}

// Verify checks header against body. Any of secrets may have signed it (the
// current and the previous one, for rotation), and its timestamp must be
// within tolerance of now, in either direction. Comparisons are constant-time.
func Verify(header string, body []byte, secrets [][]byte, now time.Time, tolerance time.Duration) error {
	ts, sigs, err := parse(header)
	if err != nil {
		return err
	}
	if d := now.Sub(time.Unix(ts, 0)); d > tolerance || d < -tolerance {
		return ErrStaleSignature
	}
	for _, s := range secrets {
		want := mac(s, ts, body)
		for _, got := range sigs {
			if hmac.Equal(want, got) {
				return nil
			}
		}
	}
	return ErrBadSignature
}

func mac(secret []byte, ts int64, body []byte) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(strconv.FormatInt(ts, 10) + "."))
	m.Write(body)
	return m.Sum(nil)
}

func parse(header string) (int64, [][]byte, error) {
	var ts int64
	var haveTS bool
	var sigs [][]byte
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return 0, nil, ErrMalformedSignature
		}
		switch k {
		case "t":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return 0, nil, ErrMalformedSignature
			}
			ts, haveTS = n, true
		case "v1":
			b, err := hex.DecodeString(v)
			if err != nil || len(b) == 0 {
				return 0, nil, ErrMalformedSignature
			}
			sigs = append(sigs, b)
		}
	}
	if !haveTS || len(sigs) == 0 {
		return 0, nil, ErrMalformedSignature
	}
	return ts, sigs, nil
}
