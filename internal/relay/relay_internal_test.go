package relay

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

// Final review, minor 3: a cut in the middle of a character would make
// Postgres reject the update on every attempt, so the event would never DEAD.
func TestTruncateError_KeepsValidUTF8(t *testing.T) {
	long := strings.Repeat("a", maxErrorLen-1) + "é" + strings.Repeat("b", 50) // é straddles the limit
	got := truncateError(long)
	require.True(t, utf8.ValidString(got))
	require.LessOrEqual(t, len(got), maxErrorLen)
	require.Equal(t, "short", truncateError("short"))
}
