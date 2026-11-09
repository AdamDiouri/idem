//go:build nofailpoint

package failpoint

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Review finding I4: asking a release build for failpoints must fail loudly,
// not silently run without them.
func TestEnableFromEnv_RejectedWhenCompiledOut(t *testing.T) {
	err := EnableFromEnv("after_claim=exit")
	require.Error(t, err)
	require.Contains(t, err.Error(), "nofailpoint")
	require.NoError(t, EnableFromEnv(""), "no failpoints requested: fine")
}
