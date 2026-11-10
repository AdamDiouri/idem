package webhook_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/testutil"
	"github.com/AdamDiouri/idem/internal/webhook"
)

var dsn string

func TestMain(m *testing.M) {
	os.Exit(testutil.RunWithPostgres(m, &dsn))
}

// The inbox stores each provider event once, byte-exact (F15).
func TestInbox_StoresOnceByteExact(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	in := webhook.NewInbox(pool)
	payload := []byte(`{"id":"evt_1",  "type":"payout.paid"}`) // odd spacing must survive

	inserted, err := in.Store(ctx, "evt_1", "payout.paid", payload)
	require.NoError(t, err)
	require.True(t, inserted)
	inserted, err = in.Store(ctx, "evt_1", "payout.paid", payload)
	require.NoError(t, err)
	require.False(t, inserted, "a duplicate is not stored again")

	var got []byte
	var status string
	require.NoError(t, pool.QueryRow(ctx, `SELECT payload, status FROM webhook_inbox WHERE provider_event_id = 'evt_1'`).Scan(&got, &status))
	require.Equal(t, payload, got)
	require.Equal(t, "PENDING", status)
}
