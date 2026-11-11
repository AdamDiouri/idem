package main

import (
	"context"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/ledger"
	"github.com/AdamDiouri/idem/internal/store/db"
	"github.com/AdamDiouri/idem/internal/testutil"
)

func reconcileEnv(t *testing.T, providerURL string) {
	t.Helper()
	t.Setenv("IDEM_DATABASE_URL", dsn)
	t.Setenv("IDEM_PROVIDER_URL", providerURL)
}

func fakeProviderURL(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(fakeprovider.New(fakeprovider.Options{}).Handler())
	t.Cleanup(srv.Close)
	return srv.URL
}

var window = []string{"--from", time.Now().Add(-time.Hour).UTC().Format(time.RFC3339), "--to", time.Now().Add(time.Hour).UTC().Format(time.RFC3339)}

// Slice 2.6: the window is given whole or not at all.
func TestReconcileCmd_WindowFlagsTogether(t *testing.T) {
	reconcileEnv(t, "http://127.0.0.1:1")
	code, out := run(t, "reconcile", "--from", time.Now().UTC().Format(time.RFC3339))
	require.Equal(t, 2, code, out)
	code, out = run(t, "reconcile", "--from", "yesterday", "--to", "today")
	require.Equal(t, 2, code, out)
}

// 0 clean, 3 when non-parked workflows disagree (a cron can alert on it), 1
// when the run itself failed.
func TestReconcileCmd_ExitCodes(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	reconcileEnv(t, fakeProviderURL(t))
	code, out := run(t, append([]string{"reconcile"}, window...)...)
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "0 discrepancies")

	wf, step := parkedCharge(t, pool) // charge DEAD, nothing moved
	testutil.SetWorkflowStatus(t, pool, wf, "FAILED")
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	require.NoError(t, ledger.Post(ctx, db.New(tx), ledger.Txn{WorkflowID: wf, StepID: step, Kind: "charge", Entries: ledger.ChargeEntries(10000, 175)}))
	require.NoError(t, tx.Commit(ctx))
	code, out = run(t, append([]string{"reconcile"}, window...)...)
	require.Equal(t, 3, code, out)
	require.Contains(t, out, "1 discrepancies (0 parked)")

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	reconcileEnv(t, "http://"+l.Addr().String())
	require.NoError(t, l.Close())
	code, out = run(t, append([]string{"reconcile"}, window...)...)
	require.Equal(t, 1, code, out)
}
