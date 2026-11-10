package engine_test

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/testutil"
)

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

func testutil_NewPool(t *testing.T) *pgxpool.Pool { return testutil.NewPool(t, dsn) } //nolint:revive // test helper

// setWorkflowStatus sets a status as a real transition would (with its outbox
// event when terminal); see testutil.SetWorkflowStatus.
func setWorkflowStatus(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, status string, set ...string) {
	t.Helper()
	testutil.SetWorkflowStatus(t, pool, id, status, set...)
}
