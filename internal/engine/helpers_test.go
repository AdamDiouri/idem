package engine_test

import (
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/testutil"
)

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

func testutil_NewPool(t *testing.T) *pgxpool.Pool { return testutil.NewPool(t, dsn) } //nolint:revive // test helper
