// Package testutil provides a real Postgres for integration tests.
package testutil

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/AdamDiouri/idem/internal/store"
)

// RunWithPostgres starts a postgres:18 container, applies migrations, stores the
// connection string in *dsn, runs the package's tests and tears the container down.
// Use it from TestMain: os.Exit(testutil.RunWithPostgres(m, &dsn)).
func RunWithPostgres(m *testing.M, dsn *string) int {
	ctx := context.Background()

	ctr, err := postgres.Run(ctx, "postgres:18",
		postgres.WithDatabase("idem"),
		postgres.WithUsername("idem"),
		postgres.WithPassword("idem"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()

	*dsn, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		return 1
	}

	pool, err := store.Open(ctx, *dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open pool: %v\n", err)
		return 1
	}
	err = store.Migrate(ctx, pool)
	pool.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
		return 1
	}

	return m.Run()
}

// NewPool opens a pool against dsn, empties every application table and closes
// the pool when the test ends. Tests using it must not run in parallel.
func NewPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	pool, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	// TRUNCATE bypasses the ledger's append-only row triggers; accounts are seed data.
	if _, err := pool.Exec(ctx, `TRUNCATE ledger_entries, ledger_transactions, step_attempts,
		api_idempotency_keys, steps, workflows CASCADE`); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	return pool
}
