// Package testutil provides a real Postgres for integration tests.
package testutil

import (
	"context"
	"fmt"
	"net/url"
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
	if err == nil {
		err = CreateAppUser(ctx, pool, AppUser, AppPassword)
	}
	pool.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
		return 1
	}

	return m.Run()
}

// AppUser and AppPassword are the restricted login (a member of idem_app,
// migration 00013) that NewPool connects as, so every integration test runs
// with the application's privileges: a missing grant fails a test, not
// production.
const (
	AppUser     = "idem_app_user"
	AppPassword = "idem_app_pw" //nolint:gosec // throwaway test container
)

// CreateAppUser creates the login user as a member of idem_app, as an
// operator's infrastructure would (compose: deploy/postgres/initdb). idem
// itself never creates logins. user and password are trusted constants.
func CreateAppUser(ctx context.Context, owner *pgxpool.Pool, user, password string) error {
	_, err := owner.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s' IN ROLE idem_app`, user, password))
	return err
}

// NewOwnerPool opens a pool as the owner in dsn (the migrations role): for
// tests that change the schema or manage roles. It does not empty tables.
func NewOwnerPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := store.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open owner pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// AppDSN rewrites an owner dsn to log in as the restricted AppUser.
func AppDSN(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	u.User = url.UserPassword(AppUser, AppPassword)
	return u.String(), nil
}

// NewPool empties every application table (as the owner: TRUNCATE is never
// granted to the application) and returns a pool connected as AppUser, closed
// when the test ends. Tests using it must not run in parallel.
func NewPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	owner := NewOwnerPool(t, dsn)
	// TRUNCATE bypasses the ledger's append-only row triggers; accounts are seed data.
	if _, err := owner.Exec(ctx, `TRUNCATE admin_actions, ledger_entries, ledger_transactions, step_attempts,
		api_idempotency_keys, steps, workflows CASCADE`); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}

	appDSN, err := AppDSN(dsn)
	if err != nil {
		t.Fatalf("app dsn: %v", err)
	}
	pool, err := store.Open(ctx, appDSN)
	if err != nil {
		t.Fatalf("open app pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
