// Package testutil provides a real Postgres for integration tests.
package testutil

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
	if _, err := owner.Exec(ctx, `TRUNCATE recon_discrepancies, recon_runs, outbox, admin_actions, ledger_entries, ledger_transactions, step_attempts,
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

// SetWorkflowStatus moves a workflow to status as real transitions do: along
// allowed edges only (slice 2.5b; e.g. PENDING → RUNNING → COMPENSATING →
// COMPENSATED), and a terminal status gets its outbox event in the same
// transaction (D13). A state no legal path reaches needs Plant. set adds SQL assignments to the
// UPDATE, e.g. "updated_at = now() - interval '2 hours'".
func SetWorkflowStatus(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, status string, set ...string) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var from string
	if err := tx.QueryRow(ctx, `SELECT status FROM workflows WHERE id = $1`, id).Scan(&from); err != nil {
		t.Fatalf("set workflow status: %v", err)
	}
	path, err := legalPath(ctx, tx, from, status)
	if err != nil {
		t.Fatalf("set workflow status: %v", err)
	}
	assignments := "status = $2"
	for _, s := range set {
		assignments += ", " + s
	}
	if len(path) == 0 && len(set) > 0 { // no transition, but the assignments still apply
		if _, err := tx.Exec(ctx, `UPDATE workflows SET `+assignments+` WHERE id = $1`, id, status); err != nil {
			t.Fatalf("set workflow status: %v", err)
		}
	}
	for i, next := range path {
		sql := `UPDATE workflows SET status = $2 WHERE id = $1`
		if i == len(path)-1 {
			sql = `UPDATE workflows SET ` + assignments + ` WHERE id = $1`
		}
		if _, err := tx.Exec(ctx, sql, id, next); err != nil {
			t.Fatalf("set workflow status %s → %s: %v", from, next, err)
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO outbox (id, client_id, workflow_id, type, payload)
		SELECT gen_random_uuid(), client_id, id, workflow_event_type(status), convert_to(workflow_view(id), 'UTF8')
		FROM workflows WHERE id = $1 AND workflow_event_type(status) IS NOT NULL
		ON CONFLICT (workflow_id, type) DO NOTHING`, id); err != nil {
		t.Fatalf("write outbox event: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit workflow status: %v", err)
	}
}

// Plant runs one statement as the table owner with every trigger off
// (session_replication_role = replica): for fixtures that need a state no
// legal path reaches, such as a step already DEAD or a corrupted row. Every
// trigger means all of them: transition checks, but also foreign keys, the
// ledger's balance and immutability, and the outbox's one-event rule, so a
// planted row may break rules the services never could (NewPool truncates
// before each test). It needs a superuser owner, as the test container has.
// The services can never do this; only tests, through this helper.
func Plant(t *testing.T, dsn, sql string, args ...any) {
	t.Helper()
	ctx := context.Background()
	owner := NewOwnerPool(t, dsn)
	tx, err := owner.Begin(ctx)
	if err != nil {
		t.Fatalf("plant: begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
		t.Fatalf("plant: %v", err)
	}
	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("plant: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("plant: commit: %v", err)
	}
}

// legalPath is the shortest sequence of allowed workflow transitions from one
// status to another (empty when equal), asking the database's own table
// (workflow_transition_allowed), so tests never encode a second copy.
func legalPath(ctx context.Context, tx pgx.Tx, from, to string) ([]string, error) {
	statuses := []string{"PENDING", "RUNNING", "SUCCEEDED", "FAILED", "COMPENSATING", "COMPENSATED", "NEEDS_ATTENTION"}
	prev := map[string]string{from: ""}
	queue := []string{from}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur == to {
			var path []string
			for s := to; s != from; s = prev[s] {
				path = append([]string{s}, path...)
			}
			return path, nil
		}
		for _, next := range statuses {
			if _, seen := prev[next]; seen {
				continue
			}
			var ok bool
			if err := tx.QueryRow(ctx, `SELECT workflow_transition_allowed($1, $2)`, cur, next).Scan(&ok); err != nil {
				return nil, err
			}
			if ok {
				prev[next] = cur
				queue = append(queue, next)
			}
		}
	}
	return nil, fmt.Errorf("no allowed path from %s to %s (use testutil.Plant for a fixture state)", from, to)
}
