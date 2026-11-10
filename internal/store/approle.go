package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// EnsureAppUser creates (or updates the password of) a login user that is a
// member of idem_app, the restricted application role (migration 00013). pool
// must belong to a role that may manage roles (the migrations owner). Names
// and passwords are quoted by Postgres (format %I / %L), never in Go.
func EnsureAppUser(ctx context.Context, pool *pgxpool.Pool, user, password string) error {
	if user == "" || password == "" {
		return errors.New("app database user and password must both be set")
	}
	// An existing role must be a plain one: taking over the owner, the current
	// user or a privileged role would change its password and run the services
	// with rights the application role exists to deny.
	var exists, isCurrent, privileged, owns bool
	if err := pool.QueryRow(ctx, `
		SELECT r.oid IS NOT NULL,
		       $1 = current_user,
		       COALESCE(r.rolsuper OR r.rolcreaterole OR r.rolbypassrls OR r.rolcreatedb OR r.rolreplication, false),
		       EXISTS (SELECT 1 FROM pg_class c WHERE c.relowner = r.oid)
		FROM (SELECT 1) one LEFT JOIN pg_roles r ON r.rolname = $1`, user).Scan(&exists, &isCurrent, &privileged, &owns); err != nil {
		return fmt.Errorf("look up role: %w", err)
	}
	switch {
	case isCurrent:
		return fmt.Errorf("IDEM_APP_DB_USER %q is the user running the migrations: the application needs its own, restricted login", user)
	case privileged:
		return fmt.Errorf("IDEM_APP_DB_USER %q is a privileged role (superuser, CREATEROLE, CREATEDB, BYPASSRLS or REPLICATION): choose a plain login", user)
	case owns:
		return fmt.Errorf("IDEM_APP_DB_USER %q owns tables in this database: an owner may rewrite the ledger; choose a plain login", user)
	}
	verb := "CREATE"
	if exists {
		verb = "ALTER"
	}
	steps := []struct {
		format string
		args   []string
	}{
		{verb + " ROLE %I WITH LOGIN PASSWORD %L", []string{user, password}},
		{"GRANT idem_app TO %I", []string{user}},
	}
	for _, st := range steps {
		var stmt string
		if err := pool.QueryRow(ctx, `SELECT format($1::text, VARIADIC $2::text[])`, st.format, st.args).Scan(&stmt); err != nil {
			return fmt.Errorf("quote role statement: %w", err)
		}
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("ensure app user %q: %w", user, err)
		}
	}
	return nil
}
