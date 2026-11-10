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
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, user).Scan(&exists); err != nil {
		return fmt.Errorf("look up role: %w", err)
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
