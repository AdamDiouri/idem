// Package store owns the Postgres connection pool, schema migrations and
// helpers for classifying Postgres errors.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/AdamDiouri/idem/migrations"
)

const maxConns = 20

// Open creates a connection pool for dsn and verifies the database is reachable.
func Open(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// Migrate applies all pending migrations.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return withProvider(pool, func(p *goose.Provider) error {
		_, err := p.Up(ctx)
		return err
	})
}

// MigrateDownTo rolls back to version. Intended for tests.
func MigrateDownTo(ctx context.Context, pool *pgxpool.Pool, version int64) error {
	return withProvider(pool, func(p *goose.Provider) error {
		_, err := p.DownTo(ctx, version)
		return err
	})
}

// MigrateDown rolls back every migration. Intended for tests.
func MigrateDown(ctx context.Context, pool *pgxpool.Pool) error {
	return withProvider(pool, func(p *goose.Provider) error {
		_, err := p.DownTo(ctx, 0)
		return err
	})
}

func withProvider(pool *pgxpool.Pool, fn func(*goose.Provider) error) error {
	// Closing this *sql.DB wrapper does not close the underlying pool.
	db := stdlib.OpenDBFromPool(pool)
	defer func() { _ = db.Close() }()

	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		return fmt.Errorf("create migration provider: %w", err)
	}
	if err := fn(p); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}
	return nil
}

// IsLockTimeout reports whether err is Postgres lock_not_available (SQLSTATE 55P03),
// raised when a statement waits on a lock longer than lock_timeout.
func IsLockTimeout(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55P03"
}
