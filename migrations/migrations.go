// Package migrations embeds the ingot Postgres migrations and exposes
// a runner that applies them via goose against a caller-provided
// *pgxpool.Pool.
//
// All ingot tables live in the `ingot` schema and goose tracks them in
// ingot.goose_db_version, so this package can run against the same
// database as sprue's internal/migrations without colliding.
package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"go.uber.org/zap"

	"embed"
)

//go:embed sql/*.sql
var FS embed.FS

const (
	schemaName       = "ingot"
	gooseVersionName = schemaName + ".goose_db_version"
)

// migrationLockKey is the pg_advisory_lock key that serializes concurrent Up
// runs against one database: parallel test packages, and multiple ingot
// instances migrating a shared Postgres at startup, would otherwise race on
// the first-run DDL (goose's version table, the schema). Arbitrary but fixed
// (ASCII "ingot"). The lock is session-scoped, so a crashed process releases
// it the moment Postgres notices the connection is gone — no persistent state
// can leak.
const migrationLockKey int64 = 0x696e676f74

// Up applies all pending migrations embedded in FS to the database
// behind pool. The ingot schema is created if it does not already
// exist, then goose is configured to track its version in
// ingot.goose_db_version. Concurrent Up calls (across processes) serialize
// on a Postgres advisory lock; a blocked caller waits until the holder
// finishes or its ctx is done.
func Up(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger) error {
	// The lock must live on its own connection for the whole run: advisory
	// locks are session-scoped, and goose runs over other pool connections.
	lockConn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("ingot migrations: acquire lock connection: %w", err)
	}
	defer lockConn.Release()
	if _, err := lockConn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		return fmt.Errorf("ingot migrations: acquire migration lock: %w", err)
	}
	defer func() {
		// Best-effort: releasing the connection would free the lock anyway
		// once the session ends, but unlocking promptly lets a waiting
		// instance proceed while this session returns to the pool.
		_, _ = lockConn.Exec(ctx, "SELECT pg_advisory_unlock($1)", migrationLockKey)
	}()

	if _, err := pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+schemaName); err != nil {
		return fmt.Errorf("ingot migrations: ensure schema: %w", err)
	}

	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()

	goose.SetBaseFS(FS)
	goose.SetLogger(&zapGooseLogger{logger: logger})
	goose.SetTableName(gooseVersionName)
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("ingot migrations: set dialect: %w", err)
	}
	if err := goose.UpContext(ctx, db, "sql"); err != nil {
		return fmt.Errorf("ingot migrations: up: %w", err)
	}
	return nil
}

type zapGooseLogger struct {
	logger *zap.Logger
}

func (l *zapGooseLogger) Fatalf(format string, v ...interface{}) {
	l.logger.Sugar().Fatalf(format, v...)
}

func (l *zapGooseLogger) Printf(format string, v ...interface{}) {
	l.logger.Sugar().Infof(format, v...)
}
