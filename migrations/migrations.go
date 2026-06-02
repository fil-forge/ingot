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

// Up applies all pending migrations embedded in FS to the database
// behind pool. The ingot schema is created if it does not already
// exist, then goose is configured to track its version in
// ingot.goose_db_version.
func Up(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger) error {
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
