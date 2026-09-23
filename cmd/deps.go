package cmd

import (
	"context"
	"fmt"

	"github.com/exaring/otelpgx"
	"github.com/fil-forge/libforge/identity"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/config"
)

// loadAgentIdentity builds the agent identity (the issuer of every outbound
// invocation, supplied to ingot.Module) from the identity config: the
// PEM-encoded ed25519 key, wrapped with the configured did:web service DID when
// one is set. Without a service DID the agent identifies by the key's own
// did:key.
func loadAgentIdentity(cfg config.IdentityConfig) (identity.Identity, error) {
	id, err := identity.NewFromPEMFileWithDID(cfg.KeyFile, cfg.ServiceID)
	if err != nil {
		return identity.Identity{}, fmt.Errorf("loading agent identity from %s: %w", cfg.KeyFile, err)
	}
	return id, nil
}

// openPool dials the Postgres registry/meta database. Every query is a span
// on the caller's trace.
func openPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing postgres dsn: %w", err)
	}
	cfg.ConnConfig.Tracer = otelpgx.NewTracer()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}
	return pool, nil
}

// buildLogger builds a production zap logger at the given level.
func buildLogger(level string) (*zap.Logger, error) {
	var lvl zap.AtomicLevel
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = zap.NewAtomicLevelAt(zap.InfoLevel)
	}
	cfg := zap.NewProductionConfig()
	cfg.Level = lvl
	return cfg.Build()
}
