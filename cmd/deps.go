package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/fil-forge/libforge/identity"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot"
)

// loadAgentIdentity reads the agent's PEM-encoded ed25519 key and wraps
// it as an ingot.ServiceIdentity (the signer that issues invocations to
// sprue).
func loadAgentIdentity(keyFile string) (ingot.ServiceIdentity, error) {
	data, err := os.ReadFile(keyFile)
	if err != nil {
		return ingot.ServiceIdentity{}, fmt.Errorf("reading agent key %s: %w", keyFile, err)
	}
	signer, err := identity.DecodeEd25519SignerFromPEM(data)
	if err != nil {
		return ingot.ServiceIdentity{}, fmt.Errorf("decoding agent key %s: %w", keyFile, err)
	}
	return ingot.ServiceIdentity{Signer: signer}, nil
}

// openPool dials the Postgres registry/meta database.
func openPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
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
