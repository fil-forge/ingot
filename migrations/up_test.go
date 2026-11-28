package migrations_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/migrations"
)

// TestUp applies the embedded migrations to an empty database and
// asserts the ingot schema's tables exist, guarding against DDL errors in a
// new migration breaking startup.
func TestUp(t *testing.T) {
	ctx := context.Background()
	pool, _ := pgtest.NewEmpty(t)

	require.NoError(t, migrations.Up(ctx, pool, zaptest.NewLogger(t)), "migrations.Up")
	// Idempotent: a second Up is a no-op (already-applied versions).
	require.NoError(t, migrations.Up(ctx, pool, zaptest.NewLogger(t)), "migrations.Up (second)")

	want := []string{
		"buckets", "segments", "segment_op_roots",
		"blob_refs", "upload_intents", "blob_locations", "blob_encryption_params",
		"multipart_sessions", "multipart_parts", "gc_candidates",
	}
	for _, tbl := range want {
		var exists bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables
			 WHERE table_schema = 'ingot' AND table_name = $1)`, tbl).Scan(&exists)
		require.NoError(t, err, "check table %s", tbl)
		assert.True(t, exists, "table ingot.%s does not exist after migration", tbl)
	}

	// The reserved/added bucket columns exist.
	for _, col := range []string{"space", "versioning", "next_version_seq", "tenant"} {
		var exists bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.columns
			 WHERE table_schema = 'ingot' AND table_name = 'buckets' AND column_name = $1)`, col).Scan(&exists)
		require.NoError(t, err, "check column buckets.%s", col)
		assert.True(t, exists, "column ingot.buckets.%s does not exist after migration", col)
	}

	// buckets.tenant (00017) is NOT NULL once the backfill has run.
	{
		var nullable string
		err := pool.QueryRow(ctx,
			`SELECT is_nullable FROM information_schema.columns
			 WHERE table_schema = 'ingot' AND table_name = 'buckets' AND column_name = 'tenant'`).Scan(&nullable)
		if assert.NoError(t, err, "column ingot.buckets.tenant missing after migration") {
			assert.Equal(t, "NO", nullable, "column ingot.buckets.tenant is_nullable")
		}
	}

	// multipart_sessions (00016): the committed result Complete records for
	// replay, NULL until the session completes.
	for _, col := range []string{"committed_etag", "committed_version_id"} {
		var nullable string
		err := pool.QueryRow(ctx,
			`SELECT is_nullable FROM information_schema.columns
			 WHERE table_schema = 'ingot' AND table_name = 'multipart_sessions' AND column_name = $1`, col).Scan(&nullable)
		if !assert.NoError(t, err, "column ingot.multipart_sessions.%s missing after migration", col) {
			continue
		}
		assert.Equal(t, "YES", nullable, "column ingot.multipart_sessions.%s is_nullable", col)
	}

	// blob_encryption_params (00014, 00029): every column exists and is NOT
	// NULL — the presence of a row is what marks a blob as encrypted, so there
	// is no such thing as a half-populated parameter set.
	for _, col := range []string{
		"space", "digest", "region_wrapped_cek", "region_key_version",
		"header_len", "aad", "created_at",
	} {
		var nullable string
		err := pool.QueryRow(ctx,
			`SELECT is_nullable FROM information_schema.columns
			 WHERE table_schema = 'ingot' AND table_name = 'blob_encryption_params' AND column_name = $1`, col).Scan(&nullable)
		if !assert.NoError(t, err, "column ingot.blob_encryption_params.%s missing after migration", col) {
			continue
		}
		assert.Equal(t, "NO", nullable, "column ingot.blob_encryption_params.%s is_nullable", col)
	}
	// The base nonce and chunk size are read from aad (00029).
	for _, col := range []string{"base_nonce", "chunk_size"} {
		var exists bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.columns
			 WHERE table_schema = 'ingot' AND table_name = 'blob_encryption_params' AND column_name = $1)`, col).Scan(&exists)
		require.NoError(t, err, "check column blob_encryption_params.%s", col)
		assert.False(t, exists, "column ingot.blob_encryption_params.%s still exists after migration", col)
	}
}

// TestUp_Concurrent races several Up calls against a fresh database:
// the pg_advisory_lock in Up must serialize them, or the first-run DDL
// (goose's version table, the schema) collides — the exact failure mode of
// parallel test packages migrating the shared CI database, and of multiple
// ingot instances starting against one Postgres. The race needs a database
// with no ingot schema yet, which the test gets as an empty database of its
// own.
func TestUp_Concurrent(t *testing.T) {
	ctx := context.Background()
	empty, dsn := pgtest.NewEmpty(t)
	empty.Close()
	scratchCfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err, "parse dsn")

	const racers = 4
	errs := make(chan error, racers)
	for i := 0; i < racers; i++ {
		go func() {
			pool, err := pgxpool.NewWithConfig(ctx, scratchCfg.Copy())
			if err != nil {
				errs <- err
				return
			}
			defer pool.Close()
			errs <- migrations.Up(ctx, pool, zaptest.NewLogger(t))
		}()
	}
	for i := 0; i < racers; i++ {
		assert.NoError(t, <-errs, "concurrent migrations.Up")
	}
}
