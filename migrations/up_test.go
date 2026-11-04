package migrations_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
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

	if err := migrations.Up(ctx, pool, zaptest.NewLogger(t)); err != nil {
		t.Fatalf("migrations.Up: %v", err)
	}
	// Idempotent: a second Up is a no-op (already-applied versions).
	if err := migrations.Up(ctx, pool, zaptest.NewLogger(t)); err != nil {
		t.Fatalf("migrations.Up (second): %v", err)
	}

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
		if err != nil {
			t.Fatalf("check table %s: %v", tbl, err)
		}
		if !exists {
			t.Errorf("table ingot.%s does not exist after migration", tbl)
		}
	}

	// The reserved/added bucket columns exist.
	for _, col := range []string{"space", "versioning", "next_version_seq", "tenant"} {
		var exists bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.columns
			 WHERE table_schema = 'ingot' AND table_name = 'buckets' AND column_name = $1)`, col).Scan(&exists)
		if err != nil {
			t.Fatalf("check column buckets.%s: %v", col, err)
		}
		if !exists {
			t.Errorf("column ingot.buckets.%s does not exist after migration", col)
		}
	}

	// buckets.tenant (00017) is NOT NULL once the backfill has run.
	{
		var nullable string
		err := pool.QueryRow(ctx,
			`SELECT is_nullable FROM information_schema.columns
			 WHERE table_schema = 'ingot' AND table_name = 'buckets' AND column_name = 'tenant'`).Scan(&nullable)
		if err != nil {
			t.Errorf("column ingot.buckets.tenant missing after migration: %v", err)
		} else if nullable != "NO" {
			t.Errorf("column ingot.buckets.tenant is_nullable = %q, want NO", nullable)
		}
	}

	// multipart_sessions (00016): the committed result Complete records for
	// replay, NULL until the session completes.
	for _, col := range []string{"committed_etag", "committed_version_id"} {
		var nullable string
		err := pool.QueryRow(ctx,
			`SELECT is_nullable FROM information_schema.columns
			 WHERE table_schema = 'ingot' AND table_name = 'multipart_sessions' AND column_name = $1`, col).Scan(&nullable)
		if err != nil {
			t.Errorf("column ingot.multipart_sessions.%s missing after migration: %v", col, err)
			continue
		}
		if nullable != "YES" {
			t.Errorf("column ingot.multipart_sessions.%s is_nullable = %q, want YES", col, nullable)
		}
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
		if err != nil {
			t.Errorf("column ingot.blob_encryption_params.%s missing after migration: %v", col, err)
			continue
		}
		if nullable != "NO" {
			t.Errorf("column ingot.blob_encryption_params.%s is_nullable = %q, want NO", col, nullable)
		}
	}
	// The base nonce and chunk size are read from aad (00029).
	for _, col := range []string{"base_nonce", "chunk_size"} {
		var exists bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.columns
			 WHERE table_schema = 'ingot' AND table_name = 'blob_encryption_params' AND column_name = $1)`, col).Scan(&exists)
		if err != nil {
			t.Fatalf("check column blob_encryption_params.%s: %v", col, err)
		}
		if exists {
			t.Errorf("column ingot.blob_encryption_params.%s still exists after migration", col)
		}
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
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}

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
		if err := <-errs; err != nil {
			t.Errorf("concurrent migrations.Up: %v", err)
		}
	}
}
