package migrations_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/migrations"
)

// TestUp_Live applies the embedded migrations against a real Postgres and
// asserts the ingot schema's tables exist. It is skipped unless INGOT_TEST_DSN
// names a reachable database (e.g. a throwaway docker postgres):
//
//	INGOT_TEST_DSN=postgres://postgres:pw@127.0.0.1:55432/ingot \
//	  GOWORK=off go test ./migrations/ -run TestUp_Live -v
//
// It guards against DDL errors in a new migration breaking forge-mode startup,
// which the in-memory suite cannot catch.
func TestUp_Live(t *testing.T) {
	dsn := os.Getenv("INGOT_TEST_DSN")
	if dsn == "" {
		t.Skip("set INGOT_TEST_DSN to run the live migration test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

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
		"blob_release_intents",
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
	for _, col := range []string{"space", "versioning", "next_version_seq"} {
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

	// blob_encryption_params (00014): every column exists and is NOT NULL — the
	// presence of a row is what marks a blob as encrypted, so there is no such
	// thing as a half-populated parameter set.
	for _, col := range []string{
		"space", "digest", "region_wrapped_cek", "region_key_version",
		"header_len", "base_nonce", "chunk_size", "aad", "created_at",
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
}

// TestUp_Live_Concurrent races several Up calls against a fresh database:
// the pg_advisory_lock in Up must serialize them, or the first-run DDL
// (goose's version table, the schema) collides — the exact failure mode of
// parallel test packages migrating the shared CI database, and of multiple
// ingot instances starting against one Postgres. The race needs a database
// with no ingot schema yet, so the test creates and drops a scratch database
// rather than touching the shared one (which other packages' live tests may
// be using concurrently).
func TestUp_Live_Concurrent(t *testing.T) {
	dsn := os.Getenv("INGOT_TEST_DSN")
	if dsn == "" {
		t.Skip("set INGOT_TEST_DSN to run the live migration test")
	}
	ctx := context.Background()

	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer admin.Close()

	const scratchDB = "ingot_migrate_race"
	if _, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+scratchDB+" WITH (FORCE)"); err != nil {
		t.Fatalf("drop stale scratch database: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+scratchDB); err != nil {
		t.Fatalf("create scratch database: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+scratchDB+" WITH (FORCE)")
	})

	scratchCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	scratchCfg.ConnConfig.Database = scratchDB

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
