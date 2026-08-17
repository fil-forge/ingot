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
		"blob_refs", "upload_intents", "blob_locations",
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

	// The FEE wrap columns added to blob_locations (00013) exist and are nullable.
	for _, col := range []string{
		"region_wrapped_cek", "region_key_version", "tenant_recipient_kid",
		"base_nonce", "chunk_size", "protected_header",
	} {
		var nullable string
		err := pool.QueryRow(ctx,
			`SELECT is_nullable FROM information_schema.columns
			 WHERE table_schema = 'ingot' AND table_name = 'blob_locations' AND column_name = $1`, col).Scan(&nullable)
		if err != nil {
			t.Errorf("column ingot.blob_locations.%s missing after migration: %v", col, err)
			continue
		}
		if nullable != "YES" {
			t.Errorf("column ingot.blob_locations.%s is_nullable = %q, want YES", col, nullable)
		}
	}
}
