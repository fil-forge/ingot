// Package pgtest gives each test a fresh, migrated Postgres database.
//
// Ingot's tables live in a schema whose name the SQL spells out, so tests
// cannot be kept apart by schema; each gets a database of its own instead. The
// first test in a run migrates a template database, named after a hash of the
// migrations so a changed migration gets a new one, and every test copies it,
// which takes milliseconds. A test's database is dropped when it ends, so tests
// share nothing and may run in parallel.
//
// The server is the one INGOT_TEST_DSN names. `make test` starts one.
package pgtest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/migrations"
)

// DSNEnv names the environment variable holding the test server's DSN.
const DSNEnv = "INGOT_TEST_DSN"

// templateLock serializes template creation across the test processes of a
// run, which `go test ./...` starts one per package.
const templateLock int64 = 0x696e676f74746d70 // "ingottmp"

var (
	templateOnce sync.Once
	templateName string
	templateErr  error
)

// New returns a pool on a fresh, migrated database, dropped when t ends. It
// fails t when DSNEnv is unset.
func New(t testing.TB) *pgxpool.Pool {
	t.Helper()
	pool, _ := NewWithDSN(t)
	return pool
}

// NewWithDSN is New, also returning the DSN of the test's database, for code
// that opens its own connections.
func NewWithDSN(t testing.TB) (*pgxpool.Pool, string) {
	t.Helper()
	base := DSN(t)
	templateOnce.Do(func() { templateName, templateErr = ensureTemplate(context.Background(), base) })
	if templateErr != nil {
		t.Fatalf("pgtest: template database: %v", templateErr)
	}
	return create(t, base, ` TEMPLATE `+pgx.Identifier{templateName}.Sanitize())
}

// NewEmpty is NewWithDSN for a database with no schema at all, for tests of
// the migrations themselves.
func NewEmpty(t testing.TB) (*pgxpool.Pool, string) {
	t.Helper()
	return create(t, DSN(t), "")
}

// create creates a database of the test's own on the server base names, with
// the given CREATE DATABASE options, and drops it when t ends.
func create(t testing.TB, base, opts string) (*pgxpool.Pool, string) {
	t.Helper()
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatalf("pgtest: connect: %v", err)
	}
	defer admin.Close(ctx)
	var b [6]byte
	_, _ = rand.Read(b[:])
	name := "ingot_test_" + hex.EncodeToString(b[:])
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{name}.Sanitize()+opts); err != nil {
		t.Fatalf("pgtest: create database: %v", err)
	}
	dsn, err := withDatabase(base, name)
	if err != nil {
		t.Fatalf("pgtest: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgtest: connect to the test database: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		conn, err := pgx.Connect(context.Background(), base)
		if err != nil {
			return
		}
		defer conn.Close(context.Background())
		_, _ = conn.Exec(context.Background(), `DROP DATABASE IF EXISTS `+pgx.Identifier{name}.Sanitize()+` WITH (FORCE)`)
	})
	return pool, dsn
}

// DSN returns the test server's DSN, failing t when it is unset.
func DSN(t testing.TB) string {
	t.Helper()
	dsn := os.Getenv(DSNEnv)
	if dsn == "" {
		t.Fatalf("pgtest: %s is unset; run `make test`, which starts Postgres, or set it to a server the tests may create databases on", DSNEnv)
	}
	return dsn
}

// ensureTemplate creates and migrates the template database for this run's
// migrations, unless an earlier run already has, and returns its name.
func ensureTemplate(ctx context.Context, base string) (string, error) {
	name, err := templateFor()
	if err != nil {
		return "", err
	}
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		return "", fmt.Errorf("connect: %w", err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, `SELECT pg_advisory_lock($1)`, templateLock); err != nil {
		return "", fmt.Errorf("lock: %w", err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, templateLock) }()
	var exists bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, name).Scan(&exists); err != nil {
		return "", fmt.Errorf("look for the template: %w", err)
	}
	if exists {
		return name, nil
	}
	// Built under a scratch name and renamed once migrated, so a run that
	// dies part way leaves nothing a later run would take for finished.
	scratch := name + "_building"
	if _, err := admin.Exec(ctx, `DROP DATABASE IF EXISTS `+pgx.Identifier{scratch}.Sanitize()+` WITH (FORCE)`); err != nil {
		return "", fmt.Errorf("drop a half-built template: %w", err)
	}
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{scratch}.Sanitize()); err != nil {
		return "", fmt.Errorf("create the template: %w", err)
	}
	dsn, err := withDatabase(base, scratch)
	if err != nil {
		return "", err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return "", fmt.Errorf("connect to the template: %w", err)
	}
	err = migrations.Up(ctx, pool, zap.NewNop())
	pool.Close()
	if err != nil {
		return "", fmt.Errorf("migrate the template: %w", err)
	}
	if _, err := admin.Exec(ctx, `ALTER DATABASE `+pgx.Identifier{scratch}.Sanitize()+` RENAME TO `+pgx.Identifier{name}.Sanitize()); err != nil {
		return "", fmt.Errorf("name the template: %w", err)
	}
	return name, nil
}

// templateFor names the template after a hash of the embedded migrations.
func templateFor() (string, error) {
	h := sha256.New()
	err := fs.WalkDir(migrations.FS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := fs.ReadFile(migrations.FS, path)
		if err != nil {
			return err
		}
		fmt.Fprintf(h, "%s\x00%d\x00", path, len(data))
		h.Write(data)
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("hash the migrations: %w", err)
	}
	return "ingot_template_" + hex.EncodeToString(h.Sum(nil)[:8]), nil
}

// withDatabase returns dsn pointing at database name instead. dsn is a URL
// (postgres://…) or keyword/value pairs, where a later dbname wins.
func withDatabase(dsn, name string) (string, error) {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", fmt.Errorf("parse %s: %w", DSNEnv, err)
		}
		u.Path = "/" + name
		return u.String(), nil
	}
	return dsn + " dbname=" + name, nil
}
