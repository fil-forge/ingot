package pgtest_test

import (
	"testing"

	"github.com/fil-forge/ingot/internal/pgtest"
)

// Each test's database is migrated and its own.
func TestNewGivesEachTestAMigratedDatabase(t *testing.T) {
	a, b := pgtest.New(t), pgtest.New(t)
	if _, err := a.Exec(t.Context(), `INSERT INTO ingot.revocation_cursor (recorded_at, revoke) VALUES (now(), '\x01')`); err != nil {
		t.Fatalf("migrated tables: %v", err)
	}
	var n int
	if err := b.QueryRow(t.Context(), `SELECT count(*) FROM ingot.revocation_cursor`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("the second database saw %d rows (%v), want none", n, err)
	}
}
