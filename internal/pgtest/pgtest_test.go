package pgtest_test

import (
	"testing"

	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/stretchr/testify/require"
)

// Each test's database is migrated and its own.
func TestNewGivesEachTestAMigratedDatabase(t *testing.T) {
	a, b := pgtest.New(t), pgtest.New(t)
	_, err := a.Exec(t.Context(), `INSERT INTO ingot.revocation_cursor (token) VALUES ('1')`)
	require.NoError(t, err, "migrated tables")
	var n int
	require.NoError(t, b.QueryRow(t.Context(), `SELECT count(*) FROM ingot.revocation_cursor`).Scan(&n))
	require.Zero(t, n, "rows the second database saw")
}
