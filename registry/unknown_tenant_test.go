package registry

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The sentinel has two homes, the Go constant and the migration that
// backfills it; they must name the same string, and it must be a DID.
func TestUnknownTenantMatchesMigration(t *testing.T) {
	require.True(t, UnknownTenant.Defined(), "UnknownTenant is undefined")
	require.False(t, strings.HasPrefix(UnknownTenant.String(), "did:plc:"), "UnknownTenant %q is a did:plc and could collide with a hilt tenant", UnknownTenant)
	sql, err := os.ReadFile("../migrations/sql/00017_bucket_tenant.sql")
	require.NoError(t, err, "read migration")
	require.Contains(t, string(sql), "'"+UnknownTenant.String()+"'", "migration 00017 does not backfill %q", UnknownTenant)
}
