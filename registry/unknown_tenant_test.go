package registry

import (
	"os"
	"strings"
	"testing"
)

// The sentinel has two homes, the Go constant and the migration that
// backfills it; they must name the same string, and it must be a DID.
func TestUnknownTenantMatchesMigration(t *testing.T) {
	if !UnknownTenant.Defined() {
		t.Fatal("UnknownTenant is undefined")
	}
	if strings.HasPrefix(UnknownTenant.String(), "did:plc:") {
		t.Fatalf("UnknownTenant %q is a did:plc and could collide with a hilt tenant", UnknownTenant)
	}
	sql, err := os.ReadFile("../migrations/sql/00017_bucket_tenant.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if !strings.Contains(string(sql), "'"+UnknownTenant.String()+"'") {
		t.Fatalf("migration 00017 does not backfill %q", UnknownTenant)
	}
}
