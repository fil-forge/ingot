//go:build itest

package itest

import (
	"bytes"
	"testing"

	ingottest "github.com/fil-forge/ingot/testing"
)

// TestForgeNativeProvision proves the hilt onboarding path end-to-end on a
// fresh stack: a tenant provisioned through hilt's Tenant API (which mints
// the tenant's did:plc space, registers it as an upload-service customer,
// and issues SigV4 credentials) can immediately create a bucket and
// round-trip a PUT/GET through the forge-mode listener — exercising the
// full ship path (sprue -> piri -> indexer) on a hilt-provisioned space.
// (This replaced the retired `ingot login` + `ingot space generate`
// self-provisioning flow when hilt took ownership of tenancy.)
//
//	go test -tags itest ./itest -run TestForgeNativeProvision -v -timeout 900s
func TestForgeNativeProvision(t *testing.T) {
	ctx := t.Context()

	s, ingotEndpoint := forgeStack(t)

	// 1. Provision a tenant + access key through hilt (the sole onboarding
	// path — ingot itself no longer logs in or generates spaces).
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "native")

	// 2. PUT/GET round-trip against the forge-mode ingot — exercises the full
	// blob ship path (sprue -> piri -> indexer) on the hilt-provisioned
	// space. The exhaustive S3 surface is TestForgeVersity's job.
	cfg := forgeConfig(ingotEndpoint, accessKey, secretKey)
	const bucket, key = "native-provision", "obj"
	data := patternBytes(512 << 10)
	if err := ingottest.CreateBucket(ctx, cfg, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if err := ingottest.PutBytes(ctx, cfg, bucket, key, data); err != nil {
		t.Fatalf("put object: %v", err)
	}
	got, err := ingottest.GetBytes(ctx, cfg, bucket, key)
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(got), len(data))
	}
	t.Logf("hilt-provisioned round-trip OK: %d bytes through sprue/piri", len(data))
}
