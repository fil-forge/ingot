//go:build itest

package itest

import (
	"bytes"
	"testing"

	ingottest "github.com/fil-forge/ingot/testing"
)

// TestForgeReadAfterEviction proves the appliance read tier: after the local
// spool is wiped, a GET must re-fetch the object's body blobs from piri by
// resolving their location from the local blob_locations table
// (registry.LocalLocator) and issuing a /content/retrieve — not from
// read-after-write. Body blobs live only in the spool; the manifest/MST live
// in the catalog log and survive the wipe, so only the body read exercises
// the network tier.
//
//	go test -tags itest ./itest -run TestForgeReadAfterEviction -v -timeout 900s
func TestForgeReadAfterEviction(t *testing.T) {
	ctx := t.Context()

	s, ingotEndpoint := forgeStack(t)

	// Provision a hilt tenant so uploads (and the /content/retrieve read
	// path) have a space and credentials.
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "eviction")

	cfg := forgeConfig(ingotEndpoint, accessKey, secretKey)
	const bucket, key = "evict-bucket", "obj"

	// A deterministic body large enough to be a real blob shipped to piri.
	data := make([]byte, 512*1024)
	for i := range data {
		data[i] = byte(i*7 + 3)
	}

	if err := ingottest.CreateBucket(ctx, cfg, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if err := ingottest.PutBytes(ctx, cfg, bucket, key, data); err != nil {
		t.Fatalf("put object: %v", err)
	}

	// Wipe the local spool so the next GET cannot read-after-write — its body
	// blobs must be re-fetched from piri.
	if out, errOut, err := s.Exec(ctx, "ingot", "sh", "-c", "rm -rf /data/spool"); err != nil {
		t.Fatalf("evict spool: %v (stdout=%s stderr=%s)", err, out, errOut)
	}

	got, err := ingottest.GetBytes(ctx, cfg, bucket, key)
	if err != nil {
		t.Fatalf("get after eviction (forge read tier): %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read-after-eviction mismatch: got %d bytes, want %d", len(got), len(data))
	}
	t.Logf("read-after-eviction OK: %d bytes re-fetched from piri via the local locator", len(got))
}
