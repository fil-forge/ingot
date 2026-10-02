//go:build itest

package itest

import (
	"bytes"
	"testing"

	ingottest "github.com/fil-forge/ingot/testing"
)

// TestForgeReadAfterEviction proves the appliance read tier: a GET re-fetches
// the object's body blobs from piri by resolving their location from the
// local blob_locations table (registry.LocalLocator) and issuing a
// /content/retrieve. Ingot keeps no local copy of a body blob, so every body
// read exercises the network tier; the manifest/MST live in the catalog log.
// The test name predates the spool's removal.
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

	got, err := ingottest.GetBytes(ctx, cfg, bucket, key)
	if err != nil {
		t.Fatalf("get (forge read tier): %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read-after-eviction mismatch: got %d bytes, want %d", len(got), len(data))
	}
	t.Logf("read-after-eviction OK: %d bytes re-fetched from piri via the local locator", len(got))
}
