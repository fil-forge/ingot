//go:build itest

package itest

import (
	"bytes"
	"fmt"
	"testing"
	"time"

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

// TestForgeSpoolBudget proves the spool sweeper: with a 4 MiB spool_max_bytes
// and both retention windows off (testdata/config-spoolbudget.yaml), 16 MiB
// of objects are evicted down to the budget within a few sweeps, and every
// object then reads back byte-exact, most of them from piri.
//
//	go test -tags itest ./itest -run TestForgeSpoolBudget -v -timeout 900s
func TestForgeSpoolBudget(t *testing.T) {
	ctx := t.Context()

	s, ingotEndpoint := forgeStack(t, withSpoolBudgetConfig())
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "spoolbudget")
	cfg := forgeConfig(ingotEndpoint, accessKey, secretKey)
	const bucket = "budget-bucket"
	const budget = 4 << 20

	if err := ingottest.CreateBucket(ctx, cfg, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	objects := make(map[string][]byte)
	for i := range 16 {
		key := fmt.Sprintf("obj-%02d", i)
		data := make([]byte, 1<<20)
		for j := range data {
			data[j] = byte(i*31 + j*7)
		}
		if err := ingottest.PutBytes(ctx, cfg, bucket, key, data); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		objects[key] = data
	}

	deadline := time.Now().Add(2 * time.Minute)
	for {
		used := spoolBytes(t, ctx, s)
		if used <= budget {
			t.Logf("spool usage %d bytes, within the %d-byte budget", used, budget)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("spool usage %d bytes still over the %d-byte budget after 2 minutes", used, budget)
		}
		time.Sleep(5 * time.Second)
	}

	for key, want := range objects {
		got, err := ingottest.GetBytes(ctx, cfg, bucket, key)
		if err != nil {
			t.Fatalf("get %s after eviction: %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("get %s after eviction: got %d bytes, want %d matching bytes", key, len(got), len(want))
		}
	}
}
