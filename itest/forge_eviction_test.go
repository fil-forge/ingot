//go:build itest

package itest

import (
	"fmt"
	"testing"
	"time"

	ingottest "github.com/fil-forge/ingot/testing"
	"github.com/stretchr/testify/require"
)

// TestForgeReadAfterEviction proves the appliance read tier: after the local
// blob copies are wiped, a GET must re-fetch the object's body blobs from piri by
// resolving their location from the local blob_locations table
// (registry.LocalLocator) and issuing a /content/retrieve — not from
// read-after-write. Body blobs live only in the cache; the manifest/MST live
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

	require.NoError(t, ingottest.CreateBucket(ctx, cfg, bucket), "create bucket")
	require.NoError(t, ingottest.PutBytes(ctx, cfg, bucket, key, data), "put object")

	// Wipe the local blob copies (the cache) so the next GET cannot
	// read-after-write — its body blobs must be re-fetched from piri.
	out, errOut, err := s.Exec(ctx, "ingot", "sh", "-c", "rm -rf /data/cache")
	require.NoError(t, err, "wipe local blob copies (stdout=%s stderr=%s)", out, errOut)

	got, err := ingottest.GetBytes(ctx, cfg, bucket, key)
	require.NoError(t, err, "get after eviction (forge read tier)")
	require.Equal(t, data, got, "read-after-eviction mismatch")
	t.Logf("read-after-eviction OK: %d bytes re-fetched from piri via the local locator", len(got))
}

// TestForgeLocalBlobBudget proves the local blob sweeper: with a 4 MiB
// local_blob_max_bytes (testdata/config-localblobbudget.yaml), 16 MiB of
// objects are evicted down to the budget within a few sweeps, well inside
// their time to live, at least 12 of their blobs leave local disk (so their
// reads must go to piri), and every object then reads back byte-exact.
//
//	go test -tags itest ./itest -run TestForgeLocalBlobBudget -v -timeout 900s
func TestForgeLocalBlobBudget(t *testing.T) {
	ctx := t.Context()

	s, ingotEndpoint := forgeStack(t, withLocalBlobBudgetConfig())
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "localblobbudget")
	cfg := forgeConfig(ingotEndpoint, accessKey, secretKey)
	const bucket = "budget-bucket"
	const budget = 4 << 20

	require.NoError(t, ingottest.CreateBucket(ctx, cfg, bucket), "create bucket")
	objects := make(map[string][]byte)
	for i := range 16 {
		key := fmt.Sprintf("obj-%02d", i)
		data := make([]byte, 1<<20)
		for j := range data {
			data[j] = byte(i*31 + j*7)
		}
		require.NoError(t, ingottest.PutBytes(ctx, cfg, bucket, key, data), "put %s", key)
		objects[key] = data
	}

	deadline := time.Now().Add(2 * time.Minute)
	for {
		used := localBlobBytes(t, ctx, s)
		if used <= budget {
			t.Logf("local blob usage %d bytes, within the %d-byte budget", used, budget)
			break
		}
		require.False(t, time.Now().After(deadline), "local blob usage %d bytes still over the %d-byte budget after 2 minutes", used, budget)
		time.Sleep(5 * time.Second)
	}
	// 16 MiB down to at most 4 MiB leaves room for at most four of the
	// 1 MiB blobs; the rest are read back from piri below.
	n := localBlobCount(t, ctx, s)
	require.LessOrEqual(t, n, 4, "local blobs after eviction")

	for key, want := range objects {
		got, err := ingottest.GetBytes(ctx, cfg, bucket, key)
		require.NoError(t, err, "get %s after eviction", key)
		require.Equal(t, want, got, "get %s after eviction: got %d bytes, want %d matching bytes", key, len(got), len(want))
	}
}

// TestForgeUncachedWrites proves cache_writes off
// (testdata/config-uncachedwrites.yaml): a PUT leaves no body on local disk
// once it returns, and the object reads back byte-exact from piri.
//
//	go test -tags itest ./itest -run TestForgeUncachedWrites -v -timeout 900s
func TestForgeUncachedWrites(t *testing.T) {
	ctx := t.Context()

	s, ingotEndpoint := forgeStack(t, withUncachedWritesConfig())
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "uncached")
	cfg := forgeConfig(ingotEndpoint, accessKey, secretKey)
	const bucket, key = "uncached-bucket", "obj"

	require.NoError(t, ingottest.CreateBucket(ctx, cfg, bucket), "create bucket")
	data := make([]byte, 3<<20)
	for i := range data {
		data[i] = byte(i*13 + 5)
	}
	before := localBlobCount(t, ctx, s)
	require.NoError(t, ingottest.PutBytes(ctx, cfg, bucket, key, data), "put")
	require.Zero(t, localBlobCount(t, ctx, s)-before, "local blobs a PUT left with cache_writes off")
	got, err := ingottest.GetBytes(ctx, cfg, bucket, key)
	require.NoError(t, err, "get")
	require.Equal(t, data, got, "get: got %d bytes, want %d matching bytes", len(got), len(data))
}
