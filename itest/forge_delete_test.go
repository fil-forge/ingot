//go:build itest

package itest

import (
	"testing"
	"time"

	ingottest "github.com/fil-forge/ingot/testing"
)

// TestForgeDeleteReleasesNetworkBlob is the delete-finality regression gate
// (FIL-588): DeleteObject must release the blob on the network, not just drop
// registry rows. The chain under test is ingot's reference index (claims→0 ⇒
// RemoveBlob) → forgeclient /blob/remove → sprue (forward + deregister) →
// piri /blob/release (claim release; deferred physical deletion once the
// PDP aggregate root retires on-chain).
//
// Runs on the stock smelt-SDK images like every other itest. It needs
// piri:main ≥ fil-forge/piri#30 (the /blob/release handler) and sprue:main ≥
// fil-forge/sprue#33 (the forward); until piri#30 publishes, point
// INGOT_ITEST_PIRI_IMAGE at a branch image (the forgeStack escape hatch).
func TestForgeDeleteReleasesNetworkBlob(t *testing.T) {
	ctx := t.Context()

	s, ingotEndpoint := forgeStack(t)
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "delete")
	cfg := forgeConfig(ingotEndpoint, accessKey, secretKey)

	const bucket, key = "delete-bucket", "obj"

	// Unique deterministic body, large enough to be a real blob on piri.
	data := make([]byte, 512*1024)
	for i := range data {
		data[i] = byte(i*13 + 11)
	}

	if err := ingottest.CreateBucket(ctx, cfg, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if err := ingottest.PutBytes(ctx, cfg, bucket, key, data); err != nil {
		t.Fatalf("put object: %v", err)
	}

	// Precondition: the blob really lives on piri — wipe the spool and prove
	// the GET re-fetches from the network (same tier the eviction test pins).
	if out, errOut, err := s.Exec(ctx, "ingot", "sh", "-c", "rm -rf /data/spool"); err != nil {
		t.Fatalf("evict spool: %v (stdout=%s stderr=%s)", err, out, errOut)
	}
	if got, err := ingottest.GetBytes(ctx, cfg, bucket, key); err != nil || len(got) != len(data) {
		t.Fatalf("read-through from piri before delete: err=%v len=%d", err, len(got))
	}

	if err := ingottest.DeleteObject(ctx, cfg, bucket, key); err != nil {
		t.Fatalf("delete object: %v", err)
	}

	// The object is gone from the gateway.
	if _, err := ingottest.GetBytes(ctx, cfg, bucket, key); err == nil {
		t.Fatalf("GET after delete succeeded, want NoSuchKey")
	}

	// And the release traversed the network: piri's /blob/release handler ran
	// and, with the last claim gone, queued the piece for removal. Byte
	// release is fully asynchronous — ingot's releaseBlobs is best-effort
	// post-commit, and piri's removal sweep (PDPRemoveSweep, 30s ticks)
	// re-verifies claims and pipeline state before finalizing — so poll the
	// provider's logs through to the finalization line.
	waitForPiriLog(t, ctx, s, "/blob/release", 2*time.Minute)
	waitForPiriLog(t, ctx, s, "queueing piece removal", 2*time.Minute)
	waitForPiriLog(t, ctx, s, "finalized piece removal", 3*time.Minute)
	t.Logf("delete finality OK: /blob/release executed on piri and the sweep finalized the byte release")
}
