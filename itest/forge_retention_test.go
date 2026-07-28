//go:build itest

package itest

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	ingottest "github.com/fil-forge/ingot/testing"
	"github.com/fil-forge/smelt/pkg/stack"
)

// TestForgeReadAfterCatalogRetention proves catalog retention behaves as a
// cache, not an availability cliff: after a shipped catalog segment is retired
// off local disk, its blocks (object manifests, MST nodes) must still resolve
// through the fallthrough read tier — the shard_inclusions row (block → shard
// CAR + byte range, recorded at ship) joined to the shard's blob_locations
// row, retrieved as a ranged /content/retrieve against piri.
//
// The config seals the catalog every 1s, retains ONE shipped segment, and
// disables the read cache. An early object is written, then later writes roll
// the catalog past the retain window until the early object's segment is
// retired; the early object must then still GET (its manifest is only in the
// retired segment) and the bucket must still list without a delimiter (the
// walk fetches every leaf's manifest).
//
//	go test -tags itest ./itest -run TestForgeReadAfterCatalogRetention -v -timeout 900s
func TestForgeReadAfterCatalogRetention(t *testing.T) {
	ctx := t.Context()

	s, ingotEndpoint := forgeStack(t, stack.WithServiceConfig("ingot", "testdata/config-retention.yaml"))
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "retention")
	cfg := forgeConfig(ingotEndpoint, accessKey, secretKey)

	const bucket = "retention-bucket"
	if err := ingottest.CreateBucket(ctx, cfg, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	// The early object: its manifest lands in the first catalog segment(s),
	// which the later writes will push out of the retain window.
	early := patternBytes(64 << 10)
	if err := ingottest.PutBytes(ctx, cfg, bucket, "early-obj", early); err != nil {
		t.Fatalf("put early object: %v", err)
	}

	// Count the segment CARs currently on disk; the early manifest lives in
	// one of these. Retirement is proven when every one of them is gone.
	initialSegs := catalogSegments(t, s, bucket)
	if len(initialSegs) == 0 {
		// The first segment may not have sealed yet; wait for it so we have
		// a concrete set to watch retire.
		waitFor(t, time.Minute, "first catalog segment to seal", func() bool {
			initialSegs = catalogSegments(t, s, bucket)
			return len(initialSegs) > 0
		})
	}
	t.Logf("early object's manifest is in segment(s): %v", initialSegs)

	// Roll the catalog: spaced writes each seal (1s seal_age) and ship a new
	// segment; retain=1 retires everything older. Keep writing until every
	// initial segment file is gone from the container.
	waitFor(t, 5*time.Minute, "initial catalog segments to retire", func() bool {
		key := fmt.Sprintf("filler/obj-%d", time.Now().UnixNano())
		if err := ingottest.PutBytes(ctx, cfg, bucket, key, patternBytes(4<<10)); err != nil {
			t.Fatalf("put filler object: %v", err)
		}
		// Slower than seal_age so each segment seals and ships with headroom
		// (no flush-queue overflow → no re-ships through the dedup path).
		time.Sleep(3 * time.Second)
		remaining := catalogSegments(t, s, bucket)
		for _, seg := range initialSegs {
			for _, r := range remaining {
				if seg == r {
					return false
				}
			}
		}
		return true
	})
	t.Logf("initial segments retired; segments now on disk: %v", catalogSegments(t, s, bucket))

	// GET the early object: its manifest exists only in a retired segment, so
	// this read MUST come through inclusion → shard → ranged piri retrieval.
	got, err := ingottest.GetBytes(ctx, cfg, bucket, "early-obj")
	if err != nil {
		t.Fatalf("get early object after its catalog segment retired: %v", err)
	}
	if !bytes.Equal(got, early) {
		t.Fatalf("early object mismatch after retention: got %d bytes, want %d", len(got), len(early))
	}

	// Undelimited list walks every leaf and fetches every manifest — the
	// regression that motivated this test (root listing failed with
	// "blockstore: not found" once a manifest's segment was retired).
	keys, err := ingottest.ListKeys(ctx, cfg, bucket)
	if err != nil {
		t.Fatalf("list bucket after retention: %v", err)
	}
	found := false
	for _, k := range keys {
		if k == "early-obj" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("early-obj missing from listing: %v", keys)
	}
	t.Logf("read-after-retention OK: %d bytes via ranged shard retrieval; %d keys listed", len(got), len(keys))
}

// catalogSegments lists the catalog-plane CAR files currently on the ingot
// container's disk for bucket (segments live under
// /data/segments/<bucket>/catalog/).
func catalogSegments(t *testing.T, s *stack.Stack, bucket string) []string {
	t.Helper()
	out, _, err := s.Exec(t.Context(), "ingot", "sh", "-c",
		"ls /data/segments/"+bucket+"/catalog/ 2>/dev/null | grep '\\.car$' || true")
	if err != nil {
		t.Fatalf("list catalog segments: %v", err)
	}
	fields := strings.Fields(strings.TrimSpace(out))
	return fields
}

// waitFor polls cond (which may do work per attempt) until true or fatal after
// timeout.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}
