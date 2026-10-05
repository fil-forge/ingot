package s3frontend

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fil-forge/versitygw/backend"
	"github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/fil-forge/ingot/inmem"
)

// collectLocalBlobMetrics reads every local blob instrument: gauges by name
// and directory, if any ("ingot.local_blobs.usage/cache"), and the removal
// counters by name and reason ("ingot.local_blobs.removals/budget").
func collectLocalBlobMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Gauge[int64]:
				for _, dp := range d.DataPoints {
					name := m.Name
					if dir, ok := dp.Attributes.Value(attribute.Key("dir")); ok {
						name += "/" + dir.AsString()
					}
					got[name] = dp.Value
				}
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					reason, _ := dp.Attributes.Value(attribute.Key("reason"))
					got[m.Name+"/"+reason.AsString()] = dp.Value
				}
			}
		}
	}
	return got
}

func meteredBackend(reader *sdkmetric.ManualReader) func(*Deps) {
	return func(d *Deps) { d.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)) }
}

func TestLocalBlobMetrics_BudgetPass(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, _ := newSweepBackend(t, meteredBackend(reader))
	digests := putSweepObjects(t, b, 4)
	size := blobSize(t, b, digests[0])
	budgetToEvict(b, 2, size)

	sweepLocalBlobs(t, b)

	got := collectLocalBlobMetrics(t, reader)
	want := map[string]int64{
		"ingot.local_blobs.usage/spool":          b.spool.Usage(),
		"ingot.local_blobs.usage/cache":          b.cache.Usage(),
		"ingot.local_blobs.budget":               b.localBlobMaxBytes,
		"ingot.local_blobs.removals/budget":      2,
		"ingot.local_blobs.removed_bytes/budget": 2 * size,
	}
	for name, v := range want {
		if got[name] != v {
			t.Fatalf("%s = %d, want %d (all: %v)", name, got[name], v, got)
		}
	}

	if err := b.CloseMetrics(); err != nil {
		t.Fatalf("CloseMetrics: %v", err)
	}
	if _, ok := collectLocalBlobMetrics(t, reader)["ingot.local_blobs.usage/cache"]; ok {
		t.Fatal("usage gauge still reported after CloseMetrics")
	}
}

func TestLocalBlobMetrics_ParkedPart(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, mem := newDeferredBackend(t, &parkingUploader{}, meteredBackend(reader))
	key := "parked"

	uploadID := mpCreate(t, b, key, "", "")
	if _, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	parts := int64(len(hygienePartDigests(t, mem, uploadID, 1)))

	got := collectLocalBlobMetrics(t, reader)
	if got["ingot.local_blobs.removals/parked"] != parts || got["ingot.local_blobs.removed_bytes/parked"] <= 0 {
		t.Fatalf("parked removals = %d files, %d bytes; want %d files (all: %v)",
			got["ingot.local_blobs.removals/parked"], got["ingot.local_blobs.removed_bytes/parked"], parts, got)
	}
	if got["ingot.local_blobs.usage/spool"] != 0 || got["ingot.local_blobs.usage/cache"] != 0 {
		t.Fatalf("usage after the part parked = %d in the spool, %d in the cache, want none",
			got["ingot.local_blobs.usage/spool"], got["ingot.local_blobs.usage/cache"])
	}
}

func TestLocalBlobMetrics_Release(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, _ := newDeferredBackend(t, inmem.NopUploader{}, meteredBackend(reader))
	key := "deleted"
	putObj(t, b, key, testBody(1<<10))
	size := blobSize(t, b, blobDigestOf(t, b, key, ""))
	deleteObj(t, b, key)

	if n, err := b.SweepPendingReleases(context.Background()); err != nil || n != 1 {
		t.Fatalf("release sweep executed %d releases (err=%v), want 1", n, err)
	}

	got := collectLocalBlobMetrics(t, reader)
	if got["ingot.local_blobs.removals/released"] != 1 || got["ingot.local_blobs.removed_bytes/released"] != size {
		t.Fatalf("released removals = %d files, %d bytes; want 1 file, %d bytes (all: %v)",
			got["ingot.local_blobs.removals/released"], got["ingot.local_blobs.removed_bytes/released"], size, got)
	}
}

func TestLocalBlobMetrics_ForcedPass(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, _ := newSweepBackend(t, meteredBackend(reader), func(d *Deps) { d.CacheMinResidency = time.Hour })
	digests := putSweepObjects(t, b, 4)
	size := blobSize(t, b, digests[0])
	budgetToEvict(b, 2, size)

	sweepLocalBlobs(t, b)

	got := collectLocalBlobMetrics(t, reader)
	if got["ingot.local_blobs.removals/budget_forced"] != 2 || got["ingot.local_blobs.removed_bytes/budget_forced"] != 2*size {
		t.Fatalf("forced removals = %d files, %d bytes; want 2 files, %d bytes (all: %v)",
			got["ingot.local_blobs.removals/budget_forced"], got["ingot.local_blobs.removed_bytes/budget_forced"], 2*size, got)
	}
	if _, ok := got["ingot.local_blobs.removals/budget"]; ok {
		t.Fatalf("the budget pass recorded removals inside the residency window: %v", got)
	}
}

// TestLocalBlobMetrics_BudgetPassSkipsAFileAlreadyGone: a row whose file was
// already gone is not counted as an eviction.
func TestLocalBlobMetrics_BudgetPassSkipsAFileAlreadyGone(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, _ := newSweepBackend(t, meteredBackend(reader))
	digests := putSweepObjects(t, b, 2)
	size := blobSize(t, b, digests[1])
	budgetToEvict(b, 1, size)
	if err := os.Remove(localPath(b, digests[0])); err != nil {
		t.Fatal(err)
	}

	sweepLocalBlobs(t, b)

	// The usage count still held the missing file's bytes, so the pass went
	// on to evict the second blob, the only removal counted.
	got := collectLocalBlobMetrics(t, reader)
	if got["ingot.local_blobs.removals/budget"] != 1 || got["ingot.local_blobs.removed_bytes/budget"] != size {
		t.Fatalf("budget removals = %d files, %d bytes; want 1 file, %d bytes (all: %v)",
			got["ingot.local_blobs.removals/budget"], got["ingot.local_blobs.removed_bytes/budget"], size, got)
	}
}

// TestLocalBlobMetrics_OrphanPass: the orphan pass counts the temp files and
// intent-less blob files it deletes.
func TestLocalBlobMetrics_OrphanPass(t *testing.T) {
	ctx := t.Context()
	reader := sdkmetric.NewManualReader()
	b, _ := newSweepBackend(t, meteredBackend(reader))
	old := time.Now().Add(-2 * DefaultLocalBlobOrphanAge)
	temp := filepath.Join(filepath.Dir(b.spool.Path(multihash.Multihash{0})), ".tmp-old")
	if err := os.WriteFile(temp, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	d, _, err := b.spool.WriteBlob(ctx, bytes.NewReader([]byte("old orphan")))
	if err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}
	for _, path := range []string{temp, b.spool.Path(d)} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}

	sweepLocalBlobs(t, b)

	got := collectLocalBlobMetrics(t, reader)
	wantBytes := int64(len("partial") + len("old orphan"))
	if got["ingot.local_blobs.removals/orphan"] != 2 || got["ingot.local_blobs.removed_bytes/orphan"] != wantBytes {
		t.Fatalf("orphan removals = %d files, %d bytes; want 2 files, %d bytes (all: %v)",
			got["ingot.local_blobs.removals/orphan"], got["ingot.local_blobs.removed_bytes/orphan"], wantBytes, got)
	}
}

// TestLocalBlobMetrics_ReleaseOfACopyAlreadyGone: a release whose spool copy is
// already gone, as on a retry after the copy was removed, counts nothing.
func TestLocalBlobMetrics_ReleaseOfACopyAlreadyGone(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, _ := newDeferredBackend(t, inmem.NopUploader{}, meteredBackend(reader))
	key := "deleted"
	putObj(t, b, key, testBody(1<<10))
	if err := os.Remove(localPath(b, blobDigestOf(t, b, key, ""))); err != nil {
		t.Fatal(err)
	}
	deleteObj(t, b, key)

	if n, err := b.SweepPendingReleases(context.Background()); err != nil || n != 1 {
		t.Fatalf("release sweep executed %d releases (err=%v), want 1", n, err)
	}

	got := collectLocalBlobMetrics(t, reader)
	if got["ingot.local_blobs.removals/released"] != 0 || got["ingot.local_blobs.removed_bytes/released"] != 0 {
		t.Fatalf("released removals = %d files, %d bytes; want none (all: %v)",
			got["ingot.local_blobs.removals/released"], got["ingot.local_blobs.removed_bytes/released"], got)
	}
}
