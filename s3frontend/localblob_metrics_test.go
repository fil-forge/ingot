package s3frontend

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fil-forge/versitygw/backend"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/registry"
)

// collectLocalBlobMetrics reads every local blob instrument: gauges by name
// and directory, if any ("ingot.local_blobs.usage/cache"), and the removal
// counters by name and reason ("ingot.local_blobs.removals/budget").
func collectLocalBlobMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm), "collect")
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
					if state, ok := dp.Attributes.Value(attribute.Key("state")); ok {
						name += "/" + state.AsString()
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
		"ingot.local_blobs.usage/cache":          b.cache.Usage(),
		"ingot.local_blobs.budget":               b.localBlobMaxBytes,
		"ingot.local_blobs.removals/budget":      2,
		"ingot.local_blobs.removed_bytes/budget": 2 * size,
	}
	for name, v := range want {
		require.Equal(t, v, got[name], "%s (all: %v)", name, got)
	}

	require.NoError(t, b.CloseMetrics(), "CloseMetrics")
	_, ok := collectLocalBlobMetrics(t, reader)["ingot.local_blobs.usage/cache"]
	require.False(t, ok, "usage gauge still reported after CloseMetrics")
}

// TestLocalBlobMetrics_ParkedPart: a part keeps no local copy, so its upload
// leaves the cache empty and removes nothing.
func TestLocalBlobMetrics_ParkedPart(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, _ := newDeferredBackend(t, &parkingUploader{}, meteredBackend(reader))
	key := "parked"

	uploadID := mpCreate(t, b, key, "", "")
	_, err := mpUploadPart(t, b, key, uploadID, 1, testBody(int(backend.MinPartSize)), nil)
	require.NoError(t, err, "UploadPart")

	got := collectLocalBlobMetrics(t, reader)
	for name, v := range got {
		if strings.HasPrefix(name, "ingot.local_blobs.removals/") {
			require.Zero(t, v, "%s after a part upload (all: %v)", name, got)
		}
	}
	require.Zero(t, got["ingot.local_blobs.usage/cache"], "usage in the cache after the part parked")
}

func TestLocalBlobMetrics_Release(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, _ := newDeferredBackend(t, testfakes.NopUploader{}, meteredBackend(reader))
	key := "deleted"
	putObj(t, b, key, testBody(1<<10))
	size := blobSize(t, b, blobDigestOf(t, b, key, ""))
	deleteObj(t, b, key)

	n, err := b.SweepPendingReleases(context.Background())
	require.NoError(t, err, "release sweep executed %d releases (err=%v), want 1", n, err)
	require.Equal(t, 1, n, "release sweep executed %d releases (err=%v), want 1", n, err)

	// A cached copy goes by an eviction the release queues, a moment later.
	var got map[string]int64
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if got = collectLocalBlobMetrics(t, reader); got["ingot.local_blobs.removals/released"] > 0 {
			break
		}
	}
	require.Equal(t, int64(1), got["ingot.local_blobs.removals/released"], "released removals (all: %v)", got)
	require.Equal(t, size, got["ingot.local_blobs.removed_bytes/released"], "released bytes (all: %v)", got)
}

// TestLocalBlobMetrics_BudgetPassSkipsAFileAlreadyGone: a row whose file was
// already gone is not counted as an eviction.
func TestLocalBlobMetrics_BudgetPassSkipsAFileAlreadyGone(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, _ := newSweepBackend(t, meteredBackend(reader))
	digests := putSweepObjects(t, b, 2)
	size := blobSize(t, b, digests[1])
	budgetToEvict(b, 1, size)
	require.NoError(t, os.Remove(localPath(b, digests[0])))

	sweepLocalBlobs(t, b)

	// The usage count still held the missing file's bytes, so the pass went
	// on to evict the second blob, the only removal counted.
	got := collectLocalBlobMetrics(t, reader)
	require.Equal(t, int64(1), got["ingot.local_blobs.removals/budget"], "budget removals (all: %v)", got)
	require.Equal(t, size, got["ingot.local_blobs.removed_bytes/budget"], "budget bytes (all: %v)", got)
}

// TestLocalBlobMetrics_OrphanPass: the orphan pass counts the temp files it
// deletes.
func TestLocalBlobMetrics_OrphanPass(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, _ := newSweepBackend(t, meteredBackend(reader))
	old := time.Now().Add(-2 * DefaultLocalBlobOrphanAge)
	dir := filepath.Dir(b.cache.Path(multihash.Multihash{0}))
	for _, name := range []string{".tmp-old", ".tmp-older"} {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte("partial"), 0o644))
		require.NoError(t, os.Chtimes(path, old, old))
	}

	sweepLocalBlobs(t, b)

	got := collectLocalBlobMetrics(t, reader)
	wantBytes := int64(2 * len("partial"))
	require.Equal(t, int64(2), got["ingot.local_blobs.removals/orphan"], "orphan removals (all: %v)", got)
	require.Equal(t, wantBytes, got["ingot.local_blobs.removed_bytes/orphan"], "orphan bytes (all: %v)", got)
}

// TestLocalBlobMetrics_ReleaseOfACopyAlreadyGone: a release whose cached copy
// is already gone, as on a retry after the copy was removed, counts nothing.
func TestLocalBlobMetrics_ReleaseOfACopyAlreadyGone(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	b, _ := newDeferredBackend(t, testfakes.NopUploader{}, meteredBackend(reader))
	key := "deleted"
	putObj(t, b, key, testBody(1<<10))
	require.NoError(t, os.Remove(localPath(b, blobDigestOf(t, b, key, ""))))
	deleteObj(t, b, key)

	n, err := b.SweepPendingReleases(context.Background())
	require.NoError(t, err, "release sweep executed %d releases (err=%v), want 1", n, err)
	require.Equal(t, 1, n, "release sweep executed %d releases (err=%v), want 1", n, err)

	got := collectLocalBlobMetrics(t, reader)
	require.Zero(t, got["ingot.local_blobs.removals/released"], "released removals (all: %v)", got)
	require.Zero(t, got["ingot.local_blobs.removed_bytes/released"], "released bytes (all: %v)", got)
}

// TestLocalBlobMetrics_StalledBytes: the stalled_bytes gauge reports nothing
// until a sweep has summed the stalled uploads, then the bytes of intents
// uploading for over an hour, not those of a body uploading just now.
func TestLocalBlobMetrics_StalledBytes(t *testing.T) {
	ctx := t.Context()
	reader := sdkmetric.NewManualReader()
	b, mem := newSweepBackend(t, meteredBackend(reader))
	uploading := func(body string) (multihash.Multihash, int64) {
		t.Helper()
		d := mustSum([]byte(body))
		n := int64(len(body))
		require.NoError(t, mem.PutIntent(ctx, registry.UploadIntent{Space: sweepSpace(t, mem), Digest: d, Size: n, State: registry.IntentUploading}), "PutIntent")
		return d, n
	}
	stalled, n := uploading("an upload that started and failed an hour ago")
	mem.AgeIntent(stalled, 2*stalledUploadAge)
	uploading("an upload in progress")

	for key := range collectLocalBlobMetrics(t, reader) {
		require.False(t, strings.HasPrefix(key, "ingot.local_blobs.stalled_bytes"), "%s reported before a sweep", key)
	}
	sweepLocalBlobs(t, b)
	got := collectLocalBlobMetrics(t, reader)
	v, ok := got["ingot.local_blobs.stalled_bytes/uploading"]
	require.True(t, ok, "stalled_bytes after a sweep: uploading %d (reported %v); want %d", v, ok, n)
	require.Equal(t, n, v, "stalled_bytes after a sweep: uploading %d (reported %v)", v, ok)
}
