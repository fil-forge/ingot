package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/versitygw/s3response"

	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/registry"
)

// These tests drive SweepSpool over a real on-disk spool. inmem.NopUploader
// records a location for every blob, and inmem.NopBaseReader cannot serve an
// evicted body, so they assert which files remain and never read an evicted
// object back; the network read of an evicted blob is covered by the itest.

const sweepBucket = "sweep"

// newSweepBackend returns a backend over an on-disk spool with a bucket that
// has a space and a tenant, and no residency or read-retention window unless
// a mod sets one.
func newSweepBackend(t *testing.T, mods ...func(*Deps)) (*Backend, *inmem.MemStore) {
	t.Helper()
	b, mem := newDeferredBackend(t, inmem.NopUploader{}, mods...)
	if err := mem.Create(context.Background(), sweepBucket, testutil.RandomDID(t), registry.CreateState{Tenant: testutil.RandomDID(t)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return b, mem
}

// putSweepObjects writes n single-blob objects, oldest first, and returns
// their blob digests in the same order.
func putSweepObjects(t *testing.T, b *Backend, n int) []multihash.Multihash {
	t.Helper()
	ctx := context.Background()
	var digests []multihash.Multihash
	for i := range n {
		bucket, key := sweepBucket, fmt.Sprintf("obj-%d", i)
		body := bytes.Repeat([]byte{byte('a' + i)}, 1000)
		if _, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(body)}); err != nil {
			t.Fatalf("PutObject %s: %v", key, err)
		}
		rv, err := b.resolveVersion(ctx, bucket, key, "")
		if err != nil {
			t.Fatalf("resolveVersion %s: %v", key, err)
		}
		if len(rv.mf.Body.Blobs) != 1 {
			t.Fatalf("object %s has %d blobs, want 1", key, len(rv.mf.Body.Blobs))
		}
		digests = append(digests, rv.mf.Body.Blobs[0].Digest)
		// Distinct commit times, so eviction order is by age, not by the
		// digest tiebreak.
		time.Sleep(2 * time.Millisecond)
	}
	return digests
}

// budgetToEvict sets the spool budget so the budget pass must evict exactly
// n of blobs, each blobSize bytes, to reach its low watermark.
func budgetToEvict(b *Backend, n int, blobSize int64) {
	target := b.spool.Usage() - int64(n)*blobSize
	b.spoolMaxBytes = (target/spoolLowWatermarkPercent + 1) * 100
}

// blobState is what a test observes of one blob after a sweep.
type blobState struct {
	OnDisk  bool
	Evicted bool
}

func blobStates(t *testing.T, b *Backend, mem *inmem.MemStore, digests []multihash.Multihash) []blobState {
	t.Helper()
	var out []blobState
	for _, d := range digests {
		_, err := os.Stat(b.spool.Path(d))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat %x: %v", d, err)
		}
		out = append(out, blobState{OnDisk: err == nil, Evicted: mem.IsEvicted(d)})
	}
	return out
}

func sweepSpool(t *testing.T, b *Backend) SpoolSweepStats {
	t.Helper()
	stats, err := b.SweepSpool(context.Background())
	if err != nil {
		t.Fatalf("SweepSpool: %v", err)
	}
	return stats
}

func blobSize(t *testing.T, b *Backend, d multihash.Multihash) int64 {
	t.Helper()
	info, err := os.Stat(b.spool.Path(d))
	if err != nil {
		t.Fatalf("stat %x: %v", d, err)
	}
	return info.Size()
}

func TestSweepSpool_EvictsOldestFirstToTheLowWatermark(t *testing.T) {
	b, mem := newSweepBackend(t)
	digests := putSweepObjects(t, b, 4)
	budgetToEvict(b, 2, blobSize(t, b, digests[0]))

	sweepSpool(t, b)

	want := []blobState{{false, true}, {false, true}, {true, false}, {true, false}}
	if got := blobStates(t, b, mem, digests); !reflect.DeepEqual(got, want) {
		t.Fatalf("blobs after the sweep = %+v, want %+v", got, want)
	}
}

func TestSweepSpool_UsageEndsAtOrBelowTheLowWatermark(t *testing.T) {
	b, _ := newSweepBackend(t)
	digests := putSweepObjects(t, b, 4)
	budgetToEvict(b, 2, blobSize(t, b, digests[0]))

	sweepSpool(t, b)

	if target := b.spoolMaxBytes / 100 * spoolLowWatermarkPercent; b.spool.Usage() > target {
		t.Fatalf("usage after the sweep = %d, want at most %d", b.spool.Usage(), target)
	}
}

// TestSweepSpool_ResidencyHoldsUntilTheForcedPass: with every blob inside the
// residency window the budget pass evicts nothing, and the forced pass then
// evicts to the watermark anyway.
func TestSweepSpool_ResidencyHoldsUntilTheForcedPass(t *testing.T) {
	b, _ := newSweepBackend(t, func(d *Deps) { d.SpoolMinResidency = time.Hour })
	digests := putSweepObjects(t, b, 4)
	size := blobSize(t, b, digests[0])
	budgetToEvict(b, 2, size)

	stats := sweepSpool(t, b)

	want := SpoolSweepStats{ForcedFiles: 2, ForcedBytes: 2 * size}
	if stats != want {
		t.Fatalf("sweep stats = %+v, want %+v", stats, want)
	}
}

// TestSweepSpool_SkipsRecentlyReadBlobs: the oldest blob was just read from
// the spool, so the budget pass passes over it and evicts the next one.
func TestSweepSpool_SkipsRecentlyReadBlobs(t *testing.T) {
	b, mem := newSweepBackend(t, func(d *Deps) { d.SpoolReadRetention = time.Hour })
	digests := putSweepObjects(t, b, 4)
	bucket, key := sweepBucket, "obj-0"
	got, err := b.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	_, _ = io.Copy(io.Discard, got.Body)
	got.Body.Close()
	budgetToEvict(b, 1, blobSize(t, b, digests[0]))

	sweepSpool(t, b)

	want := []blobState{{true, false}, {false, true}, {true, false}, {true, false}}
	if states := blobStates(t, b, mem, digests); !reflect.DeepEqual(states, want) {
		t.Fatalf("blobs after the sweep = %+v, want %+v", states, want)
	}
}

// TestSweepSpool_KeepsBlobsTheProviderMayNotHold: no pass removes the file of
// a spooled or uploading intent (the only copy), or of an accepted intent
// whose location was never recorded, however far over budget the spool is.
func TestSweepSpool_KeepsBlobsTheProviderMayNotHold(t *testing.T) {
	ctx := context.Background()
	b, mem := newSweepBackend(t, func(d *Deps) { d.SpoolMaxBytes = 1 })
	var digests []multihash.Multihash
	for _, state := range []string{registry.IntentSpooled, registry.IntentUploading, registry.IntentAccepted} {
		d, n, err := b.spool.WriteBlob(ctx, bytes.NewReader([]byte("only copy: "+state)))
		if err != nil {
			t.Fatalf("WriteBlob: %v", err)
		}
		if err := mem.PutIntent(ctx, registry.UploadIntent{Digest: d, LocalPath: b.spool.Path(d), Size: n, State: state}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		digests = append(digests, d)
	}

	sweepSpool(t, b)

	want := []blobState{{true, false}, {true, false}, {true, false}}
	if got := blobStates(t, b, mem, digests); !reflect.DeepEqual(got, want) {
		t.Fatalf("blobs after the sweep = %+v, want %+v", got, want)
	}
}

func TestSweepSpool_NoBudgetEvictsNothing(t *testing.T) {
	b, _ := newSweepBackend(t)
	putSweepObjects(t, b, 3)

	stats := sweepSpool(t, b)

	if stats != (SpoolSweepStats{}) {
		t.Fatalf("sweep stats = %+v, want nothing removed", stats)
	}
}

// TestSweepSpool_FileAlreadyGoneIsMarkedEvicted: a crash between the unlink
// and the mark leaves a row describing a missing file; the next pass marks it.
func TestSweepSpool_FileAlreadyGoneIsMarkedEvicted(t *testing.T) {
	b, mem := newSweepBackend(t)
	digests := putSweepObjects(t, b, 2)
	budgetToEvict(b, 1, blobSize(t, b, digests[0]))
	if err := os.Remove(b.spool.Path(digests[0])); err != nil {
		t.Fatal(err)
	}

	sweepSpool(t, b)

	want := []blobState{{false, true}}
	if got := blobStates(t, b, mem, digests[:1]); !reflect.DeepEqual(got, want) {
		t.Fatalf("blobs after the sweep = %+v, want %+v", got, want)
	}
}

// TestSweepSpool_OrphanPass: old .tmp-* files and old blob files with no
// intent are deleted; young ones, and old files with an intent, stay; the
// usage count is reset to what remains.
func TestSweepSpool_OrphanPass(t *testing.T) {
	ctx := context.Background()
	b, mem := newSweepBackend(t)
	old := time.Now().Add(-2 * DefaultSpoolOrphanAge)

	writeFile := func(name, body string, modTime time.Time) string {
		t.Helper()
		path := filepath.Join(filepath.Dir(b.spool.Path(multihash.Multihash{0})), name)
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, modTime, modTime); err != nil {
			t.Fatal(err)
		}
		return path
	}
	writeBlob := func(body string, modTime time.Time, withIntent bool) string {
		t.Helper()
		d, n, err := b.spool.WriteBlob(ctx, bytes.NewReader([]byte(body)))
		if err != nil {
			t.Fatalf("WriteBlob: %v", err)
		}
		if withIntent {
			if err := mem.PutIntent(ctx, registry.UploadIntent{Digest: d, LocalPath: b.spool.Path(d), Size: n, State: registry.IntentSpooled}); err != nil {
				t.Fatalf("PutIntent: %v", err)
			}
		}
		if err := os.Chtimes(b.spool.Path(d), modTime, modTime); err != nil {
			t.Fatal(err)
		}
		return b.spool.Path(d)
	}
	paths := map[string]string{
		"old temp":             writeFile(".tmp-old", "partial", old),
		"young temp":           writeFile(".tmp-young", "partial", time.Now()),
		"old orphan blob":      writeBlob("old orphan", old, false),
		"young orphan blob":    writeBlob("young orphan", time.Now(), false),
		"old blob with intent": writeBlob("old with intent", old, true),
	}

	sweepSpool(t, b)

	got := struct {
		OnDisk map[string]bool
		Usage  int64
	}{OnDisk: map[string]bool{}, Usage: b.spool.Usage()}
	for name, path := range paths {
		_, err := os.Stat(path)
		got.OnDisk[name] = err == nil
	}
	want := struct {
		OnDisk map[string]bool
		Usage  int64
	}{
		OnDisk: map[string]bool{
			"old temp":             false,
			"young temp":           true,
			"old orphan blob":      false,
			"young orphan blob":    true,
			"old blob with intent": true,
		},
		Usage: int64(len("young orphan") + len("old with intent")),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("spool after the orphan pass = %+v, want %+v", got, want)
	}
}
