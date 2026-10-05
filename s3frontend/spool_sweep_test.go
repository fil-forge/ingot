package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

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
// has a space and a tenant.
func newSweepBackend(t *testing.T, mods ...func(*Deps)) (*Backend, *inmem.MemStore) {
	t.Helper()
	b, mem := newDeferredBackend(t, inmem.NopUploader{}, mods...)
	if err := mem.Create(t.Context(), sweepBucket, testutil.RandomDID(t), registry.CreateState{Tenant: testutil.RandomDID(t)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return b, mem
}

// putSweepObjects writes n single-blob objects, oldest first, and returns
// their blob digests in the same order.
func putSweepObjects(t *testing.T, b *Backend, n int) []multihash.Multihash {
	t.Helper()
	ctx := t.Context()
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
	stats, err := b.SweepSpool(t.Context())
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

	if target := b.spoolMaxBytes * spoolLowWatermarkPercent / 100; b.spool.Usage() > target {
		t.Fatalf("usage after the sweep = %d, want at most %d", b.spool.Usage(), target)
	}
}

// TestSweepSpool_KeepsBlobsTheProviderMayNotHold: no pass removes the file of
// a spooled or uploading intent (the only copy), or of an accepted intent
// whose location was never recorded, however far over budget the spool is.
func TestSweepSpool_KeepsBlobsTheProviderMayNotHold(t *testing.T) {
	ctx := t.Context()
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

	stats := sweepSpool(t, b)

	// The missing file is marked but not counted as evicted; the count still
	// held its bytes, so the pass went on to evict the next blob too.
	got := struct {
		Blob  []blobState
		Files int64
	}{blobStates(t, b, mem, digests[:1]), stats.BudgetFiles}
	want := struct {
		Blob  []blobState
		Files int64
	}{[]blobState{{false, true}}, 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("after the sweep = %+v, want %+v", got, want)
	}
}

// TestSweepSpool_OrphanPass: old .tmp-* files and old blob files with no
// intent are deleted; young ones, and old files with an intent, stay; the
// usage count is reset to what remains, the young temp file included.
func TestSweepSpool_OrphanPass(t *testing.T) {
	ctx := t.Context()
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
		Usage: int64(len("partial") + len("young orphan") + len("old with intent")),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("spool after the orphan pass = %+v, want %+v", got, want)
	}
}

// TestSweepSpool_PagesThroughEvictableRows: a budget pass that needs more
// rows than one query returns pages on, oldest first.
func TestSweepSpool_PagesThroughEvictableRows(t *testing.T) {
	b, mem := newSweepBackend(t)
	b.spoolSweepBatch = 2
	digests := putSweepObjects(t, b, 5)
	budgetToEvict(b, 4, blobSize(t, b, digests[0]))

	sweepSpool(t, b)

	want := []blobState{{false, true}, {false, true}, {false, true}, {false, true}, {true, false}}
	if got := blobStates(t, b, mem, digests); !reflect.DeepEqual(got, want) {
		t.Fatalf("blobs after the sweep = %+v, want %+v", got, want)
	}
}

// TestSweepSpool_SkipsAFileItCannotRemove: a file whose removal fails is left
// unmarked, and the pass goes on to the next one instead of stopping there on
// every sweep.
func TestSweepSpool_SkipsAFileItCannotRemove(t *testing.T) {
	b, mem := newSweepBackend(t)
	digests := putSweepObjects(t, b, 3)
	budgetToEvict(b, 1, blobSize(t, b, digests[0]))
	// A non-empty directory at the blob's path makes its removal fail.
	path := b.spool.Path(digests[0])
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(path, "stuck"), 0o755); err != nil {
		t.Fatal(err)
	}

	sweepSpool(t, b)

	want := []blobState{{true, false}, {false, true}, {true, false}}
	if got := blobStates(t, b, mem, digests); !reflect.DeepEqual(got, want) {
		t.Fatalf("blobs after the sweep = %+v, want %+v", got, want)
	}
}

// TestSweepSpool_EvictsAParkedBlob: a parked intent's file goes on its park
// row, as an accepted one's does on its location.
func TestSweepSpool_EvictsAParkedBlob(t *testing.T) {
	ctx := t.Context()
	b, mem := newSweepBackend(t, func(d *Deps) { d.SpoolMaxBytes = 1 })
	d, n, err := b.spool.WriteBlob(ctx, bytes.NewReader([]byte("parked part")))
	if err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}
	if err := mem.PutIntent(ctx, registry.UploadIntent{Digest: d, LocalPath: b.spool.Path(d), Size: n, State: registry.IntentParked}); err != nil {
		t.Fatalf("PutIntent: %v", err)
	}
	if err := mem.PutPark(ctx, registry.BlobPark{Digest: d, Size: n}); err != nil {
		t.Fatalf("PutPark: %v", err)
	}

	sweepSpool(t, b)

	want := []blobState{{false, true}}
	if got := blobStates(t, b, mem, []multihash.Multihash{d}); !reflect.DeepEqual(got, want) {
		t.Fatalf("blob after the sweep = %+v, want %+v", got, want)
	}
}

// TestEvictToBudget_ReportsWhyItStopped: a pass past its deadline evicts
// nothing and is not exhausted; a pass with no evictable rows is.
func TestEvictToBudget_ReportsWhyItStopped(t *testing.T) {
	ctx := t.Context()
	t.Run("deadline", func(t *testing.T) {
		b, _ := newSweepBackend(t)
		digests := putSweepObjects(t, b, 2)
		budgetToEvict(b, 1, blobSize(t, b, digests[0]))
		pass, err := b.evictToBudget(ctx, time.Now().Add(-time.Second))
		if err != nil {
			t.Fatalf("evictToBudget: %v", err)
		}
		if pass != (budgetPass{}) {
			t.Fatalf("pass = %+v, want nothing evicted and not exhausted", pass)
		}
	})
	t.Run("no evictable rows", func(t *testing.T) {
		b, _ := newSweepBackend(t, func(d *Deps) { d.SpoolMaxBytes = 1 })
		if _, _, err := b.spool.WriteBlob(ctx, bytes.NewReader([]byte("no intent"))); err != nil {
			t.Fatalf("WriteBlob: %v", err)
		}
		pass, err := b.evictToBudget(ctx, time.Now().Add(time.Minute))
		if err != nil {
			t.Fatalf("evictToBudget: %v", err)
		}
		if pass != (budgetPass{exhausted: true}) {
			t.Fatalf("pass = %+v, want exhausted", pass)
		}
	})
}

// TestSweepSpool_OrphanPassRunsHourly: a sweep within the hour after an
// orphan pass leaves an old orphan alone; the next one due removes it.
func TestSweepSpool_OrphanPassRunsHourly(t *testing.T) {
	b, _ := newSweepBackend(t)
	sweepSpool(t, b)
	path := filepath.Join(filepath.Dir(b.spool.Path(multihash.Multihash{0})), ".tmp-old")
	if err := os.WriteFile(path, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * DefaultSpoolOrphanAge)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}

	sweepSpool(t, b)
	_, errSoon := os.Stat(path)
	b.lastOrphanPass = b.lastOrphanPass.Add(-spoolOrphanPassInterval)
	sweepSpool(t, b)
	_, errDue := os.Stat(path)

	got := [2]bool{errSoon == nil, errDue == nil}
	if got != [2]bool{true, false} {
		t.Fatalf("orphan present after [a sweep within the hour, the next one due] = %v, want [true false]", got)
	}
}

// writingIntents writes a blob to the spool from inside MissingIntents, as a
// request can while the orphan pass waits on its queries.
type writingIntents struct {
	registry.IntentStore
	write func()
}

func (w writingIntents) MissingIntents(ctx context.Context, digests []multihash.Multihash) ([]multihash.Multihash, error) {
	w.write()
	return w.IntentStore.MissingIntents(ctx, digests)
}

// TestSweepSpool_OrphanPassKeepsConcurrentWrites: a blob written while the
// orphan pass runs is still counted once the pass has corrected the usage
// count to its scan.
func TestSweepSpool_OrphanPassKeepsConcurrentWrites(t *testing.T) {
	ctx := t.Context()
	b, _ := newSweepBackend(t)
	d, _, err := b.spool.WriteBlob(ctx, bytes.NewReader([]byte("old orphan")))
	if err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}
	old := time.Now().Add(-2 * DefaultSpoolOrphanAge)
	if err := os.Chtimes(b.spool.Path(d), old, old); err != nil {
		t.Fatal(err)
	}
	const late = "written during the pass"
	b.intents = writingIntents{IntentStore: b.intents, write: func() {
		if _, _, err := b.spool.WriteBlob(ctx, bytes.NewReader([]byte(late))); err != nil {
			t.Errorf("WriteBlob during the pass: %v", err)
		}
	}}

	sweepSpool(t, b)

	if got, want := b.spool.Usage(), int64(len(late)); got != want {
		t.Fatalf("usage after the orphan pass = %d, want %d (the late blob, the orphan gone)", got, want)
	}
}

// TestSweepSpool_OrphanPassSkipsAFileItCannotRemove: an orphan whose removal
// fails is skipped; the pass removes the others and still counts as run, so
// the next sweep does not repeat its scan.
func TestSweepSpool_OrphanPassSkipsAFileItCannotRemove(t *testing.T) {
	ctx := t.Context()
	b, _ := newSweepBackend(t)
	old := time.Now().Add(-2 * DefaultSpoolOrphanAge)
	var digests []multihash.Multihash
	for _, body := range []string{"stuck orphan", "other orphan"} {
		d, _, err := b.spool.WriteBlob(ctx, bytes.NewReader([]byte(body)))
		if err != nil {
			t.Fatalf("WriteBlob: %v", err)
		}
		if err := os.Chtimes(b.spool.Path(d), old, old); err != nil {
			t.Fatal(err)
		}
		digests = append(digests, d)
	}
	// After the scan, a non-empty directory replaces the first orphan, so
	// its removal fails.
	stuck := b.spool.Path(digests[0])
	b.intents = writingIntents{IntentStore: b.intents, write: func() {
		if err := os.Remove(stuck); err != nil {
			t.Errorf("remove: %v", err)
		}
		if err := os.MkdirAll(filepath.Join(stuck, "child"), 0o755); err != nil {
			t.Errorf("mkdir: %v", err)
		}
	}}

	stats, err := b.SweepSpool(ctx)
	if err != nil {
		t.Fatalf("SweepSpool: %v", err)
	}

	_, otherErr := os.Stat(b.spool.Path(digests[1]))
	got := struct {
		OrphanFiles int64
		OtherGone   bool
		Recorded    bool
	}{stats.OrphanFiles, os.IsNotExist(otherErr), !b.lastOrphanPass.IsZero()}
	want := struct {
		OrphanFiles int64
		OtherGone   bool
		Recorded    bool
	}{1, true, true}
	if got != want {
		t.Fatalf("orphan pass = %+v, want %+v", got, want)
	}
}
