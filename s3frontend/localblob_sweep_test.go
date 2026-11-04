package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/versitygw/s3response"

	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ucantone/did"
)

// These tests drive SweepLocalBlobs over a real on-disk spool and cache. The
// backend has no task queues, so a held blob moves into the cache directly.
// testfakes.NopUploader records a location for every blob, and
// testfakes.NopBaseReader cannot serve an evicted body, so they assert which files
// remain and never read an evicted object back; the network read of an evicted
// blob is covered by the itest.

const sweepBucket = "sweep"

// newSweepBackend returns a backend over an on-disk spool with a bucket that
// has a space and a tenant.
func newSweepBackend(t *testing.T, mods ...func(*Deps)) (*Backend, *testStore) {
	t.Helper()
	b, mem := newDeferredBackend(t, testfakes.NopUploader{}, mods...)
	if err := mem.Create(t.Context(), sweepBucket, testutil.RandomDID(t), registry.CreateState{Tenant: testutil.RandomDID(t)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return b, mem
}

// sweepSpace is the sweep bucket's space, which its intents and parks carry.
func sweepSpace(t *testing.T, mem *testStore) did.DID {
	t.Helper()
	st, err := mem.Get(t.Context(), sweepBucket)
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	return st.Space
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
		blobs := storedBlobs(t, b, rv)
		if len(blobs) != 1 {
			t.Fatalf("object %s has %d blobs, want 1", key, len(blobs))
		}
		digests = append(digests, blobs[0].Digest)
		// Distinct modification times, so eviction order is by age.
		time.Sleep(2 * time.Millisecond)
	}
	return digests
}

// budgetToEvict sets the spool budget so the budget pass must evict exactly
// n of blobs, each blobSize bytes, to reach its low watermark.
func budgetToEvict(b *Backend, n int, blobSize int64) {
	target := b.localUsage() - int64(n)*blobSize
	b.localBlobMaxBytes = (target/lowWatermarkPercent + 1) * 100
}

// onDisk reports, for each digest, whether its blob has a local copy.
func onDisk(t *testing.T, b *Backend, digests []multihash.Multihash) []bool {
	t.Helper()
	var out []bool
	for _, d := range digests {
		_, err := os.Stat(localPath(b, d))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat %x: %v", d, err)
		}
		out = append(out, err == nil)
	}
	return out
}

func sweepLocalBlobs(t *testing.T, b *Backend) LocalBlobSweepStats {
	t.Helper()
	stats, err := b.SweepLocalBlobs(t.Context())
	if err != nil {
		t.Fatalf("SweepLocalBlobs: %v", err)
	}
	return stats
}

func blobSize(t *testing.T, b *Backend, d multihash.Multihash) int64 {
	t.Helper()
	info, err := os.Stat(localPath(b, d))
	if err != nil {
		t.Fatalf("stat %x: %v", d, err)
	}
	return info.Size()
}

func TestSweepLocalBlobs_EvictsOldestFirstToTheLowWatermark(t *testing.T) {
	b, _ := newSweepBackend(t)
	digests := putSweepObjects(t, b, 4)
	budgetToEvict(b, 2, blobSize(t, b, digests[0]))

	sweepLocalBlobs(t, b)

	want := []bool{false, false, true, true}
	if got := onDisk(t, b, digests); !reflect.DeepEqual(got, want) {
		t.Fatalf("blobs on disk after the sweep = %v, want %v", got, want)
	}
}

// TestSweepLocalBlobs_EvictsInOrderOfRenewal: a blob whose time in the cache
// was renewed since (a read, or another write of the same bytes) goes after
// the ones that were not.
func TestSweepLocalBlobs_EvictsInOrderOfRenewal(t *testing.T) {
	b, _ := newSweepBackend(t)
	digests := putSweepObjects(t, b, 3)
	if ok, err := b.cache.Touch(digests[0], time.Now().Add(time.Minute)); err != nil || !ok {
		t.Fatalf("Touch = %v, %v", ok, err)
	}
	budgetToEvict(b, 1, blobSize(t, b, digests[0]))

	sweepLocalBlobs(t, b)

	want := []bool{true, false, true}
	if got := onDisk(t, b, digests); !reflect.DeepEqual(got, want) {
		t.Fatalf("blobs on disk after the sweep = %v, want %v", got, want)
	}
}

func TestSweepLocalBlobs_UsageEndsAtOrBelowTheLowWatermark(t *testing.T) {
	b, _ := newSweepBackend(t)
	digests := putSweepObjects(t, b, 4)
	budgetToEvict(b, 2, blobSize(t, b, digests[0]))

	sweepLocalBlobs(t, b)

	if target := b.localBlobMaxBytes * lowWatermarkPercent / 100; b.localUsage() > target {
		t.Fatalf("usage after the sweep = %d, want at most %d", b.localUsage(), target)
	}
}

// TestSweepLocalBlobs_KeepsTheSpool: the budget pass removes no spool file,
// however far over budget the spool is: a body being uploaded, whose only copy
// it is, or one the provider holds that has yet to move into the cache.
func TestSweepLocalBlobs_KeepsTheSpool(t *testing.T) {
	ctx := t.Context()
	b, mem := newSweepBackend(t, func(d *Deps) { d.LocalBlobMaxBytes = 1 })
	var digests []multihash.Multihash
	for _, state := range []string{registry.IntentSpooled, registry.IntentUploading, registry.IntentAccepted} {
		d, n, err := b.spool.WriteBlob(ctx, bytes.NewReader([]byte("only copy: "+state)))
		if err != nil {
			t.Fatalf("WriteBlob: %v", err)
		}
		if err := mem.PutIntent(ctx, registry.UploadIntent{Space: sweepSpace(t, mem), Digest: d, LocalPath: localPath(b, d), Size: n, State: state}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		digests = append(digests, d)
	}

	sweepLocalBlobs(t, b)

	want := []bool{true, true, true}
	if got := onDisk(t, b, digests); !reflect.DeepEqual(got, want) {
		t.Fatalf("blobs on disk after the sweep = %v, want %v", got, want)
	}
}

func TestSweepLocalBlobs_NoBudgetEvictsNothing(t *testing.T) {
	b, _ := newSweepBackend(t)
	putSweepObjects(t, b, 3)

	stats := sweepLocalBlobs(t, b)

	if stats != (LocalBlobSweepStats{}) {
		t.Fatalf("sweep stats = %+v, want nothing removed", stats)
	}
}

// TestSweepLocalBlobs_OrphanPass: old .tmp-* files are deleted from the spool
// and the cache, and old spool blob files with no intent; young ones, old
// files with an intent, and cached blob files (never checked against intents)
// stay; the usage count comes to the blob files that remain.
// A temp file counts only while a live write holds it, so the young one this
// test wrote by hand does not.
func TestSweepLocalBlobs_OrphanPass(t *testing.T) {
	ctx := t.Context()
	b, mem := newSweepBackend(t)
	old := time.Now().Add(-2 * DefaultLocalBlobOrphanAge)

	writeFile := func(dir, name, body string, modTime time.Time) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, modTime, modTime); err != nil {
			t.Fatal(err)
		}
		return path
	}
	writeBlob := func(body string, modTime time.Time, withIntent, cached bool) string {
		t.Helper()
		d, n, err := b.spool.WriteBlob(ctx, bytes.NewReader([]byte(body)))
		if err != nil {
			t.Fatalf("WriteBlob: %v", err)
		}
		if cached {
			if _, err := b.cache.Take(b.spool, d); err != nil {
				t.Fatalf("Take: %v", err)
			}
		}
		if withIntent {
			if err := mem.PutIntent(ctx, registry.UploadIntent{Space: sweepSpace(t, mem), Digest: d, LocalPath: localPath(b, d), Size: n, State: registry.IntentSpooled}); err != nil {
				t.Fatalf("PutIntent: %v", err)
			}
		}
		if err := os.Chtimes(localPath(b, d), modTime, modTime); err != nil {
			t.Fatal(err)
		}
		return localPath(b, d)
	}
	spoolDir := filepath.Dir(b.spool.Path(multihash.Multihash{0}))
	cacheDir := filepath.Dir(b.cache.Path(multihash.Multihash{0}))
	paths := map[string]string{
		"old temp":                    writeFile(spoolDir, ".tmp-old", "partial", old),
		"young temp":                  writeFile(spoolDir, ".tmp-young", "partial", time.Now()),
		"old temp in cache":           writeFile(cacheDir, ".tmp-old", "partial", old),
		"old orphan blob":             writeBlob("old orphan", old, false, false),
		"young orphan blob":           writeBlob("young orphan", time.Now(), false, false),
		"old blob with intent":        writeBlob("old with intent", old, true, false),
		"old orphan blob in cache":    writeBlob("old cached orphan", old, false, true),
		"old cached blob with intent": writeBlob("old cached with intent", old, true, true),
	}

	sweepLocalBlobs(t, b)

	got := struct {
		OnDisk map[string]bool
		Usage  int64
	}{OnDisk: map[string]bool{}, Usage: b.localUsage()}
	for name, path := range paths {
		_, err := os.Stat(path)
		got.OnDisk[name] = err == nil
	}
	want := struct {
		OnDisk map[string]bool
		Usage  int64
	}{
		OnDisk: map[string]bool{
			"old temp":                    false,
			"young temp":                  true,
			"old temp in cache":           false,
			"old orphan blob":             false,
			"young orphan blob":           true,
			"old blob with intent":        true,
			"old orphan blob in cache":    true,
			"old cached blob with intent": true,
		},
		Usage: int64(len("young orphan") + len("old with intent") + len("old cached orphan") + len("old cached with intent")),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("spool after the orphan pass = %+v, want %+v", got, want)
	}
}

// TestSweepLocalBlobs_SkipsAFileItCannotRemove: a file whose removal fails is
// left in place and the pass goes on to the next one, logging the failures
// once.
func TestSweepLocalBlobs_SkipsAFileItCannotRemove(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	b, _ := newSweepBackend(t, func(d *Deps) { d.Logger = zap.New(core) })
	digests := putSweepObjects(t, b, 3)
	budgetToEvict(b, 1, blobSize(t, b, digests[0]))
	// A read-only cache directory makes every removal fail.
	dir := filepath.Dir(b.cache.Path(digests[0]))
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	stats := sweepLocalBlobs(t, b)

	got := fmt.Sprint(onDisk(t, b, digests), stats.BudgetFiles, logs.FilterMessageSnippet("could not remove some files").Len())
	if want := fmt.Sprint([]bool{true, true, true}, 0, 1); got != want {
		t.Fatalf("[on disk] [evicted] [failure logs] = %s, want %s", got, want)
	}
}

// TestEvictToBudget_ReportsWhyItStopped: each way a pass ends is reported.
func TestEvictToBudget_ReportsWhyItStopped(t *testing.T) {
	ctx := t.Context()
	t.Run("deadline", func(t *testing.T) {
		b, _ := newSweepBackend(t)
		digests := putSweepObjects(t, b, 2)
		budgetToEvict(b, 1, blobSize(t, b, digests[0]))
		pass, err := b.evictToBudget(ctx, -time.Second)
		if err != nil {
			t.Fatalf("evictToBudget: %v", err)
		}
		if pass != (budgetPass{stop: stopDeadline}) {
			t.Fatalf("pass = %+v, want nothing evicted, stopped at the deadline", pass)
		}
	})
	t.Run("no cached blobs", func(t *testing.T) {
		b, _ := newSweepBackend(t, func(d *Deps) { d.LocalBlobMaxBytes = 1 })
		if _, _, err := b.spool.WriteBlob(ctx, bytes.NewReader([]byte("no intent"))); err != nil {
			t.Fatalf("WriteBlob: %v", err)
		}
		pass, err := b.evictToBudget(ctx, time.Minute)
		if err != nil {
			t.Fatalf("evictToBudget: %v", err)
		}
		if pass != (budgetPass{stop: stopExhausted}) {
			t.Fatalf("pass = %+v, want exhausted", pass)
		}
	})
	t.Run("watermark", func(t *testing.T) {
		b, _ := newSweepBackend(t)
		digests := putSweepObjects(t, b, 2)
		size := blobSize(t, b, digests[0])
		budgetToEvict(b, 1, size)
		pass, err := b.evictToBudget(ctx, time.Minute)
		if err != nil {
			t.Fatalf("evictToBudget: %v", err)
		}
		if pass != (budgetPass{files: 1, bytes: size, stop: stopWatermark}) {
			t.Fatalf("pass = %+v, want one eviction, stopped at the watermark", pass)
		}
	})
}

// TestSweepLocalBlobs_OrphanPassRunsHourly: a sweep within the hour after an
// orphan pass leaves an old orphan alone; the next one due removes it.
func TestSweepLocalBlobs_OrphanPassRunsHourly(t *testing.T) {
	b, _ := newSweepBackend(t)
	sweepLocalBlobs(t, b)
	path := filepath.Join(filepath.Dir(b.spool.Path(multihash.Multihash{0})), ".tmp-old")
	if err := os.WriteFile(path, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * DefaultLocalBlobOrphanAge)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}

	sweepLocalBlobs(t, b)
	_, errSoon := os.Stat(path)
	b.lastOrphanPass = b.lastOrphanPass.Add(-orphanPassInterval)
	sweepLocalBlobs(t, b)
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

// failingIntents fails every MissingIntents query and counts the calls.
type failingIntents struct {
	registry.IntentStore
	calls *int
}

func (f failingIntents) MissingIntents(context.Context, []multihash.Multihash) ([]multihash.Multihash, error) {
	*f.calls++
	return nil, errors.New("database unavailable")
}

// TestSweepLocalBlobs_OrphanPassBacksOffAfterAFailure: an orphan pass whose
// query fails reports the error but still counts as run, so the next sweep
// within the hour does not scan again.
func TestSweepLocalBlobs_OrphanPassBacksOffAfterAFailure(t *testing.T) {
	ctx := t.Context()
	b, _ := newSweepBackend(t)
	d, _, err := b.spool.WriteBlob(ctx, bytes.NewReader([]byte("old orphan")))
	if err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}
	old := time.Now().Add(-2 * DefaultLocalBlobOrphanAge)
	if err := os.Chtimes(localPath(b, d), old, old); err != nil {
		t.Fatal(err)
	}
	var calls int
	b.intents = failingIntents{IntentStore: b.intents, calls: &calls}

	_, firstErr := b.SweepLocalBlobs(ctx)
	_, secondErr := b.SweepLocalBlobs(ctx)

	got := struct {
		FirstFailed, SecondFailed bool
		Calls                     int
	}{firstErr != nil, secondErr != nil, calls}
	want := struct {
		FirstFailed, SecondFailed bool
		Calls                     int
	}{true, false, 1}
	if got != want {
		t.Fatalf("two sweeps against a failing database = %+v, want %+v", got, want)
	}
}

// TestSweepLocalBlobs_OrphanPassKeepsConcurrentWrites: a blob written while the
// orphan pass runs is still counted after it.
func TestSweepLocalBlobs_OrphanPassKeepsConcurrentWrites(t *testing.T) {
	ctx := t.Context()
	b, _ := newSweepBackend(t)
	d, _, err := b.spool.WriteBlob(ctx, bytes.NewReader([]byte("old orphan")))
	if err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}
	old := time.Now().Add(-2 * DefaultLocalBlobOrphanAge)
	if err := os.Chtimes(localPath(b, d), old, old); err != nil {
		t.Fatal(err)
	}
	const late = "written during the pass"
	b.intents = writingIntents{IntentStore: b.intents, write: func() {
		if _, _, err := b.spool.WriteBlob(ctx, bytes.NewReader([]byte(late))); err != nil {
			t.Errorf("WriteBlob during the pass: %v", err)
		}
	}}

	sweepLocalBlobs(t, b)

	if got, want := b.localUsage(), int64(len(late)); got != want {
		t.Fatalf("usage after the orphan pass = %d, want %d (the late blob, the orphan gone)", got, want)
	}
}

// TestSweepLocalBlobs_OrphanPassSkipsAFileItCannotRemove: an orphan whose removal
// fails is skipped; the pass removes the others and still counts as run, so
// the next sweep does not repeat its scan.
func TestSweepLocalBlobs_OrphanPassSkipsAFileItCannotRemove(t *testing.T) {
	ctx := t.Context()
	b, _ := newSweepBackend(t)
	old := time.Now().Add(-2 * DefaultLocalBlobOrphanAge)
	var digests []multihash.Multihash
	for _, body := range []string{"stuck orphan", "other orphan"} {
		d, _, err := b.spool.WriteBlob(ctx, bytes.NewReader([]byte(body)))
		if err != nil {
			t.Fatalf("WriteBlob: %v", err)
		}
		if err := os.Chtimes(localPath(b, d), old, old); err != nil {
			t.Fatal(err)
		}
		digests = append(digests, d)
	}
	// After the scan, a non-empty directory replaces the first orphan, so
	// its removal fails.
	stuck := localPath(b, digests[0])
	b.intents = writingIntents{IntentStore: b.intents, write: func() {
		if err := os.Remove(stuck); err != nil {
			t.Errorf("remove: %v", err)
		}
		if err := os.MkdirAll(filepath.Join(stuck, "child"), 0o755); err != nil {
			t.Errorf("mkdir: %v", err)
		}
	}}

	stats, err := b.SweepLocalBlobs(ctx)
	if err != nil {
		t.Fatalf("SweepLocalBlobs: %v", err)
	}

	_, otherErr := os.Stat(localPath(b, digests[1]))
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

// TestHeldBlobMovesToTheCache: once a PUT's blob is accepted and its location
// recorded, its copy is in the cache and the spool holds nothing.
func TestHeldBlobMovesToTheCache(t *testing.T) {
	b, _ := newSweepBackend(t)
	d := putSweepObjects(t, b, 1)[0]
	size := blobSize(t, b, d)

	_, spoolErr := os.Stat(b.spool.Path(d))
	_, cacheErr := os.Stat(b.cache.Path(d))
	got := fmt.Sprint(os.IsNotExist(spoolErr), cacheErr == nil, b.spool.Usage(), b.cache.Usage())
	if want := fmt.Sprint(true, true, 0, size); got != want {
		t.Fatalf("[spool copy gone] [cache copy] spool cache = %s, want %s", got, want)
	}
}

// TestSweepLocalBlobs_WarnsOncePerOverBudgetEpisode: the warning that nothing is
// left to evict is armed until usage falls back to the low watermark.
func TestSweepLocalBlobs_WarnsOncePerOverBudgetEpisode(t *testing.T) {
	ctx := t.Context()
	b, _ := newSweepBackend(t, func(d *Deps) { d.LocalBlobMaxBytes = 1 })
	if _, _, err := b.spool.WriteBlob(ctx, bytes.NewReader([]byte("unevictable"))); err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}
	var got []bool
	sweepLocalBlobs(t, b)
	got = append(got, b.overBudgetWarned)
	sweepLocalBlobs(t, b)
	got = append(got, b.overBudgetWarned)
	b.localBlobMaxBytes = 1 << 20
	sweepLocalBlobs(t, b)
	got = append(got, b.overBudgetWarned)
	if want := []bool{true, true, false}; !reflect.DeepEqual(got, want) {
		t.Fatalf("warned after [over, still over, back under] = %v, want %v", got, want)
	}
}

// TestSweepLocalBlobs_LatchesResetAtTheLowWatermark: the once-per-episode
// latches stay set while usage is under the budget but over the low
// watermark, and reset at the low watermark.
func TestSweepLocalBlobs_LatchesResetAtTheLowWatermark(t *testing.T) {
	b, _ := newSweepBackend(t, func(d *Deps) { d.LocalBlobMaxBytes = 1 })
	if _, _, err := b.spool.WriteBlob(t.Context(), bytes.NewReader(bytes.Repeat([]byte("u"), 1000))); err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}
	b.timeLimitLogged = true
	type latches struct{ Warned, TimeLimitLogged bool }
	var got []latches
	for _, percent := range []int64{0, 95, 89} {
		if percent > 0 {
			// Usage is percent% of the budget.
			b.localBlobMaxBytes = b.localUsage() * 100 / percent
		}
		sweepLocalBlobs(t, b)
		got = append(got, latches{b.overBudgetWarned, b.timeLimitLogged})
	}
	want := []latches{{true, true}, {true, true}, {false, false}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("latches after [over budget, at 95%%, at 89%%] = %+v, want %+v", got, want)
	}
}

// TestSweepLocalBlobs_TimeLimit: a scan cut off by the budget pass's own time
// limit is the time limit, not a failure: the pass says so once and leaves the
// rest to the next sweep.
func TestSweepLocalBlobs_TimeLimit(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	b, _ := newSweepBackend(t, func(d *Deps) { d.Logger = zap.New(core) })
	digests := putSweepObjects(t, b, 1)
	b.localBlobMaxBytes = 1
	defer func(d time.Duration) { budgetPassTimeLimit = d }(budgetPassTimeLimit)
	budgetPassTimeLimit = 0

	sweepLocalBlobs(t, b)

	got := fmt.Sprint(onDisk(t, b, digests), logs.FilterMessageSnippet("budget pass reached its time limit").Len(), b.timeLimitLogged)
	if want := fmt.Sprint([]bool{true}, 1, true); got != want {
		t.Fatalf("[on disk] [logs] [latched] = %s, want %s", got, want)
	}
}

// TestRemoveLocalRacingTakeLeavesNoCopy: a removal racing the move of the
// same blob into the cache leaves no copy in either directory, whichever
// runs first.
func TestRemoveLocalRacingTakeLeavesNoCopy(t *testing.T) {
	ctx := t.Context()
	b, _ := newSweepBackend(t)
	for i := range 500 {
		d, _, err := b.spool.WriteBlob(ctx, bytes.NewReader(fmt.Appendf(nil, "racing blob %d", i)))
		if err != nil {
			t.Fatalf("WriteBlob: %v", err)
		}
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := b.cache.Take(b.spool, d); err != nil {
				t.Errorf("Take: %v", err)
			}
		}()
		if _, err := b.removeLocal(d); err != nil {
			t.Fatalf("removeLocal: %v", err)
		}
		wg.Wait()
		if fileExists(b.spool.Path(d)) || fileExists(b.cache.Path(d)) {
			t.Fatalf("iteration %d: a copy survived removeLocal", i)
		}
	}
	if got := b.localUsage(); got != 0 {
		t.Fatalf("local usage = %d, want 0", got)
	}
}

// TestSweepLocalBlobs_OrphanPassCorrectsCountsAfterAManualRemoval: a cached
// blob deleted by hand stays counted until the next orphan pass, which finds
// the directory quiet and corrects the count.
func TestSweepLocalBlobs_OrphanPassCorrectsCountsAfterAManualRemoval(t *testing.T) {
	b, _ := newSweepBackend(t)
	digests := putSweepObjects(t, b, 2)
	size := blobSize(t, b, digests[0])
	before := b.localUsage()
	if err := os.Remove(localPath(b, digests[0])); err != nil {
		t.Fatal(err)
	}
	stale := b.localUsage()

	sweepLocalBlobs(t, b)

	if got, want := [2]int64{stale, b.localUsage()}, [2]int64{before, before - size}; got != want {
		t.Fatalf("usage [after the manual removal, after the sweep] = %v, want %v", got, want)
	}
}

// TestSweepLocalBlobs_OrphanPassFinishesTheSpoolFirst: a cache scan that fails
// does not keep the spool's orphans from being removed.
func TestSweepLocalBlobs_OrphanPassFinishesTheSpoolFirst(t *testing.T) {
	ctx := t.Context()
	b, _ := newSweepBackend(t)
	d, _, err := b.spool.WriteBlob(ctx, bytes.NewReader([]byte("old orphan")))
	if err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}
	old := time.Now().Add(-2 * DefaultLocalBlobOrphanAge)
	if err := os.Chtimes(b.spool.Path(d), old, old); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Dir(b.cache.Path(d))); err != nil {
		t.Fatal(err)
	}

	stats, err := b.SweepLocalBlobs(ctx)

	if err == nil {
		t.Fatal("SweepLocalBlobs with the cache directory gone: want an error")
	}
	if stats.OrphanFiles != 1 || fileExists(b.spool.Path(d)) {
		t.Fatalf("spool orphan: removed %d files, still on disk %v; want 1 removed", stats.OrphanFiles, fileExists(b.spool.Path(d)))
	}
}

// failNthIntents fails the nth MissingIntents call and passes the others on.
type failNthIntents struct {
	registry.IntentStore
	n     int
	calls *int
}

func (f failNthIntents) MissingIntents(ctx context.Context, digests []multihash.Multihash) ([]multihash.Multihash, error) {
	*f.calls++
	if *f.calls == f.n {
		return nil, errors.New("database unavailable")
	}
	return f.IntentStore.MissingIntents(ctx, digests)
}

// TestSweepLocalBlobs_OrphanPassChecksInBatches: old spool blobs are checked a
// batch at a time as the scan reaches them; orphans go and blobs with intents
// stay, and a query that fails partway stops the pass with its error,
// removing nothing it has not checked.
func TestSweepLocalBlobs_OrphanPassChecksInBatches(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failCall int // 0: no failure
	}{{"all succeed", 0}, {"second query fails", 2}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			b, mem := newSweepBackend(t)
			b.localBlobSweepBatch = 2
			old := time.Now().Add(-2 * DefaultLocalBlobOrphanAge)
			var orphans, kept []multihash.Multihash
			for i := range 6 {
				d, n, err := b.spool.WriteBlob(ctx, bytes.NewReader(fmt.Appendf(nil, "old blob %d", i)))
				if err != nil {
					t.Fatalf("WriteBlob: %v", err)
				}
				if i%2 == 0 {
					orphans = append(orphans, d)
				} else {
					if err := mem.PutIntent(ctx, registry.UploadIntent{Space: sweepSpace(t, mem), Digest: d, LocalPath: localPath(b, d), Size: n, State: registry.IntentSpooled}); err != nil {
						t.Fatalf("PutIntent: %v", err)
					}
					kept = append(kept, d)
				}
				if err := os.Chtimes(localPath(b, d), old, old); err != nil {
					t.Fatal(err)
				}
			}
			var calls int
			if tc.failCall > 0 {
				b.intents = failNthIntents{IntentStore: b.intents, n: tc.failCall, calls: &calls}
			}

			stats, err := b.SweepLocalBlobs(ctx)

			if (err != nil) != (tc.failCall > 0) {
				t.Fatalf("SweepLocalBlobs error = %v, want failure %v", err, tc.failCall > 0)
			}
			for _, d := range kept {
				if !fileExists(localPath(b, d)) {
					t.Fatalf("blob %x with an intent was removed", d)
				}
			}
			removed := 0
			for _, d := range orphans {
				if !fileExists(localPath(b, d)) {
					removed++
				}
			}
			want := len(orphans)
			if tc.failCall > 0 {
				// Only the first batch was checked before the failure.
				want = int(stats.OrphanFiles)
				if want >= len(orphans) {
					t.Fatalf("removed %d orphans despite the failed query, want fewer than %d", want, len(orphans))
				}
			}
			if removed != want || int(stats.OrphanFiles) != want {
				t.Fatalf("orphans removed = %d (stats %d), want %d", removed, stats.OrphanFiles, want)
			}
		})
	}
}
