package s3frontend

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/versitygw/s3response"

	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ucantone/did"
)

// These tests drive SweepLocalBlobs over a real on-disk cache.
// testfakes.NopUploader records a location for every blob, and
// testfakes.NopBaseReader cannot serve an evicted body, so they assert which files
// remain and never read an evicted object back; the network read of an evicted
// blob is covered by the itest.

const sweepBucket = "sweep"

// newSweepBackend returns a backend over an on-disk cache with a bucket that
// has a space and a tenant.
func newSweepBackend(t *testing.T, mods ...func(*Deps)) (*Backend, *testStore) {
	t.Helper()
	b, mem := newDeferredBackend(t, testfakes.NopUploader{}, mods...)
	require.NoError(t, mem.Create(t.Context(), sweepBucket, testutil.RandomDID(t), registry.CreateState{Tenant: testutil.RandomDID(t)}), "create bucket")
	return b, mem
}

// sweepSpace is the sweep bucket's space, which its intents and parks carry.
func sweepSpace(t *testing.T, mem *testStore) did.DID {
	t.Helper()
	st, err := mem.Get(t.Context(), sweepBucket)
	require.NoError(t, err, "get bucket")
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
		_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(body), ContentLength: byteLen(body)})
		require.NoError(t, err, "PutObject %s", key)
		rv, err := b.resolveVersion(ctx, bucket, key, "")
		require.NoError(t, err, "resolveVersion %s", key)
		blobs := storedBlobs(t, b, rv)
		require.Len(t, blobs, 1, "object %s has %d blobs", key, len(blobs))
		digests = append(digests, blobs[0].Digest)
		settleCache(t, b, blobs[0].Digest)
		// Distinct modification times, so eviction order is by age.
		time.Sleep(2 * time.Millisecond)
	}
	return digests
}

// budgetToEvict sets the cache budget so the budget pass must evict exactly
// n of blobs, each blobSize bytes, to reach its low watermark.
func budgetToEvict(b *Backend, n int, blobSize int64) {
	target := b.cache.Usage() - int64(n)*blobSize
	b.localBlobMaxBytes = (target/lowWatermarkPercent + 1) * 100
}

// onDisk reports, for each digest, whether its blob has a local copy.
func onDisk(t *testing.T, b *Backend, digests []multihash.Multihash) []bool {
	t.Helper()
	var out []bool
	for _, d := range digests {
		_, err := os.Stat(localPath(b, d))
		require.True(t, err == nil || errors.Is(err, os.ErrNotExist), "stat %x: %v", d, err)
		out = append(out, err == nil)
	}
	return out
}

func sweepLocalBlobs(t *testing.T, b *Backend) LocalBlobSweepStats {
	t.Helper()
	stats, err := b.SweepLocalBlobs(t.Context())
	require.NoError(t, err, "SweepLocalBlobs")
	return stats
}

// inProgress starts a local copy of body in b's cache, as a write in flight
// holds, which the test discards at its end.
func inProgress(t *testing.T, b *Backend, body []byte) {
	t.Helper()
	local, err := b.cache.CreateTemp()
	require.NoError(t, err, "CreateTemp")
	t.Cleanup(local.Discard)
	_, err = local.Write(body)
	require.NoError(t, err, "Write")
}

func blobSize(t *testing.T, b *Backend, d multihash.Multihash) int64 {
	t.Helper()
	info, err := os.Stat(localPath(b, d))
	require.NoError(t, err, "stat %x", d)
	return info.Size()
}

func TestSweepLocalBlobs_EvictsOldestFirstToTheLowWatermark(t *testing.T) {
	b, _ := newSweepBackend(t)
	digests := putSweepObjects(t, b, 4)
	budgetToEvict(b, 2, blobSize(t, b, digests[0]))

	sweepLocalBlobs(t, b)

	want := []bool{false, false, true, true}
	got := onDisk(t, b, digests)
	require.Equal(t, want, got, "blobs on disk after the sweep")
}

// TestSweepLocalBlobs_EvictsInOrderOfRenewal: a blob whose time in the cache
// was renewed since (a read, or another write of the same bytes) goes after
// the ones that were not.
func TestSweepLocalBlobs_EvictsInOrderOfRenewal(t *testing.T) {
	b, _ := newSweepBackend(t)
	digests := putSweepObjects(t, b, 3)
	ok, err := b.cache.Touch(digests[0], time.Now().Add(time.Minute))
	require.NoError(t, err, "Touch = %v, %v", ok, err)
	require.True(t, ok, "Touch = %v, %v", ok, err)
	budgetToEvict(b, 1, blobSize(t, b, digests[0]))

	sweepLocalBlobs(t, b)

	want := []bool{true, false, true}
	got := onDisk(t, b, digests)
	require.Equal(t, want, got, "blobs on disk after the sweep")
}

func TestSweepLocalBlobs_UsageEndsAtOrBelowTheLowWatermark(t *testing.T) {
	b, _ := newSweepBackend(t)
	digests := putSweepObjects(t, b, 4)
	budgetToEvict(b, 2, blobSize(t, b, digests[0]))

	sweepLocalBlobs(t, b)

	require.LessOrEqual(t, b.cache.Usage(), b.localBlobMaxBytes*lowWatermarkPercent/100, "usage after the sweep")
}

// TestSweepLocalBlobs_KeepsWritesInProgress: the budget pass removes no
// write in progress, however far over budget it is.
func TestSweepLocalBlobs_KeepsWritesInProgress(t *testing.T) {
	b, _ := newSweepBackend(t, func(d *Deps) { d.LocalBlobMaxBytes = 1 })
	inProgress(t, b, bytes.Repeat([]byte("w"), 1000))

	stats := sweepLocalBlobs(t, b)

	got := fmt.Sprint(stats.BudgetFiles, b.cache.InFlight())
	require.Equal(t, fmt.Sprint(0, 1000), got, "[evicted] [in flight]")
}

func TestSweepLocalBlobs_NoBudgetEvictsNothing(t *testing.T) {
	b, _ := newSweepBackend(t)
	putSweepObjects(t, b, 3)

	stats := sweepLocalBlobs(t, b)

	require.Equal(t, LocalBlobSweepStats{}, stats, "sweep stats")
}

// TestSweepLocalBlobs_OrphanPass: old .tmp-* files are deleted from the
// cache; young ones and cached blob files (never checked against intents)
// stay; the usage count comes to the blob files that remain. A temp file
// counts only while a live write holds it, so the young one this test wrote
// by hand does not.
func TestSweepLocalBlobs_OrphanPass(t *testing.T) {
	b, _ := newSweepBackend(t)
	old := time.Now().Add(-2 * DefaultLocalBlobOrphanAge)
	cacheDir := filepath.Dir(b.cache.Path(multihash.Multihash{0}))
	writeFile := func(name, body string, modTime time.Time) string {
		t.Helper()
		path := filepath.Join(cacheDir, name)
		require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
		require.NoError(t, os.Chtimes(path, modTime, modTime))
		return path
	}
	digests := putSweepObjects(t, b, 1)
	require.NoError(t, os.Chtimes(localPath(b, digests[0]), old, old))
	paths := map[string]string{
		"old temp":        writeFile(".tmp-old", "partial", old),
		"young temp":      writeFile(".tmp-young", "partial", time.Now()),
		"old cached blob": localPath(b, digests[0]),
	}

	stats := sweepLocalBlobs(t, b)

	got := struct {
		OnDisk      map[string]bool
		OrphanFiles int64
		Usage       int64
	}{OnDisk: map[string]bool{}, OrphanFiles: stats.OrphanFiles, Usage: b.cache.Usage()}
	for name, path := range paths {
		_, err := os.Stat(path)
		got.OnDisk[name] = err == nil
	}
	want := struct {
		OnDisk      map[string]bool
		OrphanFiles int64
		Usage       int64
	}{
		OnDisk: map[string]bool{
			"old temp":        false,
			"young temp":      true,
			"old cached blob": true,
		},
		OrphanFiles: 1,
		Usage:       blobSize(t, b, digests[0]),
	}
	require.Equal(t, want, got, "cache after the orphan pass")
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
	require.NoError(t, os.Chmod(dir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	stats := sweepLocalBlobs(t, b)

	got := fmt.Sprint(onDisk(t, b, digests), stats.BudgetFiles, logs.FilterMessageSnippet("could not remove some files").Len())
	want := fmt.Sprint([]bool{true, true, true}, 0, 1)
	require.Equal(t, want, got, "[on disk] [evicted] [failure logs]")
}

// TestEvictToBudget_ReportsWhyItStopped: each way a pass ends is reported.
func TestEvictToBudget_ReportsWhyItStopped(t *testing.T) {
	ctx := t.Context()
	t.Run("deadline", func(t *testing.T) {
		b, _ := newSweepBackend(t)
		digests := putSweepObjects(t, b, 2)
		budgetToEvict(b, 1, blobSize(t, b, digests[0]))
		pass, err := b.evictToBudget(ctx, -time.Second)
		require.NoError(t, err, "evictToBudget")
		require.Equal(t, budgetPass{stop: stopDeadline}, pass, "want nothing evicted, stopped at the deadline")
	})
	t.Run("no cached blobs", func(t *testing.T) {
		b, _ := newSweepBackend(t, func(d *Deps) { d.LocalBlobMaxBytes = 1 })
		inProgress(t, b, []byte("in progress"))
		pass, err := b.evictToBudget(ctx, time.Minute)
		require.NoError(t, err, "evictToBudget")
		require.Equal(t, budgetPass{stop: stopExhausted}, pass)
	})
	t.Run("watermark", func(t *testing.T) {
		b, _ := newSweepBackend(t)
		digests := putSweepObjects(t, b, 2)
		size := blobSize(t, b, digests[0])
		budgetToEvict(b, 1, size)
		pass, err := b.evictToBudget(ctx, time.Minute)
		require.NoError(t, err, "evictToBudget")
		require.Equal(t, budgetPass{files: 1, bytes: size, stop: stopWatermark}, pass, "want one eviction, stopped at the watermark")
	})
}

// TestSweepLocalBlobs_OrphanPassRunsHourly: a sweep within the hour after an
// orphan pass leaves an old orphan alone; the next one due removes it.
func TestSweepLocalBlobs_OrphanPassRunsHourly(t *testing.T) {
	b, _ := newSweepBackend(t)
	sweepLocalBlobs(t, b)
	path := filepath.Join(filepath.Dir(b.cache.Path(multihash.Multihash{0})), ".tmp-old")
	require.NoError(t, os.WriteFile(path, []byte("partial"), 0o644))
	old := time.Now().Add(-2 * DefaultLocalBlobOrphanAge)
	require.NoError(t, os.Chtimes(path, old, old))

	sweepLocalBlobs(t, b)
	_, errSoon := os.Stat(path)
	b.lastOrphanPass = b.lastOrphanPass.Add(-orphanPassInterval)
	sweepLocalBlobs(t, b)
	_, errDue := os.Stat(path)

	got := [2]bool{errSoon == nil, errDue == nil}
	require.Equal(t, [2]bool{true, false}, got, "orphan present after [a sweep within the hour, the next one due]")
}

// TestSweepLocalBlobs_OrphanPassBacksOffAfterAFailure: an orphan pass whose
// scan fails reports the error but still counts as run, so the next sweep
// within the hour does not scan again.
func TestSweepLocalBlobs_OrphanPassBacksOffAfterAFailure(t *testing.T) {
	ctx := t.Context()
	b, _ := newSweepBackend(t)
	require.NoError(t, os.RemoveAll(filepath.Dir(b.cache.Path(multihash.Multihash{0}))))

	_, firstErr := b.SweepLocalBlobs(ctx)
	_, secondErr := b.SweepLocalBlobs(ctx)

	require.Equal(t, [2]bool{true, false}, [2]bool{firstErr != nil, secondErr != nil}, "[first failed, second failed]")
}

// TestSweepLocalBlobs_OrphanPassSkipsFilesItCannotRemove: orphans whose
// removal fails are skipped and logged once, and the pass still counts as
// run, so the next sweep does not repeat its scan.
func TestSweepLocalBlobs_OrphanPassSkipsFilesItCannotRemove(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	b, _ := newSweepBackend(t, func(d *Deps) { d.Logger = zap.New(core) })
	old := time.Now().Add(-2 * DefaultLocalBlobOrphanAge)
	dir := filepath.Dir(b.cache.Path(multihash.Multihash{0}))
	for _, name := range []string{".tmp-stuck", ".tmp-other"} {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte("partial"), 0o644))
		require.NoError(t, os.Chtimes(path, old, old))
	}
	// A read-only cache directory makes every removal fail.
	require.NoError(t, os.Chmod(dir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	stats := sweepLocalBlobs(t, b)

	got := fmt.Sprint(stats.OrphanFiles, logs.FilterMessageSnippet("could not remove some files").Len(), !b.lastOrphanPass.IsZero())
	want := fmt.Sprint(0, 1, true)
	require.Equal(t, want, got, "[removed] [failure logs] [recorded]")
}

// TestWrittenBlobIsInTheCache: once a PUT's blob is accepted and its location
// recorded, its copy is in the cache and nothing is left in progress.
func TestWrittenBlobIsInTheCache(t *testing.T) {
	b, _ := newSweepBackend(t)
	d := putSweepObjects(t, b, 1)[0]
	size := blobSize(t, b, d)

	_, cacheErr := os.Stat(b.cache.Path(d))
	got := fmt.Sprint(cacheErr == nil, b.cache.InFlight(), b.cache.Usage())
	want := fmt.Sprint(true, 0, size)
	require.Equal(t, want, got, "[cache copy] in-flight usage")
}

// TestSweepLocalBlobs_WarnsOncePerOverBudgetEpisode: the warning that nothing is
// left to evict is armed until usage falls back to the low watermark.
func TestSweepLocalBlobs_WarnsOncePerOverBudgetEpisode(t *testing.T) {
	b, _ := newSweepBackend(t, func(d *Deps) { d.LocalBlobMaxBytes = 1 })
	inProgress(t, b, []byte("unevictable"))
	var got []bool
	sweepLocalBlobs(t, b)
	got = append(got, b.overBudgetWarned)
	sweepLocalBlobs(t, b)
	got = append(got, b.overBudgetWarned)
	b.localBlobMaxBytes = 1 << 20
	sweepLocalBlobs(t, b)
	got = append(got, b.overBudgetWarned)
	want := []bool{true, true, false}
	require.Equal(t, want, got, "warned after [over, still over, back under]")
}

// TestSweepLocalBlobs_LatchesResetAtTheLowWatermark: the once-per-episode
// latches stay set while usage is under the budget but over the low
// watermark, and reset at the low watermark.
func TestSweepLocalBlobs_LatchesResetAtTheLowWatermark(t *testing.T) {
	b, _ := newSweepBackend(t, func(d *Deps) { d.LocalBlobMaxBytes = 1 })
	inProgress(t, b, bytes.Repeat([]byte("u"), 1000))
	b.timeLimitLogged = true
	type latches struct{ Warned, TimeLimitLogged bool }
	var got []latches
	for _, percent := range []int64{0, 95, 89} {
		if percent > 0 {
			// Usage is percent% of the budget.
			b.localBlobMaxBytes = b.cache.Usage() * 100 / percent
		}
		sweepLocalBlobs(t, b)
		got = append(got, latches{b.overBudgetWarned, b.timeLimitLogged})
	}
	want := []latches{{true, true}, {true, true}, {false, false}}
	require.Equal(t, want, got, "latches after [over budget, at 95%, at 89%]")
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
	want := fmt.Sprint([]bool{true}, 1, true)
	require.Equal(t, want, got, "[on disk] [logs] [latched]")
}

// TestSweepLocalBlobs_OrphanPassCorrectsCountsAfterAManualRemoval: a cached
// blob deleted by hand stays counted until the next orphan pass, which finds
// the directory quiet and corrects the count.
func TestSweepLocalBlobs_OrphanPassCorrectsCountsAfterAManualRemoval(t *testing.T) {
	b, _ := newSweepBackend(t)
	digests := putSweepObjects(t, b, 2)
	size := blobSize(t, b, digests[0])
	before := b.cache.Usage()
	require.NoError(t, os.Remove(localPath(b, digests[0])))
	stale := b.cache.Usage()

	sweepLocalBlobs(t, b)

	require.Equal(t, [2]int64{before, before - size}, [2]int64{stale, b.cache.Usage()}, "usage [after the manual removal, after the sweep]")
}
