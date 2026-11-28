package blockstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/did"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestCache(t *testing.T) *BlobCache {
	t.Helper()
	c, err := NewBlobCache(filepath.Join(t.TempDir(), "cache"))
	require.NoError(t, err, "NewBlobCache")
	return c
}

// writeTemp writes body to a new temporary file in c and returns it with
// body's digest.
func writeTemp(t *testing.T, c *BlobCache, body string) (*TempBlob, mh.Multihash) {
	t.Helper()
	tb, err := c.CreateTemp()
	require.NoError(t, err, "CreateTemp")
	_, err = tb.Write([]byte(body))
	require.NoError(t, err, "Write")
	sum := sha256.Sum256([]byte(body))
	digest, err := mh.Encode(sum[:], mh.SHA2_256)
	require.NoError(t, err)
	return tb, digest
}

// cacheTestBlob writes body into c under its digest.
func cacheTestBlob(t *testing.T, c *BlobCache, body string) mh.Multihash {
	t.Helper()
	tb, digest := writeTemp(t, c, body)
	_, err := c.Adopt(tb, digest)
	require.NoError(t, err, "Adopt")
	return digest
}

// TestBlobCacheCountsTemporaryFiles: a temporary file's bytes count as they
// are written; adopting moves them to the blob files' count, and discarding
// takes them off.
func TestBlobCacheCountsTemporaryFiles(t *testing.T) {
	c := newTestCache(t)
	adopted, d := writeTemp(t, c, "hello")
	discarded, _ := writeTemp(t, c, "gone")
	midway := [2]int64{c.Usage(), c.InFlight()}
	n, err := c.Adopt(adopted, d)
	require.NoError(t, err, "Adopt")
	discarded.Discard()
	discarded.Discard()
	adopted.Discard()
	entries, err := os.ReadDir(c.dir)
	require.NoError(t, err)
	got := fmt.Sprint(midway, n, c.Usage(), c.InFlight(), len(entries))
	want := fmt.Sprint([2]int64{9, 9}, 5, 5, 0, 1)
	require.Equal(t, want, got, "[usage in-flight] midway, adopted, usage, in flight, files")
	_, err = c.Adopt(adopted, d)
	require.Error(t, err, "Adopt of an adopted file")
}

func TestBlobCacheRemoveReportsFreedBytes(t *testing.T) {
	c := newTestCache(t)
	d := cacheTestBlob(t, c, "hello")
	var freed []int64
	for range 2 {
		n, err := c.Remove(d)
		require.NoError(t, err, "Remove")
		freed = append(freed, n)
	}
	require.Equal(t, int64(5), freed[0], "freed")
	require.Zero(t, freed[1], "freed")
}

// TestNewBlobCacheReopen: reopening a cache counts the blob files, deletes a
// leftover .tmp-* file, and ignores a subdirectory and a non-digest name.
func TestNewBlobCacheReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cache")
	first, err := NewBlobCache(dir)
	require.NoError(t, err, "NewBlobCache")
	cacheTestBlob(t, first, "hello")
	cacheTestBlob(t, first, "world!")
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".tmp-123"), []byte("partial"), 0o644))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "lost+found"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "README"), []byte("not a blob"), 0o644))

	reopened, err := NewBlobCache(dir)
	require.NoError(t, err, "reopen")
	_, statErr := os.Stat(filepath.Join(dir, ".tmp-123"))
	got := struct {
		Usage       int64
		TempRemoved bool
	}{reopened.Usage(), errors.Is(statErr, os.ErrNotExist)}
	want := struct {
		Usage       int64
		TempRemoved bool
	}{11, true}
	require.Equal(t, want, got, "reopened cache")
}

// TestBlobCacheScan: Scan reports blob files and .tmp-* files and leaves the
// count alone; count resets the count to the blob files alone.
func TestBlobCacheScan(t *testing.T) {
	c := newTestCache(t)
	d := cacheTestBlob(t, c, "hello")
	require.NoError(t, os.WriteFile(filepath.Join(c.dir, ".tmp-abc"), []byte("partial"), 0o644))
	c.usage.Store(999)
	seen := map[string]bool{}
	require.NoError(t, c.Scan(t.Context(), func(f BlobFile) { seen[f.Name] = f.Digest != nil }), "Scan")
	afterScan := c.Usage()
	require.NoError(t, c.count(func(BlobFile) {}), "count")
	got := struct {
		AfterScan, AfterCount int64
		Seen                  map[string]bool
	}{afterScan, c.Usage(), seen}
	want := struct {
		AfterScan, AfterCount int64
		Seen                  map[string]bool
	}{999, 5, map[string]bool{filepath.Base(c.Path(d)): true, ".tmp-abc": false}}
	require.Equal(t, want, got, "Scan")
}

func TestBlobCacheRemoveTempRejectsBlobNames(t *testing.T) {
	c := newTestCache(t)
	d := cacheTestBlob(t, c, "hello")
	_, err := c.RemoveTemp(filepath.Base(c.Path(d)))
	require.Error(t, err, "RemoveTemp of a blob file name succeeded, want an error")
}

// TestBlobCacheRemoveTempLeavesTheCountAlone: RemoveTemp reports the size it
// freed but does not touch the in-flight count, which a live write keeps for
// itself; a second removal frees nothing.
func TestBlobCacheRemoveTempLeavesTheCountAlone(t *testing.T) {
	c := newTestCache(t)
	cacheTestBlob(t, c, "hello")
	require.NoError(t, os.WriteFile(filepath.Join(c.dir, ".tmp-abc"), []byte("partial"), 0o644))
	var freed []int64
	for range 2 {
		n, err := c.RemoveTemp(".tmp-abc")
		require.NoError(t, err, "RemoveTemp")
		freed = append(freed, n)
	}
	got := struct {
		Freed []int64
		Usage int64
	}{freed, c.Usage()}
	want := struct {
		Freed []int64
		Usage int64
	}{[]int64{7, 0}, 5}
	require.Equal(t, want, got, "RemoveTemp")
}

// TestScanKeepsCountsExact: the hourly scans and corrections racing writes,
// adoptions and removes leave the count equal to the bytes actually in the
// directory.
func TestScanKeepsCountsExact(t *testing.T) {
	ctx := t.Context()
	c := newTestCache(t)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 50 {
				tb, err := c.CreateTemp()
				if !assert.NoError(t, err, "CreateTemp") {
					return
				}
				body := fmt.Sprintf("writer %d blob %d", w, i)
				if _, err := tb.Write([]byte(body)); !assert.NoError(t, err, "Write") {
					return
				}
				sum := sha256.Sum256([]byte(body))
				d, _ := mh.Encode(sum[:], mh.SHA2_256)
				switch i % 3 {
				case 0:
					tb.Discard()
				case 1:
					_, err = c.Adopt(tb, d)
				case 2:
					if _, err = c.Adopt(tb, d); err == nil {
						_, err = c.Remove(d)
					}
				}
				if !assert.NoError(t, err, "adopt or remove") {
					return
				}
			}
		}()
	}
	var scans sync.WaitGroup
	scans.Add(1)
	go func() {
		defer scans.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = c.ScanAndCorrect(ctx, func(BlobFile) {})
		}
	}()
	wg.Wait()
	close(stop)
	scans.Wait()

	var total int64
	require.NoError(t, c.Scan(ctx, func(f BlobFile) {
		if f.Digest != nil {
			total += f.Size
		}
	}))
	counted, actual := fmt.Sprint(c.Usage(), c.InFlight()), fmt.Sprint(total, 0)
	require.Equal(t, actual, counted, "usage in-flight counted = %s, on disk = %s", counted, actual)
}

// TestBlobCacheOnRead: every hit reports its digest to the read hook, and a
// miss or an adoption reports nothing.
func TestBlobCacheOnRead(t *testing.T) {
	ctx := t.Context()
	cases := []struct {
		name string
		read func(t *testing.T, c *BlobCache, d mh.Multihash)
		want bool
	}{
		{
			name: "OpenBlob hit records the read",
			read: func(t *testing.T, c *BlobCache, d mh.Multihash) {
				r, err := c.OpenBlob(ctx, did.Undef, d)
				require.NoError(t, err, "OpenBlob")
				_ = r.Close()
			},
			want: true,
		},
		{
			name: "OpenBlobRange hit records the read",
			read: func(t *testing.T, c *BlobCache, d mh.Multihash) {
				r, err := c.OpenBlobRange(ctx, did.Undef, d, 0, 1)
				require.NoError(t, err, "OpenBlobRange")
				_ = r.Close()
			},
			want: true,
		},
		{
			name: "miss records nothing",
			read: func(t *testing.T, c *BlobCache, d mh.Multihash) {
				_, err := c.Remove(d)
				require.NoError(t, err, "Remove")
				_, err = c.OpenBlob(ctx, did.Undef, d)
				require.ErrorIs(t, err, ErrNotFound, "OpenBlob after remove")
			},
			want: false,
		},
		{
			name: "adoption alone records nothing",
			read: func(*testing.T, *BlobCache, mh.Multihash) {},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestCache(t)
			var reads []mh.Multihash
			c.OnRead(func(_ context.Context, d mh.Multihash) { reads = append(reads, d) })
			d := cacheTestBlob(t, c, "hello")
			tc.read(t, c, d)
			got := len(reads) == 1 && bytes.Equal(reads[0], d)
			require.Equal(t, tc.want, got, "reported %v", reads)
		})
	}
}

// TestBlobCacheTouch: Touch dates a cached file, which the budget pass orders
// by, and reports a blob that is not cached.
func TestBlobCacheTouch(t *testing.T) {
	c := newTestCache(t)
	tb, d := writeTemp(t, c, "hello")
	ok, err := c.Touch(d, time.Now())
	require.NoError(t, err, "Touch before Adopt")
	require.False(t, ok, "Touch before Adopt")
	_, err = c.Adopt(tb, d)
	require.NoError(t, err, "Adopt")
	at := time.Now().Add(-time.Hour).Truncate(time.Second)
	ok, err = c.Touch(d, at)
	require.NoError(t, err, "Touch")
	require.True(t, ok, "Touch")
	info, err := os.Stat(c.Path(d))
	require.NoError(t, err, "stat")
	require.True(t, info.ModTime().Equal(at), "modification time = %v, want %v", info.ModTime(), at)
}

// TestScanAndCorrect: a file removed outside the process stays counted until a
// scan that nothing overlapped corrects the count; a scan that a write
// overlapped corrects nothing.
func TestScanAndCorrect(t *testing.T) {
	ctx := t.Context()
	c := newTestCache(t)
	gone := cacheTestBlob(t, c, "removed by hand")
	cacheTestBlob(t, c, "kept")
	require.NoError(t, os.Remove(c.Path(gone)))
	stale := c.Usage()

	var wrote bool
	overlapped, err := c.ScanAndCorrect(ctx, func(BlobFile) {
		if !wrote {
			wrote = true
			cacheTestBlob(t, c, "written during the scan")
		}
	})
	require.NoError(t, err)
	afterOverlap := c.Usage()
	quiet, err := c.ScanAndCorrect(ctx, func(BlobFile) {})
	require.NoError(t, err)

	removed := int64(len("removed by hand"))
	written := int64(len("written during the scan"))
	got := [4]int64{overlapped, afterOverlap, quiet, c.Usage()}
	want := [4]int64{0, stale + written, removed, stale + written - removed}
	require.Equal(t, want, got, "[overlapped correction, usage after it, quiet correction, usage after it]")
}

// TestScanAndCorrectSkipsOverlappedRemoves: a removal during the scan stops
// the correction, even though the count would otherwise be off by the
// removed blob.
func TestScanAndCorrectSkipsOverlappedRemoves(t *testing.T) {
	ctx := t.Context()
	c := newTestCache(t)
	gone := cacheTestBlob(t, c, "removed by hand")
	changed := cacheTestBlob(t, c, "changed during the scan")
	require.NoError(t, os.Remove(c.Path(gone)))
	var done bool
	drift, err := c.ScanAndCorrect(ctx, func(BlobFile) {
		if !done {
			done = true
			_, err := c.Remove(changed)
			assert.NoError(t, err, "Remove")
		}
	})
	require.NoError(t, err)
	require.Zero(t, drift, "correction = %d, want none: a change overlapped the scan", drift)
}

// TestBlobCacheAdoptIntoMissingCacheIsAnError: a rename that fails because
// the cache directory is gone counts nothing, and leaves nothing in flight.
func TestBlobCacheAdoptIntoMissingCacheIsAnError(t *testing.T) {
	c := newTestCache(t)
	tb, d := writeTemp(t, c, "hello")
	require.NoError(t, os.RemoveAll(c.dir))
	_, err := c.Adopt(tb, d)
	require.Error(t, err, "Adopt into a missing cache directory: want an error")
	require.Equal(t, [2]int64{0, 0}, [2]int64{c.Usage(), c.InFlight()}, "[usage, in flight]")
}

// TestScanReadsLargeDirectoriesInChunks: a directory with more entries than
// one chunk is walked whole, and the correction matches the disk.
func TestScanReadsLargeDirectoriesInChunks(t *testing.T) {
	ctx := t.Context()
	c := newTestCache(t)
	var want int64
	for i := range walkChunk + 10 {
		body := fmt.Sprintf("blob %d", i)
		cacheTestBlob(t, c, body)
		want += int64(len(body))
	}
	c.usage.Store(0)
	seen := 0
	drift, err := c.ScanAndCorrect(ctx, func(BlobFile) { seen++ })
	require.NoError(t, err)
	require.Equal(t, [3]int64{walkChunk + 10, -want, want}, [3]int64{int64(seen), drift, c.Usage()}, "[files seen, correction, usage]")
}

// TestBlobCacheAdoptReplacesACachedCopy: the same bytes stored as received
// and written again arrive under the digest the cache already holds; Adopt
// replaces the cached copy and the count stays exact.
func TestBlobCacheAdoptReplacesACachedCopy(t *testing.T) {
	c := newTestCache(t)
	d := cacheTestBlob(t, c, "hello")
	again := cacheTestBlob(t, c, "hello")
	require.Equal(t, string(d), string(again), "second write stored under another digest")
	got, want := fmt.Sprint(c.Usage(), c.InFlight()), fmt.Sprint(5, 0)
	require.Equal(t, want, got, "usage in-flight")
}

// A closed temporary file holds no descriptor, still counts toward usage, and
// still adopts or discards.
func TestBlobCacheTempBlobClosesBeforeItEnds(t *testing.T) {
	c := newTestCache(t)
	tb, d := writeTemp(t, c, "hello")
	require.NoError(t, tb.Close(), "Close")
	require.NoError(t, tb.Close(), "second Close")
	_, err := tb.Write([]byte("more"))
	require.Error(t, err, "Write after Close succeeded")
	got := c.InFlight()
	require.Equal(t, int64(5), got, "in flight after Close")
	n, err := c.Adopt(tb, d)
	require.NoError(t, err, "Adopt after Close")
	require.Equal(t, int64(5), n, "Adopt after Close")
	require.Equal(t, fmt.Sprint(5, 0), fmt.Sprint(c.Usage(), c.InFlight()), "usage in-flight")

	dropped, _ := writeTemp(t, c, "bye")
	require.NoError(t, dropped.Close())
	dropped.Discard()
	_, err = os.Stat(dropped.f.Name())
	require.ErrorIs(t, err, os.ErrNotExist, "discarded file")
	require.Equal(t, int64(0), c.InFlight(), "in flight after Discard")
}
