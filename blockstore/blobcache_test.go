package blockstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

func newTestCache(t *testing.T) *BlobCache {
	t.Helper()
	c, err := NewBlobCache(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("NewBlobCache: %v", err)
	}
	return c
}

// writeTemp writes body to a new temporary file in c and returns it with
// body's digest.
func writeTemp(t *testing.T, c *BlobCache, body string) (*TempBlob, mh.Multihash) {
	t.Helper()
	tb, err := c.CreateTemp()
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if _, err := tb.Write([]byte(body)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	sum := sha256.Sum256([]byte(body))
	digest, err := mh.Encode(sum[:], mh.SHA2_256)
	if err != nil {
		t.Fatal(err)
	}
	return tb, digest
}

// cacheTestBlob writes body into c under its digest.
func cacheTestBlob(t *testing.T, c *BlobCache, body string) mh.Multihash {
	t.Helper()
	tb, digest := writeTemp(t, c, body)
	if _, err := c.Adopt(tb, digest); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	discarded.Discard()
	discarded.Discard()
	adopted.Discard()
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		t.Fatal(err)
	}
	got := fmt.Sprint(midway, n, c.Usage(), c.InFlight(), len(entries))
	if want := fmt.Sprint([2]int64{9, 9}, 5, 5, 0, 1); got != want {
		t.Fatalf("[usage in-flight] midway, adopted, usage, in flight, files = %s, want %s", got, want)
	}
	if _, err := c.Adopt(adopted, d); err == nil {
		t.Fatal("Adopt of an adopted file: want an error")
	}
}

func TestBlobCacheRemoveReportsFreedBytes(t *testing.T) {
	c := newTestCache(t)
	d := cacheTestBlob(t, c, "hello")
	var freed []int64
	for range 2 {
		n, err := c.Remove(d)
		if err != nil {
			t.Fatalf("Remove: %v", err)
		}
		freed = append(freed, n)
	}
	if freed[0] != 5 || freed[1] != 0 {
		t.Fatalf("freed = %v, want [5 0]", freed)
	}
}

// TestNewBlobCacheReopen: reopening a cache counts the blob files, deletes a
// leftover .tmp-* file, and ignores a subdirectory and a non-digest name.
func TestNewBlobCacheReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cache")
	first, err := NewBlobCache(dir)
	if err != nil {
		t.Fatalf("NewBlobCache: %v", err)
	}
	cacheTestBlob(t, first, "hello")
	cacheTestBlob(t, first, "world!")
	if err := os.WriteFile(filepath.Join(dir, ".tmp-123"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "lost+found"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README"), []byte("not a blob"), 0o644); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewBlobCache(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	_, statErr := os.Stat(filepath.Join(dir, ".tmp-123"))
	got := struct {
		Usage       int64
		TempRemoved bool
	}{reopened.Usage(), errors.Is(statErr, os.ErrNotExist)}
	want := struct {
		Usage       int64
		TempRemoved bool
	}{11, true}
	if got != want {
		t.Fatalf("reopened cache = %+v, want %+v", got, want)
	}
}

// TestBlobCacheScan: Scan reports blob files and .tmp-* files and leaves the
// count alone; count resets the count to the blob files alone.
func TestBlobCacheScan(t *testing.T) {
	c := newTestCache(t)
	d := cacheTestBlob(t, c, "hello")
	if err := os.WriteFile(filepath.Join(c.dir, ".tmp-abc"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	c.usage.Store(999)
	seen := map[string]bool{}
	if err := c.Scan(t.Context(), func(f BlobFile) { seen[f.Name] = f.Digest != nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	afterScan := c.Usage()
	if err := c.count(func(BlobFile) {}); err != nil {
		t.Fatalf("count: %v", err)
	}
	got := struct {
		AfterScan, AfterCount int64
		Seen                  map[string]bool
	}{afterScan, c.Usage(), seen}
	want := struct {
		AfterScan, AfterCount int64
		Seen                  map[string]bool
	}{999, 5, map[string]bool{filepath.Base(c.Path(d)): true, ".tmp-abc": false}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Scan = %+v, want %+v", got, want)
	}
}

func TestBlobCacheRemoveTempRejectsBlobNames(t *testing.T) {
	c := newTestCache(t)
	d := cacheTestBlob(t, c, "hello")
	if _, err := c.RemoveTemp(filepath.Base(c.Path(d))); err == nil {
		t.Fatal("RemoveTemp of a blob file name succeeded, want an error")
	}
}

// TestBlobCacheRemoveTempLeavesTheCountAlone: RemoveTemp reports the size it
// freed but does not touch the in-flight count, which a live write keeps for
// itself; a second removal frees nothing.
func TestBlobCacheRemoveTempLeavesTheCountAlone(t *testing.T) {
	c := newTestCache(t)
	cacheTestBlob(t, c, "hello")
	if err := os.WriteFile(filepath.Join(c.dir, ".tmp-abc"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	var freed []int64
	for range 2 {
		n, err := c.RemoveTemp(".tmp-abc")
		if err != nil {
			t.Fatalf("RemoveTemp: %v", err)
		}
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
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RemoveTemp = %+v, want %+v", got, want)
	}
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
				if err != nil {
					t.Errorf("CreateTemp: %v", err)
					return
				}
				body := fmt.Sprintf("writer %d blob %d", w, i)
				if _, err := tb.Write([]byte(body)); err != nil {
					t.Errorf("Write: %v", err)
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
				if err != nil {
					t.Errorf("adopt or remove: %v", err)
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
	if err := c.Scan(ctx, func(f BlobFile) {
		if f.Digest != nil {
			total += f.Size
		}
	}); err != nil {
		t.Fatal(err)
	}
	if counted, actual := fmt.Sprint(c.Usage(), c.InFlight()), fmt.Sprint(total, 0); counted != actual {
		t.Fatalf("usage in-flight counted = %s, on disk = %s", counted, actual)
	}
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
				if err != nil {
					t.Fatalf("OpenBlob: %v", err)
				}
				_ = r.Close()
			},
			want: true,
		},
		{
			name: "OpenBlobRange hit records the read",
			read: func(t *testing.T, c *BlobCache, d mh.Multihash) {
				r, err := c.OpenBlobRange(ctx, did.Undef, d, 0, 1)
				if err != nil {
					t.Fatalf("OpenBlobRange: %v", err)
				}
				_ = r.Close()
			},
			want: true,
		},
		{
			name: "GetBlock hit records the read",
			read: func(t *testing.T, c *BlobCache, d mh.Multihash) {
				if _, err := c.GetBlock(ctx, did.Undef, cid.NewCidV1(cid.Raw, d)); err != nil {
					t.Fatalf("GetBlock: %v", err)
				}
			},
			want: true,
		},
		{
			name: "miss records nothing",
			read: func(t *testing.T, c *BlobCache, d mh.Multihash) {
				if _, err := c.Remove(d); err != nil {
					t.Fatalf("Remove: %v", err)
				}
				if _, err := c.OpenBlob(ctx, did.Undef, d); !errors.Is(err, ErrNotFound) {
					t.Fatalf("OpenBlob after remove: %v, want ErrNotFound", err)
				}
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
			if got := len(reads) == 1 && bytes.Equal(reads[0], d); got != tc.want {
				t.Fatalf("reported %v, want the digest reported: %v", reads, tc.want)
			}
		})
	}
}

// TestBlobCacheTouch: Touch dates a cached file, which the budget pass orders
// by, and reports a blob that is not cached.
func TestBlobCacheTouch(t *testing.T) {
	c := newTestCache(t)
	tb, d := writeTemp(t, c, "hello")
	if ok, err := c.Touch(d, time.Now()); err != nil || ok {
		t.Fatalf("Touch before Adopt = %v, %v; want false", ok, err)
	}
	if _, err := c.Adopt(tb, d); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	at := time.Now().Add(-time.Hour).Truncate(time.Second)
	if ok, err := c.Touch(d, at); err != nil || !ok {
		t.Fatalf("Touch = %v, %v; want true", ok, err)
	}
	info, err := os.Stat(c.Path(d))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if !info.ModTime().Equal(at) {
		t.Fatalf("modification time = %v, want %v", info.ModTime(), at)
	}
}

// TestScanAndCorrect: a file removed outside the process stays counted until a
// scan that nothing overlapped corrects the count; a scan that a write
// overlapped corrects nothing.
func TestScanAndCorrect(t *testing.T) {
	ctx := t.Context()
	c := newTestCache(t)
	gone := cacheTestBlob(t, c, "removed by hand")
	cacheTestBlob(t, c, "kept")
	if err := os.Remove(c.Path(gone)); err != nil {
		t.Fatal(err)
	}
	stale := c.Usage()

	var wrote bool
	overlapped, err := c.ScanAndCorrect(ctx, func(BlobFile) {
		if !wrote {
			wrote = true
			cacheTestBlob(t, c, "written during the scan")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	afterOverlap := c.Usage()
	quiet, err := c.ScanAndCorrect(ctx, func(BlobFile) {})
	if err != nil {
		t.Fatal(err)
	}

	removed := int64(len("removed by hand"))
	written := int64(len("written during the scan"))
	got := [4]int64{overlapped, afterOverlap, quiet, c.Usage()}
	want := [4]int64{0, stale + written, removed, stale + written - removed}
	if got != want {
		t.Fatalf("[overlapped correction, usage after it, quiet correction, usage after it] = %v, want %v", got, want)
	}
}

// TestScanAndCorrectSkipsOverlappedRemoves: a removal during the scan stops
// the correction, even though the count would otherwise be off by the
// removed blob.
func TestScanAndCorrectSkipsOverlappedRemoves(t *testing.T) {
	ctx := t.Context()
	c := newTestCache(t)
	gone := cacheTestBlob(t, c, "removed by hand")
	changed := cacheTestBlob(t, c, "changed during the scan")
	if err := os.Remove(c.Path(gone)); err != nil {
		t.Fatal(err)
	}
	var done bool
	drift, err := c.ScanAndCorrect(ctx, func(BlobFile) {
		if !done {
			done = true
			if _, err := c.Remove(changed); err != nil {
				t.Errorf("Remove: %v", err)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if drift != 0 {
		t.Fatalf("correction = %d, want none: a change overlapped the scan", drift)
	}
}

// TestBlobCacheAdoptIntoMissingCacheIsAnError: a rename that fails because
// the cache directory is gone counts nothing, and leaves nothing in flight.
func TestBlobCacheAdoptIntoMissingCacheIsAnError(t *testing.T) {
	c := newTestCache(t)
	tb, d := writeTemp(t, c, "hello")
	if err := os.RemoveAll(c.dir); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Adopt(tb, d); err == nil {
		t.Fatal("Adopt into a missing cache directory: want an error")
	}
	if got := [2]int64{c.Usage(), c.InFlight()}; got != [2]int64{0, 0} {
		t.Fatalf("[usage, in flight] = %v, want [0 0]", got)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	if got := [3]int64{int64(seen), drift, c.Usage()}; got != [3]int64{walkChunk + 10, -want, want} {
		t.Fatalf("[files seen, correction, usage] = %v, want [%d %d %d]", got, walkChunk+10, -want, want)
	}
}

// TestBlobCacheAdoptReplacesACachedCopy: the same bytes stored as received
// and written again arrive under the digest the cache already holds; Adopt
// replaces the cached copy and the count stays exact.
func TestBlobCacheAdoptReplacesACachedCopy(t *testing.T) {
	c := newTestCache(t)
	d := cacheTestBlob(t, c, "hello")
	again := cacheTestBlob(t, c, "hello")
	if string(again) != string(d) {
		t.Fatalf("second write stored under another digest")
	}
	if got, want := fmt.Sprint(c.Usage(), c.InFlight()), fmt.Sprint(5, 0); got != want {
		t.Fatalf("usage in-flight = %s, want %s", got, want)
	}
}

// A closed temporary file holds no descriptor, still counts toward usage, and
// still adopts or discards.
func TestBlobCacheTempBlobClosesBeforeItEnds(t *testing.T) {
	c := newTestCache(t)
	tb, d := writeTemp(t, c, "hello")
	if err := tb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := tb.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := tb.Write([]byte("more")); err == nil {
		t.Fatal("Write after Close succeeded")
	}
	if got := c.InFlight(); got != 5 {
		t.Fatalf("in flight after Close = %d, want 5", got)
	}
	if n, err := c.Adopt(tb, d); err != nil || n != 5 {
		t.Fatalf("Adopt after Close = %d, %v; want 5", n, err)
	}
	if got, want := fmt.Sprint(c.Usage(), c.InFlight()), fmt.Sprint(5, 0); got != want {
		t.Fatalf("usage in-flight = %s, want %s", got, want)
	}

	dropped, _ := writeTemp(t, c, "bye")
	if err := dropped.Close(); err != nil {
		t.Fatal(err)
	}
	dropped.Discard()
	if _, err := os.Stat(dropped.f.Name()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("discarded file: %v, want gone", err)
	}
	if got := c.InFlight(); got != 0 {
		t.Fatalf("in flight after Discard = %d, want 0", got)
	}
}
