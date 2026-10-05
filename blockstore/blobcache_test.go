package blockstore

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/fil-forge/ucantone/did"
)

// newTestDirs returns a spool and a cache side by side, as the server lays
// them out under data_dir.
func newTestDirs(t *testing.T) (*Spool, *BlobCache) {
	t.Helper()
	root := t.TempDir()
	s, err := NewSpool(filepath.Join(root, "spool"))
	if err != nil {
		t.Fatalf("NewSpool: %v", err)
	}
	c, err := NewBlobCache(filepath.Join(root, "cache"))
	if err != nil {
		t.Fatalf("NewBlobCache: %v", err)
	}
	return s, c
}

// TestBlobCacheTake: Take moves a blob from the spool and its bytes between
// the counts; taking it again, or a blob never spooled, moves nothing.
func TestBlobCacheTake(t *testing.T) {
	s, c := newTestDirs(t)
	d := writeTestBlob(t, s, "hello")
	missing := writeTestBlob(t, s, "gone")
	if _, err := s.Remove(missing); err != nil {
		t.Fatal(err)
	}
	var moved []int64
	for _, digest := range [][]byte{d, d, missing} {
		n, err := c.Take(s, digest)
		if err != nil {
			t.Fatalf("Take: %v", err)
		}
		moved = append(moved, n)
	}
	got := fmt.Sprint(moved, s.Usage(), c.Usage())
	if want := fmt.Sprint([]int64{5, 0, 0}, 0, 5); got != want {
		t.Fatalf("[moved] spool cache = %s, want %s", got, want)
	}
}

// TestLocalBlobsReadsEitherDirectory: a blob is found in the spool before it
// moves and in the cache after; one in neither is ErrNotFound.
func TestLocalBlobsReadsEitherDirectory(t *testing.T) {
	ctx := t.Context()
	s, c := newTestDirs(t)
	local := LocalBlobs{Cache: c, Spool: s}
	d := writeTestBlob(t, s, "hello")
	read := func() string {
		t.Helper()
		rc, err := local.OpenBlob(ctx, did.Undef, d)
		if errors.Is(err, ErrNotFound) {
			return "not found"
		}
		if err != nil {
			t.Fatalf("OpenBlob: %v", err)
		}
		defer rc.Close()
		b, err := io.ReadAll(rc)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	var got []string
	got = append(got, read())
	if _, err := c.Take(s, d); err != nil {
		t.Fatal(err)
	}
	got = append(got, read())
	if _, err := c.Remove(d); err != nil {
		t.Fatal(err)
	}
	got = append(got, read())
	if want := []string{"hello", "hello", "not found"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("reads = %q, want %q", got, want)
	}
}

// TestScanKeepsCountsExact: the hourly scans and corrections racing writes,
// moves and removes leave each count equal to the bytes actually in its
// directory.
func TestScanKeepsCountsExact(t *testing.T) {
	ctx := t.Context()
	s, c := newTestDirs(t)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 50 {
				d, _, err := s.WriteBlob(ctx, strings.NewReader(fmt.Sprintf("writer %d blob %d", w, i)))
				if err != nil {
					t.Errorf("WriteBlob: %v", err)
					return
				}
				switch i % 3 {
				case 0:
					_, err = c.Take(s, d)
				case 1:
					_, err = s.Remove(d)
				}
				if err != nil {
					t.Errorf("move or remove: %v", err)
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
			_, _ = s.ScanAndCorrect(ctx, func(BlobFile) {})
			_, _ = c.ScanAndCorrect(ctx, func(BlobFile) {})
		}
	}()
	wg.Wait()
	close(stop)
	scans.Wait()

	measure := func(d *blobDir) int64 {
		var total int64
		if err := d.Scan(ctx, func(f BlobFile) {
			if f.Digest != nil {
				total += f.Size
			}
		}); err != nil {
			t.Fatal(err)
		}
		return total
	}
	counted := fmt.Sprint(s.Usage(), c.Usage(), s.InFlight())
	if actual := fmt.Sprint(measure(s.blobDir), measure(c.blobDir), 0); counted != actual {
		t.Fatalf("spool cache in-flight counted = %s, on disk = %s", counted, actual)
	}
}

// TestBlobCacheCheckTake: the probe moves between directories on one
// filesystem and leaves nothing behind; a cache it cannot move into is an
// error.
func TestBlobCacheCheckTake(t *testing.T) {
	s, c := newTestDirs(t)
	if err := c.CheckTake(s); err != nil {
		t.Fatalf("CheckTake: %v", err)
	}
	for _, dir := range []string{s.dir, c.dir} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("%s holds %d entries after the probe, want none", dir, len(entries))
		}
	}
	if err := os.Remove(c.dir); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckTake(s); err == nil {
		t.Fatal("CheckTake into a missing cache directory: want an error")
	}
}

// TestScanAndCorrect: a file removed outside the process stays counted until a
// scan that nothing overlapped corrects the count; a scan that a write
// overlapped corrects nothing.
func TestScanAndCorrect(t *testing.T) {
	ctx := t.Context()
	s, _ := newTestDirs(t)
	gone := writeTestBlob(t, s, "removed by hand")
	writeTestBlob(t, s, "kept")
	if err := os.Remove(s.Path(gone)); err != nil {
		t.Fatal(err)
	}
	stale := s.Usage()

	var wrote bool
	overlapped, err := s.ScanAndCorrect(ctx, func(BlobFile) {
		if !wrote {
			wrote = true
			writeTestBlob(t, s, "written during the scan")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	afterOverlap := s.Usage()
	quiet, err := s.ScanAndCorrect(ctx, func(BlobFile) {})
	if err != nil {
		t.Fatal(err)
	}

	removed := int64(len("removed by hand"))
	written := int64(len("written during the scan"))
	got := [4]int64{overlapped, afterOverlap, quiet, s.Usage()}
	want := [4]int64{0, stale + written, removed, stale + written - removed}
	if got != want {
		t.Fatalf("[overlapped correction, usage after it, quiet correction, usage after it] = %v, want %v", got, want)
	}
}
