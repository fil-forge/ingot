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
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
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

func TestBlobCacheLastRead(t *testing.T) {
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
			name: "read through LocalBlobs records the read",
			read: func(t *testing.T, c *BlobCache, d mh.Multihash) {
				r, err := LocalBlobs{Cache: c, Spool: newTestSpool(t)}.OpenBlob(ctx, did.Undef, d)
				if err != nil {
					t.Fatalf("OpenBlob: %v", err)
				}
				_ = r.Close()
			},
			want: true,
		},
		{
			name: "remove forgets the read",
			read: func(t *testing.T, c *BlobCache, d mh.Multihash) {
				r, err := c.OpenBlob(ctx, did.Undef, d)
				if err != nil {
					t.Fatalf("OpenBlob: %v", err)
				}
				_ = r.Close()
				if _, err := c.Remove(d); err != nil {
					t.Fatalf("Remove: %v", err)
				}
			},
			want: false,
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
			name: "take alone records nothing",
			read: func(*testing.T, *BlobCache, mh.Multihash) {},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, c := newTestDirs(t)
			d := writeTestBlob(t, s, "hello")
			if _, err := c.Take(s, d); err != nil {
				t.Fatalf("Take: %v", err)
			}
			tc.read(t, c, d)
			if _, ok := c.LastRead(d); ok != tc.want {
				t.Fatalf("LastRead ok = %v, want %v", ok, tc.want)
			}
		})
	}
}

func TestRecencyMapDropsLeastRecentlyRead(t *testing.T) {
	m := newRecencyMap(2)
	now := time.Now()
	m.touch("a", now)
	m.touch("b", now)
	m.touch("a", now) // a is now the most recent
	m.touch("c", now) // evicts b
	_, hasA := m.get("a")
	_, hasB := m.get("b")
	_, hasC := m.get("c")
	if got := [3]bool{hasA, hasB, hasC}; got != [3]bool{true, false, true} {
		t.Fatalf("remembered [a b c] = %v, want [true false true]", got)
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

// TestScanAndCorrectSkipsOverlappedRemovesAndMoves: a removal, or a move into
// the cache, during the scan stops both affected directories' corrections,
// even though each count would otherwise be off by the moved blob.
func TestScanAndCorrectSkipsOverlappedRemovesAndMoves(t *testing.T) {
	ctx := t.Context()
	for _, tc := range []struct {
		name   string
		change func(*Spool, *BlobCache, mh.Multihash) error
	}{
		{"remove", func(s *Spool, _ *BlobCache, d mh.Multihash) error { _, err := s.Remove(d); return err }},
		{"take", func(s *Spool, c *BlobCache, d mh.Multihash) error { _, err := c.Take(s, d); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, c := newTestDirs(t)
			gone := writeTestBlob(t, s, "removed by hand")
			changed := writeTestBlob(t, s, "changed during the scan")
			if err := os.Remove(s.Path(gone)); err != nil {
				t.Fatal(err)
			}
			var done bool
			drift, err := s.ScanAndCorrect(ctx, func(BlobFile) {
				if !done {
					done = true
					if err := tc.change(s, c, changed); err != nil {
						t.Errorf("change: %v", err)
					}
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			if drift != 0 {
				t.Fatalf("correction = %d, want none: a change overlapped the scan", drift)
			}
		})
	}
}

// TestBlobCacheTakeIntoMissingCacheIsAnError: a rename that fails because the
// cache directory is gone leaves the blob, and its count, in the spool.
func TestBlobCacheTakeIntoMissingCacheIsAnError(t *testing.T) {
	s, c := newTestDirs(t)
	d := writeTestBlob(t, s, "hello")
	before := s.Usage()
	if err := os.Remove(c.dir); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Take(s, d); err == nil {
		t.Fatal("Take into a missing cache directory: want an error")
	}
	if got := [2]any{fileExistsAt(s.Path(d)), s.Usage()}; got != [2]any{true, before} {
		t.Fatalf("[blob in the spool, spool usage] = %v, want [true %d]", got, before)
	}
}

func fileExistsAt(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// TestScanReadsLargeDirectoriesInChunks: a directory with more entries than
// one chunk is walked whole, and the correction matches the disk.
func TestScanReadsLargeDirectoriesInChunks(t *testing.T) {
	ctx := t.Context()
	s, _ := newTestDirs(t)
	var want int64
	for i := range walkChunk + 10 {
		body := fmt.Sprintf("blob %d", i)
		writeTestBlob(t, s, body)
		want += int64(len(body))
	}
	s.usage.Store(0)
	seen := 0
	drift, err := s.ScanAndCorrect(ctx, func(BlobFile) { seen++ })
	if err != nil {
		t.Fatal(err)
	}
	if got := [3]int64{int64(seen), drift, s.Usage()}; got != [3]int64{walkChunk + 10, -want, want} {
		t.Fatalf("[files seen, correction, usage] = %v, want [%d %d %d]", got, walkChunk+10, -want, want)
	}
}
