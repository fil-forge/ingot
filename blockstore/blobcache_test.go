package blockstore

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/did"
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

// TestScanKeepsCountsExact: scans racing writes, moves and removes leave each
// count equal to the bytes actually in its directory.
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
			_ = s.Scan(func(BlobFile) {})
			_ = c.Scan(func(BlobFile) {})
		}
	}()
	wg.Wait()
	close(stop)
	scans.Wait()

	measure := func(d *blobDir) int64 {
		var total int64
		if err := d.Scan(func(f BlobFile) {
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
			name: "read through LocalBlobs records the read",
			read: func(t *testing.T, c *BlobCache, d mh.Multihash) {
				r, err := LocalBlobs{Cache: c}.OpenBlob(ctx, did.Undef, d)
				if err != nil {
					t.Fatalf("OpenBlob: %v", err)
				}
				_ = r.Close()
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
