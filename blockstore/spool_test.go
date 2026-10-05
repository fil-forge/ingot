package blockstore

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/fil-forge/ucantone/did"
	mh "github.com/multiformats/go-multihash"
)

func newTestSpool(t *testing.T) *Spool {
	t.Helper()
	s, err := NewSpool(filepath.Join(t.TempDir(), "spool"))
	if err != nil {
		t.Fatalf("NewSpool: %v", err)
	}
	return s
}

func writeTestBlob(t *testing.T, s *Spool, body string) mh.Multihash {
	t.Helper()
	digest, _, err := s.WriteBlob(t.Context(), bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}
	return digest
}

func TestSpoolUsage(t *testing.T) {
	cases := []struct {
		name string
		run  func(t *testing.T, s *Spool)
		want int64
	}{
		{
			name: "new write adds its size",
			run:  func(t *testing.T, s *Spool) { writeTestBlob(t, s, "hello") },
			want: 5,
		},
		{
			name: "identical rewrite adds nothing",
			run: func(t *testing.T, s *Spool) {
				writeTestBlob(t, s, "hello")
				writeTestBlob(t, s, "hello")
			},
			want: 5,
		},
		{
			name: "remove of a present file subtracts its size",
			run: func(t *testing.T, s *Spool) {
				d := writeTestBlob(t, s, "hello")
				writeTestBlob(t, s, "world!")
				if _, err := s.Remove(d); err != nil {
					t.Fatalf("Remove: %v", err)
				}
			},
			want: 6,
		},
		{
			name: "remove of an absent file subtracts nothing",
			run: func(t *testing.T, s *Spool) {
				d := writeTestBlob(t, s, "hello")
				for range 2 {
					if _, err := s.Remove(d); err != nil {
						t.Fatalf("Remove: %v", err)
					}
				}
			},
			want: 0,
		},
		{
			name: "empty write adds nothing",
			run:  func(t *testing.T, s *Spool) { writeTestBlob(t, s, "") },
			want: 0,
		},
		{
			name: "failed write takes its bytes off again",
			run: func(t *testing.T, s *Spool) {
				writeTestBlob(t, s, "hello")
				r := io.MultiReader(strings.NewReader("partial"), iotest.ErrReader(errors.New("client went away")))
				if _, _, err := s.WriteBlob(t.Context(), r); err == nil {
					t.Fatal("WriteBlob succeeded, want the read error")
				}
			},
			want: 5,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestSpool(t)
			tc.run(t, s)
			if got := s.Usage(); got != tc.want {
				t.Fatalf("Usage() = %d, want %d", got, tc.want)
			}
		})
	}
}

// pausingReader yields its first chunk, then reports the spool's usage from its
// second Read, while the write is still in flight.
type pausingReader struct {
	first  []byte
	s      *Spool
	midway int64
	reads  int
}

func (p *pausingReader) Read(b []byte) (int, error) {
	p.reads++
	if p.reads == 1 {
		return copy(b, p.first), nil
	}
	p.midway = p.s.Usage()
	return 0, io.EOF
}

// TestSpoolCountsInFlightBytes: usage includes the bytes a write has written
// before the write finishes, and counts them once when it does.
func TestSpoolCountsInFlightBytes(t *testing.T) {
	s := newTestSpool(t)
	writeTestBlob(t, s, "hello")
	r := &pausingReader{first: []byte("in flight"), s: s}
	if _, _, err := s.WriteBlob(t.Context(), r); err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}
	got := struct{ Midway, After int64 }{r.midway, s.Usage()}
	want := struct{ Midway, After int64 }{14, 14}
	if got != want {
		t.Fatalf("usage = %+v, want %+v", got, want)
	}
}

func TestSpoolRemoveReportsFreedBytes(t *testing.T) {
	s := newTestSpool(t)
	d := writeTestBlob(t, s, "hello")
	var freed []int64
	for range 2 {
		n, err := s.Remove(d)
		if err != nil {
			t.Fatalf("Remove: %v", err)
		}
		freed = append(freed, n)
	}
	if freed[0] != 5 || freed[1] != 0 {
		t.Fatalf("freed = %v, want [5 0]", freed)
	}
}

// TestNewSpoolReopen: reopening a spool counts the blob files, deletes a
// leftover .tmp-* file, and ignores a subdirectory and a non-digest name.
func TestNewSpoolReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spool")
	first, err := NewSpool(dir)
	if err != nil {
		t.Fatalf("NewSpool: %v", err)
	}
	writeTestBlob(t, first, "hello")
	writeTestBlob(t, first, "world!")
	if err := os.WriteFile(filepath.Join(dir, ".tmp-123"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "lost+found"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README"), []byte("not a blob"), 0o644); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewSpool(dir)
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
		t.Fatalf("reopened spool = %+v, want %+v", got, want)
	}
}

func TestSpoolScan(t *testing.T) {
	s := newTestSpool(t)
	d := writeTestBlob(t, s, "hello")
	if err := os.WriteFile(filepath.Join(s.dir, ".tmp-abc"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	total, err := s.Scan(func(e SpoolEntry) {
		seen[e.Name] = e.Digest != nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	got := struct {
		Total int64
		Seen  map[string]bool
	}{total, seen}
	want := struct {
		Total int64
		Seen  map[string]bool
	}{12, map[string]bool{filepath.Base(s.Path(d)): true, ".tmp-abc": false}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Scan = %+v, want %+v", got, want)
	}
}

func TestSpoolRemoveTempRejectsBlobNames(t *testing.T) {
	s := newTestSpool(t)
	d := writeTestBlob(t, s, "hello")
	if _, err := s.RemoveTemp(filepath.Base(s.Path(d))); err == nil {
		t.Fatal("RemoveTemp of a blob file name succeeded, want an error")
	}
}

func TestSpoolLastRead(t *testing.T) {
	ctx := t.Context()
	cases := []struct {
		name string
		read func(t *testing.T, s *Spool, d mh.Multihash)
		want bool
	}{
		{
			name: "OpenBlob hit records the read",
			read: func(t *testing.T, s *Spool, d mh.Multihash) {
				r, err := s.OpenBlob(ctx, did.Undef, d)
				if err != nil {
					t.Fatalf("OpenBlob: %v", err)
				}
				_ = r.Close()
			},
			want: true,
		},
		{
			name: "OpenBlobRange hit records the read",
			read: func(t *testing.T, s *Spool, d mh.Multihash) {
				r, err := s.OpenBlobRange(ctx, did.Undef, d, 0, 1)
				if err != nil {
					t.Fatalf("OpenBlobRange: %v", err)
				}
				_ = r.Close()
			},
			want: true,
		},
		{
			name: "miss records nothing",
			read: func(t *testing.T, s *Spool, d mh.Multihash) {
				if _, err := s.Remove(d); err != nil {
					t.Fatalf("Remove: %v", err)
				}
				if _, err := s.OpenBlob(ctx, did.Undef, d); !errors.Is(err, ErrNotFound) {
					t.Fatalf("OpenBlob after remove: %v, want ErrNotFound", err)
				}
			},
			want: false,
		},
		{
			name: "write alone records nothing",
			read: func(*testing.T, *Spool, mh.Multihash) {},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestSpool(t)
			d := writeTestBlob(t, s, "hello")
			tc.read(t, s, d)
			if _, ok := s.LastRead(d); ok != tc.want {
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

// TestSpoolRemoveTempUncountsItsBytes: a temp file the usage count included
// (here through a reset from a scan, as the orphan pass does) comes off the
// count when RemoveTemp deletes it, and a second removal frees nothing.
func TestSpoolRemoveTempUncountsItsBytes(t *testing.T) {
	s := newTestSpool(t)
	writeTestBlob(t, s, "hello")
	if err := os.WriteFile(filepath.Join(s.dir, ".tmp-abc"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	total, err := s.Scan(func(SpoolEntry) {})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	s.ResetUsage(total)
	var freed []int64
	for range 2 {
		n, err := s.RemoveTemp(".tmp-abc")
		if err != nil {
			t.Fatalf("RemoveTemp: %v", err)
		}
		freed = append(freed, n)
	}
	got := struct {
		Freed []int64
		Usage int64
	}{freed, s.Usage()}
	want := struct {
		Freed []int64
		Usage int64
	}{[]int64{7, 0}, 5}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RemoveTemp = %+v, want %+v", got, want)
	}
}
