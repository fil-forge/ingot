package blockstore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

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
	}{5, map[string]bool{filepath.Base(s.Path(d)): true, ".tmp-abc": false}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Scan = %+v, want %+v", got, want)
	}
}

func TestSpoolRemoveTempRejectsBlobNames(t *testing.T) {
	s := newTestSpool(t)
	d := writeTestBlob(t, s, "hello")
	if err := s.RemoveTemp(filepath.Base(s.Path(d))); err == nil {
		t.Fatal("RemoveTemp of a blob file name succeeded, want an error")
	}
}
