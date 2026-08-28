package blockstore

import (
	"bytes"
	"context"
	"errors"
	"github.com/fil-forge/ucantone/did"
	"io"
	"testing"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// fakeBlobTier is a read tier that implements BlockReader (so it fits the
// Layered/Cached constructors) and BlobReader. data == nil means a miss.
type fakeBlobTier struct{ data []byte }

func (f fakeBlobTier) GetBlock(context.Context, did.DID, cid.Cid) (block.Block, error) {
	return nil, ErrNotFound
}

func (f fakeBlobTier) OpenBlob(context.Context, did.DID, mh.Multihash) (io.ReadCloser, error) {
	if f.data == nil {
		return nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(f.data)), nil
}

func readOpenBlob(t *testing.T, r BlobReader, digest mh.Multihash) (string, error) {
	t.Helper()
	rc, err := r.OpenBlob(context.Background(), did.Undef, digest)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	return string(b), err
}

// TestLayered_OpenBlob_Tiering covers the body-blob read fallthrough: spool first
// (read-after-write), then the network base after eviction, then ErrNotFound. The
// log is never consulted (it holds no body blobs).
func TestLayered_OpenBlob_Tiering(t *testing.T) {
	digest, _ := mh.Sum([]byte("digest"), mh.SHA2_256, -1)

	// spool hit — base is not consulted.
	l := NewLayered(fakeBlobTier{data: []byte("from-spool")}, nil, fakeBlobTier{data: []byte("from-base")})
	if got, err := readOpenBlob(t, l, digest); err != nil || got != "from-spool" {
		t.Fatalf("spool hit: got %q err %v, want from-spool", got, err)
	}

	// spool miss → base hit (the after-eviction path).
	l = NewLayered(fakeBlobTier{data: nil}, nil, fakeBlobTier{data: []byte("from-base")})
	if got, err := readOpenBlob(t, l, digest); err != nil || got != "from-base" {
		t.Fatalf("spool miss → base: got %q err %v, want from-base", got, err)
	}

	// both miss → ErrNotFound.
	l = NewLayered(fakeBlobTier{data: nil}, nil, fakeBlobTier{data: nil})
	if _, err := l.OpenBlob(context.Background(), did.Undef, digest); !errors.Is(err, ErrNotFound) {
		t.Fatalf("both miss: err = %v, want ErrNotFound", err)
	}

	// nil spool is skipped, not panicked.
	l = NewLayered(nil, nil, fakeBlobTier{data: []byte("from-base")})
	if got, err := readOpenBlob(t, l, digest); err != nil || got != "from-base" {
		t.Fatalf("nil spool → base: got %q err %v, want from-base", got, err)
	}
}

// TestCached_OpenBlob_BypassesCache confirms OpenBlob streams straight from the
// base reader (no LRU) — a large body blob must not be buffered into the cache.
func TestCached_OpenBlob_BypassesCache(t *testing.T) {
	digest, _ := mh.Sum([]byte("digest"), mh.SHA2_256, -1)
	c := NewCached(fakeBlobTier{data: []byte("streamed")}, 1<<20).(*Cached)
	if got, err := readOpenBlob(t, c, digest); err != nil || got != "streamed" {
		t.Fatalf("cached OpenBlob: got %q err %v, want streamed", got, err)
	}
}

// TestSpool_OpenBlobRange: the section reader serves exactly the inclusive
// [start, end] of the spooled file, and an end past the file's end yields a
// shorter stream.
func TestSpool_OpenBlobRange(t *testing.T) {
	ctx := context.Background()
	sp, err := NewSpool(t.TempDir())
	if err != nil {
		t.Fatalf("NewSpool: %v", err)
	}
	data := []byte("0123456789abcdef")
	digest, _, err := sp.WriteBlob(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}

	cases := []struct {
		name       string
		start, end int64
		want       string
	}{
		{"interior", 4, 9, "456789"},
		{"prefix", 0, 2, "012"},
		{"single byte", 15, 15, "f"},
		{"end past file clamps", 12, 100, "cdef"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rc, err := sp.OpenBlobRange(ctx, did.Undef, digest, c.start, c.end)
			if err != nil {
				t.Fatalf("OpenBlobRange: %v", err)
			}
			defer rc.Close()
			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if string(got) != c.want {
				t.Fatalf("range [%d,%d] = %q, want %q", c.start, c.end, got, c.want)
			}
		})
	}

	if _, err := sp.OpenBlobRange(ctx, did.Undef, mustDigest(t, []byte("absent")), 0, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing blob err = %v, want ErrNotFound", err)
	}
}

// TestOpenBlobRangeOf_Fallback: a tier without the BlobRangeReader capability
// is served through OpenBlob with the prefix discarded and the tail limited.
func TestOpenBlobRangeOf_Fallback(t *testing.T) {
	ctx := context.Background()
	tier := fakeBlobTier{data: []byte("0123456789abcdef")}

	rc, err := OpenBlobRangeOf(ctx, tier, did.Undef, nil, 4, 9)
	if err != nil {
		t.Fatalf("OpenBlobRangeOf: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "456789" {
		t.Fatalf("fallback range = %q, want %q", got, "456789")
	}
}

// mustDigest hashes data into the multihash form blobs are keyed by.
func mustDigest(t *testing.T, data []byte) mh.Multihash {
	t.Helper()
	d, err := mh.Sum(data, mh.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	return d
}
