package blockstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// fakeBlobTier is a read tier that implements BlockReader (so it fits the
// Layered/Cached constructors) and BlobReader. data == nil means a miss.
type fakeBlobTier struct{ data []byte }

func (f fakeBlobTier) GetBlock(context.Context, cid.Cid) (block.Block, error) {
	return nil, ErrNotFound
}

func (f fakeBlobTier) OpenBlob(context.Context, mh.Multihash) (io.ReadCloser, error) {
	if f.data == nil {
		return nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(f.data)), nil
}

func readOpenBlob(t *testing.T, r BlobReader, digest mh.Multihash) (string, error) {
	t.Helper()
	rc, err := r.OpenBlob(context.Background(), digest)
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
	if _, err := l.OpenBlob(context.Background(), digest); !errors.Is(err, ErrNotFound) {
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
