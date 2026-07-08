package s3frontend

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/logstore"
)

// recordingRemover captures the digests RemoveBlob is called with so the
// reference-index tests can assert exactly when a blob's last claim is released.
type recordingRemover struct {
	mu      sync.Mutex
	removed [][]byte
}

func (r *recordingRemover) RemoveBlob(_ context.Context, _ did.DID, d multihash.Multihash) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removed = append(r.removed, append([]byte(nil), d...))
	return nil
}

func (r *recordingRemover) removedDigests() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := map[string]int{}
	for _, d := range r.removed {
		m[string(d)]++
	}
	return m
}

// newRefTestBackend builds a Backend over real in-process collaborators (a
// MemStore, an on-disk spool, a catalog log) plus a recording remover, so a
// test can drive PutObject/DeleteObject and inspect blob_refs + releases.
func newRefTestBackend(t *testing.T, maxBlob ...int64) (*Backend, *inmem.MemStore, *recordingRemover) {
	t.Helper()
	var mbs int64
	if len(maxBlob) > 0 {
		mbs = maxBlob[0]
	}
	ctx := context.Background()
	dir := t.TempDir()
	mem := inmem.NewMemStore()
	spool, err := blockstore.NewSpool(filepath.Join(dir, "spool"))
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	log, err := logstore.Open(ctx, logstore.Config{
		Dir:     filepath.Join(dir, "segments"),
		Meta:    mem,
		Catalog: logstore.PlaneConfig{Ship: false}, // retained locally; no uploader needed
		Logger:  zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatalf("logstore: %v", err)
	}
	t.Cleanup(func() { _ = log.Close(ctx) })

	rm := &recordingRemover{}
	b := New(Deps{
		Registry:    mem,
		Intents:     mem,
		Locations:   mem,
		BlobRefs:    mem,
		GC:          mem,
		Reads:       blockstore.NewLayered(spool, log, inmem.NopBaseReader{}),
		Log:         log,
		Spool:       spool,
		Uploader:    inmem.NopUploader{},
		Remover:     rm,
		MaxBlobSize: mbs,
	})
	if err := mem.Create(ctx, "bk"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return b, mem, rm
}

func digestOf(t *testing.T, data []byte) []byte {
	t.Helper()
	mh, err := multihash.Sum(data, multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	return []byte(mh)
}

func putObj(t *testing.T, b *Backend, key string, data []byte) {
	t.Helper()
	bucket := "bk"
	if _, err := b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	}); err != nil {
		t.Fatalf("PutObject %s: %v", key, err)
	}
}

func deleteObj(t *testing.T, b *Backend, key string) {
	t.Helper()
	bucket := "bk"
	if _, err := b.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: &bucket, Key: &key}); err != nil {
		t.Fatalf("DeleteObject %s: %v", key, err)
	}
}

func claims(t *testing.T, mem *inmem.MemStore, digest []byte) int {
	t.Helper()
	// MemStore buckets carry no space (did.Undef), so claims recorded via
	// the backend are keyed under did.Undef; count under the same key.
	// Space-keyed claim counting is covered by the live Postgres test.
	n, err := mem.CountClaims(context.Background(), did.Undef, digest)
	if err != nil {
		t.Fatalf("CountClaims: %v", err)
	}
	return n
}

func TestRefIndex_DedupAcrossKeys(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	data := []byte("identical bytes for two keys")
	d := digestOf(t, data)

	putObj(t, b, "k1", data)
	putObj(t, b, "k2", data) // same content, different key — dedup

	if got := claims(t, mem, d); got != 2 {
		t.Fatalf("claims = %d, want 2 (both keys reference the one blob)", got)
	}
	if len(rm.removed) != 0 {
		t.Fatalf("removed %d blobs, want 0 (blob still referenced)", len(rm.removed))
	}
}

func TestRefIndex_OverwriteSameContentKeepsClaim(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	data := []byte("same bytes re-put")
	d := digestOf(t, data)

	putObj(t, b, "k1", data)
	putObj(t, b, "k1", data) // overwrite with identical bytes

	if got := claims(t, mem, d); got != 1 {
		t.Fatalf("claims = %d, want 1 (one row for one (digest,key))", got)
	}
	if len(rm.removed) != 0 {
		t.Fatalf("removed %d blobs, want 0 (digest unchanged across overwrite)", len(rm.removed))
	}
}

func TestRefIndex_OverwriteDifferentContentReleasesOld(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	a := []byte("version A bytes")
	bb := []byte("version B bytes — different")
	da, db := digestOf(t, a), digestOf(t, bb)

	putObj(t, b, "k1", a)
	putObj(t, b, "k1", bb) // overwrite-in-place with new content

	if got := claims(t, mem, da); got != 0 {
		t.Fatalf("claims(A) = %d, want 0 (superseded)", got)
	}
	if got := claims(t, mem, db); got != 1 {
		t.Fatalf("claims(B) = %d, want 1", got)
	}
	if rm.removedDigests()[string(da)] != 1 {
		t.Fatalf("expected exactly one RemoveBlob(A); got removals %v", rm.removedDigests())
	}
	if rm.removedDigests()[string(db)] != 0 {
		t.Fatalf("B must not be removed; got removals %v", rm.removedDigests())
	}
}

func TestRefIndex_DeleteReleasesAtZero(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	data := []byte("delete me")
	d := digestOf(t, data)

	putObj(t, b, "k1", data)
	deleteObj(t, b, "k1")

	if got := claims(t, mem, d); got != 0 {
		t.Fatalf("claims = %d, want 0 after delete", got)
	}
	if rm.removedDigests()[string(d)] != 1 {
		t.Fatalf("expected one RemoveBlob; got %v", rm.removedDigests())
	}
}

// TestRefIndex_DuplicateBlobInManifestReleasedOnce is the regression for the
// duplicate-digest double-remove bug: a body whose blobs share a digest (here
// two identical 1 KiB halves) holds exactly one claim and is released exactly
// once on delete, not once per duplicate BlobRef.
func TestRefIndex_DuplicateBlobInManifestReleasedOnce(t *testing.T) {
	b, mem, rm := newRefTestBackend(t, 1024) // 1 KiB blob ceiling
	data := bytes.Repeat([]byte{0x7}, 2048)  // → two identical 1 KiB blobs
	d := digestOf(t, data[:1024])

	putObj(t, b, "k1", data)
	if got := claims(t, mem, d); got != 1 {
		t.Fatalf("claims = %d, want 1 (duplicate blobs share one claim row)", got)
	}

	deleteObj(t, b, "k1")
	if got := claims(t, mem, d); got != 0 {
		t.Fatalf("claims after delete = %d, want 0", got)
	}
	if n := rm.removedDigests()[string(d)]; n != 1 {
		t.Fatalf("RemoveBlob called %d times for the shared blob, want exactly 1", n)
	}
}

func TestRefIndex_DeleteOneOfTwoKeepsBlob(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	data := []byte("shared across two keys")
	d := digestOf(t, data)

	putObj(t, b, "k1", data)
	putObj(t, b, "k2", data)

	deleteObj(t, b, "k1")
	if got := claims(t, mem, d); got != 1 {
		t.Fatalf("claims after first delete = %d, want 1 (k2 still references it)", got)
	}
	if len(rm.removed) != 0 {
		t.Fatalf("removed %d blobs after first delete, want 0", len(rm.removed))
	}

	deleteObj(t, b, "k2")
	if got := claims(t, mem, d); got != 0 {
		t.Fatalf("claims after second delete = %d, want 0", got)
	}
	if rm.removedDigests()[string(d)] != 1 {
		t.Fatalf("expected one RemoveBlob after the last reference dropped; got %v", rm.removedDigests())
	}
}
