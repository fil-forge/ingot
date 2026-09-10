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
	"github.com/fil-forge/ingot/registry"
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
		Authority:       mem,
		Registry:        mem,
		Intents:         mem,
		Locations:       mem,
		BlobRefs:        mem,
		GC:              mem,
		Multipart:       mem,
		Parks:           mem,
		Reads:           blockstore.NewLayered(spool, log, inmem.NopBaseReader{}),
		Log:             log,
		Spool:           spool,
		Uploader:        inmem.NopUploader{},
		Deferred:        inmem.NopUploader{},
		Remover:         rm,
		EncParams:       mem,
		RegionKeys:      testRegionKeys(t),
		TenantKeys:      testTenantKeys(),
		PendingReleases: mem,
		// ReleaseGrace stays zero: releases are due immediately, and tests
		// drain them explicitly with drainReleases.
		MaxBlobSize: mbs,
	})
	if err := mem.Create(ctx, "bk", did.Undef, registry.CreateState{}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return b, mem, rm
}

// blobDigestsOf reads one version's manifest through the backend's own
// resolver and returns its body blob digests — the OBSERVED stored digests.
// Encryption mints a fresh CEK per blob, so a stored digest can no longer be
// predicted as hash(plaintext); tests key claim and removal assertions on
// what the write path actually stored. versionID "" resolves the current
// version.
func blobDigestsOf(t *testing.T, b *Backend, key, versionID string) []multihash.Multihash {
	t.Helper()
	rv, err := b.resolveVersion(context.Background(), "bk", key, versionID)
	if err != nil {
		t.Fatalf("resolve %s@%q: %v", key, versionID, err)
	}
	ds := make([]multihash.Multihash, 0, len(rv.mf.Body.Blobs))
	for _, ref := range rv.mf.Body.Blobs {
		ds = append(ds, ref.Digest)
	}
	return ds
}

// blobDigestOf is blobDigestsOf for the common single-blob object.
func blobDigestOf(t *testing.T, b *Backend, key, versionID string) multihash.Multihash {
	t.Helper()
	ds := blobDigestsOf(t, b, key, versionID)
	if len(ds) != 1 {
		t.Fatalf("object %s@%q has %d blobs, want 1", key, versionID, len(ds))
	}
	return ds[0]
}

func digestOf(t *testing.T, data []byte) multihash.Multihash {
	t.Helper()
	mh, err := multihash.Sum(data, multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	return mh
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

// TestRefIndex_IdenticalContentDistinctBlobs: encryption ended content dedup
// — a fresh CEK per write makes every stored digest unique, so identical
// bytes under two keys are two blobs with one claim each.
func TestRefIndex_IdenticalContentDistinctBlobs(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	data := []byte("identical bytes for two keys")

	putObj(t, b, "k1", data)
	putObj(t, b, "k2", data)

	d1 := blobDigestOf(t, b, "k1", "")
	d2 := blobDigestOf(t, b, "k2", "")
	if bytes.Equal(d1, d2) {
		t.Fatalf("identical content produced the same stored digest %x; encryption must make them distinct", d1)
	}
	if bytes.Equal(d1, digestOf(t, data)) {
		t.Fatalf("stored digest equals hash(plaintext); blob was not encrypted")
	}
	if got1, got2 := claims(t, mem, d1), claims(t, mem, d2); got1 != 1 || got2 != 1 {
		t.Fatalf("claims = %d/%d, want 1/1 (one claim per distinct blob)", got1, got2)
	}
	if len(rm.removed) != 0 {
		t.Fatalf("removed %d blobs, want 0 (both still referenced)", len(rm.removed))
	}
}

// TestRefIndex_OverwriteSameContentReleasesOld: with encryption an
// identical-bytes overwrite is a NEW blob (fresh CEK, new digest); the
// superseded blob's claim drops to zero and it is released.
func TestRefIndex_OverwriteSameContentReleasesOld(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	data := []byte("same bytes re-put")

	putObj(t, b, "k1", data)
	old := blobDigestOf(t, b, "k1", "")
	putObj(t, b, "k1", data) // overwrite with identical bytes
	cur := blobDigestOf(t, b, "k1", "")

	if bytes.Equal(old, cur) {
		t.Fatalf("overwrite reused stored digest %x; encryption must mint a new one", old)
	}
	if got := claims(t, mem, old); got != 0 {
		t.Fatalf("claims(old) = %d, want 0 (superseded)", got)
	}
	if got := claims(t, mem, cur); got != 1 {
		t.Fatalf("claims(current) = %d, want 1", got)
	}
	drainReleases(t, b)
	if rm.removedDigests()[string(old)] != 1 {
		t.Fatalf("expected exactly one RemoveBlob(old); got removals %v", rm.removedDigests())
	}
}

func TestRefIndex_OverwriteDifferentContentReleasesOld(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	a := []byte("version A bytes")
	bb := []byte("version B bytes — different")

	putObj(t, b, "k1", a)
	da := blobDigestOf(t, b, "k1", "")
	putObj(t, b, "k1", bb) // overwrite-in-place with new content
	db := blobDigestOf(t, b, "k1", "")

	if got := claims(t, mem, da); got != 0 {
		t.Fatalf("claims(A) = %d, want 0 (superseded)", got)
	}
	if got := claims(t, mem, db); got != 1 {
		t.Fatalf("claims(B) = %d, want 1", got)
	}
	drainReleases(t, b)
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

	putObj(t, b, "k1", data)
	d := blobDigestOf(t, b, "k1", "")
	deleteObj(t, b, "k1")

	if got := claims(t, mem, d); got != 0 {
		t.Fatalf("claims = %d, want 0 after delete", got)
	}
	drainReleases(t, b)
	if rm.removedDigests()[string(d)] != 1 {
		t.Fatalf("expected one RemoveBlob; got %v", rm.removedDigests())
	}
}

// TestRefIndex_IdenticalPiecesInOneBody: a body of two identical 1 KiB
// plaintext halves is stored as two DISTINCT blobs (fresh CEK per piece),
// each with its own claim, each released exactly once on delete. (The old
// duplicate-BlobRef double-remove scenario cannot be produced by the write
// path any more; releaseBlobs still guards it.)
func TestRefIndex_IdenticalPiecesInOneBody(t *testing.T) {
	b, mem, rm := newRefTestBackend(t, 1024) // 1 KiB plaintext blob ceiling
	data := bytes.Repeat([]byte{0x7}, 2048)  // → two identical 1 KiB pieces

	putObj(t, b, "k1", data)
	ds := blobDigestsOf(t, b, "k1", "")
	if len(ds) != 2 {
		t.Fatalf("blobs = %d, want 2", len(ds))
	}
	if bytes.Equal(ds[0], ds[1]) {
		t.Fatalf("identical pieces share stored digest %x; encryption must make them distinct", ds[0])
	}
	for i, d := range ds {
		if got := claims(t, mem, d); got != 1 {
			t.Fatalf("claims(blob %d) = %d, want 1", i, got)
		}
	}

	deleteObj(t, b, "k1")
	drainReleases(t, b)
	for i, d := range ds {
		if got := claims(t, mem, d); got != 0 {
			t.Fatalf("claims(blob %d) after delete = %d, want 0", i, got)
		}
		if n := rm.removedDigests()[string(d)]; n != 1 {
			t.Fatalf("RemoveBlob called %d times for blob %d, want exactly 1", n, i)
		}
	}
}

// TestRefIndex_SharedDigestAcrossKeys: same-bucket CopyObject is the one
// remaining way two keys reference ONE blob. The copy adds a second claim on
// the source's digest; the blob survives the first delete and is released
// exactly once when the last claim drops.
func TestRefIndex_SharedDigestAcrossKeys(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	data := []byte("shared across two keys")

	putObj(t, b, "k1", data)
	d := blobDigestOf(t, b, "k1", "")

	bucket, dstKey, src := "bk", "k2", "bk/k1"
	if _, err := b.CopyObject(context.Background(), s3response.CopyObjectInput{
		Bucket:     &bucket,
		Key:        &dstKey,
		CopySource: &src,
	}); err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	if d2 := blobDigestOf(t, b, "k2", ""); !bytes.Equal(d, d2) {
		t.Fatalf("copy stored digest %x, want the source's %x (copies share blobs)", d2, d)
	}
	if got := claims(t, mem, d); got != 2 {
		t.Fatalf("claims after copy = %d, want 2", got)
	}

	deleteObj(t, b, "k1")
	drainReleases(t, b)
	if got := claims(t, mem, d); got != 1 {
		t.Fatalf("claims after first delete = %d, want 1 (k2 still references it)", got)
	}
	if len(rm.removed) != 0 {
		t.Fatalf("removed %d blobs after first delete, want 0", len(rm.removed))
	}

	deleteObj(t, b, "k2")
	drainReleases(t, b)
	if got := claims(t, mem, d); got != 0 {
		t.Fatalf("claims after second delete = %d, want 0", got)
	}
	if rm.removedDigests()[string(d)] != 1 {
		t.Fatalf("expected one RemoveBlob after the last reference dropped; got %v", rm.removedDigests())
	}
}
