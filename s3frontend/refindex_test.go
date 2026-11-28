package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/internal/testfakes"
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
// MemStore, an on-disk cache, a catalog log) plus a recording remover, so a
// test can drive PutObject/DeleteObject and inspect blob_refs + releases.
func newRefTestBackend(t *testing.T, maxBlob ...int64) (*Backend, *testStore, *recordingRemover) {
	t.Helper()
	return newRefTestBackendOn(t, newTestStore(t), maxBlob...)
}

// newRefTestBackendOn is newRefTestBackend over a given store.
func newRefTestBackendOn(t *testing.T, mem *testStore, maxBlob ...int64) (*Backend, *testStore, *recordingRemover) {
	t.Helper()
	var mbs int64
	if len(maxBlob) > 0 {
		mbs = maxBlob[0]
	}
	return newRefTestBackendOver(t, mem, newRetainedLog(t, mem), mbs)
}

// newRetainedLog is a catalog log over mem that retains its segments
// locally, closed with the test.
func newRetainedLog(t *testing.T, mem *testStore) blockstore.Log {
	t.Helper()
	ctx := context.Background()
	log, err := logstore.Open(ctx, logstore.Config{
		Dir:     filepath.Join(t.TempDir(), "segments"),
		Meta:    mem,
		Catalog: logstore.PlaneConfig{Ship: false}, // retained locally; no uploader needed
		Logger:  zaptest.NewLogger(t),
	})
	require.NoError(t, err, "logstore")
	t.Cleanup(func() { _ = log.Close(ctx) })
	return log
}

// newRefTestBackendOver is newRefTestBackend over a given catalog log. Each
// opt adjusts the Deps before the Backend is built.
func newRefTestBackendOver(t *testing.T, mem *testStore, log blockstore.Log, mbs int64, opts ...func(*Deps)) (*Backend, *testStore, *recordingRemover) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	cache, err := blockstore.NewBlobCache(filepath.Join(dir, "cache"))
	require.NoError(t, err, "cache")

	rm := &recordingRemover{}
	deps := Deps{
		Authority:  mem,
		Registry:   mem,
		Intents:    mem,
		Locations:  mem,
		Multipart:  mem,
		Parks:      mem,
		Reads:      blockstore.NewLayered(cache, log, testfakes.NopBaseReader{}),
		Log:        log,
		Cache:      cache,
		Deferred:   testfakes.NopUploader{},
		Remover:    rm,
		Capturer:   testfakes.NopUploader{},
		EncParams:  mem,
		RegionKeys: testRegionKeys(t),
		TenantKeys: testTenantKeys(),
		Claims:     mem,
		// ReleaseGrace stays zero: releases are due immediately, and tests
		// drain them explicitly with drainReleases.
		MaxBlobSize: mbs,
	}
	for _, opt := range opts {
		opt(&deps)
	}
	withStreaming(t, &deps, dir)
	b := mem.newBackend(t, deps)
	require.NoError(t, mem.Create(ctx, "bk", did.Undef, registry.CreateState{}), "create bucket")
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
	require.NoError(t, err, "resolve %s@%q", key, versionID)
	ds := make([]multihash.Multihash, 0)
	for _, ref := range storedBlobs(t, b, rv) {
		ds = append(ds, ref.Digest)
	}
	return ds
}

// storedBlobs reads a resolved version's blob list, extents included.
func storedBlobs(t testing.TB, b *Backend, rv *resolvedVersion) msbucket.BlobRefs {
	t.Helper()
	refs, err := msbucket.ReadBlobRefs(context.Background(), rv.mf.Body.Index(b.read, rv.st.Space))
	require.NoError(t, err, "read blobs of %s", rv.mf.Key)
	return refs
}

// blobDigestOf is blobDigestsOf for the common single-blob object.
func blobDigestOf(t *testing.T, b *Backend, key, versionID string) multihash.Multihash {
	t.Helper()
	ds := blobDigestsOf(t, b, key, versionID)
	require.Len(t, ds, 1, "object %s@%q has %d blobs", key, versionID, len(ds))
	return ds[0]
}

func digestOf(t *testing.T, data []byte) multihash.Multihash {
	t.Helper()
	mh, err := multihash.Sum(data, multihash.SHA2_256, -1)
	require.NoError(t, err, "multihash")
	return mh
}

func putObj(t *testing.T, b *Backend, key string, data []byte) {
	t.Helper()
	bucket := "bk"
	_, err := b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(data), ContentLength: byteLen(data),
	})
	require.NoError(t, err, "PutObject %s", key)
}

func deleteObj(t *testing.T, b *Backend, key string) {
	t.Helper()
	bucket := "bk"
	_, err := b.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: &bucket, Key: &key})
	require.NoError(t, err, "DeleteObject %s", key)
}

func claims(t *testing.T, mem *testStore, digest []byte) int {
	t.Helper()
	// MemStore buckets carry no space (mem.bk()), so claims recorded via
	// the backend are keyed under mem.bk(); count under the same key.
	// Space-keyed claim counting is covered by the live Postgres test.
	n, err := mem.CountReferences(context.Background(), mem.bk(), digest, uuid.Nil)
	require.NoError(t, err, "CountReferences")
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
	require.False(t, bytes.Equal(d1, d2), "identical content produced the same stored digest %x; encryption must make them distinct", d1)
	require.False(t, bytes.Equal(d1, digestOf(t, data)), "stored digest equals hash(plaintext); blob was not encrypted")
	got1, got2 := claims(t, mem, d1), claims(t, mem, d2)
	require.Equal(t, 1, got1, "claims = %d/%d, want 1/1 (one claim per distinct blob)", got1, got2)
	require.Equal(t, 1, got2, "claims = %d/%d, want 1/1 (one claim per distinct blob)", got1, got2)
	require.Empty(t, rm.removed, "removed %d blobs, want 0 (both still referenced)", len(rm.removed))
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

	require.False(t, bytes.Equal(old, cur), "overwrite reused stored digest %x; encryption must mint a new one", old)
	got := claims(t, mem, old)
	require.Zero(t, got, "claims(old) (superseded)")
	require.Equal(t, 1, claims(t, mem, cur), "claims(current)")
	drainReleases(t, b)
	removed := rm.removedDigests()
	require.Equal(t, 1, removed[string(old)], "RemoveBlob(old) calls (all removals: %v)", removed)
}

func TestRefIndex_OverwriteDifferentContentReleasesOld(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	a := []byte("version A bytes")
	bb := []byte("version B bytes — different")

	putObj(t, b, "k1", a)
	da := blobDigestOf(t, b, "k1", "")
	putObj(t, b, "k1", bb) // overwrite-in-place with new content
	db := blobDigestOf(t, b, "k1", "")

	got := claims(t, mem, da)
	require.Zero(t, got, "claims(A) (superseded)")
	require.Equal(t, 1, claims(t, mem, db), "claims(B)")
	drainReleases(t, b)
	removed := rm.removedDigests()
	require.Equal(t, 1, removed[string(da)], "RemoveBlob(A) calls (all removals: %v)", removed)
	require.Zero(t, removed[string(db)], "B must not be removed (all removals: %v)", removed)
}

func TestRefIndex_DeleteReleasesAtZero(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	data := []byte("delete me")

	putObj(t, b, "k1", data)
	d := blobDigestOf(t, b, "k1", "")
	deleteObj(t, b, "k1")

	got := claims(t, mem, d)
	require.Zero(t, got, "claims after delete")
	drainReleases(t, b)
	removed := rm.removedDigests()
	require.Equal(t, 1, removed[string(d)], "RemoveBlob calls (all removals: %v)", removed)
}

// TestRefIndex_IdenticalPiecesInOneBody: a body of two identical 1 KiB
// plaintext halves is stored as two DISTINCT blobs (fresh CEK per piece),
// each with its own claim, each released exactly once on delete. (The old
// duplicate-BlobRef double-remove scenario cannot be produced by the write
// path any more; runRelease's claim count still guards it.)
func TestRefIndex_IdenticalPiecesInOneBody(t *testing.T) {
	b, mem, rm := newRefTestBackend(t, 1024) // 1 KiB plaintext blob ceiling
	data := bytes.Repeat([]byte{0x7}, 2048)  // → two identical 1 KiB pieces

	putObj(t, b, "k1", data)
	ds := blobDigestsOf(t, b, "k1", "")
	require.Len(t, ds, 2, "blobs")
	require.False(t, bytes.Equal(ds[0], ds[1]), "identical pieces share stored digest %x; encryption must make them distinct", ds[0])
	for i, d := range ds {
		got := claims(t, mem, d)
		require.Equal(t, 1, got, "claims(blob %d)", i)
	}

	deleteObj(t, b, "k1")
	drainReleases(t, b)
	for i, d := range ds {
		got := claims(t, mem, d)
		require.Zero(t, got, "claims(blob %d) after delete", i)
		n := rm.removedDigests()[string(d)]
		require.Equal(t, 1, n, "RemoveBlob called %d times for blob %d", n, i)
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
	_, err := b.CopyObject(context.Background(), s3response.CopyObjectInput{
		Bucket:     &bucket,
		Key:        &dstKey,
		CopySource: &src,
	})
	require.NoError(t, err, "CopyObject")
	d2 := blobDigestOf(t, b, "k2", "")
	require.True(t, bytes.Equal(d, d2), "copy stored digest %x, want the source's %x (copies share blobs)", d2, d)
	got := claims(t, mem, d)
	require.Equal(t, 2, got, "claims after copy")

	deleteObj(t, b, "k1")
	drainReleases(t, b)
	require.Equal(t, 1, claims(t, mem, d), "claims after first delete (k2 still references it)")
	require.Empty(t, rm.removed, "removed blobs after first delete")

	deleteObj(t, b, "k2")
	drainReleases(t, b)
	require.Zero(t, claims(t, mem, d), "claims after second delete")
	removed := rm.removedDigests()
	require.Equal(t, 1, removed[string(d)], "RemoveBlob calls after the last reference dropped (all removals: %v)", removed)
}

// failCommitRoot is a registry whose next root commits fail, as a Postgres
// error at the swap would, after the commit's claims were handed to it.
type failCommitRoot struct {
	registry.Registry
	fails int
}

func (f *failCommitRoot) RootCommitWrites(ctx context.Context, name string, expect, next cid.Cid, c registry.RootCommit, gc []cid.Cid) (registry.CommitWrites, error) {
	if f.fails > 0 {
		f.fails--
		return registry.CommitWrites{}, errors.New("postgres unavailable")
	}
	return f.Registry.RootCommitWrites(ctx, name, expect, next, c, gc)
}

// TestCommitFailureWritesNoClaim: the reference index is written in the
// root swap's transaction, so a commit that fails at the swap claims nothing
// for the version it did not publish, keeps the claim of the version it did
// not supersede, and records no release. The retry claims as the first
// attempt would have.
func TestCommitFailureWritesNoClaim(t *testing.T) {
	reg := &failCommitRoot{}
	b, mem := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) {
		reg.Registry = d.Registry
		d.Registry = reg
	})
	ctx := context.Background()
	key := "atomic"

	putObj(t, b, key, []byte("first body"))
	old := blobDigestOf(t, b, key, "")
	n := claims(t, mem, old)
	require.Equal(t, 1, n, "claims on the first version")

	reg.fails = 1
	bucket := "bk"
	_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader([]byte("second body")), ContentLength: byteLen([]byte("second body"))})
	require.Error(t, err, "PutObject succeeded although its root swap failed")
	// The overwrite published nothing: the first version stands, with its
	// claim, and no release is recorded.
	got := blobDigestOf(t, b, key, "")
	require.Equal(t, string(old), string(got), "current version after the failed overwrite names %x", got)
	require.Equal(t, 1, claims(t, mem, old), "claims on the first version after the failed overwrite")
	pending, _ := mem.ListReleasesBySpace(ctx, mem.bk())
	require.Empty(t, pending, "a failed overwrite recorded releases")
	// A claim publishes its blob's intent; the failed write's blob was never
	// claimed, so the first version's is the only published intent.
	published, err := mem.ListIntentsByState(ctx, registry.IntentPublished)
	require.NoError(t, err, "ListIntentsByState")
	require.Len(t, published, 1, "published intents after the failed overwrite = %v, want the first version's blob only", published)
	require.Equal(t, string(old), string(published[0].Digest), "published intents after the failed overwrite = %v, want the first version's blob only", published)

	// The retry commits, claims its blob and drops the first version's.
	putObj(t, b, key, []byte("second body"))
	next := blobDigestOf(t, b, key, "")
	require.Equal(t, 1, claims(t, mem, next), "claims on the retried version")
	require.Zero(t, claims(t, mem, old), "claims on the superseded version")
	pending, _ = mem.ListReleasesBySpace(ctx, mem.bk())
	require.Len(t, pending, 1, "releases after the retry")
	require.Equal(t, string(old), string(pending[0].Digest), "releases after the retry")
}

// stallCommitRoot is a registry whose root commits wait delay before they
// run, as a slow fsync or lock wait ahead of the swap would.
type stallCommitRoot struct {
	registry.Registry
	delay time.Duration
}

func (s *stallCommitRoot) RootCommitWrites(ctx context.Context, name string, expect, next cid.Cid, c registry.RootCommit, gc []cid.Cid) (registry.CommitWrites, error) {
	time.Sleep(s.delay)
	return s.Registry.RootCommitWrites(ctx, name, expect, next, c, gc)
}

// TestReleaseGraceRunsFromTheCommit: a reader that resolved the superseded
// version just before the swap has the whole release grace to open its
// blobs, so the grace is measured when the swap commits, not when the
// drops are staged. With the commit stalled longer than the grace, a grace
// measured at staging would have the release due before the swap.
func TestReleaseGraceRunsFromTheCommit(t *testing.T) {
	const grace, stall = 150 * time.Millisecond, 250 * time.Millisecond
	reg := &stallCommitRoot{delay: stall}
	b, mem := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) {
		d.ReleaseGrace = grace
		reg.Registry = d.Registry
		d.Registry = reg
	})
	ctx := context.Background()
	key := "grace"
	putObj(t, b, key, []byte("first body"))
	old := blobDigestOf(t, b, key, "")

	start := time.Now()
	putObj(t, b, key, []byte("second body"))
	pending, err := mem.ListReleasesBySpace(ctx, mem.bk())
	require.NoError(t, err, "releases after the overwrite")
	require.Len(t, pending, 1, "releases after the overwrite")
	require.Equal(t, string(old), string(pending[0].Digest), "releases after the overwrite")
	require.False(t, pending[0].NotBefore.Before(start.Add(stall+grace)), "release due %v after the overwrite began, want at least the stall plus the grace (%v): the grace ran from staging, not the commit", pending[0].NotBefore.Sub(start), stall+grace)
}
