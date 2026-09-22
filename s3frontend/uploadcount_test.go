package s3frontend

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
)

// The object count a space reports is one content entry per committed object
// version: /upload/add when a version commits, /upload/remove when one is
// retired for good. These tests pin that mapping, which follows what AWS
// reports for NumberOfObjects — current and noncurrent versions plus delete
// markers all count.

// recordingRegistrar captures the roots registered and retracted, so a test can
// assert exactly which versions a request counted.
type recordingRegistrar struct {
	mu        sync.Mutex
	added     []cid.Cid
	retracted []cid.Cid
}

func (r *recordingRegistrar) RegisterUpload(_ context.Context, _ did.DID, root cid.Cid) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.added = append(r.added, root)
	return nil
}

func (r *recordingRegistrar) RetractUpload(_ context.Context, _ did.DID, root cid.Cid) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.retracted = append(r.retracted, root)
	return nil
}

func (r *recordingRegistrar) snapshot() (added, retracted []cid.Cid) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]cid.Cid(nil), r.added...), append([]cid.Cid(nil), r.retracted...)
}

// count is the space's object count as sprue would derive it: adds minus
// removes.
func (r *recordingRegistrar) count() int {
	added, retracted := r.snapshot()
	return len(added) - len(retracted)
}

// newCountingBackend mirrors newRefTestBackend but swaps the no-op registrar
// for a recording one.
func newCountingBackend(t *testing.T) (*Backend, *recordingRegistrar) {
	t.Helper()
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
		Catalog: logstore.PlaneConfig{Ship: false},
		Logger:  zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatalf("logstore: %v", err)
	}
	t.Cleanup(func() { _ = log.Close(ctx) })

	reg := &recordingRegistrar{}
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
		Remover:         &recordingRemover{},
		Registrar:       reg,
		EncParams:       mem,
		RegionKeys:      testRegionKeys(t),
		TenantKeys:      testTenantKeys(),
		PendingReleases: mem,
	})
	if err := mem.Create(ctx, "bk", did.Undef, registry.CreateState{}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return b, reg
}

// TestPutRegistersTheCommittedVersion: a PUT registers exactly the manifest it
// committed, and nothing else.
func TestPutRegistersTheCommittedVersion(t *testing.T) {
	b, reg := newCountingBackend(t)

	putObjV(t, b, "a", []byte("hello"))

	added, retracted := reg.snapshot()
	require.Len(t, added, 1, "one version committed, one entry registered")
	require.Empty(t, retracted, "nothing was superseded")
	require.Equal(t, currentManifestCID(t, b, "a"), added[0])
}

// TestOverwriteUnversionedSwapsTheEntry: in a bucket with no versioning the
// prior generation is discarded, so the count stays at one object.
func TestOverwriteUnversionedSwapsTheEntry(t *testing.T) {
	b, reg := newCountingBackend(t)

	putObjV(t, b, "a", []byte("first"))
	first, _ := reg.snapshot()
	putObjV(t, b, "a", []byte("second"))

	added, retracted := reg.snapshot()
	require.Len(t, added, 2, "both versions registered as they committed")
	require.Len(t, retracted, 1, "the discarded generation retracted")
	require.Equal(t, first[0], retracted[0], "the retracted root is the superseded one")
	require.Equal(t, 1, reg.count(), "one live object")
}

// TestOverwriteVersionedRetainsBothEntries: an enabled bucket retains the
// noncurrent version, and AWS counts it, so nothing is retracted.
func TestOverwriteVersionedRetainsBothEntries(t *testing.T) {
	b, reg := newCountingBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)

	putObjV(t, b, "a", []byte("first"))
	putObjV(t, b, "a", []byte("second"))

	added, retracted := reg.snapshot()
	require.Len(t, added, 2)
	require.Empty(t, retracted, "a retained noncurrent version still counts")
	require.Equal(t, 2, reg.count())
}

// TestDeleteMarkerCounts: a delete marker is a version and AWS counts it, so
// the versioned DELETE registers rather than retracts.
func TestDeleteMarkerCounts(t *testing.T) {
	b, reg := newCountingBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)

	putObjV(t, b, "a", []byte("hello"))
	if _, err := deleteObjV(t, b, "a", ""); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	added, retracted := reg.snapshot()
	require.Len(t, added, 2, "the object version and the delete marker")
	require.Empty(t, retracted)
	require.Equal(t, 2, reg.count())
}

// TestUnversionedDeleteRetractsTheEntry: the key is gone for good, so the
// count returns to zero.
func TestUnversionedDeleteRetractsTheEntry(t *testing.T) {
	b, reg := newCountingBackend(t)

	putObjV(t, b, "a", []byte("hello"))
	added, _ := reg.snapshot()
	if _, err := deleteObjV(t, b, "a", ""); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	_, retracted := reg.snapshot()
	require.Len(t, retracted, 1)
	require.Equal(t, added[0], retracted[0])
	require.Zero(t, reg.count())
}

// TestVersionScopedDeleteRetractsThatVersion: deleting one version by id
// retracts exactly that version's entry and leaves the other counted.
func TestVersionScopedDeleteRetractsThatVersion(t *testing.T) {
	b, reg := newCountingBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)

	first := putObjV(t, b, "a", []byte("first"))
	putObjV(t, b, "a", []byte("second"))
	added, _ := reg.snapshot()

	if _, err := deleteObjV(t, b, "a", first.VersionID); err != nil {
		t.Fatalf("DeleteObject(versionId): %v", err)
	}

	_, retracted := reg.snapshot()
	require.Len(t, retracted, 1)
	require.Equal(t, added[0], retracted[0], "the named version's root, not the current one")
	require.Equal(t, 1, reg.count())
}

// TestDeletingAMissingKeyCountsNothing: an idempotent DELETE of an absent key
// must not move the count.
func TestDeletingAMissingKeyCountsNothing(t *testing.T) {
	b, reg := newCountingBackend(t)

	if _, err := deleteObjV(t, b, "gone", ""); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	added, retracted := reg.snapshot()
	require.Empty(t, added)
	require.Empty(t, retracted)
}

// TestCompleteMultipartRegistersOnce: a multipart object counts as one, not one
// per part, and the entry is the manifest Complete committed.
func TestCompleteMultipartRegistersOnce(t *testing.T) {
	b, reg := newCountingBackend(t)

	uploadID := mpCreate(t, b, "big", "", "")
	p1, err := mpUploadPart(t, b, "big", uploadID, 1, bytes.Repeat([]byte("a"), 5*1024*1024), nil)
	require.NoError(t, err)
	p2, err := mpUploadPart(t, b, "big", uploadID, 2, []byte("tail"), nil)
	require.NoError(t, err)

	// Parts are blobs, not versions: nothing counts until Complete commits.
	added, _ := reg.snapshot()
	require.Empty(t, added, "uploading parts registers nothing")

	one, two := int32(1), int32(2)
	_, err = mpComplete(t, b, "big", uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: p1.ETag},
		{PartNumber: &two, ETag: p2.ETag},
	}, nil)
	require.NoError(t, err)

	added, retracted := reg.snapshot()
	require.Len(t, added, 1, "one object, however many parts")
	require.Empty(t, retracted)
	require.Equal(t, currentManifestCID(t, b, "big"), added[0])
}

// currentManifestCID returns the CID of the key's current version manifest —
// the root the space counts it under.
func currentManifestCID(t *testing.T, b *Backend, key string) cid.Cid {
	t.Helper()
	rv, err := b.resolveVersion(context.Background(), "bk", key, "")
	require.NoError(t, err)
	return rv.node.Manifest
}
