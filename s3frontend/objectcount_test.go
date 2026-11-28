package s3frontend

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/libforge/testutil"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
)

// A bucket counts one object per committed object version: it gains one when
// a version commits and loses one when a version is removed for good, in the
// commit's own transaction. It also counts each part of a multipart upload
// in progress. These tests pin that mapping, which follows what AWS reports
// for NumberOfObjects — current and noncurrent versions, delete markers and
// the parts of incomplete multipart uploads all count.

// wantObjects asserts how many objects the bucket has gained and lost.
func wantObjects(t *testing.T, mem *testStore, added, removed int64, msg string) {
	t.Helper()
	gotAdded, gotRemoved := mem.objectTotals(t, "bk")
	require.Equal(t, [2]int64{added, removed}, [2]int64{gotAdded, gotRemoved}, "objects gained and lost: %s", msg)
}

// newCountingBackend mirrors newRefTestBackend, with a bucket "bk" in a space
// of its own. Each of opts may adjust the dependencies first.
func newCountingBackend(t *testing.T, opts ...func(*Deps)) (*Backend, *testStore) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	mem := newTestStore(t)
	cache, err := blockstore.NewBlobCache(filepath.Join(dir, "cache"))
	require.NoError(t, err, "cache")
	log, err := logstore.Open(ctx, logstore.Config{
		Dir:     filepath.Join(dir, "segments"),
		Meta:    mem,
		Catalog: logstore.PlaneConfig{Ship: false},
		Logger:  zaptest.NewLogger(t),
	})
	require.NoError(t, err, "logstore")
	t.Cleanup(func() { _ = log.Close(ctx) })

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
		Remover:    &recordingRemover{},
		Capturer:   testfakes.NopUploader{},
		EncParams:  mem,
		RegionKeys: testRegionKeys(t),
		TenantKeys: testTenantKeys(),
		Claims:     mem,
	}
	withStreaming(t, &deps, dir)
	for _, opt := range opts {
		opt(&deps)
	}
	b := mem.newBackend(t, deps)
	require.NoError(t, mem.Create(ctx, "bk", testutil.RandomDID(t), registry.CreateState{}), "create bucket")
	return b, mem
}

// currentManifestCID returns the CID of the key's current version manifest.
func currentManifestCID(t *testing.T, b *Backend, key string) cid.Cid {
	t.Helper()
	rv, err := b.resolveVersion(context.Background(), "bk", key, "")
	require.NoError(t, err)
	return rv.node.Manifest
}

// TestPutCountsTheCommittedVersion: a PUT adds one object, and nothing else.
func TestPutCountsTheCommittedVersion(t *testing.T) {
	b, mem := newCountingBackend(t)

	putObjV(t, b, "a", []byte("hello"))

	wantObjects(t, mem, 1, 0, "one version committed, nothing superseded")
}

// TestOverwriteUnversionedSwapsTheObject: in a bucket with no versioning the
// prior generation is discarded, so the count stays at one object.
func TestOverwriteUnversionedSwapsTheObject(t *testing.T) {
	b, mem := newCountingBackend(t)

	putObjV(t, b, "a", []byte("first"))
	putObjV(t, b, "a", []byte("second"))

	wantObjects(t, mem, 2, 1, "both versions counted as they committed, the discarded one taken away")
}

// TestOverwriteVersionedRetainsBothObjects: an enabled bucket retains the
// noncurrent version, and AWS counts it, so nothing is taken away.
func TestOverwriteVersionedRetainsBothObjects(t *testing.T) {
	b, mem := newCountingBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)

	putObjV(t, b, "a", []byte("first"))
	putObjV(t, b, "a", []byte("second"))

	wantObjects(t, mem, 2, 0, "a retained noncurrent version still counts")
}

// TestDeleteMarkerCounts: a delete marker is a version and AWS counts it, so
// the versioned DELETE adds an object rather than taking one away.
func TestDeleteMarkerCounts(t *testing.T) {
	b, mem := newCountingBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)

	putObjV(t, b, "a", []byte("hello"))
	_, err := deleteObjV(t, b, "a", "")
	require.NoError(t, err, "DeleteObject")

	wantObjects(t, mem, 2, 0, "the object version and the delete marker")
}

// TestUnversionedDeleteTakesTheObjectAway: the key is gone for good, so the
// count returns to zero.
func TestUnversionedDeleteTakesTheObjectAway(t *testing.T) {
	b, mem := newCountingBackend(t)

	putObjV(t, b, "a", []byte("hello"))
	_, err := deleteObjV(t, b, "a", "")
	require.NoError(t, err, "DeleteObject")

	wantObjects(t, mem, 1, 1, "the key is gone")
}

// TestVersionScopedDeleteTakesThatVersionAway: deleting one version by id
// takes away exactly one object and leaves the other counted.
func TestVersionScopedDeleteTakesThatVersionAway(t *testing.T) {
	b, mem := newCountingBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)

	first := putObjV(t, b, "a", []byte("first"))
	putObjV(t, b, "a", []byte("second"))

	_, err := deleteObjV(t, b, "a", first.VersionID)
	require.NoError(t, err, "DeleteObject(versionId)")

	wantObjects(t, mem, 2, 1, "one of two versions deleted")
}

// TestDeletingAMissingKeyCountsNothing: an idempotent DELETE of an absent key
// must not move the count.
func TestDeletingAMissingKeyCountsNothing(t *testing.T) {
	b, mem := newCountingBackend(t)

	_, err := deleteObjV(t, b, "gone", "")
	require.NoError(t, err, "DeleteObject")

	wantObjects(t, mem, 0, 0, "nothing was there")
}

// TestMultipartPartsCountUntilTheUploadEnds: each part of an upload in
// progress counts as an object, as AWS reports for NumberOfObjects, and a
// part number uploaded again replaces its part without counting twice.
// Complete takes every part away, the ones the object left out included, and
// the object it commits counts as one.
func TestMultipartPartsCountUntilTheUploadEnds(t *testing.T) {
	b, mem := newCountingBackend(t)

	uploadID := mpCreate(t, b, "big", "", "")
	wantObjects(t, mem, 0, 0, "an upload with no parts counts nothing")

	_, err := mpUploadPart(t, b, "big", uploadID, 1, bytes.Repeat([]byte("x"), 5*1024*1024), nil)
	require.NoError(t, err)
	wantObjects(t, mem, 1, 0, "one part in progress")

	p1, err := mpUploadPart(t, b, "big", uploadID, 1, bytes.Repeat([]byte("a"), 5*1024*1024), nil)
	require.NoError(t, err)
	wantObjects(t, mem, 1, 0, "a part uploaded again replaces the part")

	p2, err := mpUploadPart(t, b, "big", uploadID, 2, []byte("tail"), nil)
	require.NoError(t, err)
	_, err = mpUploadPart(t, b, "big", uploadID, 3, []byte("left out"), nil)
	require.NoError(t, err)
	wantObjects(t, mem, 3, 0, "three parts in progress")

	one, two := int32(1), int32(2)
	_, err = mpComplete(t, b, "big", uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: p1.ETag},
		{PartNumber: &two, ETag: p2.ETag},
	}, nil)
	require.NoError(t, err)
	wantObjects(t, mem, 4, 3, "one object, and no parts")

	// A repeated Complete replays its result and counts nothing more.
	_, err = mpComplete(t, b, "big", uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: p1.ETag},
		{PartNumber: &two, ETag: p2.ETag},
	}, nil)
	require.NoError(t, err)
	wantObjects(t, mem, 4, 3, "a repeated Complete")
}

// TestAbortedMultipartPartsStopCounting: an abort takes the upload's parts
// away, and the bucket is back to what it held.
func TestAbortedMultipartPartsStopCounting(t *testing.T) {
	b, mem := newCountingBackend(t)
	putObjV(t, b, "a", []byte("hello"))

	uploadID := mpCreate(t, b, "big", "", "")
	_, err := mpUploadPart(t, b, "big", uploadID, 1, bytes.Repeat([]byte("a"), 5*1024*1024), nil)
	require.NoError(t, err)
	_, err = mpUploadPart(t, b, "big", uploadID, 2, []byte("tail"), nil)
	require.NoError(t, err)
	wantObjects(t, mem, 3, 0, "one object and two parts in progress")

	bucket, key := "bk", "big"
	require.NoError(t, b.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
		Bucket: &bucket, Key: &key, UploadId: &uploadID,
	}))
	wantObjects(t, mem, 3, 2, "the parts went with the upload")
}

// TestObjectCountIsKeptByTheHourForTheReport: the hour's usage count holds
// the bucket's totals under its space, due for the next report.
func TestObjectCountIsKeptByTheHourForTheReport(t *testing.T) {
	b, mem := newCountingBackend(t)

	putObjV(t, b, "a", []byte("first"))
	putObjV(t, b, "a", []byte("second"))
	putObjV(t, b, "b", []byte("other"))

	st, err := mem.Get(context.Background(), "bk")
	require.NoError(t, err)
	due, err := mem.DueObjects(context.Background(), 10)
	require.NoError(t, err)
	require.NotEmpty(t, due)
	// The latest hour holds the totals: the writes share an hour unless one
	// fell either side of its turn.
	last := due[len(due)-1]
	require.Equal(t, st.Space, last.Space)
	require.Equal(t, [2]int64{3, 1}, [2]int64{last.Added, last.Removed})
}
