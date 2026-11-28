package s3frontend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fil-forge/versitygw/s3response"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/testutil"
	"go.uber.org/zap/zaptest"
)

// extentBlobSize splits a body into blobs small enough that a test body of
// a megabyte or two passes the 1,024 blobs a manifest keeps inline.
const extentBlobSize = 1 << 10

// smallLists are blob list sizes that put a body of a few dozen blobs
// across several extents.
var smallLists = msbucket.BlobListSizes{Head: 8, Extent: 8}

// newSmallListStore is a test store whose backend writes blob lists of the
// given sizes.
func newSmallListStore(t *testing.T, sizes msbucket.BlobListSizes) *testStore {
	t.Helper()
	mem := newTestStore(t)
	mem.blobList = sizes
	return mem
}

// assertExtents checks that key's stored list has a full Head and the given
// number of extents.
func assertExtents(t *testing.T, b *Backend, key string, extents int) *resolvedVersion {
	t.Helper()
	return assertExtentsIn(t, b, "bk", key, extents)
}

// assertExtentsIn is assertExtents for a key in bucket.
func assertExtentsIn(t *testing.T, b *Backend, bucket, key string, extents int) *resolvedVersion {
	t.Helper()
	rv, err := b.resolveVersion(context.Background(), bucket, key, "")
	require.NoError(t, err, "resolve %s", key)
	head := storeFor(b).blobList.Head
	if head == 0 {
		head = 1024
	}
	got := len(rv.mf.Body.Blobs.Head)
	require.Equal(t, head, got, "%s: Head holds %d blobs", key, got)
	require.Len(t, rv.mf.Body.Blobs.Extents, extents, "%s: extents", key)
	return rv
}

// TestExtents_PutObjectReadsBack: an object past the inline list reads back
// whole and in ranges inside the head, across the head/extent boundary, and
// inside an extent, decrypting through extent-by-extent prefetch. It writes
// lists of the default sizes, so one path runs as production does.
func TestExtents_PutObjectReadsBack(t *testing.T) {
	b, _, _ := newRefTestBackend(t, extentBlobSize)
	data := testBody(1100*extentBlobSize + 300) // 1,101 blobs
	bucket, key := "bk", "big"
	_, err := b.PutObject(context.Background(), s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: byteLen(data)})
	require.NoError(t, err, "PutObject")
	assertExtents(t, b, key, 1)

	got := getRange(t, b, key, "")
	require.Equal(t, data, got, "full GET: %d bytes, mismatch", len(got))
	boundary := int64(1024 * extentBlobSize)
	for _, r := range [][2]int64{
		{100, 5000},                      // inside the head
		{boundary - 100, boundary + 100}, // across the boundary
		{boundary + 10, boundary + 2000}, // inside the extent
		{int64(len(data)) - 1, int64(len(data)) - 1},
	} {
		got := getRange(t, b, key, fmt.Sprintf("bytes=%d-%d", r[0], r[1]))
		require.Equal(t, data[r[0]:r[1]+1], got, "range %v: %d bytes, mismatch", r, len(got))
	}
}

// TestExtents_DeleteReleasesEveryBlob: deleting an object drops the claim on
// every blob, including the ones listed only in its extents.
func TestExtents_DeleteReleasesEveryBlob(t *testing.T) {
	b, mem, _ := newRefTestBackendOn(t, newSmallListStore(t, smallLists), extentBlobSize)
	ctx := context.Background()
	data := testBody(20 * extentBlobSize) // a head of 8, extents of 8 and 4
	bucket, key := "bk", "big"
	_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: byteLen(data)})
	require.NoError(t, err, "PutObject")
	rv := assertExtents(t, b, key, 2)
	digests := storedBlobs(t, b, rv).DigestList()
	require.Len(t, digests, 20, "%d blobs", len(digests))
	for _, d := range digests {
		n, _ := mem.CountReferences(ctx, rv.st.Space, d, uuid.Nil)
		require.Equal(t, 1, n, "blob %x: %d claims before the delete", d, n)
	}
	deleteFrom(t, b, bucket, key)
	drainReleases(t, b)
	for _, d := range digests {
		n, _ := mem.CountReferences(ctx, rv.st.Space, d, uuid.Nil)
		require.Zero(t, n, "blob %x: %d claims after the delete", d, n)
	}
}

// TestExtents_CopyWritesItsOwnExtents: a copy into another bucket, so another
// space, claims each of its blobs in that space and reads back through
// extents its own commit wrote into that bucket's catalog.
func TestExtents_CopyWritesItsOwnExtents(t *testing.T) {
	// One log per bucket, as in production, so dropping the source bucket's
	// catalog drops every block only it holds.
	ctx := context.Background()
	mem := newSmallListStore(t, smallLists)
	logs, err := logstore.OpenManager(ctx, logstore.ManagerConfig{Dir: t.TempDir(), Meta: mem, Logger: zaptest.NewLogger(t)})
	require.NoError(t, err, "logstore")
	t.Cleanup(func() { _ = logs.Close(ctx) })
	b, mem, _ := newRefTestBackendOver(t, mem, logs, extentBlobSize)
	tenant := testutil.RandomDID(t)
	for _, name := range []string{"src", "dst"} {
		require.NoError(t, mem.Create(ctx, name, testutil.RandomDID(t), registry.CreateState{Tenant: tenant}), "create bucket %s", name)
	}
	data := testBody(20 * extentBlobSize) // a head of 8, extents of 8 and 4
	bucket, key := "src", "big"
	_, err = b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: byteLen(data)})
	require.NoError(t, err, "PutObject")
	dstBucket, dst, src := "dst", "copy", "src/big"
	_, err = b.CopyObject(ctx, s3response.CopyObjectInput{Bucket: &dstBucket, Key: &dst, CopySource: &src})
	require.NoError(t, err, "CopyObject")
	rv := assertExtentsIn(t, b, dstBucket, dst, 2)
	for _, d := range storedBlobs(t, b, rv).DigestList() {
		n, _ := mem.CountReferences(ctx, rv.st.Space, d, uuid.Nil)
		require.Equal(t, 1, n, "blob %x: %d claims in the copy's space", d, n)
	}
	// The source bucket's catalog goes, as DeleteBucket removes it: the copy
	// still reads back whole from its own.
	require.NoError(t, logs.RemoveBucketLog(ctx, bucket), "RemoveBucketLog")
	out, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &dstBucket, Key: &dst})
	require.NoError(t, err, "GetObject copy")
	got, err := io.ReadAll(out.Body)
	out.Body.Close()
	require.NoError(t, err, "copy reads back %d bytes, err %v", len(got), err)
	require.Equal(t, data, got, "copy reads back %d bytes, err %v", len(got), err)
}

// TestExtents_MultipartPartNumberAcrossExtents: a multipart object whose
// parts span several extents serves ?partNumber and ranges from them.
func TestExtents_MultipartPartNumberAcrossExtents(t *testing.T) {
	// A part before the last is at least 5 MiB, so the blobs are large and
	// the lists short: part 1 fills the head and four extents, and part 2
	// shares the fifth with part 1's last blob.
	b, _, _ := newRefTestBackendOn(t, newSmallListStore(t, msbucket.BlobListSizes{Head: 2, Extent: 2}), 512<<10)
	part1 := testBody(5 << 20)                           // 10 blobs
	part2 := bytes.Repeat([]byte("second part "), 30000) // 1 blob
	key := "mp"
	whole := completeAll(t, b, key, [][]byte{part1, part2}, []int32{1, 2})
	rv := assertExtents(t, b, key, 5)
	require.Equal(t, int64(len(whole)), rv.mf.Body.Size, "Size")

	bucket := "bk"
	for pn, want := range map[int32][]byte{1: part1, 2: part2} {
		out, err := b.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &bucket, Key: &key, PartNumber: &pn})
		require.NoError(t, err, "GetObject partNumber=%d", pn)
		got, err := io.ReadAll(out.Body)
		out.Body.Close()
		require.NoError(t, err, "read part %d", pn)
		require.Equal(t, want, got, "part %d: %d bytes, mismatch", pn, len(got))
	}
	got := getRange(t, b, key, "")
	require.Equal(t, whole, got, "full GET mismatch")
	assertObjectTree(t, b, key, whole)
}

// TestExtents_CompleteCapFollowsListSizes: a Complete refuses what the
// backend's blob lists cannot hold, whatever their sizes.
func TestExtents_CompleteCapFollowsListSizes(t *testing.T) {
	for _, sizes := range []msbucket.BlobListSizes{{}, smallLists} {
		b, _, _ := newRefTestBackendOn(t, newSmallListStore(t, sizes), extentBlobSize)
		got, want := storeFor(b).limits.MaxBodyBlobs, sizes.MaxBlobs()
		require.Equal(t, want, got, "lists of %+v: a Complete takes %d blobs, the lists hold %d", sizes, got, want)
	}
}

// TestExtents_CompleteRefusesTooManyBlobs: an upload whose parts hold more
// blobs than one manifest can list is refused before the session latches,
// so it stays open and its blobs unaccepted.
func TestExtents_CompleteRefusesTooManyBlobs(t *testing.T) {
	b, mem, _ := newRefTestBackend(t, extentBlobSize)
	storeFor(b).limits.MaxBodyBlobs = 10
	key := "mp"
	uploadID, completed, _ := uploadAll(t, b, key, [][]byte{testBody(11 * extentBlobSize)}, []int32{1})
	_, err := mpComplete(t, b, key, uploadID, completed, nil)
	got := apiErrCode(t, err)
	require.Equal(t, "EntityTooLarge", got, "Complete of 11 blobs over a cap of 10: %s (%v)", got, err)
	sess, err := mem.GetSession(context.Background(), uploadID)
	require.NoError(t, err, "GetSession")
	require.Equal(t, registry.SessionOpen, sess.State, "session %s after the refusal", sess.State)
}
