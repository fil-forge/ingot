package s3frontend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fil-forge/versitygw/s3response"

	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/testutil"
	"go.uber.org/zap/zaptest"
)

// extentBlobSize splits a body into blobs small enough that a test body of
// a megabyte or two passes the 1,024 blobs a manifest keeps inline.
const extentBlobSize = 1 << 10

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
	if err != nil {
		t.Fatalf("resolve %s: %v", key, err)
	}
	if got := len(rv.mf.Body.Blobs.Head); got != 1024 {
		t.Fatalf("%s: Head holds %d blobs, want 1024", key, got)
	}
	if got := len(rv.mf.Body.Blobs.Extents); got != extents {
		t.Fatalf("%s: %d extents, want %d", key, got, extents)
	}
	return rv
}

// TestExtents_PutObjectReadsBack: an object past the inline list reads back
// whole and in ranges inside the head, across the head/extent boundary, and
// inside an extent, decrypting through extent-by-extent prefetch.
func TestExtents_PutObjectReadsBack(t *testing.T) {
	b, _, _ := newRefTestBackend(t, extentBlobSize)
	data := testBody(1100*extentBlobSize + 300) // 1,101 blobs
	bucket, key := "bk", "big"
	if _, err := b.PutObject(context.Background(), s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data)}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	assertExtents(t, b, key, 1)

	if got := getRange(t, b, key, ""); !bytes.Equal(got, data) {
		t.Fatalf("full GET: %d bytes, mismatch", len(got))
	}
	boundary := int64(1024 * extentBlobSize)
	for _, r := range [][2]int64{
		{100, 5000},                      // inside the head
		{boundary - 100, boundary + 100}, // across the boundary
		{boundary + 10, boundary + 2000}, // inside the extent
		{int64(len(data)) - 1, int64(len(data)) - 1},
	} {
		got := getRange(t, b, key, fmt.Sprintf("bytes=%d-%d", r[0], r[1]))
		if !bytes.Equal(got, data[r[0]:r[1]+1]) {
			t.Fatalf("range %v: %d bytes, mismatch", r, len(got))
		}
	}
}

// TestExtents_DeleteReleasesEveryBlob: deleting an object drops the claim on
// every blob, including the ones listed only in its extent.
func TestExtents_DeleteReleasesEveryBlob(t *testing.T) {
	b, mem, _ := newRefTestBackend(t, extentBlobSize)
	ctx := context.Background()
	data := testBody(1100 * extentBlobSize)
	bucket, key := "bk", "big"
	if _, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data)}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	rv := assertExtents(t, b, key, 1)
	digests := storedBlobs(t, b, rv).DigestList()
	if len(digests) != 1100 {
		t.Fatalf("%d blobs, want 1100", len(digests))
	}
	for _, d := range digests {
		if n, _ := mem.CountClaims(ctx, rv.st.Space, d); n != 1 {
			t.Fatalf("blob %x: %d claims before the delete, want 1", d, n)
		}
	}
	deleteFrom(t, b, bucket, key)
	drainReleases(t, b)
	for _, d := range digests {
		if n, _ := mem.CountClaims(ctx, rv.st.Space, d); n != 0 {
			t.Fatalf("blob %x: %d claims after the delete, want 0", d, n)
		}
	}
}

// TestExtents_CopyWritesItsOwnExtents: a copy into another bucket of the
// same space claims every blob again and reads back through extents its own
// commit wrote into that bucket's catalog.
func TestExtents_CopyWritesItsOwnExtents(t *testing.T) {
	// One log per bucket, as in production, so dropping the source bucket's
	// catalog drops every block only it holds.
	ctx := context.Background()
	mem := inmem.NewMemStore()
	logs, err := logstore.OpenManager(ctx, logstore.ManagerConfig{Dir: t.TempDir(), Meta: mem, Logger: zaptest.NewLogger(t)})
	if err != nil {
		t.Fatalf("logstore: %v", err)
	}
	t.Cleanup(func() { _ = logs.Close(ctx) })
	b, mem, _ := newRefTestBackendOver(t, mem, logs, extentBlobSize)
	space, tenant := testutil.RandomDID(t), testutil.RandomDID(t)
	for _, name := range []string{"src", "dst"} {
		if err := mem.Create(ctx, name, space, registry.CreateState{Tenant: tenant}); err != nil {
			t.Fatalf("create bucket %s: %v", name, err)
		}
	}
	data := testBody(1100 * extentBlobSize)
	bucket, key := "src", "big"
	if _, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data)}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	dstBucket, dst, src := "dst", "copy", "src/big"
	if _, err := b.CopyObject(ctx, s3response.CopyObjectInput{Bucket: &dstBucket, Key: &dst, CopySource: &src}); err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	rv := assertExtentsIn(t, b, dstBucket, dst, 1)
	for _, d := range storedBlobs(t, b, rv).DigestList() {
		if n, _ := mem.CountClaims(ctx, rv.st.Space, d); n != 2 {
			t.Fatalf("blob %x: %d claims, want 2 (source and copy)", d, n)
		}
	}
	// The source bucket's catalog goes, as DeleteBucket removes it: the copy
	// still reads back whole from its own.
	if err := logs.RemoveBucketLog(ctx, bucket); err != nil {
		t.Fatalf("RemoveBucketLog: %v", err)
	}
	out, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &dstBucket, Key: &dst})
	if err != nil {
		t.Fatalf("GetObject copy: %v", err)
	}
	got, err := io.ReadAll(out.Body)
	out.Body.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("copy reads back %d bytes, err %v; want the source's %d", len(got), err, len(data))
	}
}

// TestExtents_MultipartPartNumberAcrossExtents: a multipart object whose
// parts span several extents serves ?partNumber and ranges from them.
func TestExtents_MultipartPartNumberAcrossExtents(t *testing.T) {
	b, _, _ := newRefTestBackend(t, extentBlobSize)
	part1 := testBody(5 << 20) // 5,120 blobs
	part2 := bytes.Repeat([]byte("second part "), 30000)
	key := "mp"
	whole := completeAll(t, b, key, [][]byte{part1, part2}, []int32{1, 2})
	rv := assertExtents(t, b, key, 5)
	if rv.mf.Body.Size != int64(len(whole)) {
		t.Fatalf("Size = %d, want %d", rv.mf.Body.Size, len(whole))
	}

	bucket := "bk"
	for pn, want := range map[int32][]byte{1: part1, 2: part2} {
		out, err := b.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &bucket, Key: &key, PartNumber: &pn})
		if err != nil {
			t.Fatalf("GetObject partNumber=%d: %v", pn, err)
		}
		got, err := io.ReadAll(out.Body)
		out.Body.Close()
		if err != nil {
			t.Fatalf("read part %d: %v", pn, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("part %d: %d bytes, mismatch", pn, len(got))
		}
	}
	if got := getRange(t, b, key, ""); !bytes.Equal(got, whole) {
		t.Fatal("full GET mismatch")
	}
	assertObjectTree(t, b, key, whole)
}

// TestExtents_CompleteRefusesTooManyBlobs: an upload whose parts hold more
// blobs than one manifest can list is refused before the session latches,
// so it stays open and its blobs unaccepted.
func TestExtents_CompleteRefusesTooManyBlobs(t *testing.T) {
	old := maxBodyBlobs
	maxBodyBlobs = 10
	t.Cleanup(func() { maxBodyBlobs = old })

	b, mem, _ := newRefTestBackend(t, extentBlobSize)
	key := "mp"
	uploadID, completed, _ := uploadAll(t, b, key, [][]byte{testBody(11 * extentBlobSize)}, []int32{1})
	_, err := mpComplete(t, b, key, uploadID, completed, nil)
	if got := apiErrCode(t, err); got != "EntityTooLarge" {
		t.Fatalf("Complete of 11 blobs over a cap of 10: %s (%v), want EntityTooLarge", got, err)
	}
	sess, err := mem.GetSession(context.Background(), uploadID)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if sess.State != registry.SessionOpen {
		t.Fatalf("session %s after the refusal, want open", sess.State)
	}
}
