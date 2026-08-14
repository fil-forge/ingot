package s3frontend

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
)

// Object-tagging tests over the same in-process harness, covering
// docs/s3-object-tagging.md: the gate-free check order, the empty-set
// answers, the merge with lock fields, creation-time stamping (PUT header,
// copy directives, multipart carry), the TagCount echo, and the
// discard-path cleanup tags make reachable on unversioned buckets.

func getTags(t *testing.T, b *Backend, key, versionID string) map[string]string {
	t.Helper()
	tags, err := b.GetObjectTagging(context.Background(), "bk", key, versionID)
	if err != nil {
		t.Fatalf("GetObjectTagging(%s, %q): %v", key, versionID, err)
	}
	return tags
}

func TestObjectTagging_RoundTripUnversioned(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	bucket := "bk"

	// Tagging has no bucket gate: an unversioned, lock-free bucket carries
	// tags. The untagged answer is the empty set, never a sentinel.
	putObjV(t, b, "k", []byte("v1"))
	if tags := getTags(t, b, "k", ""); len(tags) != 0 {
		t.Fatalf("untagged Get = %v, want empty set", tags)
	}
	want := map[string]string{"team": "forge", "env": "dev"}
	if err := b.PutObjectTagging(ctx, "bk", "k", "", want); err != nil {
		t.Fatalf("PutObjectTagging: %v", err)
	}
	if tags := getTags(t, b, "k", ""); !reflect.DeepEqual(tags, want) {
		t.Fatalf("tag round-trip = %v, want %v", tags, want)
	}
	// The first state write upgraded the manifest-arm key; reads still work
	// and Head echoes the count.
	if _, data, err := getObjV(t, b, "k", ""); err != nil || string(data) != "v1" {
		t.Fatalf("read after tag upgrade = (%q, %v), want v1", data, err)
	}
	head, err := b.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: strPtrOrNil("k")})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if head.TagCount == nil || *head.TagCount != 2 {
		t.Fatalf("TagCount = %v, want 2", head.TagCount)
	}

	// Replacement is last-write-wins; an empty replacement clears (elision),
	// and delete is idempotent.
	if err := b.PutObjectTagging(ctx, "bk", "k", "", map[string]string{"only": "one"}); err != nil {
		t.Fatalf("replace tags: %v", err)
	}
	if tags := getTags(t, b, "k", ""); len(tags) != 1 || tags["only"] != "one" {
		t.Fatalf("replaced tags = %v, want {only:one}", tags)
	}
	if err := b.PutObjectTagging(ctx, "bk", "k", "", map[string]string{}); err != nil {
		t.Fatalf("clear via empty put: %v", err)
	}
	if tags := getTags(t, b, "k", ""); len(tags) != 0 {
		t.Fatalf("tags after empty put = %v, want empty", tags)
	}
	rv, err := b.resolveVersion(ctx, "bk", "k", "")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if rv.leaf == nil || rv.leaf.State != nil {
		t.Fatalf("leaf after clearing last state = %+v, want nil State (elision)", rv.leaf)
	}
	if err := b.DeleteObjectTagging(ctx, "bk", "k", ""); err != nil {
		t.Fatalf("idempotent DeleteObjectTagging: %v", err)
	}
	head, err = b.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: strPtrOrNil("k")})
	if err != nil || head.TagCount != nil {
		t.Fatalf("TagCount after clear = (%v, %v), want nil", head.TagCount, err)
	}

	// Overwrite on the unversioned bucket discards the old null version and
	// prunes its tag entry in the same commit (the §9 cleanup rule's discard
	// path, live for tags).
	if err := b.PutObjectTagging(ctx, "bk", "k", "", want); err != nil {
		t.Fatalf("re-tag: %v", err)
	}
	putObjV(t, b, "k", []byte("v2"))
	if tags := getTags(t, b, "k", ""); len(tags) != 0 {
		t.Fatalf("tags after unversioned overwrite = %v, want empty (new version)", tags)
	}
	if rv, err = b.resolveVersion(ctx, "bk", "k", ""); err != nil || rv.leaf == nil || rv.leaf.State != nil {
		t.Fatalf("leaf state after discard = %+v (%v), want pruned", rv.leaf, err)
	}
}

func TestObjectTagging_SentinelsAndVersions(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	if _, err := b.GetObjectTagging(ctx, "nope", "k", ""); err == nil {
		t.Fatal("missing bucket: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchBucket)
	}
	if _, err := b.GetObjectTagging(ctx, "bk", "absent", ""); err == nil {
		t.Fatal("missing key: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchKey)
	}

	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	out1 := putObjV(t, b, "k", []byte("v1"))
	out2 := putObjV(t, b, "k", []byte("v2"))

	if err := b.PutObjectTagging(ctx, "bk", "k", "not-a-version", map[string]string{"a": "b"}); err == nil {
		t.Fatal("malformed versionId: want error")
	} else if code := apiErrCode(t, err); code != "InvalidArgument" {
		t.Fatalf("malformed versionId code = %s, want InvalidArgument", code)
	}
	if _, err := b.GetObjectTagging(ctx, "bk", "k", "01G65Z755AFWAKHE12NY0CQ9FH"); err == nil {
		t.Fatal("unknown version: want error")
	} else if code := apiErrCode(t, err); code != "NoSuchVersion" {
		t.Fatalf("unknown version code = %s, want NoSuchVersion", code)
	}

	// Version-scoped tagging: the noncurrent version tagged, the current
	// untouched, and vice versa.
	if err := b.PutObjectTagging(ctx, "bk", "k", out1.VersionID, map[string]string{"gen": "old"}); err != nil {
		t.Fatalf("tag noncurrent: %v", err)
	}
	if tags := getTags(t, b, "k", out1.VersionID); tags["gen"] != "old" {
		t.Fatalf("noncurrent tags = %v, want gen:old", tags)
	}
	if tags := getTags(t, b, "k", ""); len(tags) != 0 {
		t.Fatalf("current tags = %v, want empty", tags)
	}
	if tags := getTags(t, b, "k", out2.VersionID); len(tags) != 0 {
		t.Fatalf("current-by-id tags = %v, want empty", tags)
	}
	if err := b.DeleteObjectTagging(ctx, "bk", "k", out1.VersionID); err != nil {
		t.Fatalf("delete noncurrent tags: %v", err)
	}
	if tags := getTags(t, b, "k", out1.VersionID); len(tags) != 0 {
		t.Fatalf("noncurrent tags after delete = %v, want empty", tags)
	}

	// All three methods answer 405 on a delete marker.
	del, err := deleteObjV(t, b, "k", "")
	if err != nil {
		t.Fatalf("insert marker: %v", err)
	}
	markerID := *del.VersionId
	if _, err := b.GetObjectTagging(ctx, "bk", "k", markerID); err == nil {
		t.Fatal("marker get: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrMethodNotAllowed)
	}
	if err := b.PutObjectTagging(ctx, "bk", "k", markerID, map[string]string{"a": "b"}); err == nil {
		t.Fatal("marker put: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrMethodNotAllowed)
	}
	if err := b.DeleteObjectTagging(ctx, "bk", "k", markerID); err == nil {
		t.Fatal("marker delete: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrMethodNotAllowed)
	}
}

func TestObjectTagging_MergeWithLockFields(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	lockBucket(t, b)
	putObjV(t, b, "k", []byte("v1"))

	ret := retentionDoc(t, types.ObjectLockRetentionModeGovernance, time.Now().Add(time.Hour))
	if err := b.PutObjectRetention(ctx, "bk", "k", "", ret); err != nil {
		t.Fatalf("PutObjectRetention: %v", err)
	}
	if err := b.PutObjectTagging(ctx, "bk", "k", "", map[string]string{"a": "b"}); err != nil {
		t.Fatalf("PutObjectTagging: %v", err)
	}
	// Each operation owns its field and carries the rest.
	if got, err := b.GetObjectRetention(ctx, "bk", "k", ""); err != nil || !bytes.Equal(got, ret) {
		t.Fatalf("retention after tag write = (%s, %v), want carried", got, err)
	}
	if tags := getTags(t, b, "k", ""); tags["a"] != "b" {
		t.Fatalf("tags = %v, want a:b", tags)
	}
	if err := b.DeleteObjectTagging(ctx, "bk", "k", ""); err != nil {
		t.Fatalf("DeleteObjectTagging: %v", err)
	}
	if got, err := b.GetObjectRetention(ctx, "bk", "k", ""); err != nil || !bytes.Equal(got, ret) {
		t.Fatalf("retention after tag delete = (%s, %v), want carried", got, err)
	}
	// The block is not elided while a lock field remains.
	if rv, err := b.resolveVersion(ctx, "bk", "k", ""); err != nil || rv.leaf == nil || rv.leaf.State == nil {
		t.Fatalf("state after tag delete = %+v (%v), want retention block kept", rv, err)
	}
}

func TestObjectTagging_CreationTimeStamping(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	bucket := "bk"

	// PUT with x-amz-tagging: parsed before ingest, stamped in the commit.
	key, tagging := "tagged", "team=forge&env=dev"
	if _, err := b.PutObject(ctx, s3response.PutObjectInput{
		Bucket: &bucket, Key: &key, Body: bytes.NewReader([]byte("x")), Tagging: &tagging,
	}); err != nil {
		t.Fatalf("PutObject with tagging: %v", err)
	}
	if tags := getTags(t, b, key, ""); tags["team"] != "forge" || tags["env"] != "dev" {
		t.Fatalf("stamped tags = %v, want header set", tags)
	}
	// An invalid header fails the PUT and writes nothing.
	badKey, badTagging := "bad", "k="+string(bytes.Repeat([]byte("v"), 300))
	if _, err := b.PutObject(ctx, s3response.PutObjectInput{
		Bucket: &bucket, Key: &badKey, Body: bytes.NewReader([]byte("x")), Tagging: &badTagging,
	}); err == nil {
		t.Fatal("oversize tag value: want error")
	}
	if _, err := b.GetObjectTagging(ctx, "bk", badKey, ""); err == nil {
		t.Fatal("failed PUT wrote the key")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchKey)
	}

	// Copy directives: COPY inherits the source version's tags, REPLACE takes
	// the request header (or nothing when it carries none).
	src := bucket + "/" + key
	dstCopy := "copied"
	if _, err := b.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket: &bucket, Key: &dstCopy, CopySource: &src,
		TaggingDirective: types.TaggingDirectiveCopy,
	}); err != nil {
		t.Fatalf("CopyObject COPY: %v", err)
	}
	if tags := getTags(t, b, dstCopy, ""); tags["team"] != "forge" || len(tags) != 2 {
		t.Fatalf("COPY-directive tags = %v, want source set", tags)
	}
	dstReplace, replTagging := "replaced", "fresh=yes"
	if _, err := b.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket: &bucket, Key: &dstReplace, CopySource: &src,
		TaggingDirective: types.TaggingDirectiveReplace, Tagging: &replTagging,
	}); err != nil {
		t.Fatalf("CopyObject REPLACE: %v", err)
	}
	if tags := getTags(t, b, dstReplace, ""); len(tags) != 1 || tags["fresh"] != "yes" {
		t.Fatalf("REPLACE-directive tags = %v, want {fresh:yes}", tags)
	}
	dstBare := "replaced-bare"
	if _, err := b.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket: &bucket, Key: &dstBare, CopySource: &src,
		TaggingDirective: types.TaggingDirectiveReplace,
	}); err != nil {
		t.Fatalf("CopyObject REPLACE bare: %v", err)
	}
	if tags := getTags(t, b, dstBare, ""); len(tags) != 0 {
		t.Fatalf("bare-REPLACE tags = %v, want empty", tags)
	}

	// Multipart: the header carried on the session stamps at Complete.
	mpKey, mpTagging := "mp-tagged", "stage=upload"
	res, err := b.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket, Key: &mpKey, Tagging: &mpTagging,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	part, err := mpUploadPart(t, b, mpKey, res.UploadId, 1, []byte("part-1-bytes"), nil)
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	one := int32(1)
	if _, err := mpComplete(t, b, mpKey, res.UploadId, []types.CompletedPart{{ETag: part.ETag, PartNumber: &one}}, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if tags := getTags(t, b, mpKey, ""); len(tags) != 1 || tags["stage"] != "upload" {
		t.Fatalf("MPU-stamped tags = %v, want {stage:upload}", tags)
	}
	// An invalid header fails the create.
	badMp := "mp-bad"
	if _, err := b.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket, Key: &badMp, Tagging: &badTagging,
	}); err == nil {
		t.Fatal("MPU with oversize tag value: want error")
	}
}
