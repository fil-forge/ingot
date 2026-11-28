package s3frontend

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/stretchr/testify/require"
)

// Object-tagging tests over the same in-process harness, covering
// docs/s3-object-tagging.md: the gate-free check order, the empty-set
// answers, the merge with lock fields, creation-time stamping (PUT header,
// copy directives, multipart carry), the TagCount echo, and the
// discard-path cleanup tags make reachable on unversioned buckets.

func getTags(t *testing.T, b *Backend, key, versionID string) map[string]string {
	t.Helper()
	tags, err := b.GetObjectTagging(context.Background(), "bk", key, versionID)
	require.NoError(t, err, "GetObjectTagging(%s, %q)", key, versionID)
	return tags
}

func TestObjectTagging_RoundTripUnversioned(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	bucket := "bk"

	// Tagging has no bucket gate: an unversioned, lock-free bucket carries
	// tags. The untagged answer is the empty set, never a sentinel.
	putObjV(t, b, "k", []byte("v1"))
	tags := getTags(t, b, "k", "")
	require.Empty(t, tags, "untagged Get")
	want := map[string]string{"team": "forge", "env": "dev"}
	require.NoError(t, b.PutObjectTagging(ctx, "bk", "k", "", want), "PutObjectTagging")
	require.Equal(t, want, getTags(t, b, "k", ""), "tag round-trip")
	// The first state write upgraded the manifest-arm key; reads still work
	// and Head echoes the count.
	_, data, err := getObjV(t, b, "k", "")
	require.NoError(t, err, "read after tag upgrade")
	require.Equal(t, "v1", string(data), "read after tag upgrade")
	head, err := b.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: strPtrOrNil("k")})
	require.NoError(t, err, "HeadObject")
	require.NotNil(t, head.TagCount, "TagCount")
	require.Equal(t, int32(2), *head.TagCount, "TagCount")

	// Replacement is last-write-wins; an empty replacement clears (elision),
	// and delete is idempotent.
	require.NoError(t, b.PutObjectTagging(ctx, "bk", "k", "", map[string]string{"only": "one"}), "replace tags")
	require.Equal(t, map[string]string{"only": "one"}, getTags(t, b, "k", ""), "replaced tags")
	require.NoError(t, b.PutObjectTagging(ctx, "bk", "k", "", map[string]string{}), "clear via empty put")
	require.Empty(t, getTags(t, b, "k", ""), "tags after empty put")
	rv, err := b.resolveVersion(ctx, "bk", "k", "")
	require.NoError(t, err, "resolve")
	require.NotNil(t, rv.leaf, "leaf after clearing last state")
	require.Nil(t, rv.leaf.State, "leaf after clearing last state")
	require.NoError(t, b.DeleteObjectTagging(ctx, "bk", "k", ""), "idempotent DeleteObjectTagging")
	head, err = b.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: strPtrOrNil("k")})
	require.NoError(t, err, "HeadObject after clear")
	require.Nil(t, head.TagCount, "TagCount after clear")

	// Overwrite on the unversioned bucket discards the old null version and
	// prunes its tag entry in the same commit (the §9 cleanup rule's discard
	// path, live for tags).
	require.NoError(t, b.PutObjectTagging(ctx, "bk", "k", "", want), "re-tag")
	putObjV(t, b, "k", []byte("v2"))
	require.Empty(t, getTags(t, b, "k", ""), "tags after unversioned overwrite (new version)")
	rv, err = b.resolveVersion(ctx, "bk", "k", "")
	require.NoError(t, err, "resolve after discard")
	require.NotNil(t, rv.leaf, "leaf after discard")
	require.Nil(t, rv.leaf.State, "leaf state after discard")
}

func TestObjectTagging_SentinelsAndVersions(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	_, err := b.GetObjectTagging(ctx, "nope", "k", "")
	require.Error(t, err, "missing bucket")
	wantAPIErr(t, err, s3err.ErrNoSuchBucket)
	_, err = b.GetObjectTagging(ctx, "bk", "absent", "")
	require.Error(t, err, "missing key")
	wantAPIErr(t, err, s3err.ErrNoSuchKey)

	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	out1 := putObjV(t, b, "k", []byte("v1"))
	out2 := putObjV(t, b, "k", []byte("v2"))

	err = b.PutObjectTagging(ctx, "bk", "k", "not-a-version", map[string]string{"a": "b"})
	require.Error(t, err, "malformed versionId")
	require.Equal(t, "InvalidArgument", apiErrCode(t, err), "malformed versionId code")
	_, err = b.GetObjectTagging(ctx, "bk", "k", "01G65Z755AFWAKHE12NY0CQ9FH")
	require.Error(t, err, "unknown version")
	require.Equal(t, "NoSuchVersion", apiErrCode(t, err), "unknown version code")

	// Version-scoped tagging: the noncurrent version tagged, the current
	// untouched, and vice versa.
	require.NoError(t, b.PutObjectTagging(ctx, "bk", "k", out1.VersionID, map[string]string{"gen": "old"}), "tag noncurrent")
	tags := getTags(t, b, "k", out1.VersionID)
	require.Equal(t, "old", tags["gen"], "noncurrent tags")
	require.Empty(t, getTags(t, b, "k", ""), "current tags")
	require.Empty(t, getTags(t, b, "k", out2.VersionID), "current-by-id tags")
	require.NoError(t, b.DeleteObjectTagging(ctx, "bk", "k", out1.VersionID), "delete noncurrent tags")
	require.Empty(t, getTags(t, b, "k", out1.VersionID), "noncurrent tags after delete")

	// All three methods answer 405 on a delete marker.
	del, err := deleteObjV(t, b, "k", "")
	require.NoError(t, err, "insert marker")
	markerID := *del.VersionId
	_, err = b.GetObjectTagging(ctx, "bk", "k", markerID)
	require.Error(t, err, "marker get")
	wantAPIErr(t, err, s3err.ErrMethodNotAllowed)
	err = b.PutObjectTagging(ctx, "bk", "k", markerID, map[string]string{"a": "b"})
	require.Error(t, err, "marker put")
	wantAPIErr(t, err, s3err.ErrMethodNotAllowed)
	err = b.DeleteObjectTagging(ctx, "bk", "k", markerID)
	require.Error(t, err, "marker delete")
	wantAPIErr(t, err, s3err.ErrMethodNotAllowed)
}

func TestObjectTagging_MergeWithLockFields(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	lockBucket(t, b)
	putObjV(t, b, "k", []byte("v1"))

	ret := retentionDoc(t, types.ObjectLockRetentionModeGovernance, time.Now().Add(time.Hour))
	require.NoError(t, b.PutObjectRetention(ctx, "bk", "k", "", ret), "PutObjectRetention")
	require.NoError(t, b.PutObjectTagging(ctx, "bk", "k", "", map[string]string{"a": "b"}), "PutObjectTagging")
	// Each operation owns its field and carries the rest.
	got, err := b.GetObjectRetention(ctx, "bk", "k", "")
	require.NoError(t, err, "retention after tag write")
	require.Equal(t, ret, got, "retention after tag write")
	tags := getTags(t, b, "k", "")
	require.Equal(t, "b", tags["a"], "tags")
	require.NoError(t, b.DeleteObjectTagging(ctx, "bk", "k", ""), "DeleteObjectTagging")
	got, err = b.GetObjectRetention(ctx, "bk", "k", "")
	require.NoError(t, err, "retention after tag delete")
	require.Equal(t, ret, got, "retention after tag delete")
	// The block is not elided while a lock field remains.
	rv, err := b.resolveVersion(ctx, "bk", "k", "")
	require.NoError(t, err, "resolve after tag delete")
	require.NotNil(t, rv.leaf, "leaf after tag delete")
	require.NotNil(t, rv.leaf.State, "state after tag delete")
}

func TestObjectTagging_CreationTimeStamping(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	bucket := "bk"

	// PUT with x-amz-tagging: parsed before ingest, stamped in the commit.
	key, tagging := "tagged", "team=forge&env=dev"
	_, err := b.PutObject(ctx, s3response.PutObjectInput{
		Bucket: &bucket, Key: &key, Body: bytes.NewReader([]byte("x")), ContentLength: byteLen([]byte("x")), Tagging: &tagging,
	})
	require.NoError(t, err, "PutObject with tagging")
	tags := getTags(t, b, key, "")
	require.Equal(t, "forge", tags["team"], "stamped tags")
	require.Equal(t, "dev", tags["env"], "stamped tags")
	// An invalid header fails the PUT and writes nothing.
	badKey, badTagging := "bad", "k="+string(bytes.Repeat([]byte("v"), 300))
	_, err = b.PutObject(ctx, s3response.PutObjectInput{
		Bucket: &bucket, Key: &badKey, Body: bytes.NewReader([]byte("x")), ContentLength: byteLen([]byte("x")), Tagging: &badTagging,
	})
	require.Error(t, err, "oversize tag value")
	_, err = b.GetObjectTagging(ctx, "bk", badKey, "")
	require.Error(t, err, "failed PUT wrote the key")
	wantAPIErr(t, err, s3err.ErrNoSuchKey)

	// Copy directives: COPY inherits the source version's tags, REPLACE takes
	// the request header (or nothing when it carries none).
	src := bucket + "/" + key
	dstCopy := "copied"
	_, err = b.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket: &bucket, Key: &dstCopy, CopySource: &src,
		TaggingDirective: types.TaggingDirectiveCopy,
	})
	require.NoError(t, err, "CopyObject COPY")
	tags = getTags(t, b, dstCopy, "")
	require.Equal(t, "forge", tags["team"], "COPY-directive tags")
	require.Len(t, tags, 2, "COPY-directive tags")
	dstReplace, replTagging := "replaced", "fresh=yes"
	_, err = b.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket: &bucket, Key: &dstReplace, CopySource: &src,
		TaggingDirective: types.TaggingDirectiveReplace, Tagging: &replTagging,
	})
	require.NoError(t, err, "CopyObject REPLACE")
	require.Equal(t, map[string]string{"fresh": "yes"}, getTags(t, b, dstReplace, ""), "REPLACE-directive tags")
	dstBare := "replaced-bare"
	_, err = b.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket: &bucket, Key: &dstBare, CopySource: &src,
		TaggingDirective: types.TaggingDirectiveReplace,
	})
	require.NoError(t, err, "CopyObject REPLACE bare")
	require.Empty(t, getTags(t, b, dstBare, ""), "bare-REPLACE tags")

	// Multipart: the header carried on the session stamps at Complete.
	mpKey, mpTagging := "mp-tagged", "stage=upload"
	res, err := b.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket, Key: &mpKey, Tagging: &mpTagging,
	})
	require.NoError(t, err, "CreateMultipartUpload")
	part, err := mpUploadPart(t, b, mpKey, res.UploadId, 1, []byte("part-1-bytes"), nil)
	require.NoError(t, err, "UploadPart")
	one := int32(1)
	_, err = mpComplete(t, b, mpKey, res.UploadId, []types.CompletedPart{{ETag: part.ETag, PartNumber: &one}}, nil)
	require.NoError(t, err, "Complete")
	require.Equal(t, map[string]string{"stage": "upload"}, getTags(t, b, mpKey, ""), "MPU-stamped tags")
	// An invalid header fails the create.
	badMp := "mp-bad"
	_, err = b.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket, Key: &badMp, Tagging: &badTagging,
	})
	require.Error(t, err, "MPU with oversize tag value")
}
