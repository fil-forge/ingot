//go:build itest

package itest

import (
	"bytes"
	"encoding/hex"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fil-forge/libforge/digestutil"
	"github.com/multiformats/go-multihash"
)

// TestForgeDeleteBucketReleases is the regression gate for DeleteBucket's
// release legs, which sign with the request's own proofs: hilt delegates
// blob.Abort and blob.Remove with s3:DeleteBucket (fil-forge/hilt#78), and
// ingot invokes both from the deleting request. The delete arrives with two
// kinds of blob still registered in the bucket's space, each taking a
// different leg:
//
//	a parked part     — an open multipart upload's blob, never accepted:
//	                    /blob/abort, which piri serves as /blob/reject
//	a pending release — a deleted object's accepted blob, its release still
//	                    behind the reader grace: /blob/remove → /blob/release
//
// Both must traverse the network before hilt will delete the space, so a
// regression in the grants or in proof propagation fails the DeleteBucket
// itself. The conformance DeleteBucket cases cannot catch it: their buckets
// are empty or non-empty, never holding registrations.
//
// The delete is signed by a second access key that has never written, and
// that is what makes this a gate. A proof store belongs to one access key
// and keeps what earlier requests deposited in it, so a writer's key already
// holds blob.Abort and blob.Remove chains from its own s3:PutObject
// delegation — it would release these blobs whatever hilt grants
// s3:DeleteBucket. A key that has only ever deleted has nothing to fall back
// on.
func TestForgeDeleteBucketReleases(t *testing.T) {
	ctx := t.Context()
	s, endpoint := forgeStack(t)
	const tenant = "delbucketrel"
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, tenant)
	cl := sdkClient(forgeS3Conf(endpoint, accessKey, secretKey))

	// s3:ListBucket carries only /content/retrieve, which the emptiness walk
	// may need; no permission in this set delegates a blob command except
	// s3:DeleteBucket itself.
	delKey, delSecret := hiltCreateAccessKey(t, ctx, s, tenant, "deleter",
		[]string{"s3:DeleteBucket", "s3:ListBucket"}, nil)
	delCl := sdkClient(forgeS3Conf(endpoint, delKey, delSecret))

	const bucket, mpKey, objKey = "delete-bucket-releases", "in-flight", "obj"

	if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// The parked part, set up first because it is the slower half: an open
	// multipart upload whose part is durable on piri but not accepted.
	create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(mpKey),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if _, err := cl.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(bucket), Key: aws.String(mpKey), UploadId: create.UploadId,
		PartNumber: aws.Int32(1), Body: bytes.NewReader(tagged(patternBytes(512<<10), 0x55)),
	}); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	// The part rows are the only index to the blob and the teardown destroys
	// them, so read the digest while the session is still open.
	partDigest := partBlobDigests(t, ctx, s, aws.ToString(create.UploadId), 1)[0]
	waitForPiriLogLine(t, ctx, s, 30*time.Second, "/blob/allocate", partDigest)
	if piriLogHasLine(t, ctx, s, "/blob/accept", partDigest) {
		t.Fatalf("part blob %s was accepted while its upload is open — parking is broken", partDigest)
	}

	// The pending release, set up last: an object put and deleted, whose
	// blob release waits in the deferred queue behind the reader grace (60s
	// by default). Nothing slow runs between the delete and DeleteBucket, so
	// the record is still queued when the drain reaches it — asserted below
	// rather than assumed, since a release the sweeper already ran would
	// leave the drain leg untested.
	if _, err := cl.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(objKey),
		Body: bytes.NewReader(tagged(patternBytes(512<<10), 0x66)),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	objDigests := objectBlobDigestsHex(t, ctx, s, bucket, objKey)
	if _, err := cl.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(objKey),
	}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if n := countRowsForDigests(t, ctx, s, "ingot.blob_release_intents", "digest", objDigests); n != len(objDigests) {
		t.Fatalf("queued release records = %d, want %d: the grace elapsed before the delete, leaving the drain leg untested", n, len(objDigests))
	}

	if _, err := delCl.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}

	// The parked part was abandoned on piri, and never accepted.
	waitForPiriLogLine(t, ctx, s, 60*time.Second, "/blob/reject", partDigest)
	if piriLogHasLine(t, ctx, s, "/blob/accept", partDigest) {
		t.Fatalf("parked part blob %s was accepted by the teardown — abort/accept exclusivity is broken", partDigest)
	}

	// The deleted object's blob was released, record and all.
	for _, d := range objDigests {
		waitForPiriLogLine(t, ctx, s, 60*time.Second, "/blob/release", digestB58(t, d))
	}
	if n := countRowsForDigests(t, ctx, s, "ingot.blob_release_intents", "digest", objDigests); n != 0 {
		t.Fatalf("release records left after DeleteBucket = %d, want 0", n)
	}

	if _, err := delCl.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err == nil {
		t.Fatalf("HeadBucket after DeleteBucket succeeded, want NoSuchBucket")
	}
	t.Logf("delete bucket released a parked part (%s) and a queued object blob on the deleting key's own proofs", partDigest)
}

// digestB58 renders a hex-encoded stored digest the way piri's handlers log
// it, so a log match can name one exact blob.
func digestB58(t *testing.T, hexDigest string) string {
	t.Helper()
	raw, err := hex.DecodeString(hexDigest)
	if err != nil {
		t.Fatalf("decode digest hex %q: %v", hexDigest, err)
	}
	return digestutil.Format(multihash.Multihash(raw))
}
