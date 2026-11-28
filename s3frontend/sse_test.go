package s3frontend

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	cmds3 "github.com/fil-forge/libforge/commands/s3"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/registry"
)

// These tests cover the S3 face of bucket encryption (sse.go): the
// bucket-encryption API and its deployment gate, the request headers of the
// object operations, and the plaintext storage mode a "none" bucket selects —
// the inverse of the opacity criterion encrypt_test.go pins: what lands in
// storage IS the plaintext, under its own digest, with no key material.

// sseConfig is a one-rule ServerSideEncryptionConfiguration naming algo.
func sseConfig(algo types.ServerSideEncryption) s3response.ServerSideEncryptionConfiguration {
	return s3response.ServerSideEncryptionConfiguration{
		Rules: []s3response.ServerSideEncryptionRule{{
			ApplyServerSideEncryptionByDefault: &s3response.ServerSideEncryptionByDefault{SSEAlgorithm: algo},
		}},
	}
}

// wantInvalidArgument fails unless err is S3's 400 InvalidArgument.
func wantInvalidArgument(t *testing.T, err error) {
	t.Helper()
	var apiErr s3err.APIError
	require.ErrorAs(t, err, &apiErr, "err = %v, want 400 InvalidArgument", err)
	require.Equal(t, "InvalidArgument", apiErr.Code, "err")
	require.Equal(t, http.StatusBadRequest, apiErr.HTTPStatusCode, "err")
}

// ctxWithHeaders puts a request carrying headers into the context, as the
// front-end does for every request.
func ctxWithHeaders(headers map[string]string) context.Context {
	return context.WithValue(context.Background(), reqscope.RequestKey(), cmds3.Request{Headers: headers})
}

// aes256Ctx is a request asking for SSE-S3 explicitly.
func aes256Ctx() context.Context {
	return ctxWithHeaders(map[string]string{"X-Amz-Server-Side-Encryption": "AES256"})
}

// setNone makes "bk" a bucket whose objects are stored as received, on a
// backend that allows it.
func setNone(t *testing.T, b *Backend) {
	t.Helper()
	b.allowNoneEncryption = true
	require.NoError(t, b.PutBucketEncryption(context.Background(), "bk", sseConfig(sseNone)), "PutBucketEncryption none")
}

func putWithCtx(t *testing.T, ctx context.Context, b *Backend, key string, data []byte) s3response.PutObjectOutput {
	t.Helper()
	bucket := "bk"
	n := int64(len(data))
	out, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: &n})
	require.NoError(t, err, "PutObject %s", key)
	return out
}

func headObj(t *testing.T, b *Backend, key string) *s3.HeadObjectOutput {
	t.Helper()
	bucket := "bk"
	out, err := b.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	require.NoError(t, err, "HeadObject %s", key)
	return out
}

// assertStoredAsReceived checks that key's single blob is the plaintext
// itself: named by hash(plaintext), byte-identical on disk, with no
// blob_encryption_params row, and reported with no encryption header.
func assertStoredAsReceived(t *testing.T, b *Backend, key string, data []byte) {
	t.Helper()
	d := blobDigestOf(t, b, key, "")
	require.True(t, bytes.Equal(d, digestOf(t, data)), "%s: stored digest is not hash(plaintext); the blob was transformed", key)
	stored, err := os.ReadFile(localPath(b, d))
	require.NoError(t, err, "read stored blob")
	require.Equal(t, data, stored, "%s: stored %d bytes differ from the %d plaintext bytes", key, len(stored), len(data))
	_, err = b.encParams.GetEncryptionParams(context.Background(), b.reg.(*testStore).bk(), d)
	require.ErrorIs(t, err, registry.ErrNotFound, "%s: encryption params lookup, want ErrNotFound (no key material for a plaintext blob)", key)
	sse := headObj(t, b, key).ServerSideEncryption
	require.Empty(t, sse, "%s: HEAD reports encryption %q", key, sse)
}

// assertStoredEncrypted checks that key's single blob is an envelope with a
// params row, reported as AES256.
func assertStoredEncrypted(t *testing.T, b *Backend, key string, data []byte) {
	t.Helper()
	d := blobDigestOf(t, b, key, "")
	require.False(t, bytes.Equal(d, digestOf(t, data)), "%s: stored digest equals hash(plaintext); the blob was not encrypted", key)
	_, err := b.encParams.GetEncryptionParams(context.Background(), b.reg.(*testStore).bk(), d)
	require.NoError(t, err, "%s: GetEncryptionParams", key)
	sse := headObj(t, b, key).ServerSideEncryption
	require.Equal(t, types.ServerSideEncryptionAes256, sse, "%s: HEAD reports encryption %q", key, sse)
}

// TestBucketEncryption_Configuration: the bucket-encryption API over the
// registry. An unconfigured bucket answers not found while encrypting all
// the same; AES256 round-trips; "none" is refused until the deployment
// allows it; aws:kms is not implemented; a KMS key beside AES256 and an
// unknown algorithm are InvalidArgument; Delete clears and is idempotent.
func TestBucketEncryption_Configuration(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	_, err := b.GetBucketEncryption(ctx, "bk")
	require.Error(t, err, "unconfigured GetBucketEncryption: want error")
	wantAPIErr(t, err, s3err.ErrServerSideEncryptionConfigurationNotFound)
	_, err = b.GetBucketEncryption(ctx, "nope")
	require.Error(t, err, "missing bucket: want error")
	wantAPIErr(t, err, s3err.ErrNoSuchBucket)
	err = b.PutBucketEncryption(ctx, "nope", sseConfig(types.ServerSideEncryptionAes256))
	require.Error(t, err, "put on missing bucket: want error")
	wantAPIErr(t, err, s3err.ErrNoSuchBucket)
	err = b.DeleteBucketEncryption(ctx, "nope")
	require.Error(t, err, "delete on missing bucket: want error")
	wantAPIErr(t, err, s3err.ErrNoSuchBucket)

	// The unconfigured bucket encrypts.
	putObj(t, b, "default", []byte("stored encrypted by default"))
	assertStoredEncrypted(t, b, "default", []byte("stored encrypted by default"))

	require.NoError(t, b.PutBucketEncryption(ctx, "bk", sseConfig(types.ServerSideEncryptionAes256)), "PutBucketEncryption AES256")
	got, err := b.GetBucketEncryption(ctx, "bk")
	require.NoError(t, err, "GetBucketEncryption")
	require.Len(t, got.Rules, 1, "GetBucketEncryption")
	require.NotNil(t, got.Rules[0].ApplyServerSideEncryptionByDefault, "GetBucketEncryption")
	require.Equal(t, types.ServerSideEncryptionAes256, got.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm, "GetBucketEncryption")

	// Refused algorithms, each leaving the stored AES256 untouched.
	wantInvalidArgument(t, b.PutBucketEncryption(ctx, "bk", sseConfig(sseNone)))
	err = b.PutBucketEncryption(ctx, "bk", sseConfig(types.ServerSideEncryptionAwsKms))
	require.Error(t, err, "aws:kms: want error")
	wantAPIErr(t, err, s3err.ErrNotImplemented)
	wantInvalidArgument(t, b.PutBucketEncryption(ctx, "bk", sseConfig("ROT13")))
	withKey := sseConfig(types.ServerSideEncryptionAes256)
	keyID := "fool-me-once"
	withKey.Rules[0].ApplyServerSideEncryptionByDefault.KMSMasterKeyID = &keyID
	wantInvalidArgument(t, b.PutBucketEncryption(ctx, "bk", withKey))
	err = b.PutBucketEncryption(ctx, "bk", s3response.ServerSideEncryptionConfiguration{})
	require.Error(t, err, "no rules: want error")
	wantAPIErr(t, err, s3err.ErrMalformedXML)
	st, err := b.reg.Get(ctx, "bk")
	require.NoError(t, err, "stored encryption after refused puts")
	require.Equal(t, registry.BucketEncryptionAES256, st.Encryption, "stored encryption after refused puts")

	// A KMS key beside "none" is refused like one beside AES256, allowed or
	// not, and refused before the deployment gate is consulted.
	noneWithKey := sseConfig(sseNone)
	noneWithKey.Rules[0].ApplyServerSideEncryptionByDefault.KMSMasterKeyID = &keyID
	wantInvalidArgument(t, b.PutBucketEncryption(ctx, "bk", noneWithKey))
	// Allowed, "none" stores and reads back.
	b.allowNoneEncryption = true
	wantInvalidArgument(t, b.PutBucketEncryption(ctx, "bk", noneWithKey))
	require.NoError(t, b.PutBucketEncryption(ctx, "bk", sseConfig(sseNone)), "PutBucketEncryption none")
	got, err = b.GetBucketEncryption(ctx, "bk")
	require.NoError(t, err, "GetBucketEncryption after none")
	require.Equal(t, sseNone, got.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm, "GetBucketEncryption after none")

	// Delete clears, and is idempotent.
	for range 2 {
		require.NoError(t, b.DeleteBucketEncryption(ctx, "bk"), "DeleteBucketEncryption")
		_, err = b.GetBucketEncryption(ctx, "bk")
		require.Error(t, err, "GetBucketEncryption after delete: want not found")
		wantAPIErr(t, err, s3err.ErrServerSideEncryptionConfigurationNotFound)
	}
	// And a deleted configuration encrypts again.
	putObj(t, b, "after-delete", []byte("encrypted once more"))
	assertStoredEncrypted(t, b, "after-delete", []byte("encrypted once more"))
}

// TestRequestedEncryption: the write-side header parse, one case per outcome
// the S3 conformance vectors pin.
func TestRequestedEncryption(t *testing.T) {
	const (
		ok             = ""
		invalid        = "InvalidArgument"
		notImplemented = "NotImplemented"
	)
	sseC := map[string]string{
		"X-Amz-Server-Side-Encryption-Customer-Algorithm": "AES256",
		"X-Amz-Server-Side-Encryption-Customer-Key":       "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
		"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   "DWygnHRtgiJ77HCm+1rvHw==",
	}
	with := func(base map[string]string, extra map[string]string) map[string]string {
		m := map[string]string{}
		for k, v := range base {
			m[k] = v
		}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}
	cases := []struct {
		name    string
		headers map[string]string
		want    types.ServerSideEncryption
		code    string
	}{
		{"no sse headers", map[string]string{"Content-Type": "text/plain"}, "", ok},
		{"AES256", map[string]string{"X-Amz-Server-Side-Encryption": "AES256"}, types.ServerSideEncryptionAes256, ok},
		{"AES256, lower-case header", map[string]string{"x-amz-server-side-encryption": "AES256"}, types.ServerSideEncryptionAes256, ok},
		{"bucket-key-enabled alone is ignored", map[string]string{"X-Amz-Server-Side-Encryption-Bucket-Key-Enabled": "true"}, "", ok},
		{"present but empty", map[string]string{"X-Amz-Server-Side-Encryption": ""}, "", invalid},
		{"unknown algorithm", map[string]string{"X-Amz-Server-Side-Encryption": "aes:kms"}, "", invalid},
		{"INVALID algorithm", map[string]string{"X-Amz-Server-Side-Encryption": "INVALID"}, "", invalid},
		{"AES256 with a KMS key id", map[string]string{"X-Amz-Server-Side-Encryption": "AES256", "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": "fool-me-once"}, "", invalid},
		{"KMS key id without aws:kms", map[string]string{"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": "testkey-2"}, "", invalid},
		{"KMS context without aws:kms", map[string]string{"X-Amz-Server-Side-Encryption-Context": "e30="}, "", invalid},
		{"aws:kms", map[string]string{"X-Amz-Server-Side-Encryption": "aws:kms", "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": "k"}, "", notImplemented},
		{"aws:kms:dsse", map[string]string{"X-Amz-Server-Side-Encryption": "aws:kms:dsse"}, "", notImplemented},
		{"SSE-C alone", sseC, "", notImplemented},
		{"SSE-C beside AES256", with(sseC, map[string]string{"X-Amz-Server-Side-Encryption": "AES256"}), "", invalid},
		{"SSE-C beside aws:kms", with(sseC, map[string]string{"X-Amz-Server-Side-Encryption": "aws:kms", "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": "fool-me-once"}), "", invalid},
		{"copy-source SSE-C", map[string]string{"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": "AES256"}, "", notImplemented},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := requestedEncryption(ctxWithHeaders(c.headers))
			code := ""
			if err != nil {
				var apiErr s3err.APIError
				require.ErrorAs(t, err, &apiErr, "err = %v, want an APIError", err)
				code = apiErr.Code
			}
			require.Equal(t, c.code, code, "requestedEncryption = (%q, %q)", got, code)
			require.Equal(t, c.want, got, "requestedEncryption = (%q, %q)", got, code)
		})
	}
	// Without a request in the context (a background caller) nothing is asked.
	got, err := requestedEncryption(context.Background())
	require.NoError(t, err, "no request: (%q, %v)", got, err)
	require.Empty(t, got, "no request: (%q, %v)", got, err)

	// Reads take no encryption header at all.
	require.NoError(t, rejectEncryptionHeaders(ctxWithHeaders(map[string]string{"Range": "bytes=0-1"})), "rejectEncryptionHeaders without one")
	for _, h := range []map[string]string{
		{"X-Amz-Server-Side-Encryption": "AES256"},
		{"x-amz-server-side-encryption": "aws:kms", "x-amz-server-side-encryption-aws-kms-key-id": "testkey-1"},
		sseC,
	} {
		wantInvalidArgument(t, rejectEncryptionHeaders(ctxWithHeaders(h)))
	}
}

// TestPlaintextWrite_StoredAsReceived: under a "none" bucket a body is
// stored as received, across a blob split, reads back whole and ranged, and
// reports no encryption; the stored sizes are the plaintext sizes. An
// explicit AES256 on the same bucket encrypts that one object, and an empty
// object stores nothing either way.
func TestPlaintextWrite_StoredAsReceived(t *testing.T) {
	const blobCeiling = 300 << 10
	b, mem, _ := newRefTestBackend(t, blobCeiling)
	ctx := context.Background()
	setNone(t, b)
	data := testBody(700 << 10) // → 3 blobs of ≤300 KiB

	out := putWithCtx(t, ctx, b, "plain", data)
	require.Empty(t, out.ServerSideEncryption, "PutObject reports encryption %q", out.ServerSideEncryption)
	digests := blobDigestsOf(t, b, "plain", "")
	require.Len(t, digests, 3, "blobs")
	// Each blob is its plaintext piece: named by its hash, stored verbatim,
	// its recorded size the plaintext length, with no key material.
	for i, d := range digests {
		lo, hi := i*blobCeiling, min((i+1)*blobCeiling, len(data))
		piece := data[lo:hi]
		require.True(t, bytes.Equal(d, digestOf(t, piece)), "blob %d: stored digest is not hash(plaintext piece)", i)
		stored, err := os.ReadFile(localPath(b, d))
		require.NoError(t, err, "read stored blob %d", i)
		require.Equal(t, piece, stored, "blob %d: stored %d bytes differ from the %d plaintext bytes", i, len(stored), len(piece))
		_, err = mem.GetEncryptionParams(ctx, mem.bk(), d)
		require.ErrorIs(t, err, registry.ErrNotFound, "blob %d: encryption params lookup", i)
		loc, err := mem.GetLocation(ctx, mem.bk(), d)
		require.NoError(t, err, "blob %d: GetLocation", i)
		require.Equal(t, int64(len(piece)), loc.Size, "blob %d: recorded size", i)
	}
	got := getRange(t, b, "plain", "")
	require.Equal(t, data, got, "whole GET differs from plaintext (%d vs %d bytes)", len(got), len(data))
	require.Equal(t, data[307190:307211], getRange(t, b, "plain", "bytes=307190-307210"), "ranged GET across the blob boundary differs from plaintext")
	sse := headObj(t, b, "plain").ServerSideEncryption
	require.Empty(t, sse, "HEAD reports encryption %q", sse)
	bucket, key := "bk", "plain"
	gout, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	require.NoError(t, err, "GetObject")
	gout.Body.Close()
	require.Empty(t, gout.ServerSideEncryption, "GET reports encryption %q", gout.ServerSideEncryption)

	// An explicit AES256 on the "none" bucket encrypts that object alone.
	small := testBody(4 << 10)
	require.Equal(t, types.ServerSideEncryptionAes256, putWithCtx(t, aes256Ctx(), b, "asked", small).ServerSideEncryption, "PutObject with AES256")
	assertStoredEncrypted(t, b, "asked", small)
	require.Equal(t, small, getRange(t, b, "asked", ""), "encrypted object on a none bucket does not read back")
	putWithCtx(t, ctx, b, "plain-small", small)
	assertStoredAsReceived(t, b, "plain-small", small)

	// Empty bodies store nothing in either mode.
	putWithCtx(t, ctx, b, "empty", nil)
	n := len(blobDigestsOf(t, b, "empty", ""))
	require.Zero(t, n, "empty plaintext object has %d blobs", n)
	require.Empty(t, getRange(t, b, "empty", ""), "empty GET")

	// Reads refuse an encryption header.
	hctx := aes256Ctx()
	_, err = b.HeadObject(hctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	require.Error(t, err, "HEAD with an SSE header: want error")
	wantInvalidArgument(t, err)
	_, err = b.GetObject(hctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	require.Error(t, err, "GET with an SSE header: want error")
	wantInvalidArgument(t, err)
}

// TestPlaintextWrite_SameBytesShareABlob: two plaintext bodies with the
// same bytes are the same blob — the content addressing encryption gives up
// — and each keeps its own claim, so deleting one leaves the other readable.
func TestPlaintextWrite_SameBytesShareABlob(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	setNone(t, b)
	data := testBody(8 << 10)

	putWithCtx(t, ctx, b, "one", data)
	putWithCtx(t, ctx, b, "two", data)
	d1, d2 := blobDigestOf(t, b, "one", ""), blobDigestOf(t, b, "two", "")
	require.True(t, bytes.Equal(d1, d2), "identical plaintext bodies stored under different digests")
	bucket, key := "bk", "one"
	_, err := b.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key})
	require.NoError(t, err, "DeleteObject")
	drainReleases(t, b)
	got := getRange(t, b, "two", "")
	require.Equal(t, data, got, "the surviving object no longer reads back after its twin was deleted")
}

// TestPlaintextCopy_ModeFollowsDestination: a copy is stored the way its
// destination asks — the destination bucket's encryption, or AES256 named
// on the request — so copying across modes re-ingests, in both directions,
// and the copy's bytes survive the change.
func TestPlaintextCopy_ModeFollowsDestination(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	setNone(t, b)
	data := testBody(16 << 10)
	bucket := "bk"
	copyObj := func(ctx context.Context, src, dst string) s3response.CopyObjectOutput {
		t.Helper()
		source := bucket + "/" + src
		out, err := b.CopyObject(ctx, s3response.CopyObjectInput{Bucket: &bucket, Key: &dst, CopySource: &source})
		require.NoError(t, err, "CopyObject %s → %s", src, dst)
		return out
	}

	putWithCtx(t, ctx, b, "plain", data)
	// plaintext → plaintext pins the source's blob.
	out := copyObj(ctx, "plain", "plain-copy")
	require.Empty(t, out.ServerSideEncryption, "plain copy")
	assertStoredAsReceived(t, b, "plain-copy", data)
	// plaintext → AES256 asked: re-ingested, encrypted.
	require.Equal(t, types.ServerSideEncryptionAes256, copyObj(aes256Ctx(), "plain", "enc-copy").ServerSideEncryption, "encrypting copy")
	assertStoredEncrypted(t, b, "enc-copy", data)
	got := getRange(t, b, "enc-copy", "")
	require.Equal(t, data, got, "encrypting copy does not read back")
	// encrypted → the bucket's "none": re-ingested, stored as received.
	copyObj(ctx, "enc-copy", "plain-again")
	assertStoredAsReceived(t, b, "plain-again", data)
	require.Equal(t, data, getRange(t, b, "plain-again", ""), "decrypting copy does not read back")
	// encrypted → AES256 asked, same space: pinned, still encrypted.
	copyObj(aes256Ctx(), "enc-copy", "enc-copy-2")
	require.True(t, bytes.Equal(blobDigestOf(t, b, "enc-copy", ""), blobDigestOf(t, b, "enc-copy-2", "")), "an encrypted copy under the same mode was re-ingested rather than pinned")
	assertStoredEncrypted(t, b, "enc-copy-2", data)
}

// TestPlaintextMultipart_SessionCarriesTheMode: a multipart upload stores
// its parts the way its create decided — the bucket's "none", or AES256
// named at create — and every response along the way reports it.
func TestPlaintextMultipart_SessionCarriesTheMode(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	setNone(t, b)
	bucket := "bk"
	// Every part but the last must be at least 5 MiB.
	p1, p2 := testBody(5<<20), testBody(3<<10)

	for _, tc := range []struct {
		name   string
		ctx    context.Context
		want   types.ServerSideEncryption
		stored func(t *testing.T, key string, data []byte)
	}{
		{"none", ctx, "", nil},
		{"AES256 at create", aes256Ctx(), types.ServerSideEncryptionAes256, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := "mp-" + tc.name
			res, err := b.CreateMultipartUpload(tc.ctx, s3response.CreateMultipartUploadInput{Bucket: &bucket, Key: &key})
			require.NoError(t, err, "CreateMultipartUpload")
			require.Equal(t, tc.want, res.ServerSideEncryption, "create")
			var parts []types.CompletedPart
			for i, data := range [][]byte{p1, p2} {
				n := int32(i + 1)
				part, err := mpUploadPart(t, b, key, res.UploadId, n, data, nil)
				require.NoError(t, err, "UploadPart %d", n)
				require.Equal(t, tc.want, part.ServerSideEncryption, "part %d", n)
				parts = append(parts, types.CompletedPart{ETag: part.ETag, PartNumber: &n})
			}
			cres, err := mpComplete(t, b, key, res.UploadId, parts, nil)
			require.NoError(t, err, "Complete")
			require.Equal(t, tc.want, cres.ServerSideEncryption, "complete")
			whole := append(append([]byte{}, p1...), p2...)
			got := getRange(t, b, key, "")
			require.Equal(t, whole, got, "multipart object does not read back (%d vs %d bytes)", len(got), len(whole))
			const boundary = 5 << 20
			require.Equal(t, whole[boundary-5:boundary+6], getRange(t, b, key, fmt.Sprintf("bytes=%d-%d", boundary-5, boundary+5)), "ranged GET across the part boundary differs")
			sse := headObj(t, b, key).ServerSideEncryption
			require.Equal(t, tc.want, sse, "HEAD")
			for i, d := range blobDigestsOf(t, b, key, "") {
				_, err := mem.GetEncryptionParams(ctx, mem.bk(), d)
				if tc.want == "" {
					require.ErrorIs(t, err, registry.ErrNotFound, "plaintext part blob %d has key material", i)
				} else {
					require.NoError(t, err, "encrypted part blob %d", i)
				}
			}
		})
	}
}

// TestBucketEncryption_AllowNoneGatesOnlyTheRequest: encryption.allow_none
// is consulted by PutBucketEncryption alone. A bucket keeps the setting it
// was given: once the deployment stops allowing "none", an existing "none"
// bucket still stores as received and still reports "none", and only a new
// request for "none" is refused.
func TestBucketEncryption_AllowNoneGatesOnlyTheRequest(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	setNone(t, b)
	data := testBody(8 << 10)

	b.allowNoneEncryption = false
	putWithCtx(t, ctx, b, "still-none", data)
	assertStoredAsReceived(t, b, "still-none", data)
	cfg, err := b.GetBucketEncryption(ctx, "bk")
	require.NoError(t, err, "GetBucketEncryption after allow_none was turned off = %+v, %v", cfg, err)
	require.Equal(t, sseNone, cfg.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm, "GetBucketEncryption after allow_none was turned off = %+v, %v", cfg, err)
	require.NoError(t, mem.Create(ctx, "bk2", did.Undef, registry.CreateState{}), "create bk2")
	wantInvalidArgument(t, b.PutBucketEncryption(ctx, "bk2", sseConfig(sseNone)))
}
