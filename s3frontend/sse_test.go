package s3frontend

import (
	"bytes"
	"context"
	"errors"
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
	if !errors.As(err, &apiErr) || apiErr.Code != "InvalidArgument" || apiErr.HTTPStatusCode != http.StatusBadRequest {
		t.Fatalf("err = %v, want 400 InvalidArgument", err)
	}
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
	if err := b.PutBucketEncryption(context.Background(), "bk", sseConfig(sseNone)); err != nil {
		t.Fatalf("PutBucketEncryption none: %v", err)
	}
}

func putWithCtx(t *testing.T, ctx context.Context, b *Backend, key string, data []byte) s3response.PutObjectOutput {
	t.Helper()
	bucket := "bk"
	n := int64(len(data))
	out, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: &n})
	if err != nil {
		t.Fatalf("PutObject %s: %v", key, err)
	}
	return out
}

func headObj(t *testing.T, b *Backend, key string) *s3.HeadObjectOutput {
	t.Helper()
	bucket := "bk"
	out, err := b.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		t.Fatalf("HeadObject %s: %v", key, err)
	}
	return out
}

// assertStoredAsReceived checks that key's single blob is the plaintext
// itself: named by hash(plaintext), byte-identical on disk, with no
// blob_encryption_params row, and reported with no encryption header.
func assertStoredAsReceived(t *testing.T, b *Backend, key string, data []byte) {
	t.Helper()
	d := blobDigestOf(t, b, key, "")
	if !bytes.Equal(d, digestOf(t, data)) {
		t.Fatalf("%s: stored digest is not hash(plaintext); the blob was transformed", key)
	}
	stored, err := os.ReadFile(localPath(b, d))
	if err != nil {
		t.Fatalf("read spooled blob: %v", err)
	}
	if !bytes.Equal(stored, data) {
		t.Fatalf("%s: stored %d bytes differ from the %d plaintext bytes", key, len(stored), len(data))
	}
	if _, err := b.encParams.GetEncryptionParams(context.Background(), did.Undef, d); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("%s: encryption params lookup = %v, want ErrNotFound (no key material for a plaintext blob)", key, err)
	}
	if sse := headObj(t, b, key).ServerSideEncryption; sse != "" {
		t.Fatalf("%s: HEAD reports encryption %q, want none", key, sse)
	}
}

// assertStoredEncrypted checks that key's single blob is an envelope with a
// params row, reported as AES256.
func assertStoredEncrypted(t *testing.T, b *Backend, key string, data []byte) {
	t.Helper()
	d := blobDigestOf(t, b, key, "")
	if bytes.Equal(d, digestOf(t, data)) {
		t.Fatalf("%s: stored digest equals hash(plaintext); the blob was not encrypted", key)
	}
	if _, err := b.encParams.GetEncryptionParams(context.Background(), did.Undef, d); err != nil {
		t.Fatalf("%s: GetEncryptionParams: %v", key, err)
	}
	if sse := headObj(t, b, key).ServerSideEncryption; sse != types.ServerSideEncryptionAes256 {
		t.Fatalf("%s: HEAD reports encryption %q, want AES256", key, sse)
	}
}

// TestBucketEncryption_Configuration: the bucket-encryption API over the
// registry. An unconfigured bucket answers not found while encrypting all
// the same; AES256 round-trips; "none" is refused until the deployment
// allows it; aws:kms is not implemented; a KMS key beside AES256 and an
// unknown algorithm are InvalidArgument; Delete clears and is idempotent.
func TestBucketEncryption_Configuration(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	if _, err := b.GetBucketEncryption(ctx, "bk"); err == nil {
		t.Fatal("unconfigured GetBucketEncryption: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrServerSideEncryptionConfigurationNotFound)
	}
	if _, err := b.GetBucketEncryption(ctx, "nope"); err == nil {
		t.Fatal("missing bucket: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchBucket)
	}
	if err := b.PutBucketEncryption(ctx, "nope", sseConfig(types.ServerSideEncryptionAes256)); err == nil {
		t.Fatal("put on missing bucket: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchBucket)
	}
	if err := b.DeleteBucketEncryption(ctx, "nope"); err == nil {
		t.Fatal("delete on missing bucket: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrNoSuchBucket)
	}

	// The unconfigured bucket encrypts.
	putObj(t, b, "default", []byte("stored encrypted by default"))
	assertStoredEncrypted(t, b, "default", []byte("stored encrypted by default"))

	if err := b.PutBucketEncryption(ctx, "bk", sseConfig(types.ServerSideEncryptionAes256)); err != nil {
		t.Fatalf("PutBucketEncryption AES256: %v", err)
	}
	got, err := b.GetBucketEncryption(ctx, "bk")
	if err != nil {
		t.Fatalf("GetBucketEncryption: %v", err)
	}
	if len(got.Rules) != 1 || got.Rules[0].ApplyServerSideEncryptionByDefault == nil ||
		got.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm != types.ServerSideEncryptionAes256 {
		t.Fatalf("GetBucketEncryption = %+v, want one AES256 rule", got)
	}

	// Refused algorithms, each leaving the stored AES256 untouched.
	wantInvalidArgument(t, b.PutBucketEncryption(ctx, "bk", sseConfig(sseNone)))
	if err := b.PutBucketEncryption(ctx, "bk", sseConfig(types.ServerSideEncryptionAwsKms)); err == nil {
		t.Fatal("aws:kms: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrNotImplemented)
	}
	wantInvalidArgument(t, b.PutBucketEncryption(ctx, "bk", sseConfig("ROT13")))
	withKey := sseConfig(types.ServerSideEncryptionAes256)
	keyID := "fool-me-once"
	withKey.Rules[0].ApplyServerSideEncryptionByDefault.KMSMasterKeyID = &keyID
	wantInvalidArgument(t, b.PutBucketEncryption(ctx, "bk", withKey))
	if err := b.PutBucketEncryption(ctx, "bk", s3response.ServerSideEncryptionConfiguration{}); err == nil {
		t.Fatal("no rules: want error")
	} else {
		wantAPIErr(t, err, s3err.ErrMalformedXML)
	}
	if st, err := b.reg.Get(ctx, "bk"); err != nil || st.Encryption != registry.BucketEncryptionAES256 {
		t.Fatalf("stored encryption after refused puts = %q (%v), want AES256", st.Encryption, err)
	}

	// Allowed, "none" stores and reads back.
	b.allowNoneEncryption = true
	if err := b.PutBucketEncryption(ctx, "bk", sseConfig(sseNone)); err != nil {
		t.Fatalf("PutBucketEncryption none: %v", err)
	}
	if got, err := b.GetBucketEncryption(ctx, "bk"); err != nil || got.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm != sseNone {
		t.Fatalf("GetBucketEncryption after none = %+v (%v)", got, err)
	}

	// Delete clears, and is idempotent.
	for range 2 {
		if err := b.DeleteBucketEncryption(ctx, "bk"); err != nil {
			t.Fatalf("DeleteBucketEncryption: %v", err)
		}
		if _, err := b.GetBucketEncryption(ctx, "bk"); err == nil {
			t.Fatal("GetBucketEncryption after delete: want not found")
		} else {
			wantAPIErr(t, err, s3err.ErrServerSideEncryptionConfigurationNotFound)
		}
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
				if !errors.As(err, &apiErr) {
					t.Fatalf("err = %v, want an APIError", err)
				}
				code = apiErr.Code
			}
			if code != c.code || got != c.want {
				t.Fatalf("requestedEncryption = (%q, %q), want (%q, %q)", got, code, c.want, c.code)
			}
		})
	}
	// Without a request in the context (a background caller) nothing is asked.
	if got, err := requestedEncryption(context.Background()); err != nil || got != "" {
		t.Fatalf("no request: (%q, %v), want (\"\", nil)", got, err)
	}

	// Reads take no encryption header at all.
	if err := rejectEncryptionHeaders(ctxWithHeaders(map[string]string{"Range": "bytes=0-1"})); err != nil {
		t.Fatalf("rejectEncryptionHeaders without one: %v", err)
	}
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
	if out.ServerSideEncryption != "" {
		t.Fatalf("PutObject reports encryption %q, want none", out.ServerSideEncryption)
	}
	digests := blobDigestsOf(t, b, "plain", "")
	if len(digests) != 3 {
		t.Fatalf("blobs = %d, want 3", len(digests))
	}
	// Each blob is its plaintext piece: named by its hash, stored verbatim,
	// its recorded size the plaintext length, with no key material.
	for i, d := range digests {
		lo, hi := i*blobCeiling, min((i+1)*blobCeiling, len(data))
		piece := data[lo:hi]
		if !bytes.Equal(d, digestOf(t, piece)) {
			t.Fatalf("blob %d: stored digest is not hash(plaintext piece)", i)
		}
		stored, err := os.ReadFile(localPath(b, d))
		if err != nil {
			t.Fatalf("read spooled blob %d: %v", i, err)
		}
		if !bytes.Equal(stored, piece) {
			t.Fatalf("blob %d: stored %d bytes differ from the %d plaintext bytes", i, len(stored), len(piece))
		}
		if _, err := mem.GetEncryptionParams(ctx, did.Undef, d); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("blob %d: encryption params lookup = %v, want ErrNotFound", i, err)
		}
		loc, err := mem.GetLocation(ctx, did.Undef, d)
		if err != nil {
			t.Fatalf("blob %d: GetLocation: %v", i, err)
		}
		if loc.Size != int64(len(piece)) {
			t.Fatalf("blob %d: recorded size = %d, want the plaintext %d", i, loc.Size, len(piece))
		}
	}
	if got := getRange(t, b, "plain", ""); !bytes.Equal(got, data) {
		t.Fatalf("whole GET differs from plaintext (%d vs %d bytes)", len(got), len(data))
	}
	if got := getRange(t, b, "plain", "bytes=307190-307210"); !bytes.Equal(got, data[307190:307211]) {
		t.Fatalf("ranged GET across the blob boundary differs from plaintext")
	}
	if sse := headObj(t, b, "plain").ServerSideEncryption; sse != "" {
		t.Fatalf("HEAD reports encryption %q, want none", sse)
	}
	bucket, key := "bk", "plain"
	gout, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	gout.Body.Close()
	if gout.ServerSideEncryption != "" {
		t.Fatalf("GET reports encryption %q, want none", gout.ServerSideEncryption)
	}

	// An explicit AES256 on the "none" bucket encrypts that object alone.
	small := testBody(4 << 10)
	if out := putWithCtx(t, aes256Ctx(), b, "asked", small); out.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("PutObject with AES256 reports %q, want AES256", out.ServerSideEncryption)
	}
	assertStoredEncrypted(t, b, "asked", small)
	if got := getRange(t, b, "asked", ""); !bytes.Equal(got, small) {
		t.Fatalf("encrypted object on a none bucket does not read back")
	}
	putWithCtx(t, ctx, b, "plain-small", small)
	assertStoredAsReceived(t, b, "plain-small", small)

	// Empty bodies store nothing in either mode.
	putWithCtx(t, ctx, b, "empty", nil)
	if n := len(blobDigestsOf(t, b, "empty", "")); n != 0 {
		t.Fatalf("empty plaintext object has %d blobs, want 0", n)
	}
	if got := getRange(t, b, "empty", ""); len(got) != 0 {
		t.Fatalf("empty GET returned %d bytes", len(got))
	}

	// Reads refuse an encryption header.
	hctx := aes256Ctx()
	if _, err := b.HeadObject(hctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key}); err == nil {
		t.Fatal("HEAD with an SSE header: want error")
	} else {
		wantInvalidArgument(t, err)
	}
	if _, err := b.GetObject(hctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key}); err == nil {
		t.Fatal("GET with an SSE header: want error")
	} else {
		wantInvalidArgument(t, err)
	}
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
	if !bytes.Equal(d1, d2) {
		t.Fatalf("identical plaintext bodies stored under different digests")
	}
	bucket, key := "bk", "one"
	if _, err := b.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	drainReleases(t, b)
	if got := getRange(t, b, "two", ""); !bytes.Equal(got, data) {
		t.Fatalf("the surviving object no longer reads back after its twin was deleted")
	}
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
		if err != nil {
			t.Fatalf("CopyObject %s → %s: %v", src, dst, err)
		}
		return out
	}

	putWithCtx(t, ctx, b, "plain", data)
	// plaintext → plaintext pins the source's blob.
	if out := copyObj(ctx, "plain", "plain-copy"); out.ServerSideEncryption != "" {
		t.Fatalf("plain copy reports %q, want none", out.ServerSideEncryption)
	}
	assertStoredAsReceived(t, b, "plain-copy", data)
	// plaintext → AES256 asked: re-ingested, encrypted.
	if out := copyObj(aes256Ctx(), "plain", "enc-copy"); out.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("encrypting copy reports %q, want AES256", out.ServerSideEncryption)
	}
	assertStoredEncrypted(t, b, "enc-copy", data)
	if got := getRange(t, b, "enc-copy", ""); !bytes.Equal(got, data) {
		t.Fatalf("encrypting copy does not read back")
	}
	// encrypted → the bucket's "none": re-ingested, stored as received.
	copyObj(ctx, "enc-copy", "plain-again")
	assertStoredAsReceived(t, b, "plain-again", data)
	if got := getRange(t, b, "plain-again", ""); !bytes.Equal(got, data) {
		t.Fatalf("decrypting copy does not read back")
	}
	// encrypted → AES256 asked, same space: pinned, still encrypted.
	copyObj(aes256Ctx(), "enc-copy", "enc-copy-2")
	if !bytes.Equal(blobDigestOf(t, b, "enc-copy", ""), blobDigestOf(t, b, "enc-copy-2", "")) {
		t.Fatalf("an encrypted copy under the same mode was re-ingested rather than pinned")
	}
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
			if err != nil {
				t.Fatalf("CreateMultipartUpload: %v", err)
			}
			if res.ServerSideEncryption != tc.want {
				t.Fatalf("create reports %q, want %q", res.ServerSideEncryption, tc.want)
			}
			var parts []types.CompletedPart
			for i, data := range [][]byte{p1, p2} {
				n := int32(i + 1)
				part, err := mpUploadPart(t, b, key, res.UploadId, n, data, nil)
				if err != nil {
					t.Fatalf("UploadPart %d: %v", n, err)
				}
				if part.ServerSideEncryption != tc.want {
					t.Fatalf("part %d reports %q, want %q", n, part.ServerSideEncryption, tc.want)
				}
				parts = append(parts, types.CompletedPart{ETag: part.ETag, PartNumber: &n})
			}
			cres, err := mpComplete(t, b, key, res.UploadId, parts, nil)
			if err != nil {
				t.Fatalf("Complete: %v", err)
			}
			if cres.ServerSideEncryption != tc.want {
				t.Fatalf("complete reports %q, want %q", cres.ServerSideEncryption, tc.want)
			}
			whole := append(append([]byte{}, p1...), p2...)
			if got := getRange(t, b, key, ""); !bytes.Equal(got, whole) {
				t.Fatalf("multipart object does not read back (%d vs %d bytes)", len(got), len(whole))
			}
			const boundary = 5 << 20
			if got := getRange(t, b, key, fmt.Sprintf("bytes=%d-%d", boundary-5, boundary+5)); !bytes.Equal(got, whole[boundary-5:boundary+6]) {
				t.Fatalf("ranged GET across the part boundary differs")
			}
			if sse := headObj(t, b, key).ServerSideEncryption; sse != tc.want {
				t.Fatalf("HEAD reports %q, want %q", sse, tc.want)
			}
			for i, d := range blobDigestsOf(t, b, key, "") {
				_, err := mem.GetEncryptionParams(ctx, did.Undef, d)
				if tc.want == "" && !errors.Is(err, registry.ErrNotFound) {
					t.Fatalf("plaintext part blob %d has key material (%v)", i, err)
				}
				if tc.want != "" && err != nil {
					t.Fatalf("encrypted part blob %d: %v", i, err)
				}
			}
		})
	}
}
