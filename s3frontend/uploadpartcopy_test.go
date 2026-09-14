package s3frontend

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"

	"github.com/fil-forge/ingot/registry"
)

// upc issues an UploadPartCopy into bucket "bk".
func upc(t *testing.T, b *Backend, key, uploadID string, n int32, source string, mod func(*s3.UploadPartCopyInput)) (s3response.CopyPartResult, error) {
	t.Helper()
	bucket := "bk"
	in := &s3.UploadPartCopyInput{Bucket: &bucket, Key: &key, UploadId: &uploadID, PartNumber: &n, CopySource: &source}
	if mod != nil {
		mod(in)
	}
	return b.UploadPartCopy(context.Background(), in)
}

func completed(res s3response.CopyPartResult, n int32) types.CompletedPart {
	return types.CompletedPart{ETag: res.ETag, PartNumber: &n}
}

// A whole-object copy and two ranged copies that together span a source
// crossing several blob boundaries assemble into the source's bytes.
func TestUploadPartCopy_RangesRoundTrip(t *testing.T) {
	b, _, _ := newRefTestBackend(t, 1<<20) // 1 MiB blobs: every part spans several
	src := bytes.Repeat([]byte("abcdefgh"), 6<<20/8)
	putObjV(t, b, "src", src)

	// Whole object as one part.
	id := mpCreate(t, b, "whole", "", "")
	res, err := upc(t, b, "whole", id, 1, "bk/src", nil)
	if err != nil {
		t.Fatalf("whole-object copy: %v", err)
	}
	parts, err := b.ListParts(context.Background(), &s3.ListPartsInput{Bucket: strPtr("bk"), Key: strPtr("whole"), UploadId: &id})
	if err != nil || len(parts.Parts) != 1 || parts.Parts[0].Size != int64(len(src)) || trimQuotes(parts.Parts[0].ETag) != trimQuotes(*res.ETag) {
		t.Fatalf("ListParts after whole-object copy = %+v, %v; want one %d-byte part with ETag %s", parts.Parts, err, len(src), *res.ETag)
	}
	if _, err := mpComplete(t, b, "whole", id, []types.CompletedPart{completed(res, 1)}, nil); err != nil {
		t.Fatalf("complete whole: %v", err)
	}
	if _, got, err := getObjV(t, b, "whole", ""); err != nil || !bytes.Equal(got, src) {
		t.Fatalf("whole copy GET: %d bytes, %v; want the source", len(got), err)
	}

	// Two ranges: [0, 5 MiB) then the 1 MiB remainder (the last part may be
	// small; the others must meet the 5 MiB minimum).
	id = mpCreate(t, b, "ranged", "", "")
	r1, err := upc(t, b, "ranged", id, 1, "bk/src", func(in *s3.UploadPartCopyInput) { in.CopySourceRange = strPtr("bytes=0-5242879") })
	if err != nil {
		t.Fatalf("range 1: %v", err)
	}
	r2, err := upc(t, b, "ranged", id, 2, "bk/src", func(in *s3.UploadPartCopyInput) { in.CopySourceRange = strPtr("bytes=5242880-6291455") })
	if err != nil {
		t.Fatalf("range 2: %v", err)
	}
	if _, err := mpComplete(t, b, "ranged", id, []types.CompletedPart{completed(r1, 1), completed(r2, 2)}, nil); err != nil {
		t.Fatalf("complete ranged: %v", err)
	}
	if _, got, err := getObjV(t, b, "ranged", ""); err != nil || !bytes.Equal(got, src) {
		t.Fatalf("ranged copy GET: %d bytes, %v; want the source", len(got), err)
	}
}

// A ranged part's size and ETag are those of the copied bytes, as ListParts
// reports them; an empty source copies as an empty part.
func TestUploadPartCopy_PartSizeAndETag(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	src := bytes.Repeat([]byte("x"), 1024)
	putObjV(t, b, "src", src)
	putObjV(t, b, "empty", nil)

	id := mpCreate(t, b, "dst", "", "")
	res, err := upc(t, b, "dst", id, 1, "bk/src", func(in *s3.UploadPartCopyInput) { in.CopySourceRange = strPtr("bytes=100-200") })
	if err != nil {
		t.Fatalf("copy bytes=100-200: %v", err)
	}
	parts, err := b.ListParts(context.Background(), &s3.ListPartsInput{Bucket: strPtr("bk"), Key: strPtr("dst"), UploadId: &id})
	if err != nil || len(parts.Parts) != 1 || parts.Parts[0].Size != 101 || trimQuotes(parts.Parts[0].ETag) != trimQuotes(*res.ETag) {
		t.Fatalf("ListParts = %+v, %v; want one 101-byte part with ETag %s", parts.Parts, err, *res.ETag)
	}
	if want := `"` + hex.EncodeToString(md5Sum(src[100:201])) + `"`; *res.ETag != want {
		t.Fatalf("part ETag = %s, want md5 of the copied bytes %s", *res.ETag, want)
	}

	id = mpCreate(t, b, "empty-dst", "", "")
	res, err = upc(t, b, "empty-dst", id, 1, "bk/empty", nil)
	if err != nil {
		t.Fatalf("copy of an empty object: %v", err)
	}
	if _, err := mpComplete(t, b, "empty-dst", id, []types.CompletedPart{completed(res, 1)}, nil); err != nil {
		t.Fatalf("complete empty: %v", err)
	}
	if _, got, err := getObjV(t, b, "empty-dst", ""); err != nil || len(got) != 0 {
		t.Fatalf("empty copy GET: %d bytes, %v", len(got), err)
	}
}

// Range errors follow S3: malformed, open-ended and reversed ranges are
// InvalidArgument naming the header; a start past the object is
// InvalidRequest; a start at the object's size or an end past it is the
// exceeding-range InvalidArgument.
func TestUploadPartCopy_RangeErrors(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	putObjV(t, b, "src", bytes.Repeat([]byte("x"), 1024))
	id := mpCreate(t, b, "dst", "", "")

	for _, tc := range []struct{ rng, want string }{
		{"bytes=0-2000", "InvalidArgument"},
		{"bytes=1024-1030", "InvalidArgument"},
		{"bytes=2000-3000", "InvalidRequest"},
		{"bytes=100-", "InvalidArgument"},
		{"bytes=abc", "InvalidArgument"},
		{"bytes=10-5", "InvalidArgument"},
		{"0-2", "InvalidArgument"},
		{"bytes=0-2,3-5", "InvalidArgument"},
	} {
		_, err := upc(t, b, "dst", id, 1, "bk/src", func(in *s3.UploadPartCopyInput) { in.CopySourceRange = &tc.rng })
		if got := apiErrCode(t, err); got != tc.want {
			t.Fatalf("range %q: %s (%v), want %s", tc.rng, got, err, tc.want)
		}
	}
}

// Session, source bucket, key and tenant errors, in the order S3 reports them.
func TestUploadPartCopy_Errors(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	putObjV(t, b, "src", []byte("data"))
	if err := mem.Create(ctx, "foreign", testutil.RandomDID(t), registry.CreateState{Tenant: testutil.RandomDID(t)}); err != nil {
		t.Fatal(err)
	}
	fb, fk := "foreign", "obj"
	if _, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &fb, Key: &fk, Body: bytes.NewReader([]byte("theirs"))}); err != nil {
		t.Fatal(err)
	}
	id := mpCreate(t, b, "dst", "", "")

	if _, err := upc(t, b, "dst", "no-such-upload", 1, "bk/src", nil); apiErrCode(t, err) != "NoSuchUpload" {
		t.Fatalf("unknown upload id: %v", err)
	}
	if _, err := upc(t, b, "other-key", id, 1, "bk/src", nil); apiErrCode(t, err) != "NoSuchUpload" {
		t.Fatalf("mismatched key: %v", err)
	}
	// A valid upload id and key addressed through another bucket is not that
	// bucket's upload: neither a copy nor an upload may write the session.
	if err := mem.Create(ctx, "other-bucket", testutil.RandomDID(t), registry.CreateState{}); err != nil {
		t.Fatal(err)
	}
	if _, err := upc(t, b, "dst", id, 1, "bk/src", func(in *s3.UploadPartCopyInput) { in.Bucket = strPtr("other-bucket") }); apiErrCode(t, err) != "NoSuchUpload" {
		t.Fatalf("mismatched bucket on copy: %v", err)
	}
	other, one := "other-bucket", int32(1)
	if _, err := b.UploadPart(ctx, &s3.UploadPartInput{Bucket: &other, Key: strPtr("dst"), UploadId: &id, PartNumber: &one, Body: bytes.NewReader([]byte("x"))}); apiErrCode(t, err) != "NoSuchUpload" {
		t.Fatalf("mismatched bucket on upload: %v", err)
	}

	if _, err := upc(t, b, "dst", id, 1, "no-such-bucket/src", nil); apiErrCode(t, err) != "NoSuchBucket" {
		t.Fatalf("missing source bucket: %v", err)
	}
	if _, err := upc(t, b, "dst", id, 1, "bk/no-such-key", nil); apiErrCode(t, err) != "NoSuchKey" {
		t.Fatalf("missing source key: %v", err)
	}
	if _, err := upc(t, b, "dst", id, 1, "foreign/obj", nil); apiErrCode(t, err) != "AccessDenied" {
		t.Fatalf("foreign-tenant source: %v", err)
	}
	if _, err := upc(t, b, "dst", id, 1, "bk/src", func(in *s3.UploadPartCopyInput) { in.ExpectedSourceBucketOwner = strPtr("someone-else") }); apiErrCode(t, err) != "AccessDenied" {
		t.Fatalf("wrong expected source owner: %v", err)
	}
	if _, err := upc(t, b, "dst", id, 1, "bk/src", func(in *s3.UploadPartCopyInput) { in.CopySourceIfMatch = strPtr("00000000000000000000000000000000") }); !errors.Is(err, s3err.GetAPIError(s3err.ErrPreconditionFailed)) {
		t.Fatalf("failed If-Match: %v, want 412", err)
	}
	if _, err := upc(t, b, "dst", id, 1, "bk/src", func(in *s3.UploadPartCopyInput) {
		in.CopySourceIfNoneMatch = strPtr(trimQuotes(putETag(t, b, "src")))
	}); !errors.Is(err, s3err.GetAPIError(s3err.ErrPreconditionFailed)) {
		t.Fatalf("matching If-None-Match: %v, want 412", err)
	}

	// Nor may Complete assemble a session addressed through another bucket
	// or key; it stays completable under its own. Last, since it ends the
	// session.
	res, err := upc(t, b, "dst", id, 1, "bk/src", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: &other, Key: strPtr("dst"), UploadId: &id,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{completed(res, 1)}}}); apiErrCode(t, err) != "NoSuchUpload" {
		t.Fatalf("complete through another bucket: %v", err)
	}
	if _, err := mpComplete(t, b, "other-key", id, []types.CompletedPart{completed(res, 1)}, nil); apiErrCode(t, err) != "NoSuchUpload" {
		t.Fatalf("complete through another key: %v", err)
	}
	if _, err := mpComplete(t, b, "dst", id, []types.CompletedPart{completed(res, 1)}, nil); err != nil {
		t.Fatalf("complete under its own bucket and key: %v", err)
	}
}

// A copy from a specific version copies that version's bytes and reports it.
func TestUploadPartCopy_Version(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	v1 := putObjV(t, b, "src", []byte("first version"))
	putObjV(t, b, "src", []byte("second version"))

	id := mpCreate(t, b, "dst", "", "")
	res, err := upc(t, b, "dst", id, 1, "bk/src?versionId="+v1.VersionID, nil)
	if err != nil {
		t.Fatalf("copy from version: %v", err)
	}
	if res.CopySourceVersionId != v1.VersionID {
		t.Fatalf("CopySourceVersionId = %q, want %q", res.CopySourceVersionId, v1.VersionID)
	}
	if _, err := mpComplete(t, b, "dst", id, []types.CompletedPart{completed(res, 1)}, nil); err != nil {
		t.Fatal(err)
	}
	if _, got, err := getObjV(t, b, "dst", ""); err != nil || string(got) != "first version" {
		t.Fatalf("GET = %q, %v", got, err)
	}
}

// The part checksum is the session's algorithm computed over the copied bytes:
// it matches a source checksummed the same way, is recomputed for one
// checksummed differently, is absent when the session declared none (Complete
// still derives the default CRC64NVME), and needs no client value on a
// COMPOSITE session.
func TestUploadPartCopy_Checksums(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()
	data := bytes.Repeat([]byte("checksum me "), 25)
	bucket, key := "bk", "src"
	if _, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ChecksumAlgorithm: types.ChecksumAlgorithmSha1}); err != nil {
		t.Fatal(err)
	}

	id := mpCreate(t, b, "same", types.ChecksumAlgorithmCrc32c, types.ChecksumTypeFullObject)
	res, err := upc(t, b, "same", id, 1, "bk/src", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.ChecksumCRC32C == nil || *res.ChecksumCRC32C != crc32cB64(data) || res.ChecksumSHA1 != nil || res.ChecksumSHA256 != nil {
		t.Fatalf("CRC32C session: %+v, want CRC32C %s alone", res, crc32cB64(data))
	}

	id = mpCreate(t, b, "recompute", types.ChecksumAlgorithmSha256, types.ChecksumTypeFullObject)
	res, err = upc(t, b, "recompute", id, 1, "bk/src", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.ChecksumSHA256 == nil || *res.ChecksumSHA256 != sha256B64(data) || res.ChecksumSHA1 != nil {
		t.Fatalf("SHA256 session over a SHA1 source: %+v, want SHA256 %s alone", res, sha256B64(data))
	}

	id = mpCreate(t, b, "none", "", "")
	res, err = upc(t, b, "none", id, 1, "bk/src", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.ChecksumCRC32 != nil || res.ChecksumCRC32C != nil || res.ChecksumSHA1 != nil || res.ChecksumSHA256 != nil || res.ChecksumCRC64NVME != nil {
		t.Fatalf("session without a checksum echoed one: %+v", res)
	}
	if _, err := mpComplete(t, b, "none", id, []types.CompletedPart{completed(res, 1)}, nil); err != nil {
		t.Fatal(err)
	}
	if _, crc64, _, ctype := headChecksum(t, b, "none"); crc64 == nil || ctype != types.ChecksumTypeFullObject {
		t.Fatalf("completed object lacks the default CRC64NVME: %v %s", crc64, ctype)
	}

	id = mpCreate(t, b, "composite", types.ChecksumAlgorithmCrc32c, types.ChecksumTypeComposite)
	res, err = upc(t, b, "composite", id, 1, "bk/src", nil)
	if err != nil {
		t.Fatalf("COMPOSITE session: %v", err)
	}
	if res.ChecksumCRC32C == nil {
		t.Fatalf("COMPOSITE session echoed no part checksum: %+v", res)
	}
	// A composite Complete names each part's checksum, as the client would
	// from the copy result.
	part := completed(res, 1)
	part.ChecksumCRC32C = res.ChecksumCRC32C
	if _, err := mpComplete(t, b, "composite", id, []types.CompletedPart{part}, nil); err != nil {
		t.Fatalf("complete COMPOSITE: %v", err)
	}
}

func trimQuotes(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// putETag reads the current ETag of bk/key.
func putETag(t *testing.T, b *Backend, key string) string {
	t.Helper()
	out, _, err := getObjV(t, b, key, "")
	if err != nil {
		t.Fatal(err)
	}
	return *out.ETag
}

func md5Sum(b []byte) []byte {
	sum := md5.Sum(b)
	return sum[:]
}
