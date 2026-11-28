package s3frontend

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, err, "whole-object copy")
	parts, err := b.ListParts(context.Background(), &s3.ListPartsInput{Bucket: strPtr("bk"), Key: strPtr("whole"), UploadId: &id})
	require.NoError(t, err, "ListParts after whole-object copy")
	require.Len(t, parts.Parts, 1, "ListParts after whole-object copy")
	require.Equal(t, int64(len(src)), parts.Parts[0].Size, "copied part size")
	require.Equal(t, trimQuotes(*res.ETag), trimQuotes(parts.Parts[0].ETag), "copied part ETag")
	_, err = mpComplete(t, b, "whole", id, []types.CompletedPart{completed(res, 1)}, nil)
	require.NoError(t, err, "complete whole")
	_, got, err := getObjV(t, b, "whole", "")
	require.NoError(t, err, "whole copy GET")
	require.Equal(t, src, got, "whole copy GET")

	// Two ranges: [0, 5 MiB) then the 1 MiB remainder (the last part may be
	// small; the others must meet the 5 MiB minimum).
	id = mpCreate(t, b, "ranged", "", "")
	r1, err := upc(t, b, "ranged", id, 1, "bk/src", func(in *s3.UploadPartCopyInput) { in.CopySourceRange = strPtr("bytes=0-5242879") })
	require.NoError(t, err, "range 1")
	r2, err := upc(t, b, "ranged", id, 2, "bk/src", func(in *s3.UploadPartCopyInput) { in.CopySourceRange = strPtr("bytes=5242880-6291455") })
	require.NoError(t, err, "range 2")
	_, err = mpComplete(t, b, "ranged", id, []types.CompletedPart{completed(r1, 1), completed(r2, 2)}, nil)
	require.NoError(t, err, "complete ranged")
	_, got, err = getObjV(t, b, "ranged", "")
	require.NoError(t, err, "ranged copy GET")
	require.Equal(t, src, got, "ranged copy GET")
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
	require.NoError(t, err, "copy bytes=100-200")
	parts, err := b.ListParts(context.Background(), &s3.ListPartsInput{Bucket: strPtr("bk"), Key: strPtr("dst"), UploadId: &id})
	require.NoError(t, err, "ListParts")
	require.Len(t, parts.Parts, 1, "ListParts")
	require.Equal(t, int64(101), parts.Parts[0].Size, "copied part size")
	require.Equal(t, trimQuotes(*res.ETag), trimQuotes(parts.Parts[0].ETag), "copied part ETag")
	require.Equal(t, `"`+hex.EncodeToString(md5Sum(src[100:201]))+`"`, *res.ETag, "part ETag, want md5 of the copied bytes")

	id = mpCreate(t, b, "empty-dst", "", "")
	res, err = upc(t, b, "empty-dst", id, 1, "bk/empty", nil)
	require.NoError(t, err, "copy of an empty object")
	_, err = mpComplete(t, b, "empty-dst", id, []types.CompletedPart{completed(res, 1)}, nil)
	require.NoError(t, err, "complete empty")
	_, got, err := getObjV(t, b, "empty-dst", "")
	require.NoError(t, err, "empty copy GET")
	require.Empty(t, got, "empty copy GET")
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
		got := apiErrCode(t, err)
		require.Equal(t, tc.want, got, "range %q: %s (%v)", tc.rng, got, err)
	}
}

// Session, source bucket, key and tenant errors, in the order S3 reports them.
func TestUploadPartCopy_Errors(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	putObjV(t, b, "src", []byte("data"))
	require.NoError(t, mem.Create(ctx, "foreign", testutil.RandomDID(t), registry.CreateState{Tenant: testutil.RandomDID(t)}))
	fb, fk := "foreign", "obj"
	_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &fb, Key: &fk, Body: bytes.NewReader([]byte("theirs")), ContentLength: byteLen([]byte("theirs"))})
	require.NoError(t, err)
	id := mpCreate(t, b, "dst", "", "")

	_, err = upc(t, b, "dst", "no-such-upload", 1, "bk/src", nil)
	require.Equal(t, "NoSuchUpload", apiErrCode(t, err), "unknown upload id")
	_, err = upc(t, b, "other-key", id, 1, "bk/src", nil)
	require.Equal(t, "NoSuchUpload", apiErrCode(t, err), "mismatched key")
	// A valid upload id and key addressed through another bucket is not that
	// bucket's upload: neither a copy nor an upload may write the session.
	require.NoError(t, mem.Create(ctx, "other-bucket", testutil.RandomDID(t), registry.CreateState{}))
	_, err = upc(t, b, "dst", id, 1, "bk/src", func(in *s3.UploadPartCopyInput) { in.Bucket = strPtr("other-bucket") })
	require.Equal(t, "NoSuchUpload", apiErrCode(t, err), "mismatched bucket on copy")
	other, one := "other-bucket", int32(1)
	_, err = b.UploadPart(ctx, &s3.UploadPartInput{Bucket: &other, Key: strPtr("dst"), UploadId: &id, PartNumber: &one, Body: bytes.NewReader([]byte("x")), ContentLength: byteLen([]byte("x"))})
	require.Equal(t, "NoSuchUpload", apiErrCode(t, err), "mismatched bucket on upload")

	_, err = upc(t, b, "dst", id, 1, "no-such-bucket/src", nil)
	require.Equal(t, "NoSuchBucket", apiErrCode(t, err), "missing source bucket")
	_, err = upc(t, b, "dst", id, 1, "bk/no-such-key", nil)
	require.Equal(t, "NoSuchKey", apiErrCode(t, err), "missing source key")
	_, err = upc(t, b, "dst", id, 1, "foreign/obj", nil)
	require.Equal(t, "AccessDenied", apiErrCode(t, err), "foreign-tenant source")
	_, err = upc(t, b, "dst", id, 1, "bk/src", func(in *s3.UploadPartCopyInput) { in.ExpectedSourceBucketOwner = strPtr("someone-else") })
	require.Equal(t, "AccessDenied", apiErrCode(t, err), "wrong")
	_, err = upc(t, b, "dst", id, 1, "bk/src", func(in *s3.UploadPartCopyInput) { in.CopySourceIfMatch = strPtr("00000000000000000000000000000000") })
	require.ErrorIs(t, err, s3err.GetAPIError(s3err.ErrPreconditionFailed), "failed If-Match")
	_, err = upc(t, b, "dst", id, 1, "bk/src", func(in *s3.UploadPartCopyInput) {
		in.CopySourceIfNoneMatch = strPtr(trimQuotes(putETag(t, b, "src")))
	})
	require.ErrorIs(t, err, s3err.GetAPIError(s3err.ErrPreconditionFailed), "matching If-None-Match")

	// Nor may Complete assemble a session addressed through another bucket
	// or key; it stays completable under its own. Last, since it ends the
	// session.
	res, err := upc(t, b, "dst", id, 1, "bk/src", nil)
	require.NoError(t, err)
	_, _, err = b.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: &other, Key: strPtr("dst"), UploadId: &id,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{completed(res, 1)}}})
	require.Equal(t, "NoSuchUpload", apiErrCode(t, err), "complete through another bucket")
	_, err = mpComplete(t, b, "other-key", id, []types.CompletedPart{completed(res, 1)}, nil)
	require.Equal(t, "NoSuchUpload", apiErrCode(t, err), "complete through another key")
	_, err = mpComplete(t, b, "dst", id, []types.CompletedPart{completed(res, 1)}, nil)
	require.NoError(t, err, "complete under its own bucket and key")
}

// A copy from a specific version copies that version's bytes and reports it.
func TestUploadPartCopy_Version(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	v1 := putObjV(t, b, "src", []byte("first version"))
	putObjV(t, b, "src", []byte("second version"))

	id := mpCreate(t, b, "dst", "", "")
	res, err := upc(t, b, "dst", id, 1, "bk/src?versionId="+v1.VersionID, nil)
	require.NoError(t, err, "copy from version")
	require.Equal(t, v1.VersionID, res.CopySourceVersionId, "CopySourceVersionId")
	_, err = mpComplete(t, b, "dst", id, []types.CompletedPart{completed(res, 1)}, nil)
	require.NoError(t, err)
	_, got, err := getObjV(t, b, "dst", "")
	require.NoError(t, err, "GET")
	require.Equal(t, "first version", string(got), "GET")
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
	_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: byteLen(data), ChecksumAlgorithm: types.ChecksumAlgorithmSha1})
	require.NoError(t, err)

	id := mpCreate(t, b, "same", types.ChecksumAlgorithmCrc32c, types.ChecksumTypeFullObject)
	res, err := upc(t, b, "same", id, 1, "bk/src", nil)
	require.NoError(t, err)
	require.NotNil(t, res.ChecksumCRC32C, "CRC32C session: CRC32C")
	require.Equal(t, crc32cB64(data), *res.ChecksumCRC32C, "CRC32C session: CRC32C")
	require.Nil(t, res.ChecksumSHA1, "CRC32C session")
	require.Nil(t, res.ChecksumSHA256, "CRC32C session")

	id = mpCreate(t, b, "recompute", types.ChecksumAlgorithmSha256, types.ChecksumTypeFullObject)
	res, err = upc(t, b, "recompute", id, 1, "bk/src", nil)
	require.NoError(t, err)
	require.NotNil(t, res.ChecksumSHA256, "SHA256 session over a SHA1 source: SHA256")
	require.Equal(t, sha256B64(data), *res.ChecksumSHA256, "SHA256 session over a SHA1 source: SHA256")
	require.Nil(t, res.ChecksumSHA1, "SHA256 session over a SHA1 source")

	id = mpCreate(t, b, "none", "", "")
	res, err = upc(t, b, "none", id, 1, "bk/src", nil)
	require.NoError(t, err)
	require.Nil(t, res.ChecksumCRC32, "session without a checksum echoed one: %+v", res)
	require.Nil(t, res.ChecksumCRC32C, "session without a checksum echoed one: %+v", res)
	require.Nil(t, res.ChecksumSHA1, "session without a checksum echoed one: %+v", res)
	require.Nil(t, res.ChecksumSHA256, "session without a checksum echoed one: %+v", res)
	require.Nil(t, res.ChecksumCRC64NVME, "session without a checksum echoed one: %+v", res)
	_, err = mpComplete(t, b, "none", id, []types.CompletedPart{completed(res, 1)}, nil)
	require.NoError(t, err)
	_, crc64, _, ctype := headChecksum(t, b, "none")
	require.NotNil(t, crc64, "completed object lacks the default CRC64NVME: %v %s", crc64, ctype)
	require.Equal(t, types.ChecksumTypeFullObject, ctype, "completed object lacks the default CRC64NVME: %v %s", crc64, ctype)

	id = mpCreate(t, b, "composite", types.ChecksumAlgorithmCrc32c, types.ChecksumTypeComposite)
	res, err = upc(t, b, "composite", id, 1, "bk/src", nil)
	require.NoError(t, err, "COMPOSITE session")
	require.NotNil(t, res.ChecksumCRC32C, "COMPOSITE session echoed no part checksum: %+v", res)
	// A composite Complete names each part's checksum, as the client would
	// from the copy result.
	part := completed(res, 1)
	part.ChecksumCRC32C = res.ChecksumCRC32C
	_, err = mpComplete(t, b, "composite", id, []types.CompletedPart{part}, nil)
	require.NoError(t, err, "complete COMPOSITE")
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
	require.NoError(t, err)
	return *out.ETag
}

func md5Sum(b []byte) []byte {
	sum := md5.Sum(b)
	return sum[:]
}
