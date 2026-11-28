package s3frontend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/stretchr/testify/require"
)

// Multipart checksum tests (FIL-620): per-part checksums at UploadPart, the
// ListParts echo, and the composite / full-object / default final checksum at
// Complete, all against the in-process harness. The conformance partition
// pins the same behaviors end-to-end over HTTP.

func crc32cB64(data []byte) string {
	sum := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], sum)
	return base64.StdEncoding.EncodeToString(b[:])
}

func sha256B64(data []byte) string {
	sum := sha256.Sum256(data)
	return base64.StdEncoding.EncodeToString(sum[:])
}

func mpCreate(t *testing.T, b *Backend, key string, algo types.ChecksumAlgorithm, ctype types.ChecksumType) string {
	t.Helper()
	bucket := "bk"
	res, err := b.CreateMultipartUpload(context.Background(), s3response.CreateMultipartUploadInput{
		Bucket:            &bucket,
		Key:               &key,
		ChecksumAlgorithm: algo,
		ChecksumType:      ctype,
	})
	require.NoError(t, err, "CreateMultipartUpload")
	return res.UploadId
}

func mpUploadPart(t *testing.T, b *Backend, key, uploadID string, n int32, data []byte, mod func(*s3.UploadPartInput)) (*s3.UploadPartOutput, error) {
	t.Helper()
	bucket := "bk"
	in := &s3.UploadPartInput{
		Bucket:     &bucket,
		Key:        &key,
		UploadId:   &uploadID,
		PartNumber: &n,
		Body:       bytes.NewReader(data), ContentLength: byteLen(data),
	}
	if mod != nil {
		mod(in)
	}
	return b.UploadPart(context.Background(), in)
}

func mpComplete(t *testing.T, b *Backend, key, uploadID string, parts []types.CompletedPart, mod func(*s3.CompleteMultipartUploadInput)) (s3response.CompleteMultipartUploadResult, error) {
	t.Helper()
	bucket := "bk"
	in := &s3.CompleteMultipartUploadInput{
		Bucket:          &bucket,
		Key:             &key,
		UploadId:        &uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}
	if mod != nil {
		mod(in)
	}
	res, _, err := b.CompleteMultipartUpload(context.Background(), in)
	return res, err
}

func headChecksum(t *testing.T, b *Backend, key string) (crc32c, crc64, sha256v *string, ctype types.ChecksumType) {
	t.Helper()
	bucket := "bk"
	out, err := b.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket:       &bucket,
		Key:          &key,
		ChecksumMode: types.ChecksumModeEnabled,
	})
	require.NoError(t, err, "HeadObject %s", key)
	return out.ChecksumCRC32C, out.ChecksumCRC64NVME, out.ChecksumSHA256, out.ChecksumType
}

// TestMultipartChecksum_FullObjectCRC32C drives a FULL_OBJECT CRC32C session
// end-to-end with no client-supplied part checksums: UploadPart computes and
// echoes each part's CRC32C, and Complete combines them into the whole-body
// CRC32C — equal to a direct CRC over the concatenated bytes — which HEAD
// then echoes with its type.
func TestMultipartChecksum_FullObjectCRC32C(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	key := "mp-full-crc32c"
	part1 := bytes.Repeat([]byte("a"), int(backend.MinPartSize))
	part2 := []byte("tail-bytes")

	uploadID := mpCreate(t, b, key, types.ChecksumAlgorithmCrc32c, types.ChecksumTypeFullObject)
	var etags, sums [2]string
	for i, data := range [][]byte{part1, part2} {
		out, err := mpUploadPart(t, b, key, uploadID, int32(i+1), data, nil)
		require.NoError(t, err, "UploadPart %d", i+1)
		require.NotNil(t, out.ChecksumCRC32C, "part %d echo", i+1)
		require.Equal(t, crc32cB64(data), *out.ChecksumCRC32C, "part %d echo", i+1)
		etags[i], sums[i] = *out.ETag, *out.ChecksumCRC32C
	}

	one, two := int32(1), int32(2)
	res, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: &etags[0], ChecksumCRC32C: &sums[0]},
		{PartNumber: &two, ETag: &etags[1], ChecksumCRC32C: &sums[1]},
	}, nil)
	require.NoError(t, err, "Complete")
	want := crc32cB64(append(append([]byte(nil), part1...), part2...))
	require.NotNil(t, res.ChecksumCRC32C, "final")
	require.Equal(t, want, *res.ChecksumCRC32C, "final")
	require.NotNil(t, res.ChecksumType, "type")
	require.Equal(t, types.ChecksumTypeFullObject, *res.ChecksumType, "type")
	hCrc32c, _, _, hType := headChecksum(t, b, key)
	require.NotNil(t, hCrc32c, "HEAD echo = %v/%v", hCrc32c, hType)
	require.Equal(t, want, *hCrc32c, "HEAD echo = %v/%v", hCrc32c, hType)
	require.Equal(t, types.ChecksumTypeFullObject, hType, "HEAD echo = %v/%v", hCrc32c, hType)
}

// TestMultipartChecksum_CompositeSHA256 drives a COMPOSITE SHA256 session:
// every part must carry its checksum, and the final value is the
// checksum-of-checksums with the "-N" part-count suffix. A correct final
// value passes; a wrong one is a BadDigest.
func TestMultipartChecksum_CompositeSHA256(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	key := "mp-composite-sha256"
	part1 := bytes.Repeat([]byte("b"), int(backend.MinPartSize))
	part2 := []byte("tail-bytes")
	sum1, sum2 := sha256B64(part1), sha256B64(part2)

	uploadID := mpCreate(t, b, key, types.ChecksumAlgorithmSha256, types.ChecksumTypeComposite)
	var etags [2]string
	for i, tc := range []struct {
		data []byte
		sum  string
	}{{part1, sum1}, {part2, sum2}} {
		out, err := mpUploadPart(t, b, key, uploadID, int32(i+1), tc.data, func(in *s3.UploadPartInput) {
			in.ChecksumSHA256 = &tc.sum
		})
		require.NoError(t, err, "UploadPart %d", i+1)
		etags[i] = *out.ETag
	}

	raw1, _ := base64.StdEncoding.DecodeString(sum1)
	raw2, _ := base64.StdEncoding.DecodeString(sum2)
	want := sha256B64(append(raw1, raw2...)) + "-2"

	one, two := int32(1), int32(2)
	parts := []types.CompletedPart{
		{PartNumber: &one, ETag: &etags[0], ChecksumSHA256: &sum1},
		{PartNumber: &two, ETag: &etags[1], ChecksumSHA256: &sum2},
	}
	res, err := mpComplete(t, b, key, uploadID, parts, func(in *s3.CompleteMultipartUploadInput) {
		in.ChecksumType = types.ChecksumTypeComposite
		in.ChecksumSHA256 = &want
	})
	require.NoError(t, err, "Complete")
	require.NotNil(t, res.ChecksumSHA256, "final")
	require.Equal(t, want, *res.ChecksumSHA256, "final")
	require.NotNil(t, res.ChecksumType, "type")
	require.Equal(t, types.ChecksumTypeComposite, *res.ChecksumType, "type")
	// The composite value persists on the manifest with its type.
	_, _, hSha, hType := headChecksum(t, b, key)
	require.NotNil(t, hSha, "HEAD echo = %v/%v", hSha, hType)
	require.Equal(t, want, *hSha, "HEAD echo = %v/%v", hSha, hType)
	require.Equal(t, types.ChecksumTypeComposite, hType, "HEAD echo = %v/%v", hSha, hType)

	// A wrong final checksum on a fresh session of the same shape fails.
	key2 := key + "-bad-final"
	uploadID2 := mpCreate(t, b, key2, types.ChecksumAlgorithmSha256, types.ChecksumTypeComposite)
	out, err := mpUploadPart(t, b, key2, uploadID2, 1, part2, func(in *s3.UploadPartInput) {
		in.ChecksumSHA256 = &sum2
	})
	require.NoError(t, err, "UploadPart")
	bogus := sha256B64([]byte("not the composite")) + "-1"
	_, err = mpComplete(t, b, key2, uploadID2, []types.CompletedPart{
		{PartNumber: &one, ETag: out.ETag, ChecksumSHA256: &sum2},
	}, func(in *s3.CompleteMultipartUploadInput) {
		in.ChecksumSHA256 = &bogus
	})
	code := apiErrCode(t, err)
	require.Equal(t, "BadDigest", code, "wrong final → %s", code)
}

// TestMultipartChecksum_DefaultCRC64NVME: a session that declares no checksum
// still yields S3's default full-object CRC64NVME at Complete, derived from
// the internal per-part values — identical to the checksum a single PUT of
// the same bytes stores — and any client-supplied final checksum is ignored.
func TestMultipartChecksum_DefaultCRC64NVME(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	key := "mp-default-crc64"
	part1 := bytes.Repeat([]byte("c"), int(backend.MinPartSize))
	part2 := []byte("tail-bytes")

	uploadID := mpCreate(t, b, key, "", "")
	var etags [2]string
	for i, data := range [][]byte{part1, part2} {
		out, err := mpUploadPart(t, b, key, uploadID, int32(i+1), data, nil)
		require.NoError(t, err, "UploadPart %d", i+1)
		require.Nil(t, out.ChecksumCRC64NVME, "undeclared session echoed a checksum")
		etags[i] = *out.ETag
	}

	one, two := int32(1), int32(2)
	bogus := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0xff}, 8))
	res, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: &etags[0]},
		{PartNumber: &two, ETag: &etags[1]},
	}, func(in *s3.CompleteMultipartUploadInput) {
		// Ignored: the session declared no checksum, so the derived value is
		// authoritative.
		in.ChecksumCRC64NVME = &bogus
	})
	require.NoError(t, err, "Complete")
	require.NotNil(t, res.ChecksumCRC64NVME, "final = %v, want a derived (non-bogus) CRC64NVME", res.ChecksumCRC64NVME)
	require.NotEqual(t, bogus, *res.ChecksumCRC64NVME, "final = %v, want a derived (non-bogus) CRC64NVME", res.ChecksumCRC64NVME)
	require.NotNil(t, res.ChecksumType, "type")
	require.Equal(t, types.ChecksumTypeFullObject, *res.ChecksumType, "type")

	// Cross-check the combine: a single PUT of the same bytes stores the same
	// default CRC64NVME.
	putObj(t, b, "single-shot", append(append([]byte(nil), part1...), part2...))
	_, mpCrc, _, _ := headChecksum(t, b, key)
	_, singleCrc, _, _ := headChecksum(t, b, "single-shot")
	require.NotNil(t, mpCrc, "multipart CRC64NVME %v != single-PUT %v", mpCrc, singleCrc)
	require.NotNil(t, singleCrc, "multipart CRC64NVME %v != single-PUT %v", mpCrc, singleCrc)
	require.Equal(t, *singleCrc, *mpCrc, "multipart CRC64NVME %v != single-PUT %v", mpCrc, singleCrc)
}

// TestUploadPart_ChecksumNegotiation covers the UploadPart-time rejections:
// an algorithm differing from the session's, a COMPOSITE part without a
// checksum, and a supplied value that doesn't match the bytes.
func TestUploadPart_ChecksumNegotiation(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	data := []byte("part-bytes")

	// Part algorithm differs from the session's declaration.
	uploadID := mpCreate(t, b, "neg-a", types.ChecksumAlgorithmCrc32, types.ChecksumTypeFullObject)
	sha1sum := "2jmj7l5rSw0yVb/vlWAYkK/YBwk=" // sha1("")
	_, err := mpUploadPart(t, b, "neg-a", uploadID, 1, data, func(in *s3.UploadPartInput) {
		in.ChecksumSHA1 = &sha1sum
	})
	code := apiErrCode(t, err)
	require.Equal(t, "InvalidRequest", code, "algo mismatch → %s", code)

	// COMPOSITE session, no part checksum.
	uploadID = mpCreate(t, b, "neg-b", types.ChecksumAlgorithmSha256, types.ChecksumTypeComposite)
	_, err = mpUploadPart(t, b, "neg-b", uploadID, 1, data, nil)
	require.Equal(t, "InvalidRequest", apiErrCode(t, err), "composite without checksum")

	// Supplied value doesn't match the bytes.
	uploadID = mpCreate(t, b, "neg-c", types.ChecksumAlgorithmCrc32c, types.ChecksumTypeFullObject)
	wrong := crc32cB64([]byte("different bytes"))
	_, err = mpUploadPart(t, b, "neg-c", uploadID, 1, data, func(in *s3.UploadPartInput) {
		in.ChecksumCRC32C = &wrong
	})
	require.Equal(t, "BadDigest", apiErrCode(t, err), "wrong value")
}

// TestComplete_PartChecksumValidation covers the Complete-time per-part and
// type checks against a FULL_OBJECT CRC32C session.
func TestComplete_PartChecksumValidation(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	key := "complete-validate"
	data := []byte("the-part-bytes")
	sum := crc32cB64(data)

	uploadID := mpCreate(t, b, key, types.ChecksumAlgorithmCrc32c, types.ChecksumTypeFullObject)
	out, err := mpUploadPart(t, b, key, uploadID, 1, data, nil)
	require.NoError(t, err, "UploadPart")
	one := int32(1)

	// Complete's checksum type must match the session's.
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: out.ETag, ChecksumCRC32C: &sum},
	}, func(in *s3.CompleteMultipartUploadInput) {
		in.ChecksumType = types.ChecksumTypeComposite
	})
	code := apiErrCode(t, err)
	require.Equal(t, "InvalidRequest", code, "type mismatch → %s", code)

	// A declared session requires each part entry to carry its checksum.
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: out.ETag},
	}, nil)
	require.Equal(t, "InvalidRequest", apiErrCode(t, err), "missing part checksum")

	// More than one checksum on a part entry.
	shaSum := sha256B64(data)
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: out.ETag, ChecksumCRC32C: &sum, ChecksumSHA256: &shaSum},
	}, nil)
	require.Equal(t, "InvalidArgument", apiErrCode(t, err), "multiple checksums")

	// A wrong value for the session's algorithm is an InvalidPart.
	wrong := crc32cB64([]byte("different bytes"))
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: out.ETag, ChecksumCRC32C: &wrong},
	}, nil)
	require.Equal(t, "InvalidPart", apiErrCode(t, err), "wrong same-algo value")

	// A checksum for a different algorithm is a BadDigest.
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: out.ETag, ChecksumSHA256: &shaSum},
	}, nil)
	require.Equal(t, "BadDigest", apiErrCode(t, err), "wrong-algo value")

	// The valid entry completes with a checksum; the idempotent re-Complete
	// succeeds but omits the checksum, matching AWS (verified: re-Complete
	// returns the ETag with no x-amz-checksum-* value).
	res, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: out.ETag, ChecksumCRC32C: &sum},
	}, nil)
	require.NoError(t, err, "Complete")
	require.NotNil(t, res.ChecksumCRC32C, "first Complete missing ChecksumCRC32C")
	res2, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: out.ETag, ChecksumCRC32C: &sum},
	}, nil)
	require.NoError(t, err, "re-Complete")
	require.Nil(t, res2.ChecksumCRC32C, "re-Complete should omit the checksum (AWS)")
}

// TestListParts_Checksums: a declared session echoes each part's checksum and
// the session algorithm/type; an undeclared session reports the literal
// "null" pair and hides the internal CRC64NVME.
func TestListParts_Checksums(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	bucket := "bk"
	dataA, dataB := []byte("part-bytes-a"), []byte("part-bytes-b")

	key := "lp-declared"
	uploadID := mpCreate(t, b, key, types.ChecksumAlgorithmCrc32c, types.ChecksumTypeFullObject)
	_, err := mpUploadPart(t, b, key, uploadID, 1, dataA, nil)
	require.NoError(t, err, "UploadPart")
	res, err := b.ListParts(context.Background(), &s3.ListPartsInput{Bucket: &bucket, Key: &key, UploadId: &uploadID})
	require.NoError(t, err, "ListParts")
	require.Equal(t, types.ChecksumAlgorithmCrc32c, res.ChecksumAlgorithm, "declared session: algo/type = %s/%s", res.ChecksumAlgorithm, res.ChecksumType)
	require.Equal(t, types.ChecksumTypeFullObject, res.ChecksumType, "declared session: algo/type = %s/%s", res.ChecksumAlgorithm, res.ChecksumType)
	require.Len(t, res.Parts, 1, "part checksum not echoed")
	require.NotNil(t, res.Parts[0].ChecksumCRC32C, "part checksum not echoed")
	require.Equal(t, crc32cB64(dataA), *res.Parts[0].ChecksumCRC32C, "part checksum not echoed")

	// Re-uploading the part supersedes its stored checksum.
	_, err = mpUploadPart(t, b, key, uploadID, 1, dataB, nil)
	require.NoError(t, err, "re-UploadPart")
	res, err = b.ListParts(context.Background(), &s3.ListPartsInput{Bucket: &bucket, Key: &key, UploadId: &uploadID})
	require.NoError(t, err, "ListParts")
	require.Len(t, res.Parts, 1, "superseded part checksum not updated")
	require.NotNil(t, res.Parts[0].ChecksumCRC32C, "superseded part checksum not updated")
	require.Equal(t, crc32cB64(dataB), *res.Parts[0].ChecksumCRC32C, "superseded part checksum not updated")

	key = "lp-null"
	uploadID = mpCreate(t, b, key, "", "")
	_, err = mpUploadPart(t, b, key, uploadID, 1, dataA, nil)
	require.NoError(t, err, "UploadPart")
	res, err = b.ListParts(context.Background(), &s3.ListPartsInput{Bucket: &bucket, Key: &key, UploadId: &uploadID})
	require.NoError(t, err, "ListParts")
	require.Equal(t, "null", string(res.ChecksumAlgorithm), "undeclared session: algo")
	require.Equal(t, "null", string(res.ChecksumType), "undeclared session: type")
	require.Nil(t, res.Parts[0].ChecksumCRC64NVME, "internal CRC64NVME leaked into ListParts")
}

// TestCopyObject_Checksums: a copy without a checksum request propagates the
// source's checksum verbatim; a copy naming a different algorithm replaces it
// with a recomputed full-object value (the old algorithm's field goes nil),
// and the result agrees with a subsequent HEAD.
func TestCopyObject_Checksums(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	bucket := "bk"
	data := []byte("copy-source-bytes")
	crcSum := crc32cB64(data)

	srcKey := "cp-src"
	_, err := b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket: &bucket,
		Key:    &srcKey,
		Body:   bytes.NewReader(data), ContentLength: byteLen(data),
		ChecksumCRC32C: &crcSum,
	})
	require.NoError(t, err, "PutObject")

	// No checksum request → the source's CRC32C carries over.
	dstKey, src := "cp-propagate", bucket+"/"+srcKey
	out, err := b.CopyObject(context.Background(), s3response.CopyObjectInput{
		Bucket: &bucket, Key: &dstKey, CopySource: &src,
	})
	require.NoError(t, err, "CopyObject")
	require.NotNil(t, out.CopyObjectResult.ChecksumCRC32C, "propagate: result CRC32C")
	require.Equal(t, crcSum, *out.CopyObjectResult.ChecksumCRC32C, "propagate: result CRC32C")
	require.Equal(t, types.ChecksumTypeFullObject, out.CopyObjectResult.ChecksumType, "propagate: type")
	hCrc32c, _, _, _ := headChecksum(t, b, dstKey)
	require.NotNil(t, hCrc32c, "propagate: HEAD CRC32C")
	require.Equal(t, crcSum, *hCrc32c, "propagate: HEAD CRC32C")

	// A different requested algorithm replaces the checksum: SHA256 set to the
	// recomputed body digest, the source's CRC32C dropped.
	dstKey = "cp-replace"
	out, err = b.CopyObject(context.Background(), s3response.CopyObjectInput{
		Bucket: &bucket, Key: &dstKey, CopySource: &src,
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	require.NoError(t, err, "CopyObject")
	require.NotNil(t, out.CopyObjectResult.ChecksumSHA256, "replace: result SHA256")
	require.Equal(t, sha256B64(data), *out.CopyObjectResult.ChecksumSHA256, "replace: result SHA256")
	require.Nil(t, out.CopyObjectResult.ChecksumCRC32C, "replace: source CRC32C survived")
	_, _, hSha, hType := headChecksum(t, b, dstKey)
	require.NotNil(t, hSha, "replace: HEAD SHA256")
	require.Equal(t, sha256B64(data), *hSha, "replace: HEAD SHA256")
	require.Equal(t, types.ChecksumTypeFullObject, hType, "replace: HEAD checksum type")

	// Self-copy with REPLACE swapping the algorithm again.
	selfSrc := bucket + "/" + dstKey
	out, err = b.CopyObject(context.Background(), s3response.CopyObjectInput{
		Bucket: &bucket, Key: &dstKey, CopySource: &selfSrc,
		MetadataDirective: types.MetadataDirectiveReplace,
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32c,
	})
	require.NoError(t, err, "self-copy")
	require.NotNil(t, out.CopyObjectResult.ChecksumCRC32C, "self-copy: result CRC32C")
	require.Equal(t, crcSum, *out.CopyObjectResult.ChecksumCRC32C, "self-copy: result CRC32C")
	require.Nil(t, out.CopyObjectResult.ChecksumSHA256, "self-copy: SHA256 survived")
}
