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
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
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
		Body:       bytes.NewReader(data),
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
	if err != nil {
		t.Fatalf("HeadObject %s: %v", key, err)
	}
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
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i+1, err)
		}
		if got := out.ChecksumCRC32C; got == nil || *got != crc32cB64(data) {
			t.Fatalf("part %d echo = %v, want %s", i+1, got, crc32cB64(data))
		}
		etags[i], sums[i] = *out.ETag, *out.ChecksumCRC32C
	}

	one, two := int32(1), int32(2)
	res, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: &etags[0], ChecksumCRC32C: &sums[0]},
		{PartNumber: &two, ETag: &etags[1], ChecksumCRC32C: &sums[1]},
	}, nil)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	want := crc32cB64(append(append([]byte(nil), part1...), part2...))
	if res.ChecksumCRC32C == nil || *res.ChecksumCRC32C != want {
		t.Fatalf("final = %v, want %s", res.ChecksumCRC32C, want)
	}
	if res.ChecksumType == nil || *res.ChecksumType != types.ChecksumTypeFullObject {
		t.Fatalf("type = %v, want FULL_OBJECT", res.ChecksumType)
	}
	hCrc32c, _, _, hType := headChecksum(t, b, key)
	if hCrc32c == nil || *hCrc32c != want || hType != types.ChecksumTypeFullObject {
		t.Fatalf("HEAD echo = %v/%v, want %s/FULL_OBJECT", hCrc32c, hType, want)
	}
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
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i+1, err)
		}
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
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if res.ChecksumSHA256 == nil || *res.ChecksumSHA256 != want {
		t.Fatalf("final = %v, want %s", res.ChecksumSHA256, want)
	}
	if res.ChecksumType == nil || *res.ChecksumType != types.ChecksumTypeComposite {
		t.Fatalf("type = %v, want COMPOSITE", res.ChecksumType)
	}
	// The composite value persists on the manifest with its type.
	_, _, hSha, hType := headChecksum(t, b, key)
	if hSha == nil || *hSha != want || hType != types.ChecksumTypeComposite {
		t.Fatalf("HEAD echo = %v/%v, want %s/COMPOSITE", hSha, hType, want)
	}

	// A wrong final checksum on a fresh session of the same shape fails.
	key2 := key + "-bad-final"
	uploadID2 := mpCreate(t, b, key2, types.ChecksumAlgorithmSha256, types.ChecksumTypeComposite)
	out, err := mpUploadPart(t, b, key2, uploadID2, 1, part2, func(in *s3.UploadPartInput) {
		in.ChecksumSHA256 = &sum2
	})
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	bogus := sha256B64([]byte("not the composite")) + "-1"
	_, err = mpComplete(t, b, key2, uploadID2, []types.CompletedPart{
		{PartNumber: &one, ETag: out.ETag, ChecksumSHA256: &sum2},
	}, func(in *s3.CompleteMultipartUploadInput) {
		in.ChecksumSHA256 = &bogus
	})
	if code := apiErrCode(t, err); code != "BadDigest" {
		t.Fatalf("wrong final → %s, want BadDigest", code)
	}
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
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i+1, err)
		}
		if out.ChecksumCRC64NVME != nil {
			t.Fatalf("undeclared session echoed a checksum: %s", *out.ChecksumCRC64NVME)
		}
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
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if res.ChecksumCRC64NVME == nil || *res.ChecksumCRC64NVME == bogus {
		t.Fatalf("final = %v, want a derived (non-bogus) CRC64NVME", res.ChecksumCRC64NVME)
	}
	if res.ChecksumType == nil || *res.ChecksumType != types.ChecksumTypeFullObject {
		t.Fatalf("type = %v, want FULL_OBJECT", res.ChecksumType)
	}

	// Cross-check the combine: a single PUT of the same bytes stores the same
	// default CRC64NVME.
	putObj(t, b, "single-shot", append(append([]byte(nil), part1...), part2...))
	_, mpCrc, _, _ := headChecksum(t, b, key)
	_, singleCrc, _, _ := headChecksum(t, b, "single-shot")
	if mpCrc == nil || singleCrc == nil || *mpCrc != *singleCrc {
		t.Fatalf("multipart CRC64NVME %v != single-PUT %v", mpCrc, singleCrc)
	}
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
	if code := apiErrCode(t, err); code != "InvalidRequest" {
		t.Fatalf("algo mismatch → %s, want InvalidRequest", code)
	}

	// COMPOSITE session, no part checksum.
	uploadID = mpCreate(t, b, "neg-b", types.ChecksumAlgorithmSha256, types.ChecksumTypeComposite)
	_, err = mpUploadPart(t, b, "neg-b", uploadID, 1, data, nil)
	if code := apiErrCode(t, err); code != "InvalidRequest" {
		t.Fatalf("composite without checksum → %s, want InvalidRequest", code)
	}

	// Supplied value doesn't match the bytes.
	uploadID = mpCreate(t, b, "neg-c", types.ChecksumAlgorithmCrc32c, types.ChecksumTypeFullObject)
	wrong := crc32cB64([]byte("different bytes"))
	_, err = mpUploadPart(t, b, "neg-c", uploadID, 1, data, func(in *s3.UploadPartInput) {
		in.ChecksumCRC32C = &wrong
	})
	if code := apiErrCode(t, err); code != "BadDigest" {
		t.Fatalf("wrong value → %s, want BadDigest", code)
	}
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
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	one := int32(1)

	// Complete's checksum type must match the session's.
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: out.ETag, ChecksumCRC32C: &sum},
	}, func(in *s3.CompleteMultipartUploadInput) {
		in.ChecksumType = types.ChecksumTypeComposite
	})
	if code := apiErrCode(t, err); code != "InvalidRequest" {
		t.Fatalf("type mismatch → %s, want InvalidRequest", code)
	}

	// A declared session requires each part entry to carry its checksum.
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: out.ETag},
	}, nil)
	if code := apiErrCode(t, err); code != "InvalidRequest" {
		t.Fatalf("missing part checksum → %s, want InvalidRequest", code)
	}

	// More than one checksum on a part entry.
	shaSum := sha256B64(data)
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: out.ETag, ChecksumCRC32C: &sum, ChecksumSHA256: &shaSum},
	}, nil)
	if code := apiErrCode(t, err); code != "InvalidArgument" {
		t.Fatalf("multiple checksums → %s, want InvalidArgument", code)
	}

	// A wrong value for the session's algorithm is an InvalidPart.
	wrong := crc32cB64([]byte("different bytes"))
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: out.ETag, ChecksumCRC32C: &wrong},
	}, nil)
	if code := apiErrCode(t, err); code != "InvalidPart" {
		t.Fatalf("wrong same-algo value → %s, want InvalidPart", code)
	}

	// A checksum for a different algorithm is a BadDigest.
	_, err = mpComplete(t, b, key, uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: out.ETag, ChecksumSHA256: &shaSum},
	}, nil)
	if code := apiErrCode(t, err); code != "BadDigest" {
		t.Fatalf("wrong-algo value → %s, want BadDigest", code)
	}

	// The valid entry still completes, and re-Complete (idempotent) returns
	// the identical checksum.
	res, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: out.ETag, ChecksumCRC32C: &sum},
	}, nil)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	res2, err := mpComplete(t, b, key, uploadID, []types.CompletedPart{
		{PartNumber: &one, ETag: out.ETag, ChecksumCRC32C: &sum},
	}, nil)
	if err != nil {
		t.Fatalf("re-Complete: %v", err)
	}
	if res.ChecksumCRC32C == nil || res2.ChecksumCRC32C == nil || *res.ChecksumCRC32C != *res2.ChecksumCRC32C {
		t.Fatalf("idempotent re-Complete checksum %v != %v", res2.ChecksumCRC32C, res.ChecksumCRC32C)
	}
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
	if _, err := mpUploadPart(t, b, key, uploadID, 1, dataA, nil); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	res, err := b.ListParts(context.Background(), &s3.ListPartsInput{Bucket: &bucket, Key: &key, UploadId: &uploadID})
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if res.ChecksumAlgorithm != types.ChecksumAlgorithmCrc32c || res.ChecksumType != types.ChecksumTypeFullObject {
		t.Fatalf("declared session: algo/type = %s/%s", res.ChecksumAlgorithm, res.ChecksumType)
	}
	if len(res.Parts) != 1 || res.Parts[0].ChecksumCRC32C == nil || *res.Parts[0].ChecksumCRC32C != crc32cB64(dataA) {
		t.Fatalf("part checksum not echoed: %+v", res.Parts)
	}

	// Re-uploading the part supersedes its stored checksum.
	if _, err := mpUploadPart(t, b, key, uploadID, 1, dataB, nil); err != nil {
		t.Fatalf("re-UploadPart: %v", err)
	}
	res, err = b.ListParts(context.Background(), &s3.ListPartsInput{Bucket: &bucket, Key: &key, UploadId: &uploadID})
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(res.Parts) != 1 || res.Parts[0].ChecksumCRC32C == nil || *res.Parts[0].ChecksumCRC32C != crc32cB64(dataB) {
		t.Fatalf("superseded part checksum not updated: %+v", res.Parts)
	}

	key = "lp-null"
	uploadID = mpCreate(t, b, key, "", "")
	if _, err := mpUploadPart(t, b, key, uploadID, 1, dataA, nil); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	res, err = b.ListParts(context.Background(), &s3.ListPartsInput{Bucket: &bucket, Key: &key, UploadId: &uploadID})
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if string(res.ChecksumAlgorithm) != "null" || string(res.ChecksumType) != "null" {
		t.Fatalf("undeclared session: algo/type = %q/%q, want null/null", res.ChecksumAlgorithm, res.ChecksumType)
	}
	if res.Parts[0].ChecksumCRC64NVME != nil {
		t.Fatalf("internal CRC64NVME leaked into ListParts: %s", *res.Parts[0].ChecksumCRC64NVME)
	}
}
