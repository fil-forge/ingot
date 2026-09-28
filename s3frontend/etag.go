package s3frontend

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

// ETags derive from the plaintext SHA-256 digests the write path already
// computes for every body and part, so no MD5 pass runs on ingest (MD5 has no
// hardware acceleration and was the slowest pass over a body).
//
// The shape is the one S3 documents for objects whose ETag is not an MD5: the
// hex digest, a hyphen and the part count. S3 guarantees an MD5 ETag only for
// a single-shot PUT of a plaintext or SSE-S3 object; a multipart-uploaded,
// SSE-KMS or SSE-C object's ETag is opaque, and clients key off the hyphen
// (s3cmd, rclone) or the x-amz-server-side-encryption response header (s3cmd,
// the Java v1 SDK) to stop treating it as an MD5. Every Ingot object is
// encrypted server-side, so every object ETag carries the suffix ("-1" for a
// single PUT or a copy) and every response that carries an ETag carries the
// header (sseResponseHeader in server.go). A part's ETag is the bare hex
// digest, as on S3. The 64-hex digest also fails the 32-hex MD5 shape rclone
// checks for.

// objectETag is the ETag of a single-part object: the hex SHA-256 of its
// bytes with the one-part suffix.
func objectETag(sha256Digest []byte) string {
	return hex.EncodeToString(sha256Digest) + "-1"
}

// partETag is the ETag of an uploaded part: the hex SHA-256 of its bytes.
func partETag(sha256Digest []byte) string {
	return hex.EncodeToString(sha256Digest)
}

// multipartETag is the ETag of a completed multipart upload: the hex SHA-256
// over the parts' digests in part-number order, with the part count.
func multipartETag(partDigests [][]byte) string {
	h := sha256.New()
	for _, d := range partDigests {
		h.Write(d)
	}
	return hex.EncodeToString(h.Sum(nil)) + "-" + strconv.Itoa(len(partDigests))
}
