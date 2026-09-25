package s3frontend

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// TestETagShapes pins the ETag format: a 64-hex SHA-256 with a "-N" suffix
// for objects (never a 32-hex MD5 shape), a bare digest for parts, and a
// multipart ETag that hashes the part digests in the order given.
func TestETagShapes(t *testing.T) {
	d1, d2 := sha256.Sum256([]byte("one")), sha256.Sum256([]byte("two"))

	if got, want := objectETag(d1[:]), hex.EncodeToString(d1[:])+"-1"; got != want {
		t.Fatalf("objectETag = %s, want %s", got, want)
	}
	if got, want := partETag(d1[:]), hex.EncodeToString(d1[:]); got != want {
		t.Fatalf("partETag = %s, want %s", got, want)
	}
	cat := sha256.New()
	cat.Write(d1[:])
	cat.Write(d2[:])
	if got, want := multipartETag([][]byte{d1[:], d2[:]}), hex.EncodeToString(cat.Sum(nil))+"-2"; got != want {
		t.Fatalf("multipartETag = %s, want %s", got, want)
	}
	if multipartETag([][]byte{d1[:], d2[:]}) == multipartETag([][]byte{d2[:], d1[:]}) {
		t.Fatal("multipartETag must depend on part order")
	}
}
