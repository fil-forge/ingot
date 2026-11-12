package preload

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"regexp"
)

// BucketName returns the name of the i'th bucket a load writes.
func BucketName(prefix string, i int) string {
	return fmt.Sprintf("%s%03d", prefix, i)
}

// bucketNameRE is the subset of S3's bucket naming rules a load's buckets
// keep to: lowercase letters, digits and hyphens, starting and ending with a
// letter or digit.
var bucketNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)

// ValidBucketName reports whether name is one a load may write to.
func ValidBucketName(name string) bool {
	return bucketNameRE.MatchString(name)
}

// BulkKey returns the key of bulk object n in bucket: prefix followed by the
// lowercase hex SHA-256 of the seed, the bucket and n. Bulk keys look like
// pack digests, arrive in no particular order, and are the same on every run
// with the same seed.
func BulkKey(prefix, seed, bucket string, n uint64) string {
	h := sha256.New()
	writeField(h, "bulk")
	writeField(h, seed)
	writeField(h, bucket)
	writeUint(h, n)
	return prefix + hex.EncodeToString(h.Sum(nil))
}

// ValidateKey returns the key of object i a validation run writes in bucket:
// prefix followed by the lowercase hex SHA-256 of the seed, the bucket, the
// run and i, shaped like the load's keys and distinct from all of them.
func ValidateKey(prefix, seed, bucket, run string, i uint64) string {
	h := sha256.New()
	writeField(h, "validate")
	writeField(h, seed)
	writeField(h, bucket)
	writeField(h, run)
	writeUint(h, i)
	return prefix + hex.EncodeToString(h.Sum(nil))
}

// RealKey returns the key of an object whose body hashes to sum: prefix
// followed by the digest in lowercase hex, as the pack server names a pack.
func RealKey(prefix string, sum [sha256.Size]byte) string {
	return prefix + hex.EncodeToString(sum[:])
}

// realSeed returns the seed of real object j's body in bucket.
func realSeed(seed, bucket string, j uint64) [32]byte {
	h := sha256.New()
	writeField(h, "real")
	writeField(h, seed)
	writeField(h, bucket)
	writeUint(h, j)
	var out [32]byte
	h.Sum(out[:0])
	return out
}

// RealSize returns the size of real object j's body in bucket, between min
// and max bytes inclusive, the same on every run with the same seed.
func RealSize(seed, bucket string, j uint64, min, max int64) int64 {
	if max <= min {
		return min
	}
	h := sha256.New()
	writeField(h, "size")
	writeField(h, seed)
	writeField(h, bucket)
	writeUint(h, j)
	v := binary.BigEndian.Uint64(h.Sum(nil))
	return min + int64(v%uint64(max-min+1))
}

// RealBody returns real object j's body in bucket.
func RealBody(seed, bucket string, j uint64, min, max int64) *Body {
	return NewBody(realSeed(seed, bucket, j), RealSize(seed, bucket, j, min, max))
}

// writeField writes s to h prefixed with its length, so that no two
// sequences of fields hash the same bytes.
func writeField(h hash.Hash, s string) {
	writeUint(h, uint64(len(s)))
	h.Write([]byte(s))
}

func writeUint(h hash.Hash, v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	h.Write(b[:])
}
