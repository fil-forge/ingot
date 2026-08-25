package bucket

import (
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// ObjectManifest is the per-object metadata record stored as a CBOR
// block in the IPLD blockstore. The MST leaf for an object key points
// at this record's CID. Body describes the object's bytes as an
// ordered list of content-addressed blobs (see Body / BlobRef).
type ObjectManifest struct {
	Key         string `cborgen:"k"`
	ContentType string `cborgen:"ct"`
	Created     int64  `cborgen:"t"`
	Body        Body   `cborgen:"b"`

	// ETag is the object's S3 ETag stored verbatim (the hex md5 of the
	// body for a single-part object, or hex(md5(concat of part md5s)) +
	// "-N" for a multipart object). It is stored rather than derived
	// because a multipart ETag cannot be recomputed from the body bytes.
	ETag string `cborgen:"e"`

	// DeleteMarker flags a tombstone version: a zero Body, no ETag, and no
	// blob claims. Markers are versions like any other — they occupy a leaf
	// slot and carry a Seq/VersionID. See docs/s3-versioning.md §2.3.
	DeleteMarker bool `cborgen:"dm"`

	// Seq is the version's per-bucket ordinal (== its leaf/prev-tree
	// position); VersionID is its S3 client handle — "null" for a
	// null version, else the ULID token minted from Seq. Zero/empty only
	// in pre-versioning blocks. See docs/s3-versioning.md §2–§3.
	Seq       uint64 `cborgen:"sq"`
	VersionID string `cborgen:"vi"`

	// ChecksumAlgorithm / Checksum carry the S3 additional checksum the client
	// requested at PUT (x-amz-checksum-*), if any: the algorithm name (e.g.
	// "SHA256", "CRC32") and the base64 value computed over the body. Stored so
	// GET/HEAD can echo it under x-amz-checksum-mode. Empty when no additional
	// checksum was requested. Independent of the internal sha256 content address.
	ChecksumAlgorithm string `cborgen:"ca"`
	Checksum          string `cborgen:"ck"`
	// ChecksumType is the S3 checksum type of Checksum: "FULL_OBJECT" (computed
	// over the whole body) or "COMPOSITE" (a multipart checksum-of-checksums
	// with a "-N" part-count suffix). Empty in blocks written before the type
	// was recorded, which are all full-object.
	ChecksumType string `cborgen:"cy"`

	// HTTP/S3 system headers carried through PUT and replayed on
	// HEAD/GET. Empty strings are omitted from responses.
	ContentEncoding    string `cborgen:"ce"`
	ContentDisposition string `cborgen:"cd"`
	ContentLanguage    string `cborgen:"cl"`
	CacheControl       string `cborgen:"cc"`

	// Expires is the HTTP `Expires` caching header (RFC 7234) carried through
	// PUT and replayed verbatim on GET/HEAD. Despite the name it is NOT object
	// lifecycle/TTL — it never deletes the object; it is a passthrough system
	// header like CacheControl. (Real S3 Lifecycle expiration is a separate,
	// unimplemented feature — see docs/architecture.md §12.)
	Expires string `cborgen:"ex"`

	// WebsiteRedirectLocation is the S3 `x-amz-website-redirect-location`
	// system header carried through PUT/COPY and replayed on HEAD/GET. It
	// requests a redirect (or an object-scoped redirect) when the bucket is
	// configured as a static website; Ingot stores and echoes it but does not
	// itself serve website redirects. Empty when unset.
	WebsiteRedirectLocation string `cborgen:"wr"`

	// Metadata is the user metadata map (the x-amz-meta-* headers, with
	// the prefix stripped and keys lower-cased by the S3 layer). Nil when
	// the object carries no user metadata.
	Metadata map[string]string `cborgen:"md"`
}

// Body describes an object's bytes as an ordered, contiguous list of
// content-addressed blobs that together cover [0, Size). A small object
// is a single blob; an object larger than max_blob_size is a coarse
// split into N blobs (no fine chunking); a multipart object is the
// ordered union of its parts' blobs. A zero-byte object has no blobs.
//
// Size and SHA256 are whole-object values (the total byte count and the
// sha256 of the full body, for integrity). MD5 is the whole-object md5,
// the source for a single-part object's S3 ETag.
type Body struct {
	Size   int64     `cborgen:"s"`
	SHA256 []byte    `cborgen:"h"`
	MD5    []byte    `cborgen:"m"`
	Blobs  []BlobRef `cborgen:"bl"`

	// PartSizes records the byte length of each multipart part, in upload order,
	// segmenting [0, Size) into the parts the client completed. It lets a GET/HEAD
	// with ?partNumber=N return part N's byte span and the x-amz-mp-parts-count
	// header (the Blobs list alone cannot, since a part may span several blobs or
	// share a blob boundary). Nil for a single-PUT object, which has no parts — a
	// ?partNumber=1 there addresses the whole object and omits the parts count.
	// The sum of PartSizes equals Size. See docs/architecture.md §7.2.
	PartSizes []int64 `cborgen:"ps"`

	// IndexRoot is reserved (nullable) for a future UnixFS + sharded-dag-index
	// record that would make a multi-shard object reassemblable without Ingot
	// ("credible exit"). It is unused this iteration; the flat Blobs list is
	// the working index. See docs/architecture.md §8.
	IndexRoot *cid.Cid `cborgen:"ir"`
}

// BlobRef names one body blob: the sha256 multihash Piri stores it
// under (the same multihash as the blob's raw-codec CID), and the
// half-open byte span [Offset, Offset+Length) it covers within the
// object. The ordered BlobRef list lets a ranged GET map a byte range
// to the covering blob(s).
type BlobRef struct {
	Digest mh.Multihash `cborgen:"d"`
	Offset int64        `cborgen:"o"`
	Length int64        `cborgen:"l"`
}
