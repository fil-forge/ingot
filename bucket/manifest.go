package bucket

import "github.com/ipfs/go-cid"

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

	// DeleteMarker flags a tombstone version (no body). It is reserved
	// for S3 versioning, which is not implemented this iteration; it is
	// always false until versioning lands. The version id itself is a
	// function of the MST key, not stored here. See docs/architecture.md §3.
	DeleteMarker bool `cborgen:"dm"`

	// HTTP/S3 system headers carried through PUT and replayed on
	// HEAD/GET. Empty strings are omitted from responses.
	ContentEncoding    string `cborgen:"ce"`
	ContentDisposition string `cborgen:"cd"`
	ContentLanguage    string `cborgen:"cl"`
	CacheControl       string `cborgen:"cc"`

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
	Digest []byte `cborgen:"d"`
	Offset int64  `cborgen:"o"`
	Length int64  `cborgen:"l"`
}
