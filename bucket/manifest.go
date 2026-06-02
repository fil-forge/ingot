package bucket

import "github.com/ipfs/go-cid"

// ObjectManifest is the per-object metadata record stored as a CBOR
// block in the IPLD blockstore. The MST leaf for an object key
// points at this record's CID. Body identifies the body DAG; the
// shape of that DAG is determined by Body.Format and read back via
// the matching BodyCodec.
type ObjectManifest struct {
	Key         string `cborgen:"k"`
	ContentType string `cborgen:"ct"`
	Created     int64  `cborgen:"t"`
	Body        Body   `cborgen:"b"`

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

// Body identifies the bytes of an object via a CID and a format
// tag. Format routes the Body to the right BodyCodec implementation
// at read time; Content is the root of whatever block DAG that
// codec produced. Size, SHA256, and MD5 are codec-agnostic — the
// total number of body bytes, the sha256 of the full body, and the
// md5 of the full body, respectively. MD5 is the source for the S3
// ETag wire format (single-part PUTs use hex md5); SHA256 is retained
// for content addressing and integrity checks.
type Body struct {
	Size    int64   `cborgen:"s"`
	SHA256  []byte  `cborgen:"h"`
	MD5     []byte  `cborgen:"m"`
	Content cid.Cid `cborgen:"c"`
	Format  string  `cborgen:"f"`
}

// FormatFixed is the Body.Format value used by FixedChunker — a
// flat array of fixed-size raw blocks indexed by a FixedChunkerIndex
// CBOR document at Body.Content.
const FormatFixed = "fixed-v1"

// FixedChunkerIndex is the body-DAG root for FormatFixed: an
// ordered list of chunk CIDs plus the per-chunk size. The reader
// fetches the index block from Body.Content, then streams the
// chunks. Range arithmetic is direct: byte N lives in chunk
// index N/ChunkSize at offset N%ChunkSize.
type FixedChunkerIndex struct {
	ChunkSize int64     `cborgen:"cs"`
	Chunks    []cid.Cid `cborgen:"c"`
}
