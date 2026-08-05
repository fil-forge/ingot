package bucket

import "github.com/ipfs/go-cid"

// VersionNode identifies one object version: its per-bucket ordinal (Seq,
// ordering only — never exposed to clients), its S3 client handle
// (VersionID, "null" or a ULID token), and its manifest. See
// docs/s3-versioning.md §2.
type VersionNode struct {
	Seq       uint64  `cborgen:"s"`
	VersionID string  `cborgen:"v"`
	Manifest  cid.Cid `cborgen:"m"`
}

// ObjectLeaf is the per-key version group. The top-level (per-bucket) MST
// maps each plain object key to the CID of one of these.
type ObjectLeaf struct {
	// Current is the head version — what GET/HEAD/ListObjects resolve with
	// a single leaf read, no descent into Prev.
	Current VersionNode `cborgen:"c"`

	// Prev is the root of the per-key MST of noncurrent versions, keyed
	// newest-first by the fixed-width hex of the bitwise-inverted Seq
	// (value = the version's manifest CID), or nil when there are none.
	Prev *cid.Cid `cborgen:"p"`

	// NullSeq is the Seq of the null version when it is noncurrent (an
	// entry in Prev); 0 when the null version is current or absent. Needed
	// because Prev is keyed by Seq and the null version's id ("null") does
	// not encode its Seq.
	NullSeq uint64 `cborgen:"n"`
}
