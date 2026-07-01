// Package blockstore is the home for ingot's block I/O abstractions.
// It declares the contracts (Reader, Writer, Store, BlockReader,
// BlockWriter, ReadStore, BaseStore, Log) and the in-process
// implementations of the read tier (Layered), the transactional
// tier (OpStaging), and the network base (Forge). The on-disk LSM
// implementation of Log lives in pkg/ingot/logstore.
//
// Tiered architecture:
//
//	WRITE PATH
//	    client → OpStaging → (Commit) → Log → (Flush) → BaseStore (Forge)
//	             ↑                       ↑
//	             buffered until Commit;  hot (open) +
//	             reads see own writes    warm (sealed local) +
//	                                     cold (off-host)
//
//	READ PATH
//	    client → Layered (cache → Log → BaseStore)
//
// Conventions:
//
//   - Reader / Writer / Store: CBOR-typed I/O, mirroring the shape
//     of cbor.IpldStore. Method names are Get / Put.
//   - BlockReader / BlockWriter: raw-block I/O. Method names are
//     GetBlock / PutBlock so a single type can expose both halves
//     without method-name collision against the CBOR-typed Get/Put.
//   - ReadStore = Reader + BlockReader: the read seam s3frontend
//     drives. Layered is the production implementation.
//   - Log: the journaling tier — see log.go.
//   - BaseStore: alias for cbor.IpldBlockstore. The bottom tier
//     keeps the IPFS-standard naming convention so anything
//     implementing the cbor IpldBlockstore interface (Forge,
//     third-party IPLD blockstores) drops in without an adapter.
//   - OpStaging: per-transaction store. Get/Put buffer in memory;
//     Commit hands the entire batch to a Log via AppendBatch and
//     returns once the journal has fsynced; Discard rolls back.
//
// CborStore is the helper that wraps a BaseStore into a Store with
// the multihash fixed to SHA2_256, so encoded blocks address-equal
// across the codebase regardless of where in the layer stack they
// were materialized.
package blockstore

import (
	"context"
	"io"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	mh "github.com/multiformats/go-multihash"
)

// Reader fetches a CBOR-encoded value at c into out. Same shape as
// cbor.IpldStore.Get; mst.LoadMST and any code path that walks the
// MST without materializing it accept a Reader.
type Reader interface {
	Get(ctx context.Context, c cid.Cid, out any) error
}

// Writer writes a CBOR-encoded value, returning its CID. Same shape
// as cbor.IpldStore.Put.
type Writer interface {
	Put(ctx context.Context, v any) (cid.Cid, error)
}

// Store is Reader + Writer — the CBOR-typed I/O surface (manifests,
// MST nodes). Equivalent in shape to cbor.IpldStore but defined
// here so consumers don't have to import cbor.
type Store interface {
	Reader
	Writer
}

// BlockReader fetches a raw block. Used by chunker.OpenBody for
// streaming body chunks. Same shape as cbor.IpldBlockstore.Get but
// renamed to GetBlock so a single type can expose both a CBOR-typed
// Get (Reader) and a raw-block GetBlock without method-name
// collision.
type BlockReader interface {
	GetBlock(ctx context.Context, c cid.Cid) (block.Block, error)
}

// BlockWriter writes a raw block. Used by chunker.PutBody for body
// chunks. Same shape as cbor.IpldBlockstore.Put but renamed to
// PutBlock for the same reason as BlockReader.
type BlockWriter interface {
	PutBlock(ctx context.Context, blk block.Block) error
}

// BlobReader streams a stored object-body blob by its digest, without holding
// the whole blob in memory — the read counterpart to BlobWriter. Body blobs run
// up to max_blob_size (256 MiB), so the body read path (chunker.OpenBody) streams
// them rather than materializing each as a block.Block the way GetBlock would.
// Returns ErrNotFound if the blob is not present in this tier. The caller owns
// the returned reader and must Close it.
type BlobReader interface {
	OpenBlob(ctx context.Context, digest mh.Multihash) (io.ReadCloser, error)
}

// BlobWriter streams an object-body blob to local storage, computing its sha256
// digest as it writes (never buffering the whole blob), and returns that digest
// and the byte count. It copies r to EOF; an empty r stores nothing and returns
// a nil digest with n == 0. Used by chunker.SplitBody on the PUT path.
type BlobWriter interface {
	WriteBlob(ctx context.Context, r io.Reader) (digest mh.Multihash, n int64, err error)
}

// ReadStore is the read-only seam the s3frontend.Backend uses for
// CBOR-decoded reads (manifest, MST nodes), raw block reads (small
// catalog blocks), and streaming body-blob reads (OpenBlob). Layered
// is the production implementation.
type ReadStore interface {
	Reader
	BlockReader
	BlobReader
}

// WriteStore is the write seam a body codec uses: CBOR-typed Put
// (for format-specific index blocks) plus raw-block PutBlock (for
// chunk bytes). bucketop.Tx satisfies it.
type WriteStore interface {
	Writer
	BlockWriter
}

// BaseStore is the bottom-tier raw-block interface. Aliased to
// cbor.IpldBlockstore so anything implementing the IPFS-standard
// convention (Forge, third-party IPLD blockstores) drops in
// without an adapter. ingot's higher-layer interfaces (BlockReader,
// BlockWriter, Store) use the GetBlock / PutBlock / typed Get / Put
// naming convention; only this layer and CborStore work in the IPFS
// convention.
type BaseStore = cbor.IpldBlockstore

// CborStore wraps a BaseStore in a Store, fixing the multihash to
// SHA2_256 so encoded blocks address-equal across the codebase.
// Used by Layered to expose itself as a CBOR view, and by bucketop
// to wrap an OpStaging into the per-tx CBOR view.
func CborStore(bs BaseStore) Store {
	cst := cbor.NewCborStore(bs)
	cst.DefaultMultihash = mh.SHA2_256
	return cst
}
