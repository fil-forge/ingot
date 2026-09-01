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
	"fmt"
	"io"

	"github.com/fil-forge/ucantone/did"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	mh "github.com/multiformats/go-multihash"
)

// Reader fetches a CBOR-encoded value at c into out. mst.LoadMST and any
// code path that walks the MST without materializing it accept a Reader.
//
// The read interfaces carry the Forge space the block belongs to (the
// owning bucket's space DID), because the bottom read tier is the network:
// blob locations are keyed by space and retrieval is authorized per space.
// Local tiers (spool, log, staging) are content-addressed and ignore it;
// did.Undef is acceptable where no network fallthrough can occur (harness,
// standalone reads served locally).
type Reader interface {
	Get(ctx context.Context, space did.DID, c cid.Cid, out any) error
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

// BlockReader fetches a raw block from the space's read tier. Renamed from
// cbor.IpldBlockstore.Get to GetBlock so a single type can expose both a
// CBOR-typed Get (Reader) and a raw-block GetBlock without method-name
// collision. See Reader for the space parameter's semantics.
type BlockReader interface {
	GetBlock(ctx context.Context, space did.DID, c cid.Cid) (block.Block, error)
}

// BlockWriter writes a raw block. Used by chunker.PutBody for body
// chunks. Same shape as cbor.IpldBlockstore.Put but renamed to
// PutBlock for the same reason as BlockReader.
type BlockWriter interface {
	PutBlock(ctx context.Context, blk block.Block) error
}

// BlobReader streams a stored object-body blob by its digest, without holding
// the whole blob in memory — the read counterpart to BlobWriter. Body blobs run
// up to max_blob_size (~254 MiB), so the body read path (chunker.OpenBody) streams
// them rather than materializing each as a block.Block the way GetBlock would.
// Returns ErrNotFound if the blob is not present in this tier. The caller owns
// the returned reader and must Close it.
type BlobReader interface {
	OpenBlob(ctx context.Context, space did.DID, digest mh.Multihash) (io.ReadCloser, error)
}

// BlobRangeReader is the optional ranged counterpart of BlobReader: it
// streams only the stored bytes [start, end] (inclusive, HTTP Range
// semantics) of a blob. The decrypting read path uses it to fetch exactly
// the ciphertext chunks a plaintext range needs instead of the whole blob.
// Returns ErrNotFound if the blob is not in this tier; an end past the
// stored bytes yields a shorter stream, which the caller detects. A tier
// without this capability is served through OpenBlob with the prefix
// discarded (see OpenBlobRangeOf).
type BlobRangeReader interface {
	OpenBlobRange(ctx context.Context, space did.DID, digest mh.Multihash, start, end int64) (io.ReadCloser, error)
}

// OpenBlobRangeOf opens stored bytes [start, end] (inclusive) of a blob
// through br, using its BlobRangeReader capability when present and falling
// back to OpenBlob with the prefix read-and-discarded (or Seek'd, for a
// spool file) when not. It is how range-consuming callers stay correct over
// any tier.
func OpenBlobRangeOf(ctx context.Context, br BlobReader, space did.DID, digest mh.Multihash, start, end int64) (io.ReadCloser, error) {
	if rr, ok := br.(BlobRangeReader); ok {
		return rr.OpenBlobRange(ctx, space, digest, start, end)
	}
	rc, err := br.OpenBlob(ctx, space, digest)
	if err != nil {
		return nil, err
	}
	if start > 0 {
		if seeker, ok := rc.(io.Seeker); ok {
			if _, err := seeker.Seek(start, io.SeekStart); err != nil {
				_ = rc.Close()
				return nil, fmt.Errorf("blockstore: seek blob to %d: %w", start, err)
			}
		} else if _, err := io.CopyN(io.Discard, rc, start); err != nil {
			_ = rc.Close()
			return nil, fmt.Errorf("blockstore: skip into blob to %d: %w", start, err)
		}
	}
	return readerCloser{Reader: io.LimitReader(rc, end-start+1), Closer: rc}, nil
}

// readerCloser pairs a wrapped reader with the closer releasing its
// underlying stream.
type readerCloser struct {
	io.Reader
	io.Closer
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

// CborStore wraps a BaseStore in a cbor.IpldStore, fixing the multihash to
// SHA2_256 so encoded blocks address-equal across the codebase. Used by
// Layered to expose itself as a CBOR view, and by bucketop to wrap an
// OpStaging into the per-tx CBOR view.
//
// Note it returns the external cbor.IpldStore (space-less Get/Put), not this
// package's Store: cbor-gen's interface is fixed, so the space is bound into
// the BaseStore adapter handed in, not threaded per call. This is the one
// boundary where the space rides on the value instead of the signature.
func CborStore(bs BaseStore) cbor.IpldStore {
	cst := cbor.NewCborStore(bs)
	cst.DefaultMultihash = mh.SHA2_256
	return cst
}

// SpacelessReader adapts a BaseStore into a Reader that ignores the space
// parameter — for contexts where every block is already local and no network
// fallthrough exists (CAR diffing, recovery over sealed segments, tests).
func SpacelessReader(bs BaseStore) Reader {
	return spacelessReader{CborStore(bs)}
}

type spacelessReader struct{ cst cbor.IpldStore }

func (r spacelessReader) Get(ctx context.Context, _ did.DID, c cid.Cid, out any) error {
	return r.cst.Get(ctx, c, out)
}
