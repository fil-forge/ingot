package blockstore

import (
	"context"
	"errors"
	"io"

	"github.com/fil-forge/ucantone/did"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// Layered is the production ReadStore: a read-only seam that consults the local
// blob spool first (the read-after-write floor / read cache for object-body
// blobs), then the local LSM log (catalog blocks: manifests, MST nodes), then a
// base blockstore (typically *Forge — indexing-service + piri).
//
// It exposes both halves of ReadStore from a single underlying traversal:
//
//   - GetBlock returns raw blocks (body blobs).
//   - Get fetches the same blocks and CBOR-decodes them (manifests, MST nodes),
//     via an internal Store wrapped around our own GetBlock so the
//     spool → log → base ordering is preserved.
//
// Layered has no Put: body blobs are written to the spool by the write path;
// catalog blocks flow through bucketop.Tx → OpStaging → Log.AppendBatch.
//
// A body blob is found in the spool (until evicted); a catalog block is never
// spooled, so it misses the spool and resolves from the log. The single
// spool → log → base order therefore serves both without distinguishing codecs.
type Layered struct {
	spool BlockReader // local blob spool; may be nil (then skipped)
	log   Log
	base  BlockReader
}

// NewLayered wires the blob spool and the log in front of a base blockstore.
// spool may be nil.
func NewLayered(spool BlockReader, log Log, base BlockReader) *Layered {
	return &Layered{spool: spool, log: log, base: base}
}

// Get fetches a CBOR-encoded value at c and decodes it into out.
// Same read order as GetBlock (cache → log → base) — the decoder
// fetches via GetBlock under the hood, with the space bound into the
// adapter (cbor-gen's interface is space-less; see CborStore).
func (l *Layered) Get(ctx context.Context, space did.DID, c cid.Cid, out any) error {
	return CborStore(layeredAsBlockstore{l, space}).Get(ctx, c, out)
}

// GetBlock fetches a raw block: spool → log → base. Only the network
// base consults the space; the local tiers are content-addressed.
func (l *Layered) GetBlock(ctx context.Context, space did.DID, c cid.Cid) (blk block.Block, retErr error) {
	if l.spool != nil {
		b, err := l.spool.GetBlock(ctx, space, c)
		if err == nil {
			return b, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	if l.log != nil {
		b, err := l.log.Get(ctx, c)
		if err == nil {
			return b, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	return l.base.GetBlock(ctx, space, c)
}

// OpenBlob streams an object-body blob by digest: the local spool first (the
// read-after-write floor), then the network base (after eviction). The log is
// skipped — it only ever holds catalog blocks, never body blobs. Tiers that
// don't implement BlobReader are treated as a miss. Returns ErrNotFound if no
// tier has the blob.
func (l *Layered) OpenBlob(ctx context.Context, space did.DID, digest mh.Multihash) (io.ReadCloser, error) {
	if br, ok := l.spool.(BlobReader); ok {
		rc, err := br.OpenBlob(ctx, space, digest)
		if err == nil {
			return rc, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	if br, ok := l.base.(BlobReader); ok {
		return br.OpenBlob(ctx, space, digest)
	}
	return nil, ErrNotFound
}

// OpenBlobRange streams stored bytes [off, off+n) of a body blob with the
// same tiering as OpenBlob: spool first, then the network base. A tier that
// implements only BlobReader is served through OpenBlob with the prefix
// discarded (OpenBlobRangeOf).
func (l *Layered) OpenBlobRange(ctx context.Context, space did.DID, digest mh.Multihash, off, n int64) (io.ReadCloser, error) {
	if br, ok := l.spool.(BlobReader); ok {
		rc, err := OpenBlobRangeOf(ctx, br, space, digest, off, n)
		if err == nil {
			return rc, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	if br, ok := l.base.(BlobReader); ok {
		return OpenBlobRangeOf(ctx, br, space, digest, off, n)
	}
	return nil, ErrNotFound
}

// layeredAsBlockstore lifts Layered into a BaseStore for the
// CborStore wrapper, with the read's space bound in. Internal-only —
// exists so the CBOR decoder reuses Layered's fallthrough order (and
// reaches the network with the right space) rather than going around
// them.
type layeredAsBlockstore struct {
	inner *Layered
	space did.DID
}

func (a layeredAsBlockstore) Get(ctx context.Context, c cid.Cid) (block.Block, error) {
	return a.inner.GetBlock(ctx, a.space, c)
}

// Put is unused: Layered is read-only, but BaseStore (= cbor
// IpldBlockstore) requires it. The CBOR codec only ever invokes
// Get on this adapter, so this stays a no-op.
func (a layeredAsBlockstore) Put(_ context.Context, _ block.Block) error { return nil }

// Compile-time assertion: Layered is the production ReadStore.
var _ ReadStore = (*Layered)(nil)
