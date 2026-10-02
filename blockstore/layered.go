package blockstore

import (
	"context"
	"errors"
	"io"

	"github.com/fil-forge/ucantone/did"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/internal/tracing"
)

// Layered is the production ReadStore: a read-only seam that consults the local
// LSM log (catalog blocks: manifests, MST nodes), then a base blockstore
// (typically *Forge — indexing-service + piri), which is where object-body
// blobs are read from.
//
// It exposes both halves of ReadStore from a single underlying traversal:
//
//   - GetBlock returns raw blocks (body blobs).
//   - Get fetches the same blocks and CBOR-decodes them (manifests, MST nodes),
//     via an internal Store wrapped around our own GetBlock so the
//     log → base ordering is preserved.
//
// Layered has no Put: body blobs go to the provider from the write path;
// catalog blocks flow through bucketop.Tx → OpStaging → Log.AppendBatch.
//
// A catalog block resolves from the log; a body blob is never in the log, so
// it falls through to the base. The single log → base order therefore serves
// both without distinguishing codecs.
type Layered struct {
	log  Log
	base BlockReader
}

// NewLayered wires the log in front of a base blockstore.
func NewLayered(log Log, base BlockReader) *Layered {
	return &Layered{log: log, base: base}
}

// Get fetches a CBOR-encoded value at c and decodes it into out.
// Same read order as GetBlock (log → base) — the decoder
// fetches via GetBlock under the hood, with the space bound into the
// adapter (cbor-gen's interface is space-less; see CborStore).
func (l *Layered) Get(ctx context.Context, space did.DID, c cid.Cid, out any) error {
	return CborStore(layeredAsBlockstore{l, space}).Get(ctx, c, out)
}

// GetBlock fetches a raw block: log → base. Only the network base consults
// the space; the local log is content-addressed.
func (l *Layered) GetBlock(ctx context.Context, space did.DID, c cid.Cid) (blk block.Block, retErr error) {
	if l.log != nil {
		b, err := l.log.Get(ctx, c)
		if err == nil {
			tracing.CountRead(ctx, tracing.BlockLog)
			return b, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	return l.base.GetBlock(ctx, space, c)
}

// OpenBlob streams an object-body blob by digest from the network base. The
// log is skipped — it only ever holds catalog blocks, never body blobs. A base
// that doesn't implement BlobReader is treated as a miss. Returns ErrNotFound
// if the base has no such blob.
func (l *Layered) OpenBlob(ctx context.Context, space did.DID, digest mh.Multihash) (io.ReadCloser, error) {
	if br, ok := l.base.(BlobReader); ok {
		rc, err := br.OpenBlob(ctx, space, digest)
		if err == nil {
			tracing.CountRead(ctx, tracing.BlobNetwork)
		}
		return rc, err
	}
	return nil, ErrNotFound
}

// OpenBlobRange streams stored bytes [start, end] (inclusive) of a body blob
// from the network base, as OpenBlob does. A base that implements only
// BlobReader is served through OpenBlob with the prefix discarded
// (OpenBlobRangeOf).
func (l *Layered) OpenBlobRange(ctx context.Context, space did.DID, digest mh.Multihash, start, end int64) (io.ReadCloser, error) {
	if br, ok := l.base.(BlobReader); ok {
		rc, err := OpenBlobRangeOf(ctx, br, space, digest, start, end)
		if err == nil {
			tracing.CountRead(ctx, tracing.BlobNetwork)
		}
		return rc, err
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
