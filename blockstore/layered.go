package blockstore

import (
	"context"
	"errors"
	"io"

	"github.com/fil-forge/ucantone/did"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/fil-forge/ingot/internal/tracing"
)

// Layered is the production ReadStore: a read-only seam that consults the local
// blob copies first (LocalBlobs: the cache and the spool, the read-after-write
// floor for object-body blobs), then the local LSM log (catalog blocks: manifests, MST nodes), then a
// base blockstore (typically *Forge — indexing-service + piri).
//
// It exposes both halves of ReadStore from a single underlying traversal:
//
//   - GetBlock returns raw blocks (body blobs).
//   - Get fetches the same blocks and CBOR-decodes them (manifests, MST nodes),
//     via an internal Store wrapped around our own GetBlock so the
//     local → log → base ordering is preserved.
//
// Layered has no Put: body blobs are written to the spool by the write path;
// catalog blocks flow through bucketop.Tx → OpStaging → Log.AppendBatch.
//
// A body blob is found locally (until evicted); a catalog block is never
// spooled, so it misses the local tier and resolves from the log. The single
// local → log → base order therefore serves both without distinguishing codecs.
type Layered struct {
	local BlockReader // local blob copies; may be nil (then skipped)
	log   Log
	base  BlockReader
	// blobReads counts body-blob reads by the tier that served them; nil
	// until CountBlobReads.
	blobReads metric.Int64Counter
}

// NewLayered wires the local blob copies and the log in front of a base
// blockstore. local may be nil.
func NewLayered(local BlockReader, log Log, base BlockReader) *Layered {
	return &Layered{local: local, log: log, base: base}
}

// CountBlobReads reports every body-blob read through meter as
// ingot.local_blobs.reads, with a tier attribute naming where it was served
// from (local or network), so the local hit ratio can be watched. Call it
// before the first read.
func (l *Layered) CountBlobReads(meter metric.Meter) error {
	c, err := meter.Int64Counter("ingot.local_blobs.reads", metric.WithUnit("{read}"),
		metric.WithDescription("Body-blob reads, by the tier that served them (local or network)"))
	if err != nil {
		return err
	}
	l.blobReads = c
	return nil
}

var (
	blobReadLocal   = metric.WithAttributes(attribute.String("tier", "local"))
	blobReadNetwork = metric.WithAttributes(attribute.String("tier", "network"))
)

// countBlobRead records one body-blob read on the request's span and, when
// enabled, on the reads counter.
func (l *Layered) countBlobRead(ctx context.Context, src tracing.ReadSource, tier metric.AddOption) {
	tracing.CountRead(ctx, src)
	if l.blobReads != nil {
		l.blobReads.Add(ctx, 1, tier)
	}
}

// Get fetches a CBOR-encoded value at c and decodes it into out.
// Same read order as GetBlock (cache → log → base) — the decoder
// fetches via GetBlock under the hood, with the space bound into the
// adapter (cbor-gen's interface is space-less; see CborStore).
func (l *Layered) Get(ctx context.Context, space did.DID, c cid.Cid, out any) error {
	return CborStore(layeredAsBlockstore{l, space}).Get(ctx, c, out)
}

// GetBlock fetches a raw block: local → log → base. Only the network
// base consults the space; the local tiers are content-addressed.
func (l *Layered) GetBlock(ctx context.Context, space did.DID, c cid.Cid) (blk block.Block, retErr error) {
	if l.local != nil {
		b, err := l.local.GetBlock(ctx, space, c)
		if err == nil {
			tracing.CountRead(ctx, tracing.BlockSpool)
			return b, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
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

// OpenBlob streams an object-body blob by digest: the local copy first (the
// read-after-write floor), then the network base (after eviction). The log is
// skipped — it only ever holds catalog blocks, never body blobs. Tiers that
// don't implement BlobReader are treated as a miss. Returns ErrNotFound if no
// tier has the blob.
func (l *Layered) OpenBlob(ctx context.Context, space did.DID, digest mh.Multihash) (io.ReadCloser, error) {
	if br, ok := l.local.(BlobReader); ok {
		rc, err := br.OpenBlob(ctx, space, digest)
		if err == nil {
			l.countBlobRead(ctx, tracing.BlobSpool, blobReadLocal)
			return rc, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	if br, ok := l.base.(BlobReader); ok {
		rc, err := br.OpenBlob(ctx, space, digest)
		if err == nil {
			l.countBlobRead(ctx, tracing.BlobNetwork, blobReadNetwork)
		}
		return rc, err
	}
	return nil, ErrNotFound
}

// OpenBlobRange streams stored bytes [start, end] (inclusive) of a body blob with the
// same tiering as OpenBlob: the local copy first, then the network base. A tier that
// implements only BlobReader is served through OpenBlob with the prefix
// discarded (OpenBlobRangeOf).
func (l *Layered) OpenBlobRange(ctx context.Context, space did.DID, digest mh.Multihash, start, end int64) (io.ReadCloser, error) {
	if br, ok := l.local.(BlobReader); ok {
		rc, err := OpenBlobRangeOf(ctx, br, space, digest, start, end)
		if err == nil {
			l.countBlobRead(ctx, tracing.BlobSpool, blobReadLocal)
			return rc, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	if br, ok := l.base.(BlobReader); ok {
		rc, err := OpenBlobRangeOf(ctx, br, space, digest, start, end)
		if err == nil {
			l.countBlobRead(ctx, tracing.BlobNetwork, blobReadNetwork)
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
