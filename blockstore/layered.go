package blockstore

import (
	"context"
	"errors"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// Layered is the production ReadStore: a read-only seam that
// consults a small in-memory cache first, then the local LSM log,
// then a base blockstore (typically *Forge — indexing-service +
// piri).
//
// It exposes both halves of ReadStore from a single underlying
// traversal:
//
//   - GetBlock returns raw blocks (body chunks).
//   - Get fetches the same blocks and CBOR-decodes them (manifests,
//     MST nodes), via an internal Store wrapped around our own
//     GetBlock so the cache + log → base ordering is preserved.
//
// Layered has no Put: real writes flow through bucketop.Tx →
// OpStaging → Log.AppendBatch.
type Layered struct {
	log  Log
	base BlockReader

	// cstSelf is a CBOR view backed by Layered's own GetBlock. The
	// adapter exposes Layered as a BaseStore so CborStore can wrap
	// it; the CBOR decoder's block fetches come back through
	// GetBlock and reuse the cache + fallthrough.
	cstSelf Store
}

// NewLayered wires a log store in front of a base blockstore.
func NewLayered(log Log, base BlockReader) *Layered {
	l := &Layered{log: log, base: base}
	l.cstSelf = CborStore(layeredAsBlockstore{l})
	return l
}

// Get fetches a CBOR-encoded value at c and decodes it into out.
// Same read order as GetBlock (cache → log → base) — the decoder
// fetches via GetBlock under the hood.
func (l *Layered) Get(ctx context.Context, c cid.Cid, out any) error {
	return l.cstSelf.Get(ctx, c, out)
}

// GetBlock fetches a raw block: cache → log → base.
func (l *Layered) GetBlock(ctx context.Context, c cid.Cid) (blk block.Block, retErr error) {
	if l.log != nil {
		b, err := l.log.Get(ctx, c)
		if err == nil {
			return b, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	return l.base.GetBlock(ctx, c)
}

// layeredAsBlockstore lifts Layered into a BaseStore for the
// CborStore wrapper. Internal-only — exists so the CBOR decoder
// reuses Layered's cache + fallthrough order rather than going
// around them.
type layeredAsBlockstore struct{ inner *Layered }

func (a layeredAsBlockstore) Get(ctx context.Context, c cid.Cid) (block.Block, error) {
	return a.inner.GetBlock(ctx, c)
}

// Put is unused: Layered is read-only, but BaseStore (= cbor
// IpldBlockstore) requires it. The CBOR codec only ever invokes
// Get on this adapter, so this stays a no-op.
func (a layeredAsBlockstore) Put(_ context.Context, _ block.Block) error { return nil }

// Compile-time assertion: Layered is the production ReadStore.
var _ ReadStore = (*Layered)(nil)
