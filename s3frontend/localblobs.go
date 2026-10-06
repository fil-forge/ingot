package s3frontend

import (
	"context"
	"encoding/hex"
	"errors"

	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/registry"
)

// localUsage returns the bytes the local blob directories hold: the spool,
// writes in progress included, and the cache. The budget bounds this sum.
func (b *Backend) localUsage() int64 {
	return b.spool.Usage() + b.cache.Usage()
}

// cacheHeld moves a blob's local copy from the spool into the cache once the
// provider holds it: its location and accepted state are both recorded. A
// copy already elsewhere, or none at all, moves nothing. A failed move is
// logged and leaves the copy in the spool, where eviction does not look for
// a blob until the registry proves the provider holds it.
//
// With written bodies not cached (Deps.DropAcceptedBodies), it removes the
// copy instead and marks the intent evicted, so reads go to the provider.
// Like the parked-part drop, a failure costs only disk and is logged.
func (b *Backend) cacheHeld(ctx context.Context, digest multihash.Multihash) {
	if b.dropAcceptedBodies {
		b.dropAcceptedCopy(ctx, digest)
		return
	}
	if _, err := b.cache.Take(b.spool, digest); err != nil {
		b.logger.Warn("could not move a held blob's local copy into the cache; it stays in the spool",
			zap.String("digest", hex.EncodeToString(digest)),
			zap.Error(err))
	}
}

// dropAcceptedCopy removes a held blob's local copy and marks its intent
// evicted.
func (b *Backend) dropAcceptedCopy(ctx context.Context, digest multihash.Multihash) {
	freed, err := b.removeLocal(digest)
	b.localBlobMetrics.removedFile(ctx, removedAccepted, freed)
	if err != nil {
		b.logger.Warn("drop accepted blob's local copy failed; the local blob sweeper or the object's release removes it",
			zap.String("digest", hex.EncodeToString(digest)), zap.Error(err))
		return
	}
	if err := b.intents.MarkEvicted(ctx, digest); err != nil && !errors.Is(err, registry.ErrNotFound) {
		b.logger.Warn("mark accepted blob evicted failed",
			zap.String("digest", hex.EncodeToString(digest)), zap.Error(err))
	}
}

// removeLocal removes a blob's local copy, from the spool or the cache,
// and returns the bytes freed. A blob only ever moves from the spool to the
// cache, so looking in the spool first means a concurrent move cannot slip
// the copy past both removals.
func (b *Backend) removeLocal(digest multihash.Multihash) (int64, error) {
	freed, err := b.spool.Remove(digest)
	if err != nil {
		return freed, err
	}
	more, err := b.cache.Remove(digest)
	return freed + more, err
}
