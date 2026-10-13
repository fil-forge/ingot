package s3frontend

import (
	"context"
	"encoding/hex"
	"errors"

	"github.com/fil-forge/ucantone/did"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/registry"
)

// localUsage returns the bytes the local blob directories hold: the spool,
// writes in progress included, and the cache. The budget bounds this sum.
func (b *Backend) localUsage() int64 {
	return b.spool.Usage() + b.cache.Usage()
}

// cacheHeld moves a blob's local copy from the spool into the cache once the
// provider holds it for space: its location and accepted state are both
// recorded. A copy already elsewhere, or none at all, moves nothing. A failed
// move is logged and leaves the copy in the spool, where eviction does not
// look for a blob until the registry proves the provider holds it.
//
// With written bodies not cached (Deps.DropAcceptedBodies), it removes the
// copy instead and marks the intent evicted, so reads go to the provider.
// Like the parked-part drop, a failure costs only disk and is logged.
func (b *Backend) cacheHeld(ctx context.Context, space did.DID, digest multihash.Multihash) {
	if b.dropAcceptedBodies {
		b.dropLocalCopy(ctx, space, digest, removedAccepted)
		return
	}
	if _, err := b.cache.Take(b.spool, digest); err != nil {
		b.logger.Warn("could not move a held blob's local copy into the cache; it stays in the spool",
			zap.String("digest", hex.EncodeToString(digest)),
			zap.Error(err))
	}
}

// dropLocalCopy removes a blob's local copy once the provider holds it for
// space, and marks its intents evicted, unless another space holds an intent
// for the digest: that space's upload may still read the file, or its reads
// may be served from it, and the copy is then left to that space's release
// or to eviction. A failure costs only disk and is logged: the local blob
// sweeper or the blob's release removes the file later, and a copy left
// unmarked is found missing and marked by the sweeper.
func (b *Backend) dropLocalCopy(ctx context.Context, space did.DID, digest multihash.Multihash, reason string) {
	others, err := b.intents.CountOtherIntents(ctx, space, digest)
	if err != nil {
		b.logger.Warn("count other intents for a held blob failed; its local copy stays",
			zap.String("digest", hex.EncodeToString(digest)), zap.Error(err))
		return
	}
	if others > 0 {
		return
	}
	// The mark is as of this write's own intent, read before the removal: a
	// write that spools the bytes again in between has a newer intent, its
	// file on disk, and keeps its mark clear.
	in, err := b.intents.GetIntent(ctx, space, digest)
	if err != nil {
		b.logger.Warn("read a held blob's intent failed; its local copy stays",
			zap.String("digest", hex.EncodeToString(digest)), zap.Error(err))
		return
	}
	freed, err := b.removeLocal(digest)
	if errors.Is(err, blockstore.ErrHeld) {
		return // a write of the same bytes holds the file, and its own flow or eviction takes it later
	}
	b.localBlobMetrics.removedFile(ctx, reason, freed)
	if err != nil {
		b.logger.Warn("drop held blob's local copy failed; the local blob sweeper or the blob's release removes it",
			zap.String("digest", hex.EncodeToString(digest)), zap.Error(err))
		return
	}
	if err := b.intents.MarkEvicted(ctx, digest, in.UpdatedAt); err != nil && !errors.Is(err, registry.ErrNotFound) {
		b.logger.Warn("mark held blob evicted failed",
			zap.String("digest", hex.EncodeToString(digest)), zap.Error(err))
	}
}

// removeLocal removes a blob's local copy, from the spool or the cache,
// and returns the bytes freed. A blob only ever moves from the spool to the
// cache, so looking in the spool first means a concurrent move cannot slip
// the copy past both removals. A blob a write in flight holds is left
// alone in both, and the error is blockstore.ErrHeld: the caller must not
// record the file as gone.
func (b *Backend) removeLocal(digest multihash.Multihash) (int64, error) {
	freed, err := b.spool.Remove(digest)
	if err != nil {
		return freed, err
	}
	more, err := b.cache.Remove(digest)
	return freed + more, err
}

// blobKey is the key under which Backend.blobKeys serialises the writes of
// one digest to one space.
func blobKey(space did.DID, digest multihash.Multihash) string {
	return space.String() + "\x00" + string(digest)
}
