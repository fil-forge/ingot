package s3frontend

import (
	"context"
	"encoding/hex"

	"github.com/fil-forge/ucantone/did"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
)

// localUsage returns the bytes the cache holds, writes in progress included.
// The budget bounds it.
func (b *Backend) localUsage() int64 {
	return b.cache.Usage()
}

// cacheWritten adopts a written blob's local copy into the cache once the
// provider holds it: its location and accepted state are both recorded. The
// copy is in place before the write answers, so the reads that follow are
// served locally. The adoption runs in a sync queue-shard.cache task, which
// schedules the copy's eviction in the same task, so an eviction of an
// earlier copy cannot run between the two and remove this one. A write that
// kept no copy caches nothing. A failure costs only the cached copy, or its
// scheduled eviction, which the budget pass then stands in for, and is
// logged.
func (b *Backend) cacheWritten(ctx context.Context, digest multihash.Multihash, local *blockstore.TempBlob) {
	if local == nil {
		return
	}
	defer local.Discard() // nothing, once adopted
	if err := b.runShardCache(ctx, digest, local); err != nil {
		b.logger.Warn("could not cache a written blob's local copy",
			zap.String("digest", hex.EncodeToString(digest)), zap.Error(err))
	}
}

// blobKey is the key under which Backend.blobKeys serialises the writes of
// one digest to one space.
func blobKey(space did.DID, digest multihash.Multihash) string {
	return space.String() + "\x00" + string(digest)
}
