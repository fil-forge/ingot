package s3frontend

import (
	"context"
	"encoding/hex"

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
// served locally. It then queues a queue-shard.cache task, which schedules
// the copy's eviction. A write that kept no copy caches nothing. A failure
// costs only the cached copy, or its scheduled eviction, which the budget
// pass then stands in for, and is logged.
func (b *Backend) cacheWritten(ctx context.Context, digest multihash.Multihash, local *blockstore.TempBlob) {
	if local == nil {
		return
	}
	if _, err := b.cache.Adopt(local, digest); err != nil {
		b.logger.Warn("could not cache a written blob's local copy",
			zap.String("digest", hex.EncodeToString(digest)), zap.Error(err))
		return
	}
	if err := b.submitShardCache(ctx, digest); err != nil {
		b.logger.Warn("could not schedule a cached blob's eviction; the budget pass bounds it",
			zap.String("digest", hex.EncodeToString(digest)), zap.Error(err))
	}
}
