package s3frontend

import (
	"encoding/hex"

	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"
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
func (b *Backend) cacheHeld(digest multihash.Multihash) {
	if _, err := b.cache.Take(b.spool, digest); err != nil {
		b.logger.Warn("could not move a held blob's local copy into the cache; it stays in the spool",
			zap.String("digest", hex.EncodeToString(digest)),
			zap.Error(err))
	}
}

// removeLocal removes a blob's local copy, from the cache or the spool,
// and returns the bytes freed.
func (b *Backend) removeLocal(digest multihash.Multihash) (int64, error) {
	freed, err := b.cache.Remove(digest)
	if err != nil {
		return freed, err
	}
	more, err := b.spool.Remove(digest)
	return freed + more, err
}
