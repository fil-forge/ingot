package blockstore

import (
	"container/list"
	"context"
	"io"
	"sync"

	"github.com/fil-forge/ucantone/did"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// Cached wraps a BlockReader with a bounded, in-memory LRU keyed by CID. Blocks
// are content-addressed and immutable, so caching them is always safe; this
// fronts the network-backed reader (blockstore.Forge) so repeated reads of hot
// objects don't re-hit the indexer + piri on every block. The cache is
// per-instance and resets on process restart.
//
// The bound is a byte budget rather than a block count, because body chunks can
// be up to a megabyte each — a count-based cap would make peak memory depend on
// chunk size. A single block larger than the whole budget is served through but
// not cached.
type Cached struct {
	base     BlockReader
	maxBytes int64

	mu       sync.Mutex
	ll       *list.List               // front = most-recently-used
	items    map[string]*list.Element // cid.KeyString -> element
	curBytes int64
}

type cacheEntry struct {
	key  string
	blk  block.Block
	size int64
}

// NewCached returns a BlockReader that caches up to maxBytes of blocks in front
// of base. A maxBytes <= 0 disables caching and returns base unchanged, so
// callers can wire it unconditionally and toggle via config.
func NewCached(base BlockReader, maxBytes int64) BlockReader {
	if maxBytes <= 0 {
		return base
	}
	return &Cached{
		base:     base,
		maxBytes: maxBytes,
		ll:       list.New(),
		items:    make(map[string]*list.Element),
	}
}

var (
	_ BlockReader = (*Cached)(nil)
	_ BlobReader  = (*Cached)(nil)
)

// OpenBlob streams a body blob straight from the base reader, bypassing the LRU.
// Body blobs are large and streamed; caching one whole would defeat streaming
// (and a single blob can exceed the whole budget). Small catalog blocks still
// cache through GetBlock.
func (c *Cached) OpenBlob(ctx context.Context, space did.DID, digest mh.Multihash) (io.ReadCloser, error) {
	if br, ok := c.base.(BlobReader); ok {
		return br.OpenBlob(ctx, space, digest)
	}
	return nil, ErrNotFound
}

// GetBlock returns the cached block if present, otherwise fetches it from the
// base reader and caches it (subject to the byte budget). The LRU is keyed by
// CID only — blocks are content-addressed, so a hit is valid whatever space
// originally fetched it.
func (c *Cached) GetBlock(ctx context.Context, space did.DID, k cid.Cid) (block.Block, error) {
	key := k.KeyString()
	if blk, ok := c.get(key); ok {
		return blk, nil
	}
	blk, err := c.base.GetBlock(ctx, space, k)
	if err != nil {
		return nil, err
	}
	c.add(key, blk)
	return blk, nil
}

func (c *Cached) get(key string) (block.Block, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		return el.Value.(*cacheEntry).blk, true
	}
	return nil, false
}

func (c *Cached) add(key string, blk block.Block) {
	size := int64(len(blk.RawData()))
	// A block that can never fit is served through but not retained, rather
	// than evicting the entire cache to (fail to) hold it.
	if size > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		old := el.Value.(*cacheEntry)
		c.curBytes += size - old.size
		old.blk, old.size = blk, size
	} else {
		el := c.ll.PushFront(&cacheEntry{key: key, blk: blk, size: size})
		c.items[key] = el
		c.curBytes += size
	}
	for c.curBytes > c.maxBytes {
		oldest := c.ll.Back()
		if oldest == nil {
			break
		}
		e := oldest.Value.(*cacheEntry)
		c.ll.Remove(oldest)
		delete(c.items, e.key)
		c.curBytes -= e.size
	}
}
