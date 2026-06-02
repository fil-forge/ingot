package blockstore

import (
	"context"
	"testing"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// countingReader records how many times GetBlock hits the base reader and
// returns a fixed-size block for every request (the cache keys by the requested
// CID, so the returned block's own CID is irrelevant — only its size matters
// for the byte-budget tests).
type countingReader struct {
	blockSize int
	calls     map[string]int
}

func (r *countingReader) GetBlock(_ context.Context, c cid.Cid) (block.Block, error) {
	if r.calls == nil {
		r.calls = map[string]int{}
	}
	r.calls[c.KeyString()]++
	size := r.blockSize
	if size == 0 {
		size = 1
	}
	return block.NewBlock(make([]byte, size)), nil
}

// mkcid returns a distinct raw CIDv1 derived from b.
func mkcid(b byte) cid.Cid {
	pref := cid.Prefix{Version: 1, Codec: cid.Raw, MhType: mh.SHA2_256, MhLength: -1}
	c, _ := pref.Sum([]byte{b})
	return c
}

func TestCached_Disabled(t *testing.T) {
	base := &countingReader{}
	if got := NewCached(base, 0); got != BlockReader(base) {
		t.Fatalf("maxBytes<=0 should return the base reader unchanged")
	}
}

func TestCached_Hit(t *testing.T) {
	base := &countingReader{blockSize: 100}
	c := NewCached(base, 1<<20)
	k := mkcid(7)

	for i := 0; i < 3; i++ {
		if _, err := c.GetBlock(context.Background(), k); err != nil {
			t.Fatalf("get: %v", err)
		}
	}
	if got := base.calls[k.KeyString()]; got != 1 {
		t.Fatalf("expected base hit once (rest cached), got %d", got)
	}
}

func TestCached_EvictsByBytes(t *testing.T) {
	base := &countingReader{blockSize: 100}
	// Budget holds 2 of the 100-byte blocks; the 3rd evicts the LRU (first).
	c := NewCached(base, 250)

	first, second, third := mkcid(1), mkcid(2), mkcid(3)
	for _, k := range []cid.Cid{first, second, third} {
		if _, err := c.GetBlock(context.Background(), k); err != nil {
			t.Fatalf("get: %v", err)
		}
	}

	// first was evicted -> refetched (2 base hits).
	if _, err := c.GetBlock(context.Background(), first); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got := base.calls[first.KeyString()]; got != 2 {
		t.Fatalf("expected first block evicted and refetched (2 base hits), got %d", got)
	}
	// third is the most-recently-used and still cached (1 base hit).
	if _, err := c.GetBlock(context.Background(), third); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got := base.calls[third.KeyString()]; got != 1 {
		t.Fatalf("expected third block still cached (1 base hit), got %d", got)
	}
}
