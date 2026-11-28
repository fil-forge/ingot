package blockstore

import (
	"context"
	"testing"

	"github.com/fil-forge/ucantone/did"
	"github.com/stretchr/testify/require"

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

func (r *countingReader) GetBlock(_ context.Context, _ did.DID, c cid.Cid) (block.Block, error) {
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
	require.Same(t, base, NewCached(base, 0), "maxBytes<=0 should return the base reader unchanged")
}

func TestCached_Hit(t *testing.T) {
	base := &countingReader{blockSize: 100}
	c := NewCached(base, 1<<20)
	k := mkcid(7)

	for i := 0; i < 3; i++ {
		_, err := c.GetBlock(context.Background(), did.Undef, k)
		require.NoError(t, err, "get")
	}
	got := base.calls[k.KeyString()]
	require.Equal(t, 1, got, "expected base hit once (rest cached)")
}

func TestCached_EvictsByBytes(t *testing.T) {
	base := &countingReader{blockSize: 100}
	// Budget holds 2 of the 100-byte blocks; the 3rd evicts the LRU (first).
	c := NewCached(base, 250)

	first, second, third := mkcid(1), mkcid(2), mkcid(3)
	for _, k := range []cid.Cid{first, second, third} {
		_, err := c.GetBlock(context.Background(), did.Undef, k)
		require.NoError(t, err, "get")
	}

	// first was evicted -> refetched (2 base hits).
	_, err := c.GetBlock(context.Background(), did.Undef, first)
	require.NoError(t, err, "get")
	got := base.calls[first.KeyString()]
	require.Equal(t, 2, got, "expected first block evicted and refetched (2 base hits)")
	// third is the most-recently-used and still cached (1 base hit).
	_, err = c.GetBlock(context.Background(), did.Undef, third)
	require.NoError(t, err, "get")
	require.Equal(t, 1, base.calls[third.KeyString()], "expected third block still cached (1 base hit)")
}
