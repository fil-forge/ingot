package bucket

import (
	"bytes"
	"context"
	"strings"
	"testing"

	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"

	"github.com/fil-forge/ingot/blake3tree"
)

// assertTree checks a split body's BLAKE3 fields against the reference hash
// of data: the multihash, the chunk log, the block count, and that the blocks
// merge back to the root.
func assertTree(t *testing.T, body Body, data []byte) {
	t.Helper()
	want := blake3.Sum256(data)
	dec, err := mh.Decode(body.BLAKE3)
	require.NoError(t, err, "decode BLAKE3 multihash")
	require.Equal(t, uint64(mh.BLAKE3), dec.Code, "BLAKE3 = code %#x digest %x", dec.Code, dec.Digest)
	require.Equal(t, want[:], dec.Digest, "BLAKE3 = code %#x digest %x", dec.Code, dec.Digest)
	g := blake3tree.ChunkLog(int64(len(data)))
	require.Equal(t, g, body.TreeChunkLog, "TreeChunkLog")
	blocks, err := body.TreeBlockCVs()
	require.NoError(t, err)
	block := blake3tree.BlockSize(body.TreeChunkLog)
	wantN := (int64(len(data)) + block - 1) / block
	require.Equal(t, wantN, int64(len(blocks)), "%d blocks", len(blocks))
	if root, ok := blake3tree.RootFromBlocks(blocks); ok {
		require.Equal(t, blake3tree.CV(want), root, "blocks do not merge to the root")
	}
	c, ok := body.CID()
	require.True(t, ok, "CID not ok")
	// CIDv1, raw codec, blake3 multihash, base32: the fixed prefix.
	s := c.String()
	require.True(t, strings.HasPrefix(s, "bafkr4i"), "CID %s does not carry the raw/blake3 prefix", s)
}

func TestSplitBody_Tree(t *testing.T) {
	ctx := context.Background()
	for _, n := range []int{0, 1, 1000, 10000, 100_000} {
		data := makeData(n)
		body, _, err := SplitBody(ctx, testBlobs(t), bytes.NewReader(data), 4096)
		require.NoError(t, err, "SplitBody(%d)", n)
		assertTree(t, body, data)
		sized, _, err := SplitSizedBody(ctx, &sizedRecorder{}, bytes.NewReader(data), int64(n), 4096)
		require.NoError(t, err, "SplitSizedBody(%d)", n)
		require.Equal(t, body.BLAKE3, sized.BLAKE3, "SplitSizedBody(%d) tree differs from SplitBody's", n)
		require.Equal(t, body.TreeChunkLog, sized.TreeChunkLog, "SplitSizedBody(%d) tree differs from SplitBody's", n)
		require.Equal(t, body.TreeBlocks, sized.TreeBlocks, "SplitSizedBody(%d) tree differs from SplitBody's", n)
	}
}

func TestBody_CIDUnset(t *testing.T) {
	_, ok := (Body{Size: 3}).CID()
	require.False(t, ok, "a body without a BLAKE3 digest has no CID")
	blocks, err := (Body{}).TreeBlockCVs()
	require.NoError(t, err, "empty TreeBlocks")
	require.Empty(t, blocks, "empty TreeBlocks")
	_, err = (Body{TreeBlocks: make([]byte, 33)}).TreeBlockCVs()
	require.Error(t, err, "a block list that is not a multiple of 32 bytes must fail to decode")
}
