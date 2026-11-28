package cars

import (
	"bytes"
	"testing"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// TestFrameSizeMatchesWrite checks FrameSize against the bytes a block adds
// to a CAR that Write encodes.
func TestFrameSizeMatchesWrite(t *testing.T) {
	data := bytes.Repeat([]byte{7}, 300)
	mh, err := multihash.Sum(data, multihash.SHA2_256, -1)
	require.NoError(t, err, "multihash")
	blk, err := block.NewBlockWithCid(data, cid.NewCidV1(cid.DagCBOR, mh))
	require.NoError(t, err, "block")

	var withBlock, empty bytes.Buffer
	require.NoError(t, Write(&withBlock, []cid.Cid{blk.Cid()}, []block.Block{blk}), "write")
	require.NoError(t, Write(&empty, []cid.Cid{blk.Cid()}, nil), "write")
	got, want := FrameSize(blk.Cid(), uint64(len(data))), int64(withBlock.Len()-empty.Len())
	require.Equal(t, want, got, "FrameSize")
}
