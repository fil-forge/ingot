package cars

import (
	"bytes"
	"testing"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// TestFrameSizeMatchesWrite checks FrameSize against the bytes a block adds
// to a CAR that Write encodes.
func TestFrameSizeMatchesWrite(t *testing.T) {
	data := bytes.Repeat([]byte{7}, 300)
	mh, err := multihash.Sum(data, multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	blk, err := block.NewBlockWithCid(data, cid.NewCidV1(cid.DagCBOR, mh))
	if err != nil {
		t.Fatalf("block: %v", err)
	}

	var withBlock, empty bytes.Buffer
	if err := Write(&withBlock, []cid.Cid{blk.Cid()}, []block.Block{blk}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := Write(&empty, []cid.Cid{blk.Cid()}, nil); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got, want := FrameSize(blk.Cid(), uint64(len(data))), int64(withBlock.Len()-empty.Len()); got != want {
		t.Fatalf("FrameSize = %d, want %d", got, want)
	}
}
