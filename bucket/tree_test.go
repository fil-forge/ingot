package bucket

import (
	"bytes"
	"context"
	"strings"
	"testing"

	mh "github.com/multiformats/go-multihash"
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
	if err != nil {
		t.Fatalf("decode BLAKE3 multihash: %v", err)
	}
	if dec.Code != mh.BLAKE3 || !bytes.Equal(dec.Digest, want[:]) {
		t.Fatalf("BLAKE3 = code %#x digest %x, want blake3 %x", dec.Code, dec.Digest, want)
	}
	if g := blake3tree.ChunkLog(int64(len(data))); body.TreeChunkLog != g {
		t.Fatalf("TreeChunkLog = %d, want %d", body.TreeChunkLog, g)
	}
	blocks, err := body.TreeBlockCVs()
	if err != nil {
		t.Fatal(err)
	}
	block := blake3tree.BlockSize(body.TreeChunkLog)
	if wantN := (int64(len(data)) + block - 1) / block; int64(len(blocks)) != wantN {
		t.Fatalf("%d blocks, want %d", len(blocks), wantN)
	}
	if root, ok := blake3tree.RootFromBlocks(blocks); ok && root != want {
		t.Fatalf("blocks do not merge to the root")
	}
	c, ok := body.CID()
	if !ok {
		t.Fatal("CID not ok")
	}
	// CIDv1, raw codec, blake3 multihash, base32: the fixed prefix.
	if s := c.String(); !strings.HasPrefix(s, "bafkr4i") {
		t.Fatalf("CID %s does not carry the raw/blake3 prefix", s)
	}
}

func TestSplitBody_Tree(t *testing.T) {
	ctx := context.Background()
	for _, n := range []int{0, 1, 1000, 10000, 100_000} {
		data := makeData(n)
		body, err := SplitBody(ctx, testSpool(t), bytes.NewReader(data), 4096)
		if err != nil {
			t.Fatalf("SplitBody(%d): %v", n, err)
		}
		assertTree(t, body, data)
		sized, err := SplitSizedBody(ctx, &sizedRecorder{}, bytes.NewReader(data), int64(n), 4096)
		if err != nil {
			t.Fatalf("SplitSizedBody(%d): %v", n, err)
		}
		if !bytes.Equal(sized.BLAKE3, body.BLAKE3) || sized.TreeChunkLog != body.TreeChunkLog || !bytes.Equal(sized.TreeBlocks, body.TreeBlocks) {
			t.Fatalf("SplitSizedBody(%d) tree differs from SplitBody's", n)
		}
	}
}

func TestBody_CIDUnset(t *testing.T) {
	if _, ok := (Body{Size: 3}).CID(); ok {
		t.Fatal("a body without a BLAKE3 digest has no CID")
	}
	if blocks, err := (Body{}).TreeBlockCVs(); err != nil || len(blocks) != 0 {
		t.Fatalf("empty TreeBlocks: %v, %d blocks", err, len(blocks))
	}
	if _, err := (Body{TreeBlocks: make([]byte, 33)}).TreeBlockCVs(); err == nil {
		t.Fatal("a block list that is not a multiple of 32 bytes must fail to decode")
	}
}
