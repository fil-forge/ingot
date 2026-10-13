package blake3tree

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"errors"

	"lukechampine.com/blake3"
	"lukechampine.com/blake3/bao"
)

// testSizes covers the chunk, buffer and group boundaries: empty, a single
// block, exact and off-by-one chunks and buffers, a whole number of groups,
// the block cap and one byte past it (the group doubles), and sizes that
// leave a short tail at a coarser group.
var testSizes = []int64{
	0, 1, 63, 64, 65, 1023, 1024, 1025, 2048, 3000,
	16383, 16384, 16385, 32768, 100_000, 256 << 10, 256<<10 + 1,
	1 << 20, 4<<20 - 1, 4 << 20, 4<<20 + 1, 5 << 20, 8 << 20, 8<<20 + 777, 10<<20 + 123,
	16 << 20, 16<<20 + 1, 20 << 20,
}

func data(n int64) []byte {
	d := make([]byte, n)
	rand.New(rand.NewSource(n)).Read(d)
	return d
}

// writeIn feeds d to h in pseudo-random pieces, so the buffer's fill and
// flush paths see every alignment.
func writeIn(t *testing.T, h *Hasher, d []byte, seed int64) {
	t.Helper()
	r := rand.New(rand.NewSource(seed))
	for len(d) > 0 {
		n := min(len(d), 1+r.Intn(70_000))
		if _, err := h.Write(d[:n]); err != nil {
			t.Fatal(err)
		}
		d = d[n:]
	}
}

func TestChunkLog(t *testing.T) {
	cases := []struct {
		size int64
		want uint8
	}{
		{0, 4}, {1, 4}, {16 << 10, 4}, {16<<10 + 1, 4}, {64 << 10, 4}, {1 << 20, 4},
		{4 << 20, 4}, {8 << 20, 4}, {8<<20 + 1, 5}, {16 << 20, 5}, {100 << 20, 6},
		{1 << 30, 8}, {5 << 30, 9}, {10 << 30, 10},
		{32 << 30, 10}, {32<<30 + 1, 11}, // the cap binds from 32 GiB: 32,768 blocks of 1 MiB
		{100 << 30, 12}, {1 << 40, 15}, {5 << 40, 18},
		{50_000_000_000_000, 21}, // 50 TB (decimal): the cap holds it to 23,284 blocks of 2 GiB
		{1 << 60, 35},            // past the cap the block grows linearly
	}
	for _, c := range cases {
		if got := ChunkLog(c.size); got != c.want {
			t.Errorf("ChunkLog(%d) = %d, want %d", c.size, got, c.want)
		}
		if n := (c.size + BlockSize(c.want) - 1) / BlockSize(c.want); n > MaxBlocks {
			t.Errorf("ChunkLog(%d) = %d gives %d blocks", c.size, c.want, n)
		}
	}
	// Monotone, and the block doubles when the size quadruples.
	prev := uint8(0)
	for size := int64(1); size < 1<<50; size *= 2 {
		if g := ChunkLog(size); g < prev {
			t.Fatalf("ChunkLog(%d) = %d below ChunkLog of a smaller size %d", size, g, prev)
		} else {
			prev = g
		}
	}
	if ChunkLog(1<<28) != ChunkLog(1<<26)+1 || ChunkLog(1<<30) != ChunkLog(1<<26)+2 {
		t.Fatal("the block should double with each quadrupling of the size")
	}
	if ChunkLog(1<<36) != ChunkLog(1<<35)+1 || ChunkLog(1<<37) != ChunkLog(1<<35)+2 {
		t.Fatal("past the cap the block should double with each doubling of the size")
	}
}

// TestObject checks a whole body against the reference implementation: the
// root is the plain BLAKE3 hash, the group is ChunkLog(size), the block count
// is the body's size in groups, the blocks fold back to the root, and each
// block is the chaining value of its block hashed at its offset.
func TestObject(t *testing.T) {
	for _, size := range testSizes {
		d := data(size)
		h, err := NewHasher(0)
		if err != nil {
			t.Fatal(err)
		}
		writeIn(t, h, d, size)
		obj := h.FinishObject()

		if want := blake3.Sum256(d); obj.Root != want {
			t.Errorf("size %d: root mismatch", size)
		}
		if obj.Size != size {
			t.Errorf("size %d: Size = %d", size, obj.Size)
		}
		if want := ChunkLog(size); obj.ChunkLog != want {
			t.Errorf("size %d: ChunkLog = %d, want %d", size, obj.ChunkLog, want)
		}
		block := BlockSize(obj.ChunkLog)
		if want := (size + block - 1) / block; int64(len(obj.Blocks)) != want {
			t.Errorf("size %d: %d blocks, want %d", size, len(obj.Blocks), want)
		}
		if root, ok := RootFromBlocks(obj.Blocks); ok != (len(obj.Blocks) >= 2) {
			t.Errorf("size %d: RootFromBlocks ok = %v with %d blocks", size, ok, len(obj.Blocks))
		} else if ok && root != obj.Root {
			t.Errorf("size %d: blocks do not fold to the root", size)
		}
		for i, want := range obj.Blocks {
			off := int64(i) * block
			rh, _ := NewHasher(off)
			rh.Write(d[off:min(off+block, size)])
			rng := rh.FinishRange()
			// A full block is one aligned subtree; the short tail block is
			// the merge of its whole chunks and its tail chunk.
			subs := rng.Subtrees
			if tail, ok := rng.TailChunk(); ok {
				subs = append(subs, tail)
			}
			if cv, ok := MergeSubtrees(subs); !ok || cv != want {
				t.Errorf("size %d: block %d has a wrong CV (%d subtrees)", size, i, len(subs))
			}
			if off+block <= size && len(rng.Subtrees) != 1 {
				t.Errorf("size %d: full block %d hashed as %d subtrees", size, i, len(rng.Subtrees))
			}
		}
	}
}

// TestRanges cuts a body at chunk boundaries into ranges hashed at their
// offsets and checks that their subtrees merge to the body's root, and that
// the blocks wholly inside a range are the body's blocks for those blocks.
func TestRanges(t *testing.T) {
	for _, size := range testSizes {
		if size < 2*ChunkSize {
			continue
		}
		d := data(size)
		whole, _ := NewHasher(0)
		whole.Write(d)
		obj := whole.FinishObject()

		r := rand.New(rand.NewSource(size + 1))
		var cuts []int64
		for pos := int64(0); pos < size; {
			cuts = append(cuts, pos)
			pos += ChunkSize * int64(1+r.Intn(1+int(size/ChunkSize/3)))
		}
		cuts = append(cuts, size)

		var ranges []Range
		for i := 0; i+1 < len(cuts); i++ {
			start, end := cuts[i], cuts[i+1]
			h, err := NewHasher(start)
			if err != nil {
				t.Fatal(err)
			}
			writeIn(t, h, d[start:end], start)
			rng := h.FinishRange()
			if rng.Offset != start || rng.Size != end-start {
				t.Fatalf("size %d: range [%d,%d) reports offset %d size %d", size, start, end, rng.Offset, rng.Size)
			}
			ranges = append(ranges, rng)

			if rng.ChunkLog != ChunkLog(rng.Size) {
				t.Errorf("size %d: range [%d,%d) chunk log %d, want ChunkLog(size) %d", size, start, end, rng.ChunkLog, ChunkLog(rng.Size))
			}
			// A range block is a block wholly inside the range, aligned to the
			// range's block, and when the range's block is the body's it is
			// the body's block for that block.
			group := BlockSize(rng.ChunkLog)
			for _, bl := range rng.Blocks {
				if bl.Offset%group != 0 || bl.Offset < start || bl.Offset+group > end {
					t.Errorf("size %d: range [%d,%d) block at %d is not an aligned block inside it", size, start, end, bl.Offset)
				}
				if rng.ChunkLog == obj.ChunkLog && obj.Blocks[bl.Offset/group] != bl.CV {
					t.Errorf("size %d: range [%d,%d) block at %d differs from the body's", size, start, end, bl.Offset)
				}
			}
		}
		got, ok := Assemble(ranges)
		if !ok || got.Root != obj.Root {
			t.Errorf("size %d: %d ranges do not assemble to the root (ok %v)", size, len(ranges), ok)
		}
	}
}

// TestRangeLeafCap checks a range large enough to promote its blocks several
// times, with an unaligned start so leading and trailing fragments are
// dropped rather than paired.
func TestRangeLeafCap(t *testing.T) {
	const size = 17<<20 + 3*ChunkSize
	const start = 5 * ChunkSize
	d := data(size)
	h, _ := NewHasher(start)
	writeIn(t, h, d[start:], 7)
	rng := h.FinishRange()
	if len(rng.Blocks) > MaxBlocks {
		t.Fatalf("%d blocks exceed the cap", len(rng.Blocks))
	}
	if rng.ChunkLog != ChunkLog(rng.Size) || rng.ChunkLog < MinChunkLog+1 {
		t.Fatalf("chunk log %d, want %d", rng.ChunkLog, ChunkLog(rng.Size))
	}
	group := BlockSize(rng.ChunkLog)
	for _, bl := range rng.Blocks {
		rh, _ := NewHasher(bl.Offset)
		rh.Write(d[bl.Offset : bl.Offset+group])
		if sub := rh.FinishRange().Subtrees; len(sub) != 1 || sub[0].CV != bl.CV {
			t.Errorf("block at %d has a wrong CV", bl.Offset)
		}
	}
	// The first and last blocks are only partly covered and so are not
	// recorded.
	if rng.Blocks[0].Offset < group || rng.Blocks[len(rng.Blocks)-1].Offset+group > size {
		t.Errorf("a partly covered block was recorded")
	}
}

// cutRanges hashes d as ranges cut at the given byte offsets (chunk
// aligned, ascending, excluding 0 and len(d)).
func cutRanges(t *testing.T, d []byte, cuts []int64) []Range {
	t.Helper()
	bounds := append(append([]int64{0}, cuts...), int64(len(d)))
	var out []Range
	for i := 0; i+1 < len(bounds); i++ {
		h, err := NewHasher(bounds[i])
		if err != nil {
			t.Fatal(err)
		}
		writeIn(t, h, d[bounds[i]:bounds[i+1]], bounds[i])
		out = append(out, h.FinishRange())
	}
	return out
}

// TestAssemble checks that a body's tree assembled from its ranges equals
// the one hashed in a single pass, for random chunk-aligned cuts, uniform
// cuts with a short tail (the multipart norm), a single range, and ranges
// whose blocks straddle cuts at a coarse group.
func TestAssemble(t *testing.T) {
	for _, size := range testSizes {
		d := data(size)
		h, _ := NewHasher(0)
		h.Write(d)
		want := h.FinishObject()
		if size > 0 {
			rng := cutRanges(t, d, nil)[0]
			if !rng.HasRoot || rng.Root != want.Root {
				t.Errorf("size %d: single range at 0 lacks the root", size)
			}
		}

		r := rand.New(rand.NewSource(size + 2))
		var cutSets [][]int64
		cutSets = append(cutSets, nil)
		if size >= 2*ChunkSize {
			var random []int64
			for pos := ChunkSize * int64(1+r.Intn(int(size/ChunkSize))); pos < size; pos += ChunkSize * int64(1+r.Intn(1+int(size/ChunkSize/4))) {
				random = append(random, pos)
			}
			cutSets = append(cutSets, random)
			// Cuts inside chunks: random, and uniform parts of a size that is
			// not a chunk multiple, which is what the AWS SDK for Go produces
			// above its part limit.
			var odd []int64
			for pos := int64(1 + r.Intn(ChunkSize)); pos < size; pos += int64(ChunkSize + 1 + r.Intn(int(size/3))) {
				odd = append(odd, pos)
			}
			cutSets = append(cutSets, odd)
			if part := int64(5*ChunkSize + 37); size > part {
				var uniform []int64
				for pos := part; pos < size; pos += part {
					uniform = append(uniform, pos)
				}
				cutSets = append(cutSets, uniform)
			}
			// Uniform parts of a size that is not a power of two, so blocks
			// at the body's block size straddle part boundaries.
			if part := int64(5 * ChunkSize); size > part {
				var uniform []int64
				for pos := part; pos < size; pos += part {
					uniform = append(uniform, pos)
				}
				cutSets = append(cutSets, uniform)
			}
			if part := int64(5 << 20); size > part {
				var uniform []int64
				for pos := part; pos < size; pos += part {
					uniform = append(uniform, pos)
				}
				cutSets = append(cutSets, uniform)
			}
		}
		for _, cuts := range cutSets {
			ranges := cutRanges(t, d, cuts)
			// Round-trip the storage encoding on the way.
			for i := range ranges {
				subs, err := DecodeSubtrees(EncodeSubtrees(ranges[i].Subtrees))
				if err != nil {
					t.Fatal(err)
				}
				ranges[i].Subtrees = subs
			}
			got, ok := Assemble(ranges)
			if !ok {
				t.Errorf("size %d, %d ranges: Assemble not ok", size, len(ranges))
				continue
			}
			if got.Root != want.Root || got.ChunkLog != want.ChunkLog || got.Size != want.Size {
				t.Errorf("size %d, %d ranges: root/chunk log/size differ", size, len(ranges))
			}
			if len(got.Blocks) != len(want.Blocks) {
				t.Errorf("size %d, %d ranges: %d blocks, want %d", size, len(ranges), len(got.Blocks), len(want.Blocks))
				continue
			}
			for i := range got.Blocks {
				if got.Blocks[i] != want.Blocks[i] {
					t.Errorf("size %d, %d ranges: block %d differs", size, len(ranges), i)
					break
				}
			}
		}
	}
}

// TestAssembleRejects covers the inputs Assemble must refuse: a gap, a
// range hashed at the wrong offset, and an empty list.
func TestAssembleRejects(t *testing.T) {
	d := data(40 * ChunkSize)
	ranges := cutRanges(t, d, []int64{16 * ChunkSize})
	if _, ok := Assemble(nil); ok {
		t.Error("empty list accepted")
	}
	if _, ok := Assemble(ranges[1:]); ok {
		t.Error("list not starting at 0 accepted")
	}
	// The second range's bytes hashed as if at offset 0: wrong chunk
	// indices, so the blocks do not fold to the root.
	wrong, _ := NewHasher(0)
	wrong.Write(d[16*ChunkSize:])
	bad := wrong.FinishRange()
	bad.Offset = 16 * ChunkSize
	if _, ok := Assemble([]Range{ranges[0], bad}); ok {
		t.Error("range hashed at the wrong offset accepted")
	}
	if _, err := DecodeSubtrees(make([]byte, subtreeEncSize+1)); err == nil {
		t.Error("partial subtree record decoded")
	}
	// Head and tail bytes at a boundary must make exactly one chunk.
	odd := cutRanges(t, d, []int64{16*ChunkSize + 100})
	odd[1].Head = odd[1].Head[:50]
	if _, ok := Assemble(odd); ok {
		t.Error("boundary bytes short of a chunk accepted")
	}
	if first := cutRanges(t, d, []int64{100}); true {
		first[0].Head = []byte{1}
		if _, ok := Assemble(first); ok {
			t.Error("a first range with head bytes accepted")
		}
	}
}

// TestHasherOffsets covers offsets inside a chunk: the bytes up to the next
// boundary are the head, the chunks after it are hashed at their indices,
// a short end is the tail, and a negative offset is refused.
func TestHasherOffsets(t *testing.T) {
	if _, err := NewHasher(-ChunkSize); err == nil {
		t.Fatal("negative offset accepted")
	}
	d := data(10*ChunkSize + 700)
	h, _ := NewHasher(1500)
	writeIn(t, h, d[1500:], 3)
	rng := h.FinishRange()
	if rng.Offset != 1500 || rng.Size != int64(len(d))-1500 {
		t.Fatalf("range reports offset %d size %d", rng.Offset, rng.Size)
	}
	if want := d[1500:2048]; !bytes.Equal(rng.Head, want) {
		t.Fatalf("head is %d bytes, want %d", len(rng.Head), len(want))
	}
	if want := d[10*ChunkSize:]; !bytes.Equal(rng.Tail, want) {
		t.Fatalf("tail is %d bytes, want %d", len(rng.Tail), len(want))
	}
	if rng.Subtrees[0].Pos != 2 || rng.HasRoot {
		t.Fatalf("first subtree at chunk %d, root %v", rng.Subtrees[0].Pos, rng.HasRoot)
	}
	// A range starting at an aligned offset has no head, and one ending on
	// a boundary has no tail.
	h, _ = NewHasher(2 * ChunkSize)
	h.Write(d[2*ChunkSize : 5*ChunkSize])
	if rng := h.FinishRange(); len(rng.Head) != 0 || len(rng.Tail) != 0 || len(rng.Subtrees) == 0 {
		t.Fatalf("aligned range: head %d tail %d subtrees %d", len(rng.Head), len(rng.Tail), len(rng.Subtrees))
	}
}

func TestParentCV(t *testing.T) {
	// Two chunks: the root is the root-flagged parent of their CVs, and
	// RootFromBlocks over the two chunk CVs must agree with the reference.
	d := data(2 * ChunkSize)
	l, _ := NewHasher(0)
	l.Write(d[:ChunkSize])
	r, _ := NewHasher(ChunkSize)
	r.Write(d[ChunkSize:])
	lc, rc := l.FinishRange().Subtrees[0].CV, r.FinishRange().Subtrees[0].CV
	root, ok := RootFromBlocks([]CV{lc, rc})
	if !ok || root != blake3.Sum256(d) {
		t.Fatal("RootFromBlocks over two chunks disagrees with the reference")
	}
	if ParentCV(lc, rc) == root {
		t.Fatal("a non-root parent must differ from the root")
	}
	if !bytes.Equal(lc[:], lc[:]) {
		t.Fatal("unreachable")
	}
}

// TestOutboard checks the outboard against the Go Bao library: byte for byte
// equal to its own outboard encoding at the body's group, and accepted by
// its block verifier for every block.
func TestOutboard(t *testing.T) {
	for _, size := range testSizes {
		d := data(size)
		h, _ := NewHasher(0)
		h.Write(d)
		obj := h.FinishObject()
		got := Outboard(obj.Blocks, size)

		group := int(obj.ChunkLog)
		want, root := bao.EncodeBuf(d, group, true)
		if !bytes.Equal(got, want) {
			t.Errorf("size %d (chunk log %d): outboard differs from the Bao library's (%d vs %d bytes)", size, obj.ChunkLog, len(got), len(want))
			continue
		}
		if root != obj.Root {
			t.Errorf("size %d: Bao root differs", size)
		}
		groupSize := BlockSize(obj.ChunkLog)
		for off := int64(0); off < size; off += groupSize {
			block := d[off:min(off+groupSize, size)]
			if !bao.VerifyChunk(block, got, group, uint64(off), root) {
				t.Errorf("size %d: block at %d not verified by the Bao library", size, off)
			}
		}
		if size > 0 && len(obj.Blocks) > 1 {
			bad := bytes.Clone(d[:groupSize])
			bad[0] ^= 1
			if bao.VerifyChunk(bad, got, group, 0, root) {
				t.Errorf("size %d: a corrupted block verified", size)
			}
		}
	}
}

// TestVerifyBlocks covers the client side: aligned ranges verify, including
// one ending at the body's short tail; a corrupted block is named by offset
// after the good blocks before it; unaligned, short or overlong input is
// refused before any verification; and a wrong root fails the first block.
func TestVerifyBlocks(t *testing.T) {
	const size = 40_000 // three 16 KiB blocks, the last short
	d := data(size)
	h, _ := NewHasher(0)
	h.Write(d)
	obj := h.FinishObject()
	outboard := Outboard(obj.Blocks, size)
	block := BlockSize(obj.ChunkLog)
	if size/block != 2 || size%block == 0 {
		t.Fatalf("test assumes 2 full blocks and a short tail, got block %d", block)
	}

	for _, c := range []struct{ start, end int64 }{
		{0, size}, {0, block}, {block, 2 * block}, {2 * block, size}, {block, size},
	} {
		n, err := VerifyBlocks(bytes.NewReader(d[c.start:c.end]), outboard, obj.ChunkLog, c.start, obj.Root)
		if err != nil || n != c.end-c.start {
			t.Fatalf("range [%d,%d): verified %d, err %v", c.start, c.end, n, err)
		}
	}
	bad := bytes.Clone(d[block:])
	bad[block+7] ^= 1
	n, err := VerifyBlocks(bytes.NewReader(bad), outboard, obj.ChunkLog, block, obj.Root)
	var be *BlockError
	if !errors.As(err, &be) || be.Offset != 2*block || n != block {
		t.Fatalf("corrupted block: verified %d, err %v", n, err)
	}
	if _, err := VerifyBlocks(bytes.NewReader(d[1:block+1]), outboard, obj.ChunkLog, 1, obj.Root); err == nil || errors.As(err, &be) {
		t.Fatalf("unaligned offset: %v", err)
	}
	if _, err := VerifyBlocks(bytes.NewReader(d[:block+5]), outboard, obj.ChunkLog, 0, obj.Root); err == nil || errors.As(err, &be) {
		t.Fatalf("data ending mid-block: %v", err)
	}
	if _, err := VerifyBlocks(bytes.NewReader(append(bytes.Clone(d), 1)), outboard, obj.ChunkLog, 0, obj.Root); err == nil || errors.As(err, &be) {
		t.Fatalf("data past the end: %v", err)
	}
	if _, err := VerifyBlocks(bytes.NewReader(d), outboard[:len(outboard)-1], obj.ChunkLog, 0, obj.Root); err == nil {
		t.Fatal("truncated outboard accepted")
	}
	root := obj.Root
	root[0] ^= 1
	if _, err := VerifyBlocks(bytes.NewReader(d[:block]), outboard, obj.ChunkLog, 0, root); !errors.As(err, &be) {
		t.Fatalf("wrong root: %v", err)
	}
}

func TestAlignedRange(t *testing.T) {
	const size, group = 300_000, 4
	cases := []struct{ a, b, start, end int64 }{
		{0, 0, 0, 16383},
		{5000, 20000, 0, 32767},
		{16384, 16384, 16384, 32767},
		{299_000, 299_999, 294_912, 299_999}, // the short tail
		{100, 400_000, 0, 299_999},           // past the end is clamped
	}
	for _, c := range cases {
		start, end, err := AlignedRange(c.a, c.b, group, size)
		if err != nil || start != c.start || end != c.end {
			t.Errorf("AlignedRange(%d-%d) = %d-%d, %v; want %d-%d", c.a, c.b, start, end, err, c.start, c.end)
		}
	}
	for _, c := range [][2]int64{{size, size}, {20, 10}, {-1, 5}} {
		if _, _, err := AlignedRange(c[0], c[1], group, size); err == nil {
			t.Errorf("AlignedRange(%d-%d) accepted", c[0], c[1])
		}
	}
}

// TestOutboardLeaves covers the checks on a client-supplied group and
// outboard: the group's bounds, and the entry count tying the group to the
// size in the prefix.
func TestOutboardLeaves(t *testing.T) {
	const size = 300_000
	d := data(size)
	h, _ := NewHasher(0)
	h.Write(d)
	obj := h.FinishObject()
	outboard := Outboard(obj.Blocks, size)

	if n, err := OutboardBlocks(outboard, obj.ChunkLog); err != nil || n != int64(len(obj.Blocks)) {
		t.Fatalf("OutboardBlocks = %d, %v; want %d", n, err, len(obj.Blocks))
	}
	for _, g := range []uint8{obj.ChunkLog - 1, obj.ChunkLog + 1, MaxChunkLog + 1, 63, 64, 200} {
		if _, err := OutboardBlocks(outboard, g); err == nil {
			t.Errorf("chunk log %d accepted for an outboard at chunk log %d", g, obj.ChunkLog)
		}
		if _, err := VerifyBlocks(bytes.NewReader(d), outboard, g, 0, obj.Root); err == nil {
			t.Errorf("VerifyBlocks accepted chunk log %d", g)
		}
	}
	for _, g := range []uint8{MaxChunkLog + 1, 63, 64, 200} {
		if _, _, err := AlignedRange(0, 10, g, size); err == nil {
			t.Errorf("AlignedRange accepted group %d", g)
		}
	}
	// A one-block body has an empty outboard at any block that holds it, and
	// a zero-length body in none.
	small := Outboard([]CV{{1}}, 100)
	if n, err := OutboardBlocks(small, MinChunkLog); err != nil || n != 1 {
		t.Fatalf("one block: %d, %v", n, err)
	}
	if n, err := OutboardBlocks(Outboard(nil, 0), MinChunkLog); err != nil || n != 0 {
		t.Fatalf("empty body: %d, %v", n, err)
	}
}

// TestHasherAtChunkLog checks a body hashed at a caller-chosen block size:
// the root is unchanged, the blocks are one per block at that size, and the
// outboard equals the Bao library's at the same block size, from the
// original Bao chunk (chunk log 0) and iroh's block (4) up past the body's
// own.
func TestHasherAtChunkLog(t *testing.T) {
	for _, size := range []int64{0, 1, 1023, 1024, 1025, 16384, 16385, 100_000, 1 << 20, 5<<20 + 777} {
		d := data(size)
		want := blake3.Sum256(d)
		natural := ChunkLog(size)
		for _, g := range []uint8{0, 2, MinChunkLog, natural, natural + 2} {
			h, err := NewHasherAtChunkLog(0, g)
			if err != nil {
				t.Fatal(err)
			}
			writeIn(t, h, d, size+int64(g))
			obj := h.FinishObject()
			if obj.Root != want || obj.ChunkLog != g {
				t.Fatalf("size %d chunk log %d: root or chunk log wrong", size, g)
			}
			block := BlockSize(g)
			if n := (size + block - 1) / block; int64(len(obj.Blocks)) != n {
				t.Fatalf("size %d chunk log %d: %d blocks, want %d", size, g, len(obj.Blocks), n)
			}
			ob, root := bao.EncodeBuf(d, int(g), true)
			if got := Outboard(obj.Blocks, size); !bytes.Equal(got, ob) || root != want {
				t.Fatalf("size %d chunk log %d: outboard differs from the Bao library's", size, g)
			}
		}
	}
	if _, err := NewHasherAtChunkLog(0, MaxChunkLog+1); err == nil {
		t.Fatal("chunk log above the maximum accepted")
	}
}

// TestHostileClientInputs covers the client-side checks against extreme or
// malformed values a caller may supply: sizes near the int64 limit, a huge
// chunk log over a small body, negative offsets, and empty input.
func TestHostileClientInputs(t *testing.T) {
	// ChunkLog terminates for any size, and keeps the body within the block
	// cap: 2^62 bytes is exactly 32,768 blocks of 2^47, one byte more needs
	// the next block size, and the largest int64 body still fits.
	for _, c := range []struct {
		size int64
		want uint8
	}{{1 << 62, 37}, {1<<62 + 1, 38}, {math.MaxInt64, 38}} {
		if g := ChunkLog(c.size); g != c.want {
			t.Errorf("ChunkLog(%d) = %d, want %d", c.size, g, c.want)
		}
		if n := blocksIn(c.size, c.want); n > MaxBlocks {
			t.Errorf("ChunkLog(%d) = %d gives %d blocks", c.size, c.want, n)
		}
	}
	// An outboard claiming a body near the int64 limit: at chunk log 52 it
	// has two blocks, so one parent entry is required and none is wrong.
	huge := make([]byte, 8)
	binary.LittleEndian.PutUint64(huge, math.MaxInt64)
	if _, err := OutboardBlocks(huge, MaxChunkLog); err == nil {
		t.Error("an outboard with no parents accepted for a two-block body")
	}
	if n, err := OutboardBlocks(append(huge, make([]byte, 64)...), MaxChunkLog); err != nil || n != 2 {
		t.Errorf("two-block body near the limit: %d, %v", n, err)
	}
	if start, end, err := AlignedRange(math.MaxInt64-10, math.MaxInt64-1, MaxChunkLog, math.MaxInt64); err != nil || start != 1<<62 || end != math.MaxInt64-1 {
		t.Errorf("AlignedRange near the limit: %d-%d, %v", start, end, err)
	}

	// A two-block body for the end-of-body cases below.
	whole := data(2 * BlockSize(MinChunkLog))
	h, _ := NewHasherAtChunkLog(0, MinChunkLog)
	h.Write(whole)
	obj := h.FinishObject()
	if _, err := VerifyBlocks(bytes.NewReader(append(bytes.Clone(whole), 1)), Outboard(obj.Blocks, obj.Size), MinChunkLog, 0, obj.Root); err == nil {
		t.Error("a byte past a whole last block accepted")
	}

	// A small body at a huge chunk log: one block, an empty outboard, and the
	// verifier must buffer the body, not the block.
	d := data(100)
	root := blake3.Sum256(d)
	small := make([]byte, 8)
	binary.LittleEndian.PutUint64(small, 100)
	if n, err := VerifyBlocks(bytes.NewReader(d), small, MaxChunkLog, 0, root); err != nil || n != 100 {
		t.Fatalf("small body at chunk log %d: %d, %v", MaxChunkLog, n, err)
	}
	if _, err := VerifyBlocks(bytes.NewReader(d), small, MaxChunkLog, -(1 << 62), root); err == nil {
		t.Error("negative aligned offset accepted")
	}
	// Empty input is not a successful verification of a non-empty body,
	// but is of an empty one.
	if _, err := VerifyBlocks(bytes.NewReader(nil), small, MaxChunkLog, 0, root); err == nil {
		t.Error("empty input accepted for a 100-byte body")
	}
	empty := Outboard(nil, 0)
	if n, err := VerifyBlocks(bytes.NewReader(nil), empty, MinChunkLog, 0, blake3.Sum256(nil)); err != nil || n != 0 {
		t.Errorf("empty body: %d, %v", n, err)
	}
	if _, err := VerifyBlocks(bytes.NewReader([]byte{1}), empty, MinChunkLog, 0, blake3.Sum256(nil)); err == nil {
		t.Error("data accepted for an empty body")
	}
	// The empty body is authenticated by the root alone, so a wrong root is
	// a mismatch, not a pass.
	wrongRoot := blake3.Sum256([]byte("not empty"))
	var be *BlockError
	if _, err := VerifyBlocks(bytes.NewReader(nil), empty, MinChunkLog, 0, wrongRoot); !errors.As(err, &be) {
		t.Errorf("empty body with a wrong root: %v", err)
	}
	if emptyRoot() != blake3.Sum256(nil) {
		t.Error("emptyRoot differs from the reference hash of no bytes")
	}
	if _, err := VerifyBlocks(bytes.NewReader(nil), small, MaxChunkLog, 0, root); err == nil {
		t.Error("empty input accepted for a 100-byte body")
	}
	tail := Outboard(obj.Blocks, obj.Size)
	if _, err := VerifyBlocks(bytes.NewReader(nil), tail, MinChunkLog, obj.Size, obj.Root); err == nil {
		t.Error("a request at the end of a non-empty body accepted")
	}
}
