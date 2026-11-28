package blake3tree

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		_, err := h.Write(d[:n])
		require.NoError(t, err)
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
		got := ChunkLog(c.size)
		assert.Equal(t, c.want, got, "ChunkLog(%d)", c.size)
		n := (c.size + BlockSize(c.want) - 1) / BlockSize(c.want)
		assert.LessOrEqual(t, n, int64(MaxBlocks), "ChunkLog(%d) = %d gives %d blocks", c.size, c.want, n)
	}
	// Monotone, and the block doubles when the size quadruples.
	prev := uint8(0)
	for size := int64(1); size < 1<<50; size *= 2 {
		g := ChunkLog(size)
		require.GreaterOrEqual(t, g, prev, "ChunkLog(%d) below ChunkLog of a smaller size", size)
		prev = g
	}
	require.Equal(t, ChunkLog(1<<26)+1, ChunkLog(1<<28), "the block should double with each quadrupling of the size")
	require.Equal(t, ChunkLog(1<<26)+2, ChunkLog(1<<30), "the block should double with each quadrupling of the size")
	require.Equal(t, ChunkLog(1<<35)+1, ChunkLog(1<<36), "past the cap the block should double with each doubling of the size")
	require.Equal(t, ChunkLog(1<<35)+2, ChunkLog(1<<37), "past the cap the block should double with each doubling of the size")
}

// TestObject checks a whole body against the reference implementation: the
// root is the plain BLAKE3 hash, the group is ChunkLog(size), the block count
// is the body's size in groups, the blocks fold back to the root, and each
// block is the chaining value of its block hashed at its offset.
func TestObject(t *testing.T) {
	for _, size := range testSizes {
		d := data(size)
		h, err := NewHasher(0)
		require.NoError(t, err)
		writeIn(t, h, d, size)
		obj := h.FinishObject()

		assert.Equal(t, CV(blake3.Sum256(d)), obj.Root, "size %d: root", size)
		assert.Equal(t, size, obj.Size, "size %d: Size", size)
		want := ChunkLog(size)
		assert.Equal(t, want, obj.ChunkLog, "size %d: ChunkLog", size)
		block := BlockSize(obj.ChunkLog)
		assert.Equal(t, (size+block-1)/block, int64(len(obj.Blocks)), "size %d: blocks", size)
		if root, ok := RootFromBlocks(obj.Blocks); assert.Equal(t, len(obj.Blocks) >= 2, ok, "size %d: RootFromBlocks ok with %d blocks", size, len(obj.Blocks)) && ok {
			assert.Equal(t, obj.Root, root, "size %d: blocks do not fold to the root", size)
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
			cv, ok := MergeSubtrees(subs)
			assert.True(t, ok, "size %d: block %d: MergeSubtrees not ok (%d subtrees)", size, i, len(subs))
			assert.Equal(t, want, cv, "size %d: block %d has a wrong CV (%d subtrees)", size, i, len(subs))
			if off+block <= size {
				assert.Len(t, rng.Subtrees, 1, "size %d: full block %d subtrees", size, i)
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
			require.NoError(t, err)
			writeIn(t, h, d[start:end], start)
			rng := h.FinishRange()
			require.Equal(t, start, rng.Offset, "size %d: range [%d,%d) offset", size, start, end)
			require.Equal(t, end-start, rng.Size, "size %d: range [%d,%d) size", size, start, end)
			ranges = append(ranges, rng)

			assert.Equal(t, ChunkLog(rng.Size), rng.ChunkLog, "size %d: range [%d,%d) chunk log, want ChunkLog(size)", size, start, end)
			// A range block is a block wholly inside the range, aligned to the
			// range's block, and when the range's block is the body's it is
			// the body's block for that block.
			group := BlockSize(rng.ChunkLog)
			for _, bl := range rng.Blocks {
				assert.True(t, bl.Offset%group == 0 && bl.Offset >= start && bl.Offset+group <= end,
					"size %d: range [%d,%d) block at %d is not an aligned block inside it", size, start, end, bl.Offset)
				if rng.ChunkLog == obj.ChunkLog {
					assert.Equal(t, obj.Blocks[bl.Offset/group], bl.CV, "size %d: range [%d,%d) block at %d differs from the body's", size, start, end, bl.Offset)
				}
			}
		}
		got, ok := Assemble(ranges)
		assert.True(t, ok, "size %d: %d ranges do not assemble", size, len(ranges))
		assert.Equal(t, obj.Root, got.Root, "size %d: %d ranges do not assemble to the root", size, len(ranges))
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
	require.LessOrEqual(t, len(rng.Blocks), MaxBlocks, "blocks exceed the cap")
	require.Equal(t, ChunkLog(rng.Size), rng.ChunkLog, "chunk log")
	require.GreaterOrEqual(t, rng.ChunkLog, uint8(MinChunkLog+1), "chunk log")
	group := BlockSize(rng.ChunkLog)
	for _, bl := range rng.Blocks {
		rh, _ := NewHasher(bl.Offset)
		rh.Write(d[bl.Offset : bl.Offset+group])
		if sub := rh.FinishRange().Subtrees; assert.Len(t, sub, 1, "block at %d subtrees", bl.Offset) {
			assert.Equal(t, bl.CV, sub[0].CV, "block at %d has a wrong CV", bl.Offset)
		}
	}
	// The first and last blocks are only partly covered and so are not
	// recorded.
	assert.GreaterOrEqual(t, rng.Blocks[0].Offset, group, "a partly covered first block was recorded")
	assert.LessOrEqual(t, rng.Blocks[len(rng.Blocks)-1].Offset+group, int64(size), "a partly covered last block was recorded")
}

// cutRanges hashes d as ranges cut at the given byte offsets (chunk
// aligned, ascending, excluding 0 and len(d)).
func cutRanges(t *testing.T, d []byte, cuts []int64) []Range {
	t.Helper()
	return cutRangesWith(t, d, cuts, func(start, _ int64) (*Hasher, error) { return NewHasher(start) })
}

// cutRangesCoarse is cutRanges with each range's blocks at the block size
// of the body up to the range's end, as UploadPart records a part's.
func cutRangesCoarse(t *testing.T, d []byte, cuts []int64) []Range {
	t.Helper()
	return cutRangesWith(t, d, cuts, func(start, end int64) (*Hasher, error) {
		return NewHasherAtChunkLog(start, ChunkLog(end))
	})
}

func cutRangesWith(t *testing.T, d []byte, cuts []int64, newHasher func(start, end int64) (*Hasher, error)) []Range {
	t.Helper()
	bounds := append(append([]int64{0}, cuts...), int64(len(d)))
	var out []Range
	for i := 0; i+1 < len(bounds); i++ {
		h, err := newHasher(bounds[i], bounds[i+1])
		require.NoError(t, err)
		writeIn(t, h, d[bounds[i]:bounds[i+1]], bounds[i])
		out = append(out, h.FinishRange())
	}
	return out
}

// TestAssemble checks that a body's tree assembled from its ranges equals
// the one hashed in a single pass, for random chunk-aligned cuts, uniform
// cuts with a short tail (the multipart norm), a single range, and ranges
// whose blocks straddle cuts at a coarse group; each with the ranges'
// blocks sized by the range alone and by the body up to the range's end.
func TestAssemble(t *testing.T) {
	for _, size := range testSizes {
		d := data(size)
		h, _ := NewHasher(0)
		h.Write(d)
		want := h.FinishObject()
		if size > 0 {
			rng := cutRanges(t, d, nil)[0]
			assert.True(t, rng.HasRoot, "size %d: single range at 0 lacks the root", size)
			assert.Equal(t, want.Root, rng.Root, "size %d: single range at 0 root", size)
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
			for _, cut := range []func(*testing.T, []byte, []int64) []Range{cutRanges, cutRangesCoarse} {
				assembleMatches(t, want, cut(t, d, cuts))
			}
		}
	}
}

// assembleMatches checks that ranges assemble into want, round-tripping
// their subtrees through the storage encoding on the way.
func assembleMatches(t *testing.T, want Object, ranges []Range) {
	t.Helper()
	size := want.Size
	for i := range ranges {
		subs, err := DecodeSubtrees(EncodeSubtrees(ranges[i].Subtrees))
		require.NoError(t, err)
		ranges[i].Subtrees = subs
	}
	got, ok := Assemble(ranges)
	if !assert.True(t, ok, "size %d, %d ranges: Assemble not ok", size, len(ranges)) {
		return
	}
	assert.Equal(t, want.Root, got.Root, "size %d, %d ranges: root", size, len(ranges))
	assert.Equal(t, want.ChunkLog, got.ChunkLog, "size %d, %d ranges: chunk log", size, len(ranges))
	assert.Equal(t, want.Size, got.Size, "size %d, %d ranges: size", size, len(ranges))
	if !assert.Len(t, got.Blocks, len(want.Blocks), "size %d, %d ranges: blocks", size, len(ranges)) {
		return
	}
	for i := range got.Blocks {
		if !assert.Equal(t, want.Blocks[i], got.Blocks[i], "size %d, %d ranges: block %d differs", size, len(ranges), i) {
			break
		}
	}
}

// TestAssembleRejects covers the inputs Assemble must refuse: a gap, a
// range hashed at the wrong offset, and an empty list.
func TestAssembleRejects(t *testing.T) {
	d := data(40 * ChunkSize)
	ranges := cutRanges(t, d, []int64{16 * ChunkSize})
	_, ok := Assemble(nil)
	assert.False(t, ok, "empty list accepted")
	_, ok = Assemble(ranges[1:])
	assert.False(t, ok, "list not starting at 0 accepted")
	// The second range's bytes hashed as if at offset 0: wrong chunk
	// indices, so the blocks do not fold to the root.
	wrong, _ := NewHasher(0)
	wrong.Write(d[16*ChunkSize:])
	bad := wrong.FinishRange()
	bad.Offset = 16 * ChunkSize
	_, ok = Assemble([]Range{ranges[0], bad})
	assert.False(t, ok, "range hashed at the wrong offset accepted")
	_, err := DecodeSubtrees(make([]byte, subtreeEncSize+1))
	assert.Error(t, err, "partial subtree record decoded")
	// Head and tail bytes at a boundary must make exactly one chunk.
	odd := cutRanges(t, d, []int64{16*ChunkSize + 100})
	odd[1].Head = odd[1].Head[:50]
	_, ok = Assemble(odd)
	assert.False(t, ok, "boundary bytes short of a chunk accepted")
	if first := cutRanges(t, d, []int64{100}); true {
		first[0].Head = []byte{1}
		_, ok := Assemble(first)
		assert.False(t, ok, "a first range with head bytes accepted")
	}
}

// TestHasherOffsets covers offsets inside a chunk: the bytes up to the next
// boundary are the head, the chunks after it are hashed at their indices,
// a short end is the tail, and a negative offset is refused.
func TestHasherOffsets(t *testing.T) {
	_, err := NewHasher(-ChunkSize)
	require.Error(t, err, "negative offset accepted")
	d := data(10*ChunkSize + 700)
	h, _ := NewHasher(1500)
	writeIn(t, h, d[1500:], 3)
	rng := h.FinishRange()
	require.Equal(t, int64(1500), rng.Offset, "range offset")
	require.Equal(t, int64(len(d))-1500, rng.Size, "range size")
	require.Equal(t, d[1500:2048], rng.Head, "head")
	require.Equal(t, d[10*ChunkSize:], rng.Tail, "tail")
	require.Equal(t, uint64(2), rng.Subtrees[0].Pos, "first subtree's chunk")
	require.False(t, rng.HasRoot, "a range not at 0 has the root")
	// A range starting at an aligned offset has no head, and one ending on
	// a boundary has no tail.
	h, _ = NewHasher(2 * ChunkSize)
	h.Write(d[2*ChunkSize : 5*ChunkSize])
	rng = h.FinishRange()
	require.Empty(t, rng.Head, "aligned range: head")
	require.Empty(t, rng.Tail, "aligned range: tail")
	require.NotEmpty(t, rng.Subtrees, "aligned range: subtrees")
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
	require.True(t, ok, "RootFromBlocks over two chunks not ok")
	require.Equal(t, CV(blake3.Sum256(d)), root, "RootFromBlocks over two chunks disagrees with the reference")
	require.NotEqual(t, root, ParentCV(lc, rc), "a non-root parent must differ from the root")
	require.Equal(t, lc[:], lc[:], "unreachable")
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
		if !assert.Equal(t, want, got, "size %d (chunk log %d): outboard differs from the Bao library's", size, obj.ChunkLog) {
			continue
		}
		assert.Equal(t, obj.Root, CV(root), "size %d: Bao root differs", size)
		groupSize := BlockSize(obj.ChunkLog)
		for off := int64(0); off < size; off += groupSize {
			block := d[off:min(off+groupSize, size)]
			assert.True(t, bao.VerifyChunk(block, got, group, uint64(off), root), "size %d: block at %d not verified by the Bao library", size, off)
		}
		if size > 0 && len(obj.Blocks) > 1 {
			bad := bytes.Clone(d[:groupSize])
			bad[0] ^= 1
			assert.False(t, bao.VerifyChunk(bad, got, group, 0, root), "size %d: a corrupted block verified", size)
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
	require.Equal(t, int64(2), size/block, "test assumes 2 full blocks and a short tail")
	require.NotZero(t, size%block, "test assumes 2 full blocks and a short tail")

	for _, c := range []struct{ start, end int64 }{
		{0, size}, {0, block}, {block, 2 * block}, {2 * block, size}, {block, size},
	} {
		n, err := VerifyBlocks(bytes.NewReader(d[c.start:c.end]), outboard, obj.ChunkLog, c.start, obj.Root)
		require.NoError(t, err, "range [%d,%d)", c.start, c.end)
		require.Equal(t, c.end-c.start, n, "range [%d,%d): bytes verified", c.start, c.end)
	}
	bad := bytes.Clone(d[block:])
	bad[block+7] ^= 1
	n, err := VerifyBlocks(bytes.NewReader(bad), outboard, obj.ChunkLog, block, obj.Root)
	var be *BlockError
	require.ErrorAs(t, err, &be, "corrupted block")
	require.Equal(t, 2*block, be.Offset, "corrupted block: offset")
	require.Equal(t, block, n, "corrupted block: bytes verified before it")
	_, err = VerifyBlocks(bytes.NewReader(d[1:block+1]), outboard, obj.ChunkLog, 1, obj.Root)
	require.Error(t, err, "unaligned offset")
	require.NotErrorAs(t, err, &be, "unaligned offset")
	_, err = VerifyBlocks(bytes.NewReader(d[:block+5]), outboard, obj.ChunkLog, 0, obj.Root)
	require.Error(t, err, "data ending mid-block")
	require.NotErrorAs(t, err, &be, "data ending mid-block")
	_, err = VerifyBlocks(bytes.NewReader(append(bytes.Clone(d), 1)), outboard, obj.ChunkLog, 0, obj.Root)
	require.Error(t, err, "data past the end")
	require.NotErrorAs(t, err, &be, "data past the end")
	_, err = VerifyBlocks(bytes.NewReader(d), outboard[:len(outboard)-1], obj.ChunkLog, 0, obj.Root)
	require.Error(t, err, "truncated outboard accepted")
	root := obj.Root
	root[0] ^= 1
	_, err = VerifyBlocks(bytes.NewReader(d[:block]), outboard, obj.ChunkLog, 0, root)
	require.ErrorAs(t, err, &be, "wrong root")
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
		assert.NoError(t, err, "AlignedRange(%d-%d)", c.a, c.b)
		assert.Equal(t, [2]int64{c.start, c.end}, [2]int64{start, end}, "AlignedRange(%d-%d)", c.a, c.b)
	}
	for _, c := range [][2]int64{{size, size}, {20, 10}, {-1, 5}} {
		_, _, err := AlignedRange(c[0], c[1], group, size)
		assert.Error(t, err, "AlignedRange(%d-%d) accepted", c[0], c[1])
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

	n, err := OutboardBlocks(outboard, obj.ChunkLog)
	require.NoError(t, err, "OutboardBlocks")
	require.Equal(t, int64(len(obj.Blocks)), n, "OutboardBlocks")
	for _, g := range []uint8{obj.ChunkLog - 1, obj.ChunkLog + 1, MaxChunkLog + 1, 63, 64, 200} {
		_, err := OutboardBlocks(outboard, g)
		assert.Error(t, err, "chunk log %d accepted for an outboard at chunk log %d", g, obj.ChunkLog)
		_, err = VerifyBlocks(bytes.NewReader(d), outboard, g, 0, obj.Root)
		assert.Error(t, err, "VerifyBlocks accepted chunk log %d", g)
	}
	for _, g := range []uint8{MaxChunkLog + 1, 63, 64, 200} {
		_, _, err := AlignedRange(0, 10, g, size)
		assert.Error(t, err, "AlignedRange accepted group %d", g)
	}
	// A one-block body has an empty outboard at any block that holds it, and
	// a zero-length body in none.
	small := Outboard([]CV{{1}}, 100)
	n, err = OutboardBlocks(small, MinChunkLog)
	require.NoError(t, err, "one block")
	require.Equal(t, int64(1), n, "one block")
	n, err = OutboardBlocks(Outboard(nil, 0), MinChunkLog)
	require.NoError(t, err, "empty body")
	require.Zero(t, n, "empty body")
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
			require.NoError(t, err)
			writeIn(t, h, d, size+int64(g))
			obj := h.FinishObject()
			require.Equal(t, CV(want), obj.Root, "size %d chunk log %d: root", size, g)
			require.Equal(t, g, obj.ChunkLog, "size %d chunk log %d: chunk log", size, g)
			block := BlockSize(g)
			n := (size + block - 1) / block
			require.Equal(t, n, int64(len(obj.Blocks)), "size %d chunk log %d: blocks", size, g)
			ob, root := bao.EncodeBuf(d, int(g), true)
			require.Equal(t, ob, Outboard(obj.Blocks, size), "size %d chunk log %d: outboard differs from the Bao library's", size, g)
			require.Equal(t, want, root, "size %d chunk log %d: Bao root differs", size, g)
		}
	}
	_, err := NewHasherAtChunkLog(0, MaxChunkLog+1)
	require.Error(t, err, "chunk log above the maximum accepted")
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
		g := ChunkLog(c.size)
		assert.Equal(t, c.want, g, "ChunkLog(%d)", c.size)
		n := blocksIn(c.size, c.want)
		assert.LessOrEqual(t, n, int64(MaxBlocks), "ChunkLog(%d) = %d gives %d blocks", c.size, c.want, n)
	}
	// An outboard claiming a body near the int64 limit: at chunk log 52 it
	// has two blocks, so one parent entry is required and none is wrong.
	huge := make([]byte, 8)
	binary.LittleEndian.PutUint64(huge, math.MaxInt64)
	_, err := OutboardBlocks(huge, MaxChunkLog)
	assert.Error(t, err, "an outboard with no parents accepted for a two-block body")
	n, err := OutboardBlocks(append(huge, make([]byte, 64)...), MaxChunkLog)
	assert.NoError(t, err, "two-block body near the limit")
	assert.Equal(t, int64(2), n, "two-block body near the limit")
	start, end, err := AlignedRange(math.MaxInt64-10, math.MaxInt64-1, MaxChunkLog, math.MaxInt64)
	assert.NoError(t, err, "AlignedRange near the limit")
	assert.Equal(t, [2]int64{1 << 62, math.MaxInt64 - 1}, [2]int64{start, end}, "AlignedRange near the limit")

	// A two-block body for the end-of-body cases below.
	whole := data(2 * BlockSize(MinChunkLog))
	h, _ := NewHasherAtChunkLog(0, MinChunkLog)
	h.Write(whole)
	obj := h.FinishObject()
	_, err = VerifyBlocks(bytes.NewReader(append(bytes.Clone(whole), 1)), Outboard(obj.Blocks, obj.Size), MinChunkLog, 0, obj.Root)
	assert.Error(t, err, "a byte past a whole last block accepted")

	// A small body at a huge chunk log: one block, an empty outboard, and the
	// verifier must buffer the body, not the block.
	d := data(100)
	root := blake3.Sum256(d)
	small := make([]byte, 8)
	binary.LittleEndian.PutUint64(small, 100)
	n, err = VerifyBlocks(bytes.NewReader(d), small, MaxChunkLog, 0, root)
	require.NoError(t, err, "small body at chunk log %d", MaxChunkLog)
	require.Equal(t, int64(100), n, "small body at chunk log %d", MaxChunkLog)
	_, err = VerifyBlocks(bytes.NewReader(d), small, MaxChunkLog, -(1 << 62), root)
	assert.Error(t, err, "negative aligned offset accepted")
	// Empty input is not a successful verification of a non-empty body,
	// but is of an empty one.
	_, err = VerifyBlocks(bytes.NewReader(nil), small, MaxChunkLog, 0, root)
	assert.Error(t, err, "empty input accepted for a 100-byte body")
	empty := Outboard(nil, 0)
	n, err = VerifyBlocks(bytes.NewReader(nil), empty, MinChunkLog, 0, blake3.Sum256(nil))
	assert.NoError(t, err, "empty body")
	assert.Zero(t, n, "empty body")
	_, err = VerifyBlocks(bytes.NewReader([]byte{1}), empty, MinChunkLog, 0, blake3.Sum256(nil))
	assert.Error(t, err, "data accepted for an empty body")
	// The empty body is authenticated by the root alone, so a wrong root is
	// a mismatch, not a pass.
	wrongRoot := blake3.Sum256([]byte("not empty"))
	var be *BlockError
	_, err = VerifyBlocks(bytes.NewReader(nil), empty, MinChunkLog, 0, wrongRoot)
	assert.ErrorAs(t, err, &be, "empty body with a wrong root")
	assert.Equal(t, CV(blake3.Sum256(nil)), emptyRoot(), "emptyRoot differs from the reference hash of no bytes")
	_, err = VerifyBlocks(bytes.NewReader(nil), small, MaxChunkLog, 0, root)
	assert.Error(t, err, "empty input accepted for a 100-byte body")
	tail := Outboard(obj.Blocks, obj.Size)
	_, err = VerifyBlocks(bytes.NewReader(nil), tail, MinChunkLog, obj.Size, obj.Root)
	assert.Error(t, err, "a request at the end of a non-empty body accepted")
}
