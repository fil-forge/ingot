package blake3tree

import (
	"bytes"
	"math/rand"
	"testing"

	"lukechampine.com/blake3"
	"lukechampine.com/blake3/bao"
)

// testSizes covers the chunk, buffer and group boundaries: empty, a single
// block, exact and off-by-one chunks and buffers, a whole number of groups,
// the leaf cap and one byte past it (the group doubles), and sizes that
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

func TestGroupLog(t *testing.T) {
	cases := []struct {
		size int64
		want uint8
	}{
		{0, 14}, {1, 14}, {16 << 10, 14}, {16<<10 + 1, 15}, {64 << 10, 15}, {1 << 20, 17},
		{4 << 20, 18}, {4<<20 + 1, 19}, {16 << 20, 19}, {100 << 20, 21}, {1 << 30, 22},
		{10 << 30, 24}, {100 << 30, 26}, {1 << 40, 27}, {5 << 40, 29},
		{50_000_000_000_000, 31}, // 50 TB: the cap holds it to 23,283 leaves of 2 GiB
		{1 << 60, 45},            // past the cap the group grows linearly
	}
	for _, c := range cases {
		if got := GroupLog(c.size); got != c.want {
			t.Errorf("GroupLog(%d) = %d, want %d", c.size, got, c.want)
		}
		if n := (c.size + GroupSize(c.want) - 1) / GroupSize(c.want); n > MaxLeaves {
			t.Errorf("GroupLog(%d) = %d gives %d leaves", c.size, c.want, n)
		}
	}
	// Monotone, and the group doubles when the size quadruples.
	prev := uint8(0)
	for size := int64(1); size < 1<<50; size *= 2 {
		if g := GroupLog(size); g < prev {
			t.Fatalf("GroupLog(%d) = %d below GroupLog of a smaller size %d", size, g, prev)
		} else {
			prev = g
		}
	}
	if GroupLog(1<<32) != GroupLog(1<<30)+1 || GroupLog(1<<34) != GroupLog(1<<30)+2 {
		t.Fatal("the group should double with each quadrupling of the size")
	}
}

// TestObject checks a whole body against the reference implementation: the
// root is the plain BLAKE3 hash, the group is GroupLog(size), the leaf count
// is the body's size in groups, the leaves fold back to the root, and each
// leaf is the chaining value of its block hashed at its offset.
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
		if want := GroupLog(size); obj.GroupLog != want {
			t.Errorf("size %d: GroupLog = %d, want %d", size, obj.GroupLog, want)
		}
		group := GroupSize(obj.GroupLog)
		if want := (size + group - 1) / group; int64(len(obj.Leaves)) != want {
			t.Errorf("size %d: %d leaves, want %d", size, len(obj.Leaves), want)
		}
		if root, ok := RootFromLeaves(obj.Leaves); ok != (len(obj.Leaves) >= 2) {
			t.Errorf("size %d: RootFromLeaves ok = %v with %d leaves", size, ok, len(obj.Leaves))
		} else if ok && root != obj.Root {
			t.Errorf("size %d: leaves do not fold to the root", size)
		}
		for i, leaf := range obj.Leaves {
			off := int64(i) * group
			rh, _ := NewHasher(off)
			rh.Write(d[off:min(off+group, size)])
			rng := rh.FinishRange()
			// A full block is one aligned subtree; the short tail is the
			// merge of several.
			if cv, ok := MergeSubtrees(rng.Subtrees); !ok || cv != leaf {
				t.Errorf("size %d: leaf %d is not the CV of its block (%d subtrees)", size, i, len(rng.Subtrees))
			}
			if off+group <= size && len(rng.Subtrees) != 1 {
				t.Errorf("size %d: full block %d hashed as %d subtrees", size, i, len(rng.Subtrees))
			}
		}
	}
}

// TestRanges cuts a body at chunk boundaries into ranges hashed at their
// offsets and checks that their subtrees merge to the body's root, and that
// the leaves wholly inside a range are the body's leaves for those blocks.
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

		var subs []Subtree
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
			subs = append(subs, rng.Subtrees...)

			if rng.GroupLog != GroupLog(rng.Size) {
				t.Errorf("size %d: range [%d,%d) group %d, want GroupLog(size) %d", size, start, end, rng.GroupLog, GroupLog(rng.Size))
			}
			// A range leaf is a block wholly inside the range (or the body's
			// short tail block), aligned to the range's group, and when the
			// range's group is the body's it is the body's leaf for that block.
			group := GroupSize(rng.GroupLog)
			for _, leaf := range rng.Leaves {
				if leaf.Offset%group != 0 || leaf.Offset < start || (leaf.Offset+group > end && end != size) {
					t.Errorf("size %d: range [%d,%d) leaf at %d is not an aligned block inside it", size, start, end, leaf.Offset)
				}
				if rng.GroupLog == obj.GroupLog && obj.Leaves[leaf.Offset/group] != leaf.CV {
					t.Errorf("size %d: range [%d,%d) leaf at %d differs from the body's", size, start, end, leaf.Offset)
				}
			}
		}
		root, ok := RootFromSubtrees(subs)
		if !ok {
			// A body hashed as one range that is a single subtree cannot
			// be finished from its chaining value; anything else can.
			if len(subs) != 1 {
				t.Fatalf("size %d: RootFromSubtrees not ok (%d subtrees)", size, len(subs))
			}
			continue
		}
		if root != obj.Root {
			t.Errorf("size %d: %d ranges' subtrees do not merge to the root", size, len(cuts)-1)
		}
	}
}

// TestRangeLeafCap checks a range large enough to promote its leaves several
// times, with an unaligned start so leading and trailing fragments are
// dropped rather than paired.
func TestRangeLeafCap(t *testing.T) {
	const size = 17<<20 + 3*ChunkSize
	const start = 5 * ChunkSize
	d := data(size)
	h, _ := NewHasher(start)
	writeIn(t, h, d[start:], 7)
	rng := h.FinishRange()
	if len(rng.Leaves) > MaxLeaves {
		t.Fatalf("%d leaves exceed the cap", len(rng.Leaves))
	}
	if rng.GroupLog != GroupLog(rng.Size) || rng.GroupLog < MinGroupLog+1 {
		t.Fatalf("group %d, want %d", rng.GroupLog, GroupLog(rng.Size))
	}
	group := GroupSize(rng.GroupLog)
	for _, leaf := range rng.Leaves {
		rh, _ := NewHasher(leaf.Offset)
		rh.Write(d[leaf.Offset : leaf.Offset+group])
		if sub := rh.FinishRange().Subtrees; len(sub) != 1 || sub[0].CV != leaf.CV {
			t.Errorf("leaf at %d is not the CV of its block", leaf.Offset)
		}
	}
	// The first and last blocks are only partly covered and so are not leaves.
	if rng.Leaves[0].Offset < group || rng.Leaves[len(rng.Leaves)-1].Offset+group > size {
		t.Errorf("a partly covered block was recorded as a leaf")
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
			// Uniform parts of a size that is not a power of two, so blocks
			// at the body's group straddle part boundaries.
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
			if got.Root != want.Root || got.GroupLog != want.GroupLog || got.Size != want.Size {
				t.Errorf("size %d, %d ranges: root/group/size differ", size, len(ranges))
			}
			if len(got.Leaves) != len(want.Leaves) {
				t.Errorf("size %d, %d ranges: %d leaves, want %d", size, len(ranges), len(got.Leaves), len(want.Leaves))
				continue
			}
			for i := range got.Leaves {
				if got.Leaves[i] != want.Leaves[i] {
					t.Errorf("size %d, %d ranges: leaf %d differs", size, len(ranges), i)
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
	// indices, so the leaves do not fold to the root.
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
}

func TestNewHasherRejectsUnalignedOffset(t *testing.T) {
	if _, err := NewHasher(ChunkSize + 1); err == nil {
		t.Fatal("expected an error")
	}
	if _, err := NewHasher(-ChunkSize); err == nil {
		t.Fatal("expected an error")
	}
}

func TestParentCV(t *testing.T) {
	// Two chunks: the root is the root-flagged parent of their CVs, and
	// RootFromLeaves over the two chunk CVs must agree with the reference.
	d := data(2 * ChunkSize)
	l, _ := NewHasher(0)
	l.Write(d[:ChunkSize])
	r, _ := NewHasher(ChunkSize)
	r.Write(d[ChunkSize:])
	lc, rc := l.FinishRange().Subtrees[0].CV, r.FinishRange().Subtrees[0].CV
	root, ok := RootFromLeaves([]CV{lc, rc})
	if !ok || root != blake3.Sum256(d) {
		t.Fatal("RootFromLeaves over two chunks disagrees with the reference")
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
		got := Outboard(obj.Leaves, size)

		group := int(obj.GroupLog) - chunkLog
		want, root := bao.EncodeBuf(d, group, true)
		if !bytes.Equal(got, want) {
			t.Errorf("size %d (group 2^%d): outboard differs from the Bao library's (%d vs %d bytes)", size, obj.GroupLog, len(got), len(want))
			continue
		}
		if root != obj.Root {
			t.Errorf("size %d: Bao root differs", size)
		}
		groupSize := GroupSize(obj.GroupLog)
		for off := int64(0); off < size; off += groupSize {
			block := d[off:min(off+groupSize, size)]
			if !bao.VerifyChunk(block, got, group, uint64(off), root) {
				t.Errorf("size %d: block at %d not verified by the Bao library", size, off)
			}
		}
		if size > 0 && len(obj.Leaves) > 1 {
			bad := bytes.Clone(d[:groupSize])
			bad[0] ^= 1
			if bao.VerifyChunk(bad, got, group, 0, root) {
				t.Errorf("size %d: a corrupted block verified", size)
			}
		}
	}
}
