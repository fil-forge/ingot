// Package blake3tree computes the BLAKE3 Merkle-tree material Ingot records
// for an object body: the whole-object digest, the chaining values of the
// group-aligned blocks a client verifies ranged reads against, and the
// aligned subtrees of a byte range hashed at an offset, from which a
// multipart object's tree is assembled without re-reading its parts.
//
// BLAKE3 hashes its input as a binary tree over 1 KiB chunks. Every aligned
// power-of-two block of chunks that lies within the input is a node of that
// tree whatever the total length turns out to be, and a chunk's chaining
// value depends on its absolute index, so a range hashed at the right offset
// yields nodes of the final tree. See the BLAKE3 specification §2.1 and the
// BLAKE3 object digest RFC.
package blake3tree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"sort"

	"lukechampine.com/blake3/bao"
	"lukechampine.com/blake3/guts"
)

const (
	// ChunkSize is the BLAKE3 chunk: the smallest unit whose chaining value
	// is a tree node. Offsets passed to NewHasher are multiples of it.
	ChunkSize = guts.ChunkSize

	// CVSize is the size of a chaining value.
	CVSize = 32

	// MinGroupLog is the smallest group (leaf size) the tree is recorded at,
	// as a base-2 exponent of bytes: 16 KiB. It is also the unit the group
	// scales from: the group is the geometric mean of the body's size and
	// this, so the leaf count grows with the square root of the size (see
	// GroupLog).
	MinGroupLog = 14

	// MaxLeaves is the hard cap on the leaves recorded for one body, 1 MiB
	// of chaining values. The square-root rule reaches it only past about
	// 32 TB; from there the group grows linearly with the size instead.
	MaxLeaves = 32768

	chunkLog  = 10 // log2(ChunkSize)
	bufChunks = guts.MaxSIMD
)

// CV is a BLAKE3 chaining value: the non-root hash of a chunk or subtree.
type CV [CVSize]byte

// GroupLog returns the group exponent for a body of size bytes: the smallest
// power of two, at least 2^MinGroupLog, that is at least the geometric mean
// of the size and 2^MinGroupLog, so the body has about sqrt(size / 16 KiB)
// leaves. The group doubles each time the size quadruples: a 4 MiB body has
// 16 leaves of 256 KiB, a 1 GiB body 256 leaves of 4 MiB, a 1 TiB body 8192
// leaves of 128 MiB. Past MaxLeaves the group grows linearly instead. The
// rule is monotone in size, which the hasher and the multipart assembly
// rely on: a part's group never exceeds its object's.
func GroupLog(size int64) uint8 {
	g := uint8(MinGroupLog)
	for g < 38 && size > int64(1)<<(2*g-MinGroupLog) {
		g++
	}
	for size > int64(MaxLeaves)<<g {
		g++
	}
	return g
}

// GroupSize returns the leaf size for a group exponent.
func GroupSize(groupLog uint8) int64 { return 1 << groupLog }

// Object is the tree material of a whole body.
type Object struct {
	Size int64
	// Root is the BLAKE3 hash of the body.
	Root CV
	// GroupLog is the leaf size of Leaves as a base-2 exponent of bytes:
	// GroupLog(Size).
	GroupLog uint8
	// Leaves holds the chaining value of every group-aligned block of the
	// body in order; the last covers the body's tail and may be short. A
	// zero-length body has none.
	Leaves []CV
}

// Range is the tree material of a byte range of a body hashed at its offset.
type Range struct {
	Offset, Size int64
	// Root is the BLAKE3 hash of the range's bytes on their own, set only
	// when Offset is 0 (HasRoot): the chunk indices then match a body made
	// of this range alone, so a multipart object completed from one part
	// takes its digest from here.
	Root    CV
	HasRoot bool
	// Subtrees is the range's decomposition into maximal aligned subtrees,
	// left to right. Adjacent ranges' subtrees merge into their union's,
	// and a body's merge to its root (see RootFromSubtrees).
	Subtrees []Subtree
	// GroupLog is the leaf size of Leaves: GroupLog(Size), which never
	// exceeds the group of any body containing the range, so a body's leaves
	// can be built from its ranges' leaves and subtrees.
	GroupLog uint8
	// Leaves holds the chaining values of the group-aligned blocks that lie
	// wholly within the range, in order. A block the range only partly
	// covers is not a leaf, and its pieces are among Subtrees. The one
	// exception is a range ending inside a chunk: a short chunk can only be
	// the body's last, so the block it ends in is the body's tail leaf.
	Leaves []Leaf
}

// Subtree is an aligned subtree of the body's tree: 2^Height chunks starting
// at chunk index Pos, which is a multiple of 2^Height.
type Subtree struct {
	CV     CV
	Height uint8
	Pos    uint64
}

// Leaf is a group-aligned block's chaining value and its byte offset.
type Leaf struct {
	CV     CV
	Offset int64
}

// node is a tree node on the hasher's stack: cv over the 2^height chunks
// starting at absolute chunk index pos.
type node struct {
	cv     [8]uint32
	height uint8
	pos    uint64
}

// Hasher hashes a body, or a byte range of one starting at a chunk-aligned
// offset, in one streaming pass. Write the bytes in order, then call
// FinishObject for a whole body or FinishRange for a range.
//
// The hasher keeps the standard BLAKE3 stack of pending subtrees, one per
// height, keyed on absolute chunk index so a range at an offset merges the
// same nodes the whole body's tree holds. Alongside it, every node formed at
// the current leaf height is recorded; whenever the bytes written so far
// call for a larger group (GroupLog is monotone) the leaf height rises by
// one and recorded siblings merge, so the leaf list follows the rule without
// knowing the body's length in advance. The last chunk is held back until the
// end, since the root node is a chunk node when the body is a single chunk.
type Hasher struct {
	offset  uint64 // chunk index of the first byte written
	counter uint64 // chunk index of buf[0]
	size    int64  // bytes written
	buf     [bufChunks * ChunkSize]byte
	buflen  int

	stack   []node // maximal aligned subtrees of [offset, counter), left to right
	leafLog uint8  // current group exponent
	leaves  []node // complete leaves at leafLog, in order
}

// NewHasher returns a Hasher for bytes starting at offset, which must be a
// multiple of ChunkSize. A whole body starts at 0.
func NewHasher(offset int64) (*Hasher, error) {
	if offset < 0 || offset%ChunkSize != 0 {
		return nil, fmt.Errorf("blake3tree: offset %d is not a multiple of %d", offset, ChunkSize)
	}
	chunk := uint64(offset) / ChunkSize
	return &Hasher{offset: chunk, counter: chunk, leafLog: MinGroupLog}, nil
}

func (h *Hasher) leafHeight() uint8 { return h.leafLog - chunkLog }

// Write implements io.Writer. It never fails.
func (h *Hasher) Write(p []byte) (int, error) {
	n := len(p)
	for len(p) > 0 {
		// A full buffer is compressed only once more input arrives, so the
		// body's last chunk is still buffered at Finish.
		if h.buflen == len(h.buf) {
			h.flush()
		}
		c := copy(h.buf[h.buflen:], p)
		h.buflen += c
		p = p[c:]
	}
	h.size += int64(n)
	for GroupLog(h.size) > h.leafLog {
		h.promote()
	}
	return n, nil
}

// flush compresses the full buffer: as one 16-chunk subtree when it is
// aligned to one (the whole-body case, and every full buffer of a range
// once it reaches alignment), else chunk by chunk.
func (h *Hasher) flush() {
	if h.counter%bufChunks == 0 {
		nd := guts.CompressBuffer(&h.buf, len(h.buf), &guts.IV, h.counter, 0)
		h.push(guts.ChainingValue(nd), bits.TrailingZeros(bufChunks), h.counter)
	} else {
		for i := 0; i < bufChunks; i++ {
			h.pushChunk(h.buf[i*ChunkSize:(i+1)*ChunkSize], h.counter+uint64(i))
		}
	}
	h.counter += bufChunks
	h.buflen = 0
}

func (h *Hasher) pushChunk(chunk []byte, pos uint64) {
	nd := guts.CompressChunk(chunk, &guts.IV, pos, 0)
	h.push(guts.ChainingValue(nd), 0, pos)
}

// push adds the subtree (cv, height, pos) to the stack, merging it with its
// left sibling while it is a right child whose sibling is the stack top.
// Each node formed at the leaf height is recorded as a leaf.
func (h *Hasher) push(cv [8]uint32, height int, pos uint64) {
	hgt := uint8(height)
	for {
		if hgt == h.leafHeight() {
			h.leaves = append(h.leaves, node{cv, hgt, pos})
		}
		top := len(h.stack) - 1
		if top < 0 || h.stack[top].height != hgt || (pos>>hgt)&1 == 0 {
			break
		}
		left := h.stack[top]
		h.stack = h.stack[:top]
		cv = parentCV(left.cv, cv)
		pos = left.pos
		hgt++
	}
	h.stack = append(h.stack, node{cv, hgt, pos})
}

// promote raises the leaf height by one. Recorded siblings merge into their
// parent; an unpaired leaf is a fragment at the new height whose parent the
// stack forms later (a trailing leaf) or never (a leading one in a range),
// and is dropped from the leaf list. It runs between writes, so a parent
// formed while the leaf height was still lower is counted once, by the
// pairing.
func (h *Hasher) promote() {
	hgt := h.leafHeight()
	var next []node
	for i := 0; i < len(h.leaves); {
		l := h.leaves[i]
		if (l.pos>>hgt)&1 == 0 && i+1 < len(h.leaves) && h.leaves[i+1].pos == l.pos+1<<hgt {
			next = append(next, node{parentCV(l.cv, h.leaves[i+1].cv), hgt + 1, l.pos})
			i += 2
			continue
		}
		i++
	}
	h.leaves = next
	h.leafLog++
}

// drain pushes the buffered chunks up to the last one and returns the last
// chunk's bytes (short when the body is not a multiple of ChunkSize; empty
// only for an empty body). counter is left at the last chunk's index.
func (h *Hasher) drain() []byte {
	if h.buflen == 0 {
		return nil
	}
	lastStart := (h.buflen - 1) / ChunkSize * ChunkSize
	for off := 0; off < lastStart; off += ChunkSize {
		h.pushChunk(h.buf[off:off+ChunkSize], h.counter)
		h.counter++
	}
	last := h.buf[lastStart:h.buflen]
	h.buflen = 0
	return last
}

// rootNode folds the stack onto the last chunk's node, rightmost first,
// and flags the result as the root: the hash of everything written, for a
// hasher started at offset 0. It does not modify the stack.
func (h *Hasher) rootNode(last []byte) guts.Node {
	n := guts.CompressChunk(last, &guts.IV, h.counter, 0)
	for i := len(h.stack) - 1; i >= 0; i-- {
		n = guts.ParentNode(h.stack[i].cv, guts.ChainingValue(n), &guts.IV, 0)
	}
	n.Flags |= guts.FlagRoot
	return n
}

// FinishObject completes a whole body (a hasher started at offset 0) and
// returns its root and leaves. The hasher must not be written to again.
func (h *Hasher) FinishObject() Object {
	if h.offset != 0 {
		panic("blake3tree: FinishObject on a range hasher")
	}
	last := h.drain()
	root := bytesOf(guts.ChainingValue(h.rootNode(last)))

	// The body's tail (the last, possibly short, leaf) is the merge of the
	// stack entries below the leaf height with the last chunk. For a body
	// started at 0 the stack's heights fall from bottom to top, so those
	// entries are the top ones.
	hgt := h.leafHeight()
	n := guts.CompressChunk(last, &guts.IV, h.counter, 0)
	for i := len(h.stack) - 1; i >= 0 && h.stack[i].height < hgt; i-- {
		n = guts.ParentNode(h.stack[i].cv, guts.ChainingValue(n), &guts.IV, 0)
	}
	leaves := h.leaves
	if h.size > 0 {
		leaves = append(leaves[:len(leaves):len(leaves)], node{guts.ChainingValue(n), hgt, uint64(len(h.leaves)) << hgt})
	}

	// Write keeps the leaf height at GroupLog of the bytes so far, so it is
	// the body's group now.
	out := Object{Size: h.size, Root: root, GroupLog: h.leafLog, Leaves: make([]CV, len(leaves))}
	for j, l := range leaves {
		out.Leaves[j] = bytesOf(l.cv)
	}
	return out
}

// FinishRange completes a byte range and returns its subtrees and the
// leaves wholly within it. The hasher must not be written to again.
func (h *Hasher) FinishRange() Range {
	last := h.drain()
	out := Range{
		Offset:   int64(h.offset) * ChunkSize,
		Size:     h.size,
		GroupLog: h.leafLog,
	}
	if h.offset == 0 {
		out.Root, out.HasRoot = bytesOf(guts.ChainingValue(h.rootNode(last))), true
	}
	if last != nil {
		h.pushChunk(last, h.counter)
		h.counter++
	}
	out.Subtrees = make([]Subtree, len(h.stack))
	for i, s := range h.stack {
		out.Subtrees[i] = Subtree{CV: bytesOf(s.cv), Height: s.height, Pos: s.pos}
	}
	out.Leaves = make([]Leaf, len(h.leaves))
	for i, l := range h.leaves {
		out.Leaves[i] = Leaf{CV: bytesOf(l.cv), Offset: int64(l.pos) * ChunkSize}
	}
	return out
}

// LeafSubtrees returns the range's leaves as subtrees of the body's tree,
// the form Assemble consumes alongside Subtrees.
func (r Range) LeafSubtrees() []Subtree {
	out := make([]Subtree, len(r.Leaves))
	for i, l := range r.Leaves {
		out[i] = Subtree{CV: l.CV, Height: r.GroupLog - chunkLog, Pos: uint64(l.Offset) / ChunkSize}
	}
	return out
}

// subtreeEncSize is the encoded size of one Subtree: height, position and
// chaining value.
const subtreeEncSize = 1 + 8 + CVSize

// EncodeSubtrees serializes subtrees for storage: for each, one byte of
// height, the big-endian position and the chaining value.
func EncodeSubtrees(subs []Subtree) []byte {
	out := make([]byte, 0, len(subs)*subtreeEncSize)
	for _, s := range subs {
		out = append(out, s.Height)
		out = binary.BigEndian.AppendUint64(out, s.Pos)
		out = append(out, s.CV[:]...)
	}
	return out
}

// DecodeSubtrees reverses EncodeSubtrees.
func DecodeSubtrees(b []byte) ([]Subtree, error) {
	if len(b)%subtreeEncSize != 0 {
		return nil, fmt.Errorf("blake3tree: %d bytes is not a whole number of subtrees", len(b))
	}
	subs := make([]Subtree, len(b)/subtreeEncSize)
	for i := range subs {
		rec := b[i*subtreeEncSize:]
		subs[i].Height = rec[0]
		subs[i].Pos = binary.BigEndian.Uint64(rec[1:])
		copy(subs[i].CV[:], rec[9:])
	}
	return subs, nil
}

// CoverCV returns the chaining value of the node covering chunks
// [start, end) from the nodes available: those lying within the span, the
// maximal ones, must tile it. The span must itself be a node of the body's
// tree (an aligned block, or the body's tail). ok is false when the nodes
// do not tile it.
func CoverCV(nodes []Subtree, start, end uint64) (CV, bool) {
	var in []Subtree
	for _, n := range nodes {
		if n.Pos >= start && n.Pos+1<<n.Height <= end {
			in = append(in, n)
		}
	}
	sort.Slice(in, func(i, j int) bool {
		if in[i].Pos != in[j].Pos {
			return in[i].Pos < in[j].Pos
		}
		return in[i].Height > in[j].Height
	})
	var tile []Subtree
	next := start
	for _, n := range in {
		if n.Pos < next {
			continue // inside the node kept before it
		}
		if n.Pos != next {
			return CV{}, false // a gap
		}
		tile = append(tile, n)
		next = n.Pos + 1<<n.Height
	}
	if next != end {
		return CV{}, false
	}
	return MergeSubtrees(tile)
}

// Assemble builds a body's tree material from the ranges that cover it,
// hashed at their offsets and given in order: the root from the ranges'
// subtrees (or the single range's own root), and the leaves at the body's
// group from the ranges' subtrees and leaves. The leaves are checked
// against the root, so ok is false when the ranges do not describe one
// body, as well as when they are not contiguous from 0.
func Assemble(ranges []Range) (Object, bool) {
	var total int64
	var filled []Range
	for _, r := range ranges {
		if r.Offset != total || r.Size < 0 {
			return Object{}, false
		}
		total += r.Size
		if r.Size > 0 {
			filled = append(filled, r)
		}
	}
	if len(ranges) == 0 {
		return Object{}, false
	}

	var root CV
	var nodes []Subtree
	for _, r := range filled {
		nodes = append(nodes, r.Subtrees...)
	}
	switch {
	case total == 0:
		h, _ := NewHasher(0)
		root = h.FinishObject().Root
	case len(filled) == 1:
		if !filled[0].HasRoot {
			return Object{}, false
		}
		root = filled[0].Root
	default:
		var ok bool
		if root, ok = RootFromSubtrees(nodes); !ok {
			return Object{}, false
		}
	}
	for _, r := range filled {
		nodes = append(nodes, r.LeafSubtrees()...)
	}

	out := Object{Size: total, Root: root, GroupLog: GroupLog(total)}
	group := GroupSize(out.GroupLog)
	for off := int64(0); off < total; off += group {
		end := min(off+group, total)
		cv, ok := CoverCV(nodes, uint64(off)/ChunkSize, uint64(end+ChunkSize-1)/ChunkSize)
		if !ok {
			return Object{}, false
		}
		out.Leaves = append(out.Leaves, cv)
	}
	if len(out.Leaves) >= 2 {
		if got, _ := RootFromLeaves(out.Leaves); got != root {
			return Object{}, false
		}
	}
	return out, true
}

// ParentCV returns the chaining value of the parent of two sibling nodes.
func ParentCV(left, right CV) CV {
	return bytesOf(parentCV(wordsOf(left), wordsOf(right)))
}

// RootFromLeaves returns the root of a body from its leaves, as a client
// holding the leaf list computes it to check the list against the digest.
// A body of one leaf has no parent to apply the root flag to: its root is
// the hash of the bytes, and ok is false.
func RootFromLeaves(leaves []CV) (root CV, ok bool) {
	if len(leaves) < 2 {
		return CV{}, false
	}
	k := leftCount(len(leaves))
	l, r := subtreeCV(leaves[:k]), subtreeCV(leaves[k:])
	n := guts.ParentNode(wordsOf(l), wordsOf(r), &guts.IV, guts.FlagRoot)
	return bytesOf(guts.ChainingValue(n)), true
}

// subtreeCV merges equal-size aligned leaves into their subtree's chaining
// value: the left subtree takes the largest power of two of them that is
// strictly less than the count, as BLAKE3 does with chunks.
func subtreeCV(leaves []CV) CV {
	if len(leaves) == 1 {
		return leaves[0]
	}
	k := leftCount(len(leaves))
	return ParentCV(subtreeCV(leaves[:k]), subtreeCV(leaves[k:]))
}

// leftCount is the largest power of two strictly less than n (n ≥ 2).
func leftCount(n int) int {
	return 1 << (bits.Len(uint(n-1)) - 1)
}

// MergeSubtrees returns the chaining value of the union of contiguous
// subtrees, given in order. The union must itself be a node of the body's
// tree: an aligned block, or the tail of the body (everything from an
// aligned boundary to its end). ok is false when the subtrees are not
// contiguous aligned blocks.
func MergeSubtrees(subs []Subtree) (cv CV, ok bool) {
	h, ok := stackOf(subs)
	if !ok {
		return CV{}, false
	}
	i := len(h.stack) - 1
	n := h.stack[i].cv
	for i--; i >= 0; i-- {
		n = parentCV(h.stack[i].cv, n)
	}
	return bytesOf(n), true
}

// RootFromSubtrees returns the root of a body from the subtrees of ranges
// that cover it contiguously from offset 0, in order: the ranges' Subtrees
// concatenated. The root is the root-flagged parent of the body's two
// top-level subtrees, so a body given as one subtree (a single chunk, or a
// power-of-two number of chunks hashed as one range) cannot be finished
// from its chaining value, and ok is false; so is a list that does not
// start at 0 or is not contiguous.
func RootFromSubtrees(subs []Subtree) (root CV, ok bool) {
	if len(subs) < 2 || subs[0].Pos != 0 {
		return CV{}, false
	}
	var total uint64
	for _, s := range subs {
		total += 1 << s.Height
	}
	// The left child holds the largest power of two of chunks strictly
	// below the total; no aligned subtree straddles that boundary.
	left := uint64(leftCount(int(total)))
	var cut uint64
	i := 0
	for ; i < len(subs) && cut < left; i++ {
		cut += 1 << subs[i].Height
	}
	if cut != left {
		return CV{}, false
	}
	l, lok := MergeSubtrees(subs[:i])
	r, rok := MergeSubtrees(subs[i:])
	if !lok || !rok {
		return CV{}, false
	}
	n := guts.ParentNode(wordsOf(l), wordsOf(r), &guts.IV, guts.FlagRoot)
	return bytesOf(guts.ChainingValue(n)), true
}

// stackOf pushes contiguous subtrees onto a fresh stack that records no
// leaves, checking that each starts where the previous ended and is aligned
// to its size.
func stackOf(subs []Subtree) (*Hasher, bool) {
	if len(subs) == 0 {
		return nil, false
	}
	h := &Hasher{leafLog: 64 + chunkLog}
	next := subs[0].Pos
	for _, s := range subs {
		if s.Pos != next || s.Pos%(1<<s.Height) != 0 {
			return nil, false
		}
		h.push(wordsOf(s.CV), int(s.Height), s.Pos)
		next += 1 << s.Height
	}
	return h, true
}

func parentCV(left, right [8]uint32) [8]uint32 {
	return guts.ChainingValue(guts.ParentNode(left, right, &guts.IV, 0))
}

func bytesOf(w [8]uint32) (cv CV) {
	for i, x := range w {
		binary.LittleEndian.PutUint32(cv[i*4:], x)
	}
	return cv
}

func wordsOf(cv CV) (w [8]uint32) {
	for i := range w {
		w[i] = binary.LittleEndian.Uint32(cv[i*4:])
	}
	return w
}

// Outboard returns the Bao outboard of a body from its leaves: the standard
// pre-order layout a Bao library loads directly, with the body's group as
// the Bao block size. It starts with the body's size as 8 little-endian
// bytes, then one 64-byte entry per parent node above the leaves, each the
// chaining values of its two children, root first and left subtree before
// right. A body of one leaf has no parents and the outboard is the prefix
// alone. The leaves themselves are not stored: they are the children in the
// lowest entries, and a verifier recomputes them from the data.
func Outboard(leaves []CV, size int64) []byte {
	out := make([]byte, 8, 8+64*max(len(leaves)-1, 0))
	binary.LittleEndian.PutUint64(out, uint64(size))
	if len(leaves) > 1 {
		_, out = preorder(leaves, out)
	}
	return out
}

// preorder appends the pre-order parent entries of leaves to out and
// returns the subtree's chaining value. The entry for a node precedes its
// children's, so the children are computed first and their entries follow.
func preorder(leaves []CV, out []byte) (CV, []byte) {
	if len(leaves) == 1 {
		return leaves[0], out
	}
	k := leftCount(len(leaves))
	at := len(out)
	out = append(out, make([]byte, 64)...)
	l, out := preorder(leaves[:k], out)
	r, out := preorder(leaves[k:], out)
	copy(out[at:], l[:])
	copy(out[at+32:], r[:])
	return ParentCV(l, r), out
}

// OutboardSize returns the body size an outboard's prefix records.
func OutboardSize(outboard []byte) int64 {
	return int64(binary.LittleEndian.Uint64(outboard))
}

// CheckOutboard reports whether b has an outboard's shape: the 8-byte size
// prefix followed by whole 64-byte parent entries.
func CheckOutboard(b []byte) error {
	if len(b) < 8 || (len(b)-8)%64 != 0 {
		return fmt.Errorf("blake3tree: %d bytes is not an 8-byte size prefix plus 64-byte parent entries", len(b))
	}
	return nil
}

// AlignedRange widens the inclusive byte range [a, b] of a body to the
// block-aligned inclusive range a verifier needs: down to a block boundary
// and up to one, or to the body's end. A block is the only unit the outboard
// can verify, since each leaf is the hash of a whole block.
func AlignedRange(a, b int64, groupLog uint8, size int64) (start, end int64, err error) {
	if a < 0 || b < a {
		return 0, 0, fmt.Errorf("blake3tree: range %d-%d is not ascending", a, b)
	}
	if a >= size {
		return 0, 0, fmt.Errorf("blake3tree: range starts at %d but the body is %d bytes", a, size)
	}
	block := GroupSize(groupLog)
	start = a / block * block
	end = min((b/block+1)*block, size) - 1
	return start, end, nil
}

// BlockError reports a block that does not verify against the outboard and
// root.
type BlockError struct{ Offset, Length int64 }

func (e *BlockError) Error() string {
	return fmt.Sprintf("block at %d (%d bytes) does not verify against the outboard and digest", e.Offset, e.Length)
}

// VerifyBlocks is the client side of range verification: it checks the bytes
// of r, which start at offset in the body, block by block against the Bao
// outboard and the body's root, and returns how many bytes it verified. The
// offset must be a multiple of the block size and the data must hold whole
// blocks, except that it may end at the body's end. A block that fails is
// returned as a *BlockError after the bytes before it; any other error is a
// malformed input.
func VerifyBlocks(r io.Reader, outboard []byte, groupLog uint8, offset int64, root CV) (int64, error) {
	if err := CheckOutboard(outboard); err != nil {
		return 0, err
	}
	size := OutboardSize(outboard)
	block := GroupSize(groupLog)
	if offset%block != 0 {
		return 0, fmt.Errorf("blake3tree: offset %d is not a multiple of the block size %d", offset, block)
	}
	if offset > size {
		return 0, fmt.Errorf("blake3tree: offset %d is past the body's %d bytes", offset, size)
	}
	buf := make([]byte, block)
	var verified int64
	for {
		n, err := io.ReadFull(r, buf)
		if n == 0 && (err == io.EOF || err == io.ErrUnexpectedEOF) {
			return verified, nil
		}
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return verified, err
		}
		pos := offset + verified
		if pos+int64(n) > size {
			return verified, fmt.Errorf("blake3tree: data runs %d bytes past the body's end", pos+int64(n)-size)
		}
		if int64(n) < block && pos+int64(n) != size {
			return verified, fmt.Errorf("blake3tree: data ends %d bytes into the block at %d; a verifiable range holds whole blocks or ends at the body's end (%d)", n, pos, size)
		}
		if !bao.VerifyChunk(bytes.Clone(buf[:n]), outboard, int(groupLog)-chunkLog, uint64(pos), root) {
			return verified, &BlockError{pos, int64(n)}
		}
		verified += int64(n)
		if int64(n) < block {
			return verified, nil
		}
	}
}
