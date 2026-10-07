// Package blake3tree computes the BLAKE3 Merkle-tree material Ingot records
// for an object body: the whole-object digest, the chaining values of the
// blocks a client verifies ranged reads against, and the
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

	// MinChunkLog is the smallest block (block size) the tree is recorded
	// at, as a base-2 exponent of chunks: 2^4 chunks, 16 KiB. It is also the
	// unit the block scales from: the block is the geometric mean of the
	// body's size and this, so the block count grows with the square root of
	// the size (see ChunkLog).
	MinChunkLog = 4

	// MaxBlocks is the hard cap on the blocks recorded for one body, 1 MiB
	// of chaining values. The square-root rule reaches it only past about
	// 32 TB; from there the block grows linearly with the size instead.
	MaxBlocks = 32768

	// MaxChunkLog is the largest chunk log whose block size is a positive
	// int64; a chunk log a client supplies (the Blake3 attribute's ChunkLog)
	// is checked against it before any arithmetic.
	MaxChunkLog = 52

	chunkLog  = 10 // log2(ChunkSize): bytes per chunk, as an exponent
	bufChunks = guts.MaxSIMD
)

// CV is a BLAKE3 chaining value: the non-root hash of a chunk or subtree.
type CV [CVSize]byte

// ChunkLog returns the block size for a body of size bytes, as a base-2
// exponent of chunks, the unit Bao libraries take: the smallest power of
// two, at least 2^MinChunkLog chunks, whose byte size is at least the
// geometric mean of the body's size and 16 KiB, so the body has about
// sqrt(size / 16 KiB) blocks. The block doubles each time the size
// quadruples: a 4 MiB body has 16 blocks of 256 KiB (chunk log 8), a 1 GiB
// body 256 blocks of 4 MiB (12), a 1 TiB body 8192 blocks of 128 MiB (17).
// Past MaxBlocks the block grows linearly instead. The rule is monotone in
// size, which the hasher and the multipart assembly rely on: a part's block
// never exceeds its object's.
func ChunkLog(size int64) uint8 {
	// A block of 2^c chunks is 2^(c+10) bytes; the geometric-mean rule
	// wants size <= 2^(2(c+10)-14) = 2^(2c+6).
	c := uint8(MinChunkLog)
	for c < 28 && size > int64(1)<<(2*c+6) {
		c++
	}
	// The cap. The loop stops where MaxBlocks blocks would reach 2^63
	// bytes, which no int64 size exceeds, so the shift never blocks int64
	// and the comparison always settles.
	for c+chunkLog+15 < 63 && size > int64(MaxBlocks)<<(c+chunkLog) {
		c++
	}
	return c
}

// blocksIn returns how many blocks of 2^chunkLog chunks cover size bytes,
// without the overflow of adding block-1 to a size near the int64 limit.
func blocksIn(size int64, chunkLog_ uint8) int64 {
	block := BlockSize(chunkLog_)
	n := size / block
	if size%block != 0 {
		n++
	}
	return n
}

// BlockSize returns the block size in bytes for a chunk log.
func BlockSize(chunkLog_ uint8) int64 { return ChunkSize << chunkLog_ }

// Object is the tree material of a whole body.
type Object struct {
	Size int64
	// Root is the BLAKE3 hash of the body.
	Root CV
	// ChunkLog is the block size of Blocks as a base-2 exponent of chunks:
	// ChunkLog(Size).
	ChunkLog uint8
	// Blocks holds the chaining value of every block of the
	// body in order; the last covers the body's tail and may be short. A
	// zero-length body has none.
	Blocks []CV
}

// Range is the tree material of a byte range of a body hashed at its offset,
// which need not be chunk-aligned. The whole chunks between the range's
// first and last chunk boundaries are hashed; the bytes outside them, under
// a chunk at each end, are kept raw as Head and Tail so that a chunk
// straddling two ranges can be hashed once both are known (see Assemble).
type Range struct {
	Offset, Size int64
	// Root is the BLAKE3 hash of the range's bytes on their own, set only
	// when Offset is 0: the chunk indices then match a body made of this
	// range alone, so a multipart object completed from one part takes its
	// digest from here.
	Root CV
	// HasRoot reports whether Root is set, which is when Offset is 0.
	HasRoot bool
	// Head is the range's bytes before its first chunk boundary: empty when
	// Offset is a multiple of ChunkSize.
	Head []byte
	// Tail is the range's bytes after its last chunk boundary: empty when
	// the range ends on one. A range cannot know whether it is the body's
	// last, so a short final chunk is Tail rather than hashed as the body's
	// end.
	Tail []byte
	// Subtrees is the decomposition of the range's whole chunks into
	// maximal aligned subtrees, left to right. Adjacent ranges' subtrees
	// merge into their union's, and a body's merge to its root (see
	// RootFromSubtrees).
	Subtrees []Subtree
	// ChunkLog is the block size of Blocks: ChunkLog(Size), which never
	// exceeds the block of any body containing the range, so a body's blocks
	// can be built from its ranges' blocks and subtrees.
	ChunkLog uint8
	// Blocks holds the chaining values of the blocks that lie wholly within
	// the range's whole chunks, in order. A block the range only partly
	// covers is not a block, and its pieces are among Subtrees, Head and
	// Tail.
	Blocks []Block
}

// Subtree is an aligned subtree of the body's tree: 2^Height chunks starting
// at chunk index Pos, which is a multiple of 2^Height.
type Subtree struct {
	CV     CV
	Height uint8
	Pos    uint64
}

// Block is a block's chaining value and its byte offset.
type Block struct {
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
// the current block height is recorded; whenever the bytes written so far
// call for a larger block (ChunkLog is monotone) the block height rises by
// one and recorded siblings merge, so the block list follows the rule without
// knowing the body's length in advance. The last chunk is held back until the
// end, since the root node is a chunk node when the body is a single chunk.
type Hasher struct {
	start   int64  // byte offset of the first byte written
	offset  uint64 // chunk index of the first whole chunk
	counter uint64 // chunk index of buf[0]
	size    int64  // bytes written
	head    []byte // bytes before the first chunk boundary, at most ChunkSize-1
	skip    int    // bytes still to go to head before chunking begins
	buf     [bufChunks * ChunkSize]byte
	buflen  int

	stack       []node // maximal aligned subtrees of [offset, counter), left to right
	blockHeight uint8  // current block size as a chunk log (a subtree height)
	fixed       bool   // blockHeight was chosen by the caller and never rises
	blocks      []node // complete blocks at blockHeight, in order
}

// NewHasher returns a Hasher for bytes starting at offset in the body. A
// whole body starts at 0. An offset inside a chunk is allowed: the bytes up
// to the next chunk boundary become the range's Head.
func NewHasher(offset int64) (*Hasher, error) {
	if offset < 0 {
		return nil, fmt.Errorf("blake3tree: negative offset %d", offset)
	}
	chunk := (uint64(offset) + ChunkSize - 1) / ChunkSize
	return &Hasher{
		start:       offset,
		offset:      chunk,
		counter:     chunk,
		skip:        int(int64(chunk)*ChunkSize - offset),
		blockHeight: MinChunkLog,
	}, nil
}

// NewHasherAtChunkLog is NewHasher with the block size fixed at 2^chunkLog
// chunks instead of following ChunkLog as the body grows. It is for
// producing a Bao outboard at a block size chosen by the reader rather than
// by ingot: 4 is iroh's fixed 16 KiB block, 0 the original Bao chunk. The
// chunk log may run from 0 to MaxChunkLog. The blocks are held in memory,
// 32 bytes per block, so a fine block over a large body costs accordingly.
func NewHasherAtChunkLog(offset int64, chunkLog_ uint8) (*Hasher, error) {
	if chunkLog_ > MaxChunkLog {
		return nil, fmt.Errorf("blake3tree: chunk log %d is above %d", chunkLog_, MaxChunkLog)
	}
	h, err := NewHasher(offset)
	if err != nil {
		return nil, err
	}
	h.blockHeight, h.fixed = chunkLog_, true
	return h, nil
}

// Write implements io.Writer. It never fails.
func (h *Hasher) Write(p []byte) (int, error) {
	n := len(p)
	if h.skip > 0 {
		c := min(h.skip, len(p))
		h.head = append(h.head, p[:c]...)
		h.skip -= c
		p = p[c:]
	}
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
	for !h.fixed && ChunkLog(h.size) > h.blockHeight {
		h.promote()
	}
	return n, nil
}

// flush compresses the full buffer: as one 16-chunk subtree when it is
// aligned to one (the whole-body case, and every full buffer of a range
// once it reaches alignment) and the blocks are at least that large, else
// chunk by chunk, so no block is skipped over.
func (h *Hasher) flush() {
	if h.counter%bufChunks == 0 && int(h.blockHeight) >= bits.TrailingZeros(bufChunks) {
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
// Each node formed at the block height is recorded as a block.
func (h *Hasher) push(cv [8]uint32, height int, pos uint64) {
	hgt := uint8(height)
	for {
		if hgt == h.blockHeight {
			h.blocks = append(h.blocks, node{cv, hgt, pos})
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

// promote raises the block height by one. Recorded siblings merge into their
// parent; an unpaired block is a fragment at the new height whose parent the
// stack forms later (a trailing block) or never (a leading one in a range),
// and is dropped from the block list. It runs between writes, so a parent
// formed while the block height was still lower is counted once, by the
// pairing.
func (h *Hasher) promote() {
	hgt := h.blockHeight
	var next []node
	for i := 0; i < len(h.blocks); {
		l := h.blocks[i]
		if (l.pos>>hgt)&1 == 0 && i+1 < len(h.blocks) && h.blocks[i+1].pos == l.pos+1<<hgt {
			next = append(next, node{parentCV(l.cv, h.blocks[i+1].cv), hgt + 1, l.pos})
			i += 2
			continue
		}
		i++
	}
	h.blocks = next
	h.blockHeight++
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
// returns its root and blocks. The hasher must not be written to again.
func (h *Hasher) FinishObject() Object {
	if h.offset != 0 {
		panic("blake3tree: FinishObject on a range hasher")
	}
	last := h.drain()
	root := bytesOf(guts.ChainingValue(h.rootNode(last)))

	// The body's tail (the last, possibly short, block) is the merge of the
	// stack entries below the block height with the last chunk. For a body
	// started at 0 the stack's heights fall from bottom to top, so those
	// entries are the top ones.
	hgt := h.blockHeight
	n := guts.CompressChunk(last, &guts.IV, h.counter, 0)
	for i := len(h.stack) - 1; i >= 0 && h.stack[i].height < hgt; i-- {
		n = guts.ParentNode(h.stack[i].cv, guts.ChainingValue(n), &guts.IV, 0)
	}
	blocks := h.blocks
	if h.size > 0 {
		blocks = append(blocks[:len(blocks):len(blocks)], node{guts.ChainingValue(n), hgt, uint64(len(h.blocks)) << hgt})
	}

	// Write keeps the block height at ChunkLog of the bytes so far, so it is
	// the body's block now.
	out := Object{Size: h.size, Root: root, ChunkLog: h.blockHeight, Blocks: make([]CV, len(blocks))}
	for j, l := range blocks {
		out.Blocks[j] = bytesOf(l.cv)
	}
	return out
}

// FinishRange completes a byte range and returns its subtrees, the blocks
// wholly within it, and its head and tail bytes. The hasher must not be
// written to again.
func (h *Hasher) FinishRange() Range {
	last := h.drain()
	out := Range{
		Offset:   h.start,
		Size:     h.size,
		Head:     h.head,
		ChunkLog: h.blockHeight,
	}
	if h.start == 0 {
		// The range as a body of its own: its last bytes are that body's
		// final chunk.
		out.Root, out.HasRoot = bytesOf(guts.ChainingValue(h.rootNode(last))), true
	}
	if len(last) == ChunkSize {
		h.pushChunk(last, h.counter)
		h.counter++
	} else if len(last) > 0 {
		out.Tail = last
	}
	out.Subtrees = make([]Subtree, len(h.stack))
	for i, s := range h.stack {
		out.Subtrees[i] = Subtree{CV: bytesOf(s.cv), Height: s.height, Pos: s.pos}
	}
	out.Blocks = make([]Block, len(h.blocks))
	for i, l := range h.blocks {
		out.Blocks[i] = Block{CV: bytesOf(l.cv), Offset: int64(l.pos) * ChunkSize}
	}
	return out
}

// BlockSubtrees returns the range's blocks as subtrees of the body's tree,
// the form Assemble consumes alongside Subtrees.
func (r Range) BlockSubtrees() []Subtree {
	out := make([]Subtree, len(r.Blocks))
	for i, l := range r.Blocks {
		out[i] = Subtree{CV: l.CV, Height: r.ChunkLog, Pos: uint64(l.Offset) / ChunkSize}
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
// subtrees (or the single range's own root), and the blocks at the body's
// block from the ranges' subtrees and blocks. A chunk that straddles two
// ranges is hashed here from the first range's Tail and the second's Head,
// and the last range's Tail is hashed as the body's final chunk; so a range
// boundary inside a chunk costs one compression. The blocks are checked
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
	if len(filled) > 0 && len(filled[0].Head) > 0 {
		return Object{}, false
	}

	// The body's nodes: each range's subtrees, plus the chunks the ranges
	// only hold pieces of. pending collects such a chunk's bytes across the
	// ranges it spans (a range shorter than a chunk may lie wholly inside
	// one) until a range reaches a chunk boundary, where it must be whole.
	var nodes []Subtree
	var pending []byte
	var pendingChunk uint64
	for _, r := range filled {
		if len(pending) == 0 {
			pendingChunk = uint64(r.Offset) / ChunkSize
		}
		pending = append(pending, r.Head...)
		if len(r.Head) < int((ChunkSize-r.Offset%ChunkSize)%ChunkSize) {
			// The range ends inside the chunk it started in.
			if len(r.Subtrees) > 0 || len(r.Tail) > 0 {
				return Object{}, false
			}
			continue
		}
		switch len(pending) {
		case 0:
		case ChunkSize:
			nodes = append(nodes, chunkNode(pending, pendingChunk))
		default:
			return Object{}, false
		}
		nodes = append(nodes, r.Subtrees...)
		pending = bytes.Clone(r.Tail)
		pendingChunk = r.tailChunk()
	}
	if len(pending) > 0 {
		// The body's final, short chunk.
		nodes = append(nodes, chunkNode(pending, pendingChunk))
	}

	var root CV
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
		nodes = append(nodes, r.BlockSubtrees()...)
	}

	out := Object{Size: total, Root: root, ChunkLog: ChunkLog(total)}
	block := BlockSize(out.ChunkLog)
	for off := int64(0); off < total; off += block {
		end := min(off+block, total)
		cv, ok := CoverCV(nodes, uint64(off)/ChunkSize, uint64(end+ChunkSize-1)/ChunkSize)
		if !ok {
			return Object{}, false
		}
		out.Blocks = append(out.Blocks, cv)
	}
	if len(out.Blocks) >= 2 {
		if got, _ := RootFromBlocks(out.Blocks); got != root {
			return Object{}, false
		}
	}
	return out, true
}

// tailChunk is the chunk index of the range's Tail: the chunk after its
// last whole one.
func (r Range) tailChunk() uint64 {
	return uint64(r.Offset+r.Size-int64(len(r.Tail))) / ChunkSize
}

// TailChunk returns the range's Tail hashed as a chunk at its index, for a
// range that ends the body: the body's final, short chunk. ok is false when
// the range has no tail. For a range followed by another, the tail is part
// of the chunk straddling the boundary instead (see Assemble).
func (r Range) TailChunk() (Subtree, bool) {
	if len(r.Tail) == 0 {
		return Subtree{}, false
	}
	return chunkNode(r.Tail, r.tailChunk()), true
}

// chunkNode hashes one chunk, whole or short, at its index.
func chunkNode(chunk []byte, pos uint64) Subtree {
	n := guts.CompressChunk(chunk, &guts.IV, pos, 0)
	return Subtree{CV: bytesOf(guts.ChainingValue(n)), Height: 0, Pos: pos}
}

// ParentCV returns the chaining value of the parent of two sibling nodes.
func ParentCV(left, right CV) CV {
	return bytesOf(parentCV(wordsOf(left), wordsOf(right)))
}

// RootFromBlocks returns the root of a body from its blocks, as a client
// holding the block list computes it to check the list against the digest.
// A body of one block has no parent to apply the root flag to: its root is
// the hash of the bytes, and ok is false.
func RootFromBlocks(blocks []CV) (root CV, ok bool) {
	if len(blocks) < 2 {
		return CV{}, false
	}
	k := leftCount(len(blocks))
	l, r := subtreeCV(blocks[:k]), subtreeCV(blocks[k:])
	n := guts.ParentNode(wordsOf(l), wordsOf(r), &guts.IV, guts.FlagRoot)
	return bytesOf(guts.ChainingValue(n)), true
}

// subtreeCV merges equal-size aligned blocks into their subtree's chaining
// value: the left subtree takes the largest power of two of them that is
// strictly less than the count, as BLAKE3 does with chunks.
func subtreeCV(blocks []CV) CV {
	if len(blocks) == 1 {
		return blocks[0]
	}
	k := leftCount(len(blocks))
	return ParentCV(subtreeCV(blocks[:k]), subtreeCV(blocks[k:]))
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
// blocks, checking that each starts where the previous ended and is aligned
// to its size.
func stackOf(subs []Subtree) (*Hasher, bool) {
	if len(subs) == 0 {
		return nil, false
	}
	h := &Hasher{blockHeight: 64}
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

// emptyRoot is the BLAKE3 hash of no bytes.
func emptyRoot() CV {
	n := guts.CompressChunk(nil, &guts.IV, 0, guts.FlagRoot)
	return bytesOf(guts.ChainingValue(n))
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

// Outboard returns the Bao outboard of a body from its blocks: the standard
// pre-order layout a Bao library loads directly, with the blocks' chunk log
// as the Bao block size. It starts with the body's size as 8 little-endian
// bytes, then one 64-byte entry per parent node above the blocks, each the
// chaining values of its two children, root first and left subtree before
// right. A body of one block has no parents and the outboard is the prefix
// alone. The blocks themselves are not stored: they are the children in the
// lowest entries, and a verifier recomputes them from the data.
func Outboard(blocks []CV, size int64) []byte {
	out := make([]byte, 8, 8+64*max(len(blocks)-1, 0))
	binary.LittleEndian.PutUint64(out, uint64(size))
	if len(blocks) > 1 {
		_, out = preorder(blocks, out)
	}
	return out
}

// preorder appends the pre-order parent entries of blocks to out and
// returns the subtree's chaining value. The entry for a node precedes its
// children's, so the children are computed first and their entries follow.
func preorder(blocks []CV, out []byte) (CV, []byte) {
	if len(blocks) == 1 {
		return blocks[0], out
	}
	k := leftCount(len(blocks))
	at := len(out)
	out = append(out, make([]byte, 64)...)
	l, out := preorder(blocks[:k], out)
	r, out := preorder(blocks[k:], out)
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

// OutboardBlocks checks an outboard against the chunk log it is claimed to
// be at, both supplied by a client, and returns the number of blocks. The
// chunk log may be at most MaxChunkLog (any block size a Bao library
// accepts; ingot itself records none below MinChunkLog), and the outboard must
// hold exactly one parent entry per block but one, which ties the chunk log to
// the body size in its prefix. A client-supplied pair that fails this is a
// mistake, and this reports it before any verification arithmetic.
func OutboardBlocks(outboard []byte, chunkLog_ uint8) (int64, error) {
	if err := CheckOutboard(outboard); err != nil {
		return 0, err
	}
	if chunkLog_ > MaxChunkLog {
		return 0, fmt.Errorf("blake3tree: chunk log %d is above %d", chunkLog_, MaxChunkLog)
	}
	size := OutboardSize(outboard)
	if size < 0 {
		return 0, fmt.Errorf("blake3tree: outboard size prefix %d is negative", uint64(size))
	}
	blocks := blocksIn(size, chunkLog_)
	if parents := int64((len(outboard) - 8) / 64); parents != max(blocks-1, 0) {
		return 0, fmt.Errorf("blake3tree: outboard has %d parent entries but a %d-byte body at chunk log %d has %d blocks", parents, size, chunkLog_, blocks)
	}
	return blocks, nil
}

// AlignedRange widens the inclusive byte range [a, b] of a body to the
// block-aligned inclusive range a verifier needs: down to a block boundary
// and up to one, or to the body's end. A block is the only unit the outboard
// can verify, since each block is the hash of a whole block.
func AlignedRange(a, b int64, chunkLog_ uint8, size int64) (start, end int64, err error) {
	if chunkLog_ > MaxChunkLog {
		return 0, 0, fmt.Errorf("blake3tree: chunk log %d is above %d", chunkLog_, MaxChunkLog)
	}
	if a < 0 || b < a {
		return 0, 0, fmt.Errorf("blake3tree: range %d-%d is not ascending", a, b)
	}
	if a >= size {
		return 0, 0, fmt.Errorf("blake3tree: range starts at %d but the body is %d bytes", a, size)
	}
	block := BlockSize(chunkLog_)
	start = a / block * block
	// The end block's last byte, clamped to the body; computed by block
	// count so a block near the int64 limit cannot overflow the product.
	if endBlocks := b/block + 1; endBlocks >= blocksIn(size, chunkLog_) {
		end = size - 1
	} else {
		end = endBlocks*block - 1
	}
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
// offset must be a multiple of the block size and the data must hold at
// least one whole block, except that it may end at the body's end (and an
// empty body verifies with no data). A block that fails is returned as a
// *BlockError after the bytes before it; any other error is a malformed
// input. The buffer is one block or the rest of the body, whichever is
// smaller, so a huge block size over a small body costs nothing.
func VerifyBlocks(r io.Reader, outboard []byte, chunkLog_ uint8, offset int64, root CV) (int64, error) {
	if _, err := OutboardBlocks(outboard, chunkLog_); err != nil {
		return 0, err
	}
	size := OutboardSize(outboard)
	block := BlockSize(chunkLog_)
	if offset < 0 || offset%block != 0 {
		return 0, fmt.Errorf("blake3tree: offset %d is not a non-negative multiple of the block size %d", offset, block)
	}
	if offset > size {
		return 0, fmt.Errorf("blake3tree: offset %d is past the body's %d bytes", offset, size)
	}
	if offset == size {
		// Nothing left to verify: an empty body verifies with no data, but a
		// request at the end of a non-empty body is a mistake, as is any
		// data where none can be checked.
		var extra [1]byte
		if m, _ := io.ReadFull(r, extra[:]); m > 0 {
			return 0, fmt.Errorf("blake3tree: data runs past the body's end")
		}
		if size > 0 {
			return 0, fmt.Errorf("blake3tree: offset %d is the body's end; nothing to verify", offset)
		}
		// The empty body has no block to check against the outboard, so the
		// root itself is what authenticates it: it must be the hash of
		// nothing.
		if root != emptyRoot() {
			return 0, &BlockError{0, 0}
		}
		return 0, nil
	}
	buf := make([]byte, min(block, size-offset))
	var verified int64
	for {
		n, err := io.ReadFull(r, buf)
		if n == 0 && (err == io.EOF || err == io.ErrUnexpectedEOF) {
			if verified == 0 && size > 0 {
				return 0, fmt.Errorf("blake3tree: no data to verify (the body is %d bytes)", size)
			}
			return verified, nil
		}
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return verified, err
		}
		pos := offset + verified
		if int64(n) > size-pos {
			return verified, fmt.Errorf("blake3tree: data runs %d bytes past the body's end", int64(n)-(size-pos))
		}
		if int64(n) < block && pos+int64(n) != size {
			return verified, fmt.Errorf("blake3tree: data ends %d bytes into the block at %d; a verifiable range holds whole blocks or ends at the body's end (%d)", n, pos, size)
		}
		if !bao.VerifyChunk(bytes.Clone(buf[:n]), outboard, int(chunkLog_), uint64(pos), root) {
			return verified, &BlockError{pos, int64(n)}
		}
		verified += int64(n)
		if int64(n) < block || verified == size-offset {
			// The body's tail, or its last whole block: nothing follows but
			// EOF, which is what the next read must see.
			var extra [1]byte
			if m, _ := io.ReadFull(r, extra[:]); m > 0 {
				return verified, fmt.Errorf("blake3tree: data runs past the body's end")
			}
			return verified, nil
		}
	}
}
