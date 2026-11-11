package multipartqueue

import (
	"context"
	"fmt"
	"io"

	"github.com/fil-forge/ucantone/did"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blake3tree"
	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/registry"
)

// rehashNeeded is the byte count of the non-final parts whose assumed
// offset was wrong, or whose tree was not recorded, which Complete must
// read back to hash. parts are the completed parts in order, trees their
// recorded trees by part number, and offsets their true byte offsets in
// the object.
func rehashNeeded(parts []registry.MultipartPart, trees map[int]*registry.PartTree, offsets []int64) int64 {
	var n int64
	for i := 0; i+1 < len(parts); i++ {
		if t := trees[parts[i].PartNumber]; t == nil || t.Offset != offsets[i] {
			n += parts[i].Size
		}
	}
	return n
}

// treeRange decodes a part's recorded tree as the range it covers; size is
// the part's length.
func treeRange(t *registry.PartTree, size int64) (blake3tree.Range, error) {
	subs, err := blake3tree.DecodeSubtrees(t.Nodes)
	if err != nil {
		return blake3tree.Range{}, fmt.Errorf("nodes: %w", err)
	}
	leafSubs, err := blake3tree.DecodeSubtrees(t.Blocks)
	if err != nil {
		return blake3tree.Range{}, fmt.Errorf("blocks: %w", err)
	}
	rng := blake3tree.Range{Offset: t.Offset, Size: size, Subtrees: subs, ChunkLog: t.ChunkLog, Head: t.Head, Tail: t.Tail}
	for _, l := range leafSubs {
		rng.Blocks = append(rng.Blocks, blake3tree.Block{CV: l.CV, Offset: int64(l.Pos) * blake3tree.ChunkSize})
	}
	if len(t.Root) == blake3tree.CVSize {
		copy(rng.Root[:], t.Root)
		rng.HasRoot = true
	}
	return rng, nil
}

// multipartTree computes the tree material of a completed multipart body
// from its parts' records, re-reading only the parts whose assumed offset
// was wrong or whose record cannot serve. parts are the completed parts in
// order, trees their recorded trees by part number, offsets their true byte
// offsets in body, and body the assembled object (its blobs are what a
// re-hash reads, decrypting unless plaintext). The caller has already held
// the non-final re-read within the budget (rehashNeeded). Parts' trees that
// fail the assembly's consistency check are an error: the object is
// committed neither with a digest that might be wrong nor without one.
func (q *Queue) multipartTree(ctx context.Context, space did.DID, plaintext bool, parts []registry.MultipartPart, trees map[int]*registry.PartTree, offsets []int64, body msbucket.BlobRefs) (blake3tree.Object, error) {
	src := &bodySource{bodies: q.bodies, space: space, blobs: body, plaintext: plaintext}
	ranges := make([]blake3tree.Range, 0, len(parts))
	for i, p := range parts {
		if t := trees[p.PartNumber]; t != nil && t.Offset == offsets[i] {
			rng, err := treeRange(t, p.Size)
			if err == nil {
				ranges = append(ranges, rng)
				continue
			}
			q.logger.Warn("multipart part tree record unreadable; re-hashing the part", zap.Int("part", p.PartNumber), zap.Error(err))
		}
		rng, err := src.rehash(ctx, offsets[i], offsets[i]+p.Size)
		if err != nil {
			return blake3tree.Object{}, err
		}
		ranges = append(ranges, rng)
	}
	obj, ok := blake3tree.Assemble(ranges)
	if !ok {
		return blake3tree.Object{}, fmt.Errorf("the trees of %d parts do not assemble into one %d-byte body", len(parts), body.Size())
	}
	return obj, nil
}

// bodySource re-reads a completing body's plaintext for the tree. Each range
// gets its own opener, which prefetches only the blobs that range reads.
type bodySource struct {
	bodies    BodyReader
	space     did.DID
	blobs     msbucket.BlobIndex
	plaintext bool
}

// rehash hashes the body's bytes [start, end) at their offset.
func (s *bodySource) rehash(ctx context.Context, start, end int64) (blake3tree.Range, error) {
	h, err := blake3tree.NewHasher(start)
	if err != nil {
		return blake3tree.Range{}, err
	}
	if end > start {
		opener, err := s.bodies.BodyOpener(ctx, s.space, s.blobs, s.plaintext, start, end-1)
		if err != nil {
			return blake3tree.Range{}, err
		}
		rc := msbucket.OpenBodyRange(ctx, opener, s.space, s.blobs, start, end-1)
		defer rc.Close()
		if _, err := io.Copy(h, rc); err != nil {
			return blake3tree.Range{}, fmt.Errorf("re-hash object bytes [%d,%d): %w", start, end, err)
		}
	}
	return h.FinishRange(), nil
}

// partSpanLengths returns the length of the span each of p's blobs covers
// in the object, as recorded with the part. A part recorded before the
// lengths were kept takes them from its blobs' intents: the stored byte
// count is the span itself for a session stored as received, else the
// envelope's, from which the span follows by the blob's FEE geometry.
func (q *Queue) partSpanLengths(ctx context.Context, space did.DID, plaintext bool, p registry.MultipartPart) ([]int64, error) {
	if p.SpanLengths != nil {
		return p.SpanLengths, nil
	}
	sizes := make([]int64, len(p.BlobDigests))
	for i, d := range p.BlobDigests {
		in, err := q.intents.GetIntent(ctx, space, d)
		if err != nil {
			return nil, fmt.Errorf("multipartqueue: part blob %x: %w", d, err)
		}
		sizes[i] = in.Size
		if !plaintext {
			if sizes[i], err = q.bodies.PlaintextLen(ctx, space, d, in.Size); err != nil {
				return nil, fmt.Errorf("multipartqueue: part blob %x: %w", d, err)
			}
		}
	}
	return sizes, nil
}
