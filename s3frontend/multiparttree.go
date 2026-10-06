package s3frontend

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

// guessPartOffset is the object byte offset UploadPart hashes a part's tree
// at. The server learns a part's true offset only once every lower-numbered
// part exists, so:
//
//   - part 1 is at 0;
//   - when every lower part is recorded, the offset is their sizes' sum
//     (exact);
//   - when some lower part is recorded, parts are assumed uniform and the
//     lowest recorded part's size stands in for each missing one;
//   - otherwise the part's own size does.
//
// Clients upload parts of one size in parallel with a shorter final part,
// so the guess is right for every part of such an upload except a final
// part that lands before any of its predecessors. Complete checks each
// guess and re-hashes the parts whose guess was wrong. prior is the
// session's recorded parts; a part being superseded is ignored.
func guessPartOffset(prior []registry.MultipartPart, partNumber int, size int64) int64 {
	if partNumber == 1 {
		return 0
	}
	var sum, unit int64
	below, lowest := 0, 0
	for _, p := range prior {
		if p.PartNumber >= partNumber {
			continue
		}
		sum += p.Size
		below++
		if lowest == 0 || p.PartNumber < lowest {
			lowest, unit = p.PartNumber, p.Size
		}
	}
	switch {
	case below == partNumber-1:
		return sum
	case below > 0:
		return int64(partNumber-1) * unit
	default:
		return int64(partNumber-1) * size
	}
}

// recordPartTree stores a part's finished tree on its record.
func recordPartTree(p *registry.MultipartPart, rng blake3tree.Range) {
	p.TreeOffset = rng.Offset
	p.TreeGroup = rng.GroupLog
	p.TreeNodes = blake3tree.EncodeSubtrees(rng.Subtrees)
	p.TreeLeaves = blake3tree.EncodeSubtrees(rng.LeafSubtrees())
	if rng.HasRoot {
		p.TreeRoot = append([]byte(nil), rng.Root[:]...)
	}
}

// recordedPartRange decodes a part's recorded tree as the range it covers.
func recordedPartRange(p registry.MultipartPart) (blake3tree.Range, error) {
	subs, err := blake3tree.DecodeSubtrees(p.TreeNodes)
	if err != nil {
		return blake3tree.Range{}, fmt.Errorf("part %d nodes: %w", p.PartNumber, err)
	}
	leafSubs, err := blake3tree.DecodeSubtrees(p.TreeLeaves)
	if err != nil {
		return blake3tree.Range{}, fmt.Errorf("part %d leaves: %w", p.PartNumber, err)
	}
	rng := blake3tree.Range{Offset: p.TreeOffset, Size: p.Size, Subtrees: subs, GroupLog: p.TreeGroup}
	for _, l := range leafSubs {
		rng.Leaves = append(rng.Leaves, blake3tree.Leaf{CV: l.CV, Offset: int64(l.Pos) * blake3tree.ChunkSize})
	}
	if len(p.TreeRoot) == blake3tree.CVSize {
		copy(rng.Root[:], p.TreeRoot)
		rng.HasRoot = true
	}
	return rng, nil
}

// multipartTree computes the tree material of a completed multipart body
// from its parts' records, re-reading only what the records cannot supply.
// parts are the completed parts in order, offsets their true byte offsets
// in body, and body the assembled object (its blobs are what a re-hash
// reads). Should the assembly fail its own consistency check, the whole
// body is re-hashed, so the result is always right; the records only make
// it cheap.
func (b *Backend) multipartTree(ctx context.Context, space did.DID, parts []registry.MultipartPart, offsets []int64, body msbucket.Body) (blake3tree.Object, error) {
	ranges, err := b.partRanges(ctx, space, parts, offsets, body)
	if err == nil {
		if obj, ok := blake3tree.Assemble(ranges); ok {
			return obj, nil
		}
		err = fmt.Errorf("the parts' trees do not assemble into one body")
	}
	b.logger.Warn("re-hashing multipart object for its tree", zap.Int("parts", len(parts)), zap.Int64("size", body.Size), zap.Error(err))
	opener, err := b.bodyOpener(ctx, space, body)
	if err != nil {
		return blake3tree.Object{}, err
	}
	h, _ := blake3tree.NewHasher(0)
	rc := msbucket.OpenBody(ctx, opener, space, body)
	defer rc.Close()
	if _, err := io.Copy(h, rc); err != nil {
		return blake3tree.Object{}, fmt.Errorf("re-hash object: %w", err)
	}
	return h.FinishObject(), nil
}

// partRanges returns one range per part, from the part's record when it
// was hashed at its true offset and re-hashed from its bytes otherwise. A
// part other than the last whose length is not a whole number of chunks
// puts a chunk boundary inside the next part, so no record from it onward
// is usable: the body from that part to its end is re-hashed as one range.
func (b *Backend) partRanges(ctx context.Context, space did.DID, parts []registry.MultipartPart, offsets []int64, body msbucket.Body) ([]blake3tree.Range, error) {
	usable := len(parts)
	for i := 0; i+1 < len(parts); i++ {
		if parts[i].Size%blake3tree.ChunkSize != 0 {
			usable = i
			break
		}
	}
	ranges := make([]blake3tree.Range, 0, len(parts))
	for i := 0; i < usable; i++ {
		p := parts[i]
		if p.TreeGroup != 0 && p.TreeOffset == offsets[i] {
			rng, err := recordedPartRange(p)
			if err == nil {
				ranges = append(ranges, rng)
				continue
			}
			b.logger.Warn("multipart part tree record unreadable; re-hashing the part", zap.Int("part", p.PartNumber), zap.Error(err))
		}
		rng, err := b.rehashRange(ctx, space, body, offsets[i], offsets[i]+p.Size)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, rng)
	}
	if usable < len(parts) {
		rng, err := b.rehashRange(ctx, space, body, offsets[usable], body.Size)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, rng)
	}
	return ranges, nil
}

// rehashRange hashes body's bytes [start, end) at their offset.
func (b *Backend) rehashRange(ctx context.Context, space did.DID, body msbucket.Body, start, end int64) (blake3tree.Range, error) {
	h, err := blake3tree.NewHasher(start)
	if err != nil {
		return blake3tree.Range{}, err
	}
	if end > start {
		opener, err := b.bodyOpener(ctx, space, body)
		if err != nil {
			return blake3tree.Range{}, err
		}
		rc := msbucket.OpenBodyRange(ctx, opener, space, body, start, end-1)
		defer rc.Close()
		if _, err := io.Copy(h, rc); err != nil {
			return blake3tree.Range{}, fmt.Errorf("re-hash object bytes [%d,%d): %w", start, end, err)
		}
	}
	return h.FinishRange(), nil
}
