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

// assumedPartOffset is the object byte offset UploadPart hashes a part's
// tree at. The server learns a part's true offset only once every
// lower-numbered part exists, so:
//
//   - part 1 is at 0;
//   - when every lower part is recorded, the offset is their sizes' sum
//     (exact);
//   - when some lower part is recorded, parts are assumed uniform and the
//     lowest recorded part's size stands in for each missing one;
//   - otherwise the part's own size does.
//
// Clients upload parts of one size in parallel with a shorter final part,
// so the assumed offset is the true one for every part of such an upload
// except a final part that lands before any of its predecessors. Complete
// checks each assumed offset and re-hashes the parts whose assumption was
// wrong. prior is the session's recorded parts; a part being superseded is
// ignored.
func assumedPartOffset(prior []registry.MultipartPart, partNumber int, size int64) int64 {
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

// DefaultRehashBudget is the re-read CompleteMultipartUpload allows itself
// for non-final parts hashed at a wrong assumed offset: 5 GiB, S3's maximum
// part size. The final part is exempt: in a uniform-size upload it is the
// only part whose assumption can fail (it lands before any predecessor and
// its own size stands in), so it is always re-hashed when wrong, bounded by
// the part maximum. The budget therefore governs only variable-size uploads,
// and Complete stays within seconds of local reading either way.
const DefaultRehashBudget = 5 << 30 // 5 GiB

// partTreeOf serializes a part's finished tree for its record.
func partTreeOf(rng blake3tree.Range) *registry.PartTree {
	t := &registry.PartTree{
		Offset:   rng.Offset,
		ChunkLog: rng.ChunkLog,
		Nodes:    blake3tree.EncodeSubtrees(rng.Subtrees),
		Blocks:   blake3tree.EncodeSubtrees(rng.BlockSubtrees()),
		Head:     append([]byte(nil), rng.Head...),
		Tail:     append([]byte(nil), rng.Tail...),
	}
	if rng.HasRoot {
		t.Root = append([]byte(nil), rng.Root[:]...)
	}
	return t
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
// was wrong: the final part whenever it was, the others within the re-hash
// budget. parts are the completed
// parts in order, offsets their true byte offsets in body, and body the
// assembled object (its blobs are what a re-hash reads). ok is false when
// the body gets no digest: the re-read needed exceeds the budget, or the
// parts' trees fail the assembly's consistency check. Neither is an error
// for the completion; the object simply has no x-cid.
func (b *Backend) multipartTree(ctx context.Context, space did.DID, parts []registry.MultipartPart, offsets []int64, body msbucket.Body) (_ blake3tree.Object, ok bool, err error) {
	src := &bodySource{b: b, space: space, body: body}
	var rehash int64
	for i, p := range parts[:len(parts)-1] {
		if p.Tree == nil || p.Tree.Offset != offsets[i] {
			rehash += p.Size
		}
	}
	if rehash > b.rehashBudget {
		b.logger.Info("multipart object completes without a BLAKE3 digest: re-hash over budget",
			zap.Int("parts", len(parts)), zap.Int64("size", body.Size), zap.Int64("rehash", rehash), zap.Int64("budget", b.rehashBudget))
		return blake3tree.Object{}, false, nil
	}
	ranges := make([]blake3tree.Range, 0, len(parts))
	for i, p := range parts {
		if p.Tree != nil && p.Tree.Offset == offsets[i] {
			rng, err := treeRange(p.Tree, p.Size)
			if err == nil {
				ranges = append(ranges, rng)
				continue
			}
			b.logger.Warn("multipart part tree record unreadable; re-hashing the part", zap.Int("part", p.PartNumber), zap.Error(err))
		}
		rng, err := src.rehash(ctx, offsets[i], offsets[i]+p.Size)
		if err != nil {
			return blake3tree.Object{}, false, err
		}
		ranges = append(ranges, rng)
	}
	obj, ok := blake3tree.Assemble(ranges)
	if !ok {
		b.logger.Warn("multipart object completes without a BLAKE3 digest: the parts' trees do not assemble into one body",
			zap.Int("parts", len(parts)), zap.Int64("size", body.Size))
		return blake3tree.Object{}, false, nil
	}
	return obj, true, nil
}

// bodySource re-reads a completing body's plaintext for the tree, through
// one opener built on first use and shared by every range.
type bodySource struct {
	b      *Backend
	space  did.DID
	body   msbucket.Body
	opened msbucket.BlobRangeOpener
}

func (s *bodySource) opener(ctx context.Context) (msbucket.BlobRangeOpener, error) {
	if s.opened == nil {
		o, err := s.b.bodyOpener(ctx, s.space, s.body)
		if err != nil {
			return nil, err
		}
		s.opened = o
	}
	return s.opened, nil
}

// rehash hashes the body's bytes [start, end) at their offset.
func (s *bodySource) rehash(ctx context.Context, start, end int64) (blake3tree.Range, error) {
	h, err := blake3tree.NewHasher(start)
	if err != nil {
		return blake3tree.Range{}, err
	}
	if end > start {
		opener, err := s.opener(ctx)
		if err != nil {
			return blake3tree.Range{}, err
		}
		rc := msbucket.OpenBodyRange(ctx, opener, s.space, s.body, start, end-1)
		defer rc.Close()
		if _, err := io.Copy(h, rc); err != nil {
			return blake3tree.Range{}, fmt.Errorf("re-hash object bytes [%d,%d): %w", start, end, err)
		}
	}
	return h.FinishRange(), nil
}
