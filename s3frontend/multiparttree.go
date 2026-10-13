package s3frontend

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3err"
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
// the part maximum. The budget therefore governs only variable-size parts
// uploaded out of order, and bounds how long Complete spends reading parts
// back from their storage nodes. A completion that would need more is
// refused (errPartLayoutOverBudget); an object is never committed without
// its digest.
const DefaultRehashBudget = 5 << 30 // 5 GiB

// rehashNeeded is the byte count of the non-final parts whose assumed
// offset was wrong, or whose tree was not recorded, which Complete must
// read back to hash. parts are the completed parts in order and offsets
// their true byte offsets in the object.
func rehashNeeded(parts []registry.MultipartPart, offsets []int64) int64 {
	var n int64
	for i := 0; i+1 < len(parts); i++ {
		if p := parts[i]; p.Tree == nil || p.Tree.Offset != offsets[i] {
			n += p.Size
		}
	}
	return n
}

// errPartLayoutOverBudget refuses a Complete whose re-hash would exceed the
// budget: a 400 the client resolves by uploading parts of one size, or in
// part-number order, and completing again. (Not in the versitygw error
// table, hence built here.)
func errPartLayoutOverBudget(need, budget int64) s3err.APIError {
	return s3err.APIError{
		Code: "InvalidPartLayout",
		Description: fmt.Sprintf("The parts were uploaded out of order with varying sizes, so %d bytes would have to be read back to compute the object's digest, over this server's limit of %d bytes. "+
			"Upload parts of one size (the last part may be shorter), or upload them in part-number order, then complete the upload again.", need, budget),
		HTTPStatusCode: http.StatusBadRequest,
	}
}

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
// was wrong or whose record cannot serve. parts are the completed parts in
// order, offsets their true byte offsets in body, and body the assembled
// object (its blobs are what a re-hash reads, decrypting unless plaintext). The caller has already held
// the non-final re-read within the budget (rehashNeeded). Parts' trees that
// fail the assembly's consistency check are an error: the object is
// committed neither with a digest that might be wrong nor without one.
func (b *Backend) multipartTree(ctx context.Context, space did.DID, plaintext bool, parts []registry.MultipartPart, offsets []int64, body msbucket.Body) (blake3tree.Object, error) {
	src := &bodySource{b: b, space: space, body: body, plaintext: plaintext}
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
			return blake3tree.Object{}, err
		}
		ranges = append(ranges, rng)
	}
	obj, ok := blake3tree.Assemble(ranges)
	if !ok {
		return blake3tree.Object{}, fmt.Errorf("the trees of %d parts do not assemble into one %d-byte body", len(parts), body.Size)
	}
	return obj, nil
}

// bodySource re-reads a completing body's plaintext for the tree, through
// one opener built on first use and shared by every range.
type bodySource struct {
	b         *Backend
	space     did.DID
	body      msbucket.Body
	plaintext bool
	opened    msbucket.BlobRangeOpener
}

func (s *bodySource) opener(ctx context.Context) (msbucket.BlobRangeOpener, error) {
	if s.opened == nil {
		o, err := s.b.bodyOpener(ctx, s.space, s.body, s.plaintext)
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
