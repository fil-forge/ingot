package bucket

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"io"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blake3tree"
)

// CID returns the body's content identifier: a CIDv1 with the raw codec over
// the body's BLAKE3 multihash, the value GET and HEAD return in the x-cid
// header. ok is false for a body written before the digest was recorded.
func (b Body) CID() (c cid.Cid, ok bool) {
	if len(b.BLAKE3) == 0 {
		return cid.Undef, false
	}
	return cid.NewCidV1(cid.Raw, b.BLAKE3), true
}

// TreeLeafCVs decodes TreeLeaves into chaining values.
func (b Body) TreeLeafCVs() ([]blake3tree.CV, error) {
	if len(b.TreeLeaves)%blake3tree.CVSize != 0 {
		return nil, fmt.Errorf("bucket: tree leaves are %d bytes, not a multiple of %d", len(b.TreeLeaves), blake3tree.CVSize)
	}
	leaves := make([]blake3tree.CV, len(b.TreeLeaves)/blake3tree.CVSize)
	for i := range leaves {
		copy(leaves[i][:], b.TreeLeaves[i*blake3tree.CVSize:])
	}
	return leaves, nil
}

// bodyHashes are the whole-body digests SplitBody and SplitSizedBody compute
// in the pass that splits the body: the sha256 content hash, the BLAKE3 tree
// (digest and leaves), and, unless the caller holds it already, the MD5 the
// ETag is made from. The MD5 runs on its own goroutine (see asyncHash); the
// other two are fast enough to run inline.
type bodyHashes struct {
	sha  hash.Hash
	tree *blake3tree.Hasher // nil when the tree pass is skipped
	etag *lazyETagHash      // nil when the MD5 pass is skipped
}

func newBodyHashes(cfg splitConfig) *bodyHashes {
	h := &bodyHashes{sha: sha256.New()}
	if cfg.tree {
		tree, err := blake3tree.NewHasher(0)
		if err != nil {
			panic(err) // offset 0 is always aligned
		}
		h.tree = tree
	}
	if cfg.md5 {
		h.etag = &lazyETagHash{}
	}
	return h
}

// writer returns the sink the body is teed into.
func (h *bodyHashes) writer() io.Writer {
	ws := []io.Writer{h.sha}
	if h.tree != nil {
		ws = append(ws, h.tree)
	}
	if h.etag != nil {
		ws = append(ws, h.etag)
	}
	return io.MultiWriter(ws...)
}

// stop finishes the async MD5 so its goroutine exits; every return path
// must call it (Sum is idempotent, so body may follow it).
func (h *bodyHashes) stop() {
	if h.etag != nil {
		h.etag.Sum()
	}
}

// body assembles the Body for the split blobs once the whole body has been
// written.
func (h *bodyHashes) body(total int64, blobs []BlobRef) (Body, error) {
	body := Body{
		Size:   total,
		SHA256: h.sha.Sum(nil),
		Blobs:  blobs,
	}
	if h.tree != nil {
		if err := body.SetTree(h.tree.FinishObject()); err != nil {
			return Body{}, err
		}
	}
	if h.etag != nil {
		body.MD5 = h.etag.Sum()
	}
	return body, nil
}

// SetTree records a body's BLAKE3 tree material: the digest as a multihash,
// the group, and the leaves concatenated.
func (b *Body) SetTree(obj blake3tree.Object) error {
	digest, err := mh.Encode(obj.Root[:], mh.BLAKE3)
	if err != nil {
		return fmt.Errorf("encode blake3 multihash: %w", err)
	}
	b.BLAKE3, b.TreeGroup, b.TreeLeaves = digest, obj.GroupLog, nil
	if len(obj.Leaves) > 0 {
		b.TreeLeaves = make([]byte, 0, len(obj.Leaves)*blake3tree.CVSize)
		for _, cv := range obj.Leaves {
			b.TreeLeaves = append(b.TreeLeaves, cv[:]...)
		}
	}
	return nil
}
