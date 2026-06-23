package bucket

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"io"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
)

// DefaultMaxBlobSize is the blob ceiling used when callers don't supply
// one. 256 MiB matches Piri's current single-blob limit (MaxMemtreeSize);
// objects larger than this are coarsely split into ≤ max blobs.
const DefaultMaxBlobSize int64 = 256 << 20

// rawBlockPrefix produces CIDs for body blobs: CIDv1, raw codec (0x55),
// sha256 multihash. Blobs are opaque bytes — no IPLD links — so the raw
// codec is the natural fit, and the multihash is exactly the digest Piri
// stores the blob under.
var rawBlockPrefix = cid.Prefix{
	Version:  1,
	Codec:    cid.Raw,
	MhType:   mh.SHA2_256,
	MhLength: -1,
}

// BodyWriter splits the bytes from r into ordered, content-addressed
// blobs (each ≤ max_blob_size), writes each blob to w as a raw block,
// and returns a Body describing the ordered blob list plus the
// whole-object size/sha256/md5.
//
// bucketop.Tx satisfies the blockstore.WriteStore argument.
type BodyWriter interface {
	Chunk(ctx context.Context, w blockstore.WriteStore, r io.Reader) (Body, error)
}

// BodyReader streams bytes back out of a Body by reading its blobs in
// order. blockstore.Layered satisfies the blockstore.ReadStore argument.
type BodyReader interface {
	// Open returns a stream over the full body.
	Open(ctx context.Context, bs blockstore.ReadStore, body Body) io.ReadCloser
	// OpenRange returns a stream over [start, end] inclusive.
	OpenRange(ctx context.Context, bs blockstore.ReadStore, body Body, start, end int64) io.ReadCloser
}

// BodyCodec is the canonical pair: a single concrete impl satisfies
// both halves so a Body produced by Chunk can always be read back via
// Open / OpenRange of the same codec instance.
type BodyCodec interface {
	BodyWriter
	BodyReader
}

// BlobSplitter is the default codec: it coarsely splits a body into
// raw blocks of at most MaxBlobSize bytes and records them as an ordered
// BlobRef list in the Body. Implements BodyCodec.
type BlobSplitter struct {
	// MaxBlobSize is the blob ceiling in bytes. 0 → DefaultMaxBlobSize.
	MaxBlobSize int64
}

// Compile-time assertion: BlobSplitter is the canonical BodyCodec.
var _ BodyCodec = (*BlobSplitter)(nil)

// Chunk reads body bytes from r, splits them into blobs of at most
// MaxBlobSize bytes, writes each blob as a raw block, and returns a Body
// whose Blobs list covers [0, Size) contiguously. sha256 and md5 are
// computed over the whole body in a single streaming pass. A zero-byte
// body yields a Body with no blobs (and the well-known empty digests).
func (c *BlobSplitter) Chunk(ctx context.Context, w blockstore.WriteStore, r io.Reader) (Body, error) {
	max := c.MaxBlobSize
	if max <= 0 {
		max = DefaultMaxBlobSize
	}

	bodyHasher := sha256.New()
	etagHasher := md5.New()
	// Tee everything read into both hashers so the whole-body digests are
	// computed in the same pass that splits the body into blobs.
	src := io.TeeReader(r, io.MultiWriter(bodyHasher, etagHasher))

	var blobs []BlobRef
	var total int64
	for {
		// CopyN grows buf only to the actual blob size, so a small object
		// never allocates a full max-sized buffer. Each iteration uses a
		// fresh buffer, so the block we hand to the staging store keeps its
		// own backing array (reusing one buffer would corrupt earlier blobs,
		// which the staging store holds by reference until commit).
		var buf bytes.Buffer
		n, err := io.CopyN(&buf, src, max)
		if n > 0 {
			data := buf.Bytes()
			cidv, perr := putRawBlock(ctx, w, data)
			if perr != nil {
				return Body{}, fmt.Errorf("put blob: %w", perr)
			}
			blobs = append(blobs, BlobRef{Digest: cidv.Hash(), Offset: total, Length: n})
			total += n
		}
		if err == nil {
			// Read a full max-sized blob; more bytes may follow.
			continue
		}
		if err == io.EOF {
			break
		}
		return Body{}, fmt.Errorf("read body: %w", err)
	}

	return Body{
		Size:   total,
		SHA256: bodyHasher.Sum(nil),
		MD5:    etagHasher.Sum(nil),
		Blobs:  blobs,
	}, nil
}

// Open returns a reader over the full body.
func (c *BlobSplitter) Open(ctx context.Context, bs blockstore.ReadStore, body Body) io.ReadCloser {
	return &blobBodyReader{ctx: ctx, bs: bs, blobs: body.Blobs, pos: 0, end: body.Size - 1}
}

// OpenRange returns a reader over [start, end] inclusive of the body.
// Caller must ensure 0 <= start <= end <= Size-1.
func (c *BlobSplitter) OpenRange(ctx context.Context, bs blockstore.ReadStore, body Body, start, end int64) io.ReadCloser {
	return &blobBodyReader{ctx: ctx, bs: bs, blobs: body.Blobs, pos: start, end: end}
}

func putRawBlock(ctx context.Context, w blockstore.BlockWriter, data []byte) (cid.Cid, error) {
	c, err := rawBlockPrefix.Sum(data)
	if err != nil {
		return cid.Undef, err
	}
	blk, err := block.NewBlockWithCid(data, c)
	if err != nil {
		return cid.Undef, err
	}
	if err := w.PutBlock(ctx, blk); err != nil {
		return cid.Undef, err
	}
	return c, nil
}

// blobBodyReader streams a Body's blobs lazily. It walks the ordered
// BlobRef list, fetching the raw block for the blob that covers the
// current position, and serves bytes from it until the inclusive end
// position. Whole-body and ranged reads share the same loop — only the
// initial pos and the end differ.
type blobBodyReader struct {
	ctx   context.Context
	bs    blockstore.ReadStore
	blobs []BlobRef

	pos int64 // current absolute byte position (next byte to return)
	end int64 // last byte to return (inclusive)

	cur    []byte // bytes of the currently-loaded blob
	curOff int    // read position within cur
	err    error
}

func (br *blobBodyReader) Read(p []byte) (int, error) {
	if br.err != nil {
		return 0, br.err
	}
	if br.pos > br.end {
		br.err = io.EOF
		return 0, io.EOF
	}

	if br.cur == nil || br.curOff >= len(br.cur) {
		b, ok := blobAt(br.blobs, br.pos)
		if !ok {
			// The Blobs list does not cover pos — a malformed manifest.
			br.err = io.ErrUnexpectedEOF
			return 0, br.err
		}
		blk, err := br.bs.GetBlock(br.ctx, cid.NewCidV1(cid.Raw, mh.Multihash(b.Digest)))
		if err != nil {
			br.err = fmt.Errorf("read blob @%d: %w", b.Offset, err)
			return 0, br.err
		}
		br.cur = blk.RawData()
		br.curOff = int(br.pos - b.Offset)
	}

	// Don't read past the inclusive end position or the current blob.
	remaining := br.end - br.pos + 1
	available := int64(len(br.cur) - br.curOff)
	want := int64(len(p))
	if want > available {
		want = available
	}
	if want > remaining {
		want = remaining
	}

	n := copy(p[:want], br.cur[br.curOff:br.curOff+int(want)])
	br.curOff += n
	br.pos += int64(n)
	return n, nil
}

func (br *blobBodyReader) Close() error { return nil }

// blobAt returns the BlobRef whose [Offset, Offset+Length) span contains
// pos. The list is ordered and contiguous, so a forward scan suffices.
func blobAt(blobs []BlobRef, pos int64) (BlobRef, bool) {
	for _, b := range blobs {
		if pos >= b.Offset && pos < b.Offset+b.Length {
			return b, true
		}
	}
	return BlobRef{}, false
}
