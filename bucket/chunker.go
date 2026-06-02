package bucket

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
)

// DefaultChunkSize is the chunk size used when callers don't supply one.
// 1 MiB matches typical UnixFS chunking and balances per-blob piri
// overhead against range-read granularity.
const DefaultChunkSize int64 = 1 << 20

// rawBlockPrefix produces CIDs for body chunks: CIDv1, raw codec (0x55),
// sha256 multihash. Chunks are opaque bytes — no IPLD links — so the raw
// codec is the natural fit.
var rawBlockPrefix = cid.Prefix{
	Version:  1,
	Codec:    cid.Raw,
	MhType:   mh.SHA2_256,
	MhLength: -1,
}

// BodyWriter writes the bytes from r as a sequence of blocks (raw
// chunks plus whatever index/DAG blocks the codec needs) to w and
// returns a Body record describing how to reconstruct the bytes.
//
// w accepts both raw block writes (PutBlock for chunk bytes) and
// CBOR-typed writes (Put for format-specific index blocks).
// bucketop.Tx satisfies it.
type BodyWriter interface {
	Chunk(ctx context.Context, w blockstore.WriteStore, r io.Reader) (Body, error)
}

// BodyReader streams bytes back out of a Body. Format identifies
// the codec the writer produced; consumers route a Body to the
// matching BodyReader by that string.
//
// bs accepts both raw block reads (GetBlock for chunk bytes) and
// CBOR-typed reads (Get for index blocks). blockstore.Layered
// satisfies it.
type BodyReader interface {
	// Format returns the Body.Format value this reader handles.
	Format() string
	// Open returns a stream over the full body.
	Open(ctx context.Context, bs blockstore.ReadStore, body Body) io.ReadCloser
	// OpenRange returns a stream over [start, end] inclusive.
	OpenRange(ctx context.Context, bs blockstore.ReadStore, body Body, start, end int64) io.ReadCloser
}

// BodyCodec is the canonical pair: a single concrete impl satisfies
// both halves so a Body produced by Chunk can always be read back
// via Open / OpenRange of the same codec instance.
type BodyCodec interface {
	BodyWriter
	BodyReader
}

// FixedChunker is the default codec: fixed-size raw chunks indexed
// by a FixedChunkerIndex CBOR document at Body.Content. Implements
// BodyCodec.
type FixedChunker struct {
	// ChunkSize is the body chunk size in bytes. 0 → DefaultChunkSize.
	ChunkSize int64
}

// Compile-time assertion: FixedChunker is the canonical BodyCodec.
var _ BodyCodec = (*FixedChunker)(nil)

// Format returns FormatFixed.
func (c *FixedChunker) Format() string { return FormatFixed }

// Chunk reads body bytes from r, splits them at ChunkSize, writes
// each chunk as a raw block, then writes a FixedChunkerIndex CBOR
// block listing the chunks in order. The Body returned points
// Content at the index block.
func (c *FixedChunker) Chunk(ctx context.Context, w blockstore.WriteStore, r io.Reader) (Body, error) {
	chunkSize := c.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	buf := make([]byte, chunkSize)
	bodyHasher := sha256.New()
	etagHasher := md5.New()
	var chunks []cid.Cid
	var total int64

	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			chunk := buf[:n]
			bodyHasher.Write(chunk)
			etagHasher.Write(chunk)
			cidv, perr := putRawBlock(ctx, w, chunk)
			if perr != nil {
				return Body{}, fmt.Errorf("put chunk: %w", perr)
			}
			chunks = append(chunks, cidv)
			total += int64(n)
		}
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		return Body{}, fmt.Errorf("read body: %w", err)
	}

	idx := &FixedChunkerIndex{ChunkSize: chunkSize, Chunks: chunks}
	indexCID, err := w.Put(ctx, idx)
	if err != nil {
		return Body{}, fmt.Errorf("put fixed index: %w", err)
	}

	return Body{
		Size:    total,
		SHA256:  bodyHasher.Sum(nil),
		MD5:     etagHasher.Sum(nil),
		Content: indexCID,
		Format:  FormatFixed,
	}, nil
}

// Open returns a reader over the full body.
func (c *FixedChunker) Open(ctx context.Context, bs blockstore.ReadStore, body Body) io.ReadCloser {
	return &fixedBodyReader{ctx: ctx, bs: bs, body: body, end: body.Size - 1}
}

// OpenRange returns a reader over [start, end] inclusive of the
// body. Caller must ensure 0 <= start <= end <= Size-1.
func (c *FixedChunker) OpenRange(ctx context.Context, bs blockstore.ReadStore, body Body, start, end int64) io.ReadCloser {
	return &fixedBodyReader{
		ctx:       ctx,
		bs:        bs,
		body:      body,
		start:     start,
		end:       end,
		needsSeek: true,
		pos:       start,
	}
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

// fixedBodyReader streams chunks lazily for FixedChunker bodies. It
// fetches the index block on first read, then walks chunks. Both
// whole-body and ranged reads use the same loop — only the initial
// offset and end position differ.
type fixedBodyReader struct {
	ctx  context.Context
	bs   blockstore.ReadStore
	body Body

	// idx is fetched lazily on first Read.
	idx *FixedChunkerIndex

	start     int64 // first byte to return (0 for whole-body)
	end       int64 // last byte to return (inclusive)
	pos       int64 // current absolute byte position
	needsSeek bool  // whether we still owe an initial seek into the start chunk

	nextChunk int    // index into idx.Chunks of the next block to fetch
	cur       []byte // currently materialized chunk bytes
	curOff    int    // read position within cur
	err       error
}

func (br *fixedBodyReader) ensureIndex() error {
	if br.idx != nil {
		return nil
	}
	var idx FixedChunkerIndex
	if err := br.bs.Get(br.ctx, br.body.Content, &idx); err != nil {
		return fmt.Errorf("fetch fixed index %s: %w", br.body.Content, err)
	}
	br.idx = &idx
	if br.needsSeek {
		// The constructor for ranged reads stored the absolute start
		// offset; translate it to (chunk index, in-chunk offset) now
		// that we know ChunkSize.
		br.nextChunk = int(br.start / idx.ChunkSize)
		br.curOff = int(br.start % idx.ChunkSize)
	}
	return nil
}

func (br *fixedBodyReader) Read(p []byte) (int, error) {
	if br.err != nil {
		return 0, br.err
	}
	if br.pos > br.end {
		br.err = io.EOF
		return 0, io.EOF
	}
	if err := br.ensureIndex(); err != nil {
		br.err = err
		return 0, err
	}

	if br.cur == nil || (br.curOff >= len(br.cur) && !br.needsSeek) {
		if br.nextChunk >= len(br.idx.Chunks) {
			br.err = io.EOF
			return 0, io.EOF
		}
		blk, err := br.bs.GetBlock(br.ctx, br.idx.Chunks[br.nextChunk])
		if err != nil {
			br.err = fmt.Errorf("read chunk %d: %w", br.nextChunk, err)
			return 0, br.err
		}
		br.cur = blk.RawData()
		// On a ranged read the first chunk is partial — curOff was
		// pre-set in ensureIndex; consume it here and clear the flag.
		if !br.needsSeek {
			br.curOff = 0
		}
		br.needsSeek = false
		br.nextChunk++
	}

	// Don't read past the inclusive end position.
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

func (br *fixedBodyReader) Close() error { return nil }
