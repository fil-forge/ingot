package bucket

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"github.com/fil-forge/ucantone/did"
	"io"

	"github.com/fil-forge/ingot/blockstore"
)

// DefaultMaxBlobSize is the blob ceiling used when callers don't supply one.
// 256 MiB matches Piri's current single-blob limit (MaxMemtreeSize); objects
// larger than this are coarsely split into ≤ max blobs.
const DefaultMaxBlobSize int64 = 256 << 20

// SplitBody reads body bytes from r, splits them into blobs of at most
// maxBlobSize bytes, and streams each blob to w — which hashes it and writes it
// to local storage as it goes, so no blob is ever held whole in memory (a 256 MiB
// blob buffered in RAM × concurrent PUTs would sink a memory-constrained
// appliance). It returns a Body whose Blobs list covers [0, Size) contiguously;
// the whole-body sha256 and md5 are computed in the same streaming pass. A
// zero-byte body yields a Body with no blobs (and the well-known empty digests).
//
// w is the local spool in production (blockstore.Spool): the blobs land on disk
// before being uploaded to Forge by digest. SplitBody itself is storage-agnostic.
func SplitBody(ctx context.Context, w blockstore.BlobWriter, r io.Reader, maxBlobSize int64) (Body, error) {
	max := maxBlobSize
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
		// Stream up to max bytes for this blob straight through w (digest +
		// disk write happen there); nothing is buffered whole here. A full max
		// blob may be followed by more; a short read is the last blob; zero
		// bytes means the body is exhausted.
		digest, n, err := w.WriteBlob(ctx, io.LimitReader(src, max))
		if err != nil {
			return Body{}, fmt.Errorf("put blob: %w", err)
		}
		if n == 0 {
			break
		}
		blobs = append(blobs, BlobRef{Digest: digest, Offset: total, Length: n})
		total += n
		if n < max {
			break
		}
	}

	return Body{
		Size:   total,
		SHA256: bodyHasher.Sum(nil),
		MD5:    etagHasher.Sum(nil),
		Blobs:  blobs,
	}, nil
}

// OpenBody returns a reader over the full body, streaming each blob from bs by
// its digest in order. space is the owning bucket's Forge space (an evicted
// blob is re-fetched from the network by space + digest).
func OpenBody(ctx context.Context, bs blockstore.BlobReader, space did.DID, body Body) io.ReadCloser {
	return &blobBodyReader{ctx: ctx, bs: bs, space: space, blobs: body.Blobs, pos: 0, end: body.Size - 1}
}

// OpenBodyRange returns a reader over [start, end] inclusive of the body.
// Caller must ensure 0 <= start <= end <= Size-1.
func OpenBodyRange(ctx context.Context, bs blockstore.BlobReader, space did.DID, body Body, start, end int64) io.ReadCloser {
	return &blobBodyReader{ctx: ctx, bs: bs, space: space, blobs: body.Blobs, pos: start, end: end}
}

// blobBodyReader streams a Body's blobs lazily, keeping at most one open blob
// reader at a time so a read never materializes a whole blob (up to
// max_blob_size) in memory. It walks the ordered BlobRef list, opening the blob
// that covers the current position, seeking/skipping into it for a ranged read,
// and serving bytes until the inclusive end. Whole-body and ranged reads share
// the loop — only the initial pos and the end differ.
type blobBodyReader struct {
	ctx   context.Context
	bs    blockstore.BlobReader
	space did.DID
	blobs []BlobRef

	pos int64 // current absolute byte position (next byte to return)
	end int64 // last byte to return (inclusive)

	cur    io.ReadCloser // currently-open blob stream, positioned at pos; nil between blobs
	curEnd int64         // last absolute byte to serve from cur
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

	if br.cur == nil {
		if err := br.openCurrent(); err != nil {
			br.err = err
			return 0, err
		}
	}

	// Don't read past this blob's end-within-range or the caller's buffer.
	remaining := br.curEnd - br.pos + 1
	want := int64(len(p))
	if want > remaining {
		want = remaining
	}
	n, rerr := br.cur.Read(p[:want])
	br.pos += int64(n)
	if br.pos > br.curEnd { // served this blob's range — next Read opens the next blob
		_ = br.cur.Close()
		br.cur = nil
	}
	if rerr != nil && rerr != io.EOF {
		br.err = rerr
		return n, rerr
	}
	if rerr == io.EOF && br.pos <= br.curEnd {
		// The blob stream ended before its declared range — a short/corrupt blob.
		if br.cur != nil {
			_ = br.cur.Close()
			br.cur = nil
		}
		br.err = io.ErrUnexpectedEOF
		return n, br.err
	}
	return n, nil
}

// openCurrent opens the blob covering br.pos and positions it at br.pos.
func (br *blobBodyReader) openCurrent() error {
	b, ok := blobAt(br.blobs, br.pos)
	if !ok {
		// The Blobs list does not cover pos — a malformed manifest.
		return io.ErrUnexpectedEOF
	}
	rc, err := br.bs.OpenBlob(br.ctx, br.space, b.Digest)
	if err != nil {
		return fmt.Errorf("read blob @%d: %w", b.Offset, err)
	}
	// Position at pos within the blob — a ranged read may start mid-blob. Prefer
	// a seek (local spool files are seekable); fall back to read-and-discard for
	// a non-seekable network stream.
	if skip := br.pos - b.Offset; skip > 0 {
		if seeker, ok := rc.(io.Seeker); ok {
			if _, err := seeker.Seek(skip, io.SeekStart); err != nil {
				_ = rc.Close()
				return fmt.Errorf("seek blob @%d: %w", b.Offset, err)
			}
		} else if _, err := io.CopyN(io.Discard, rc, skip); err != nil {
			_ = rc.Close()
			return fmt.Errorf("skip into blob @%d: %w", b.Offset, err)
		}
	}
	br.cur = rc
	br.curEnd = b.Offset + b.Length - 1
	if br.curEnd > br.end {
		br.curEnd = br.end
	}
	return nil
}

// Close releases the currently-open blob stream (a partial read that didn't run
// to EOF leaves one open).
func (br *blobBodyReader) Close() error {
	if br.cur != nil {
		err := br.cur.Close()
		br.cur = nil
		return err
	}
	return nil
}

// blobAt returns the BlobRef whose [Offset, Offset+Length) span contains pos.
// The list is ordered and contiguous, so a forward scan suffices.
func blobAt(blobs []BlobRef, pos int64) (BlobRef, bool) {
	for _, b := range blobs {
		if pos >= b.Offset && pos < b.Offset+b.Length {
			return b, true
		}
	}
	return BlobRef{}, false
}
