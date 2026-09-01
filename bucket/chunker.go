package bucket

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"io"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/ucantone/did"

	"github.com/fil-forge/ingot/blockstore"
)

// envelopeAllowance reserves room inside the network blob ceiling
// (libforge blob.MaxBlobSize — the most raw bytes a default-configured piri
// accepts in one piece) for encryption framing: what leaves the chunker is
// plaintext, but what ships to piri is the FEE envelope — one 16-byte GCM
// tag per 256 KiB aesstream chunk (~16 KiB at this blob size) plus a
// ~211-byte single-recipient COSE header. 32 KiB is ~2× the actual
// overhead; TestDefaultMaxBlobSizeFitsPiri pins that the sum really fits.
const envelopeAllowance int64 = 32 << 10

// DefaultMaxBlobSize is the blob ceiling used when callers don't supply one:
// the largest split whose ENCRYPTED envelope still fits the network blob
// ceiling. (The previous 256 MiB default was unshippable — its envelope
// overshot piri's piece cap by ~2 MiB and piri rejected every allocation
// with BlobSizeLimitExceeded; the 5 GiB max-part itest caught it.) Objects
// larger than this are coarsely split into ≤ max blobs.
const DefaultMaxBlobSize int64 = blobcmds.MaxBlobSize - envelopeAllowance

// SplitBody reads body bytes from r, splits them into blobs of at most
// maxBlobSize bytes, and streams each blob to w — which hashes it and writes it
// to local storage as it goes, so no blob is ever held whole in memory (a ~254 MiB
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
		blobs = append(blobs, BlobRef{Digest: digest, Start: total, End: total + n - 1})
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

// BlobRangeOpener opens a body blob's plaintext. OpenBlobRange returns a
// reader over the inclusive plaintext bytes [start, end] of the blob ref
// describes — start and end are relative to the blob's own plaintext, not
// the object; callers guarantee 0 <= start <= end <= ref.Len()-1. The reader
// yields exactly end-start+1 bytes then EOF; a stream that ends earlier is a
// short or corrupt blob, which the body reader reports as
// io.ErrUnexpectedEOF.
//
// It is the seam decryption slots into: [NewPlainOpener] serves unencrypted
// blobs (stored bytes ARE the plaintext), while an encrypting deployment's
// opener resolves the blob's FEE parameters and decrypts only the ciphertext
// chunks the range overlaps.
type BlobRangeOpener interface {
	OpenBlobRange(ctx context.Context, space did.DID, ref BlobRef, start, end int64) (io.ReadCloser, error)
}

// OpenBody returns a reader over the full body, streaming each blob through
// opener in order. space is the owning bucket's Forge space (an evicted blob
// is re-fetched from the network by space + digest).
func OpenBody(ctx context.Context, opener BlobRangeOpener, space did.DID, body Body) io.ReadCloser {
	return &blobBodyReader{ctx: ctx, opener: opener, space: space, blobs: body.Blobs, pos: 0, end: body.Size - 1}
}

// OpenBodyRange returns a reader over [start, end] inclusive of the body.
// Caller must ensure 0 <= start <= end <= Size-1.
func OpenBodyRange(ctx context.Context, opener BlobRangeOpener, space did.DID, body Body, start, end int64) io.ReadCloser {
	return &blobBodyReader{ctx: ctx, opener: opener, space: space, blobs: body.Blobs, pos: start, end: end}
}

// NewPlainOpener returns the BlobRangeOpener for unencrypted blobs, whose
// stored bytes are their plaintext: open the blob, position at start (a Seek
// for a spool file, read-and-discard for a network stream), and serve
// through end.
func NewPlainOpener(bs blockstore.BlobReader) BlobRangeOpener {
	return plainOpener{bs: bs}
}

type plainOpener struct {
	bs blockstore.BlobReader
}

func (o plainOpener) OpenBlobRange(ctx context.Context, space did.DID, ref BlobRef, start, end int64) (io.ReadCloser, error) {
	rc, err := o.bs.OpenBlob(ctx, space, ref.Digest)
	if err != nil {
		return nil, err
	}
	// Position at start within the blob — a ranged read may begin mid-blob.
	// Prefer a seek (local spool files are seekable); fall back to
	// read-and-discard for a non-seekable network stream.
	if start > 0 {
		if seeker, ok := rc.(io.Seeker); ok {
			if _, err := seeker.Seek(start, io.SeekStart); err != nil {
				_ = rc.Close()
				return nil, fmt.Errorf("seek blob to %d: %w", start, err)
			}
		} else if _, err := io.CopyN(io.Discard, rc, start); err != nil {
			_ = rc.Close()
			return nil, fmt.Errorf("skip into blob to %d: %w", start, err)
		}
	}
	return readerCloser{Reader: io.LimitReader(rc, end-start+1), Closer: rc}, nil
}

// readerCloser pairs a wrapped reader with the closer that releases its
// underlying stream.
type readerCloser struct {
	io.Reader
	io.Closer
}

// blobBodyReader streams a Body's blobs lazily, keeping at most one open blob
// reader at a time so a read never materializes a whole blob (up to
// max_blob_size) in memory. It walks the ordered BlobRef list, asking the
// opener for the plaintext span of the blob that covers the current position,
// and serving bytes until the inclusive end. Whole-body and ranged reads share
// the loop — only the initial pos and the end differ.
type blobBodyReader struct {
	ctx    context.Context
	opener BlobRangeOpener
	space  did.DID
	blobs  []BlobRef

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

// openCurrent opens the plaintext span of the blob covering br.pos, from
// br.pos through the blob's end (or the read's end, whichever is first).
func (br *blobBodyReader) openCurrent() error {
	b, ok := blobAt(br.blobs, br.pos)
	if !ok {
		// The Blobs list does not cover pos — a malformed manifest.
		return io.ErrUnexpectedEOF
	}
	curEnd := b.End
	if curEnd > br.end {
		curEnd = br.end
	}
	rc, err := br.opener.OpenBlobRange(br.ctx, br.space, b, br.pos-b.Start, curEnd-b.Start)
	if err != nil {
		return fmt.Errorf("read blob @%d: %w", b.Start, err)
	}
	br.cur = rc
	br.curEnd = curEnd
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

// blobAt returns the BlobRef whose inclusive [Start, End] span contains pos.
// The list is ordered and contiguous, so a forward scan suffices.
func blobAt(blobs []BlobRef, pos int64) (BlobRef, bool) {
	for _, b := range blobs {
		if pos >= b.Start && pos <= b.End {
			return b, true
		}
	}
	return BlobRef{}, false
}
