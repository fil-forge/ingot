package aesstream

import (
	"crypto/cipher"
	"fmt"
	"io"
	"math"
)

// Range-decryption errors, in addition to the decryption sentinels in
// aesstream.go.
var (
	// ErrCiphertextSize is returned when a declared ciphertext length cannot
	// be a structurally valid stream: it is shorter than a single (empty)
	// chunk, or its trailing chunk is too short to hold an authentication
	// tag. It is a structural complaint about the length the caller supplied,
	// distinct from ErrCorrupted (a chunk that failed authentication).
	ErrCiphertextSize = fmt.Errorf("aesstream: ciphertext length is not a valid stream (the final chunk must be at least %d bytes, to hold the tag)", TagSize)

	// ErrRange is returned when a requested plaintext range is invalid: a
	// negative offset or length, or an offset past the end of the plaintext.
	// A length that runs past the end is not an error — it is clamped to the
	// bytes available from the offset.
	ErrRange = fmt.Errorf("aesstream: requested range is out of bounds")
)

// chunkLayout derives a stream's chunk geometry from its total ciphertext
// length. Each ciphertext chunk is chunkSize+TagSize bytes except the
// final one, which holds the remainder (TagSize..chunkSize+TagSize bytes;
// exactly TagSize for an empty-plaintext stream). It returns the chunk
// count, the byte length of the final chunk, and the total plaintext
// length, or ErrCiphertextSize if ciphertextSize is not a structurally
// valid stream length, or ErrTooManyChunks if it implies more than
// MaxChunks chunks.
func chunkLayout(ciphertextSize int64, chunkSize int) (numChunks, lastCipherLen, plaintextLen int64, err error) {
	enc := int64(chunkSize) + TagSize // a full ciphertext chunk
	if ciphertextSize < TagSize {
		// Even an empty plaintext seals to one TagSize-byte chunk, so any
		// shorter length cannot be a stream this package produced.
		return 0, 0, 0, ErrCiphertextSize
	}

	q, r := ciphertextSize/enc, ciphertextSize%enc
	switch {
	case r == 0:
		// The length is an exact multiple of a full chunk: the final chunk
		// is itself full-size (its plaintext is exactly chunkSize).
		numChunks, lastCipherLen = q, enc
	case r < TagSize:
		// A trailing fragment too short to even hold a tag: malformed.
		return 0, 0, 0, ErrCiphertextSize
	default:
		// A partial (1..chunkSize-byte plaintext) or empty (TagSize-byte)
		// final chunk follows q full chunks.
		numChunks, lastCipherLen = q+1, r
	}

	if numChunks > MaxChunks {
		return 0, 0, 0, ErrTooManyChunks
	}
	plaintextLen = (numChunks-1)*int64(chunkSize) + (lastCipherLen - TagSize)
	return numChunks, lastCipherLen, plaintextLen, nil
}

// DecryptedSize returns the plaintext length, in bytes, of a stream whose
// complete ciphertext is ciphertextLen bytes long at the given chunk size.
// A zero or negative chunkSize selects DefaultChunkSize. It is the inverse
// of EncryptedSize and returns ErrCiphertextSize if ciphertextLen is not a
// structurally valid stream length. Callers can use it to derive an
// object's plaintext size (and to validate a range) without fetching any
// ciphertext.
func DecryptedSize(ciphertextLen int64, chunkSize int) (int64, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	_, _, plaintextLen, err := chunkLayout(ciphertextLen, chunkSize)
	return plaintextLen, err
}

// CiphertextRange returns the single contiguous ciphertext byte range
// [start, start+n) that must be read to serve the plaintext range
// [off, off+length) of a stream whose complete ciphertext is
// ciphertextSize bytes long. A zero or negative chunkSize selects
// DefaultChunkSize.
//
// Because the chunks overlapping any plaintext range are adjacent in the
// ciphertext, the bytes needed are always one contiguous span — so a caller
// fetching from a remote store (e.g. a range request to a blob) can pull
// them in a single request and hand the result, in order, to OpenSpan /
// NewSpanReader. The returned offsets are relative to the ciphertext stream
// itself (its first byte is 0); a caller whose blob prefixes the stream
// with an envelope header must add that header length.
//
// For a random-access source already in hand (a local file or in-memory
// buffer) rather than a one-shot fetch, wrap it as the span with
// io.NewSectionReader(src, start, n) and pass that to NewSpanReader.
//
// The span is chunk-aligned, so it may include a little more than the
// requested bytes: at most the unused head of the first overlapping chunk
// and tail of the last (under 2*chunkSize total), since GCM authenticates a
// whole chunk at a time. A length past the end of the plaintext clamps; an
// empty span (n == 0) means no ciphertext need be fetched. off/length and
// ciphertextSize are validated exactly as in NewSpanReader, returning
// ErrRange or ErrCiphertextSize.
func CiphertextRange(ciphertextSize int64, chunkSize int, off, length int64) (start, n int64, err error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	numChunks, _, plaintextLen, err := chunkLayout(ciphertextSize, chunkSize)
	if err != nil {
		return 0, 0, err
	}
	if off < 0 || length < 0 || off > plaintextLen {
		return 0, 0, ErrRange
	}
	effLen := length
	if avail := plaintextLen - off; effLen > avail {
		effLen = avail
	}
	if effLen == 0 {
		return 0, 0, nil
	}

	enc := int64(chunkSize) + TagSize
	firstChunk := off / int64(chunkSize)
	lastChunk := (off + effLen - 1) / int64(chunkSize)
	start = firstChunk * enc
	end := (lastChunk + 1) * enc
	if lastChunk == numChunks-1 {
		// The final chunk may be short; clamp to the true stream end.
		end = ciphertextSize
	}
	return start, end - start, nil
}

// SpanReader decrypts an arbitrary plaintext byte range of a chunked
// AES-256-GCM STREAM from a single contiguous slice of its ciphertext,
// delivered as a sequential io.Reader. It is the streaming counterpart to a
// remote range read: the caller computes the ciphertext span with
// CiphertextRange, fetches exactly that span in one request, and hands the
// response body here — SpanReader decrypts it into the requested plaintext
// with O(chunkSize) memory.
//
// The span must begin at the offset CiphertextRange reported (the boundary
// of the first overlapping chunk) and contain the whole chunks overlapping
// the range, in order. SpanReader recomputes that geometry from the same
// (ciphertextSize, off, length) and reads exactly the chunks it needs, so:
//
//   - the total ciphertext length pins each chunk's index, final-chunk flag
//     and length up front — no last-chunk look-ahead or retry, unlike the
//     whole-stream Reader; and
//   - the head of the first chunk (before off) and the tail of the last
//     (after the range end) are trimmed, so the output is exactly the range.
//
// A SpanReader implements io.Reader. Any non-EOF error means the plaintext
// is incomplete and must be discarded: a tampered/reordered/misframed chunk
// yields ErrCorrupted, and a span shorter than the geometry requires yields
// ErrTruncated. A clean io.EOF means the whole requested range was emitted.
//
// A SpanReader is not safe for concurrent use.
type SpanReader struct {
	aead      cipher.AEAD
	src       io.Reader
	base      [BaseNonceSize]byte
	aad       []byte
	chunkSize int

	encChunk      int64 // chunkSize + TagSize: a full ciphertext chunk
	numChunks     int64 // total chunks in the stream
	lastCipherLen int64 // byte length of the final ciphertext chunk

	total     int64 // total plaintext bytes this reader will emit
	remaining int64 // plaintext bytes not yet decrypted into unread
	nextChunk int64 // index of the next chunk to read from the span
	skipFirst int   // bytes to drop from the front of the first chunk
	onFirst   bool  // nextChunk is still the first chunk of the range

	inBuf  []byte // raw ciphertext of the current chunk, cap encChunk
	outBuf []byte // plaintext backing array, cap chunkSize
	unread []byte // decrypted-but-unread plaintext, trimmed to the range
	err    error  // sticky terminal state (io.EOF on clean end)
}

// NewSpanReader returns a SpanReader that yields the plaintext bytes
// [off, off+length) of the stream whose complete ciphertext is
// ciphertextSize bytes long, reading the ciphertext from span. span must be
// positioned at the start of the contiguous range CiphertextRange reports
// for the same (ciphertextSize, chunkSize, off, length) — typically the
// body of a single range request for exactly that span. cfg must match the
// Writer that produced the stream, including the chunk size.
//
// length is clamped to the bytes available from off; off may equal the
// plaintext length (an empty read) but a larger off, or a negative off or
// length, returns ErrRange. ciphertextSize must be a structurally valid
// stream length (else ErrCiphertextSize).
func NewSpanReader(span io.Reader, ciphertextSize int64, cfg Config, off, length int64) (*SpanReader, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	aead, err := newGCM(cfg.Key)
	if err != nil {
		return nil, err
	}
	chunkSize := cfg.effectiveChunkSize()

	numChunks, lastCipherLen, plaintextLen, err := chunkLayout(ciphertextSize, chunkSize)
	if err != nil {
		return nil, err
	}
	if off < 0 || length < 0 || off > plaintextLen {
		return nil, ErrRange
	}

	// Clamp the length to what's actually available from off.
	effLen := length
	if avail := plaintextLen - off; effLen > avail {
		effLen = avail
	}

	r := &SpanReader{
		aead:          aead,
		src:           span,
		aad:           append([]byte(nil), cfg.AAD...),
		chunkSize:     chunkSize,
		encChunk:      int64(chunkSize) + TagSize,
		numChunks:     numChunks,
		lastCipherLen: lastCipherLen,
		total:         effLen,
		remaining:     effLen,
		inBuf:         make([]byte, chunkSize+TagSize),
		outBuf:        make([]byte, 0, chunkSize),
	}
	copy(r.base[:], cfg.BaseNonce)

	if effLen > 0 {
		r.nextChunk = off / int64(chunkSize)
		r.skipFirst = int(off - r.nextChunk*int64(chunkSize))
		r.onFirst = true
	} else {
		// Nothing to emit: report EOF immediately and read no ciphertext.
		r.err = io.EOF
	}
	return r, nil
}

// Len returns the number of plaintext bytes this reader will emit in total
// — the requested length clamped to the bytes available from the offset.
// It is fixed at construction and does not change as bytes are read.
func (r *SpanReader) Len() int64 { return r.total }

// ChunkSize returns the plaintext chunk size in effect.
func (r *SpanReader) ChunkSize() int { return r.chunkSize }

// Read implements io.Reader. It returns the next bytes of the requested
// range, io.EOF once the whole range has been emitted, or one of the
// package's decryption errors.
func (r *SpanReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	// Serve buffered plaintext from the current chunk first.
	if len(r.unread) > 0 {
		n := copy(p, r.unread)
		r.unread = r.unread[n:]
		return n, nil
	}
	if r.err != nil {
		return 0, r.err
	}
	if err := r.nextSpanChunk(); err != nil {
		return 0, err
	}
	n := copy(p, r.unread)
	r.unread = r.unread[n:]
	return n, nil
}

// nextSpanChunk reads, authenticates and decrypts the next chunk from the
// span, trims it to the requested range, and stores the result in r.unread.
// It records the terminal state in r.err (io.EOF once the range is
// exhausted) and returns a non-nil error only on a fatal condition.
func (r *SpanReader) nextSpanChunk() error {
	// The on-wire length and final-chunk flag come from the known stream
	// geometry — no probing, no retry.
	last := r.nextChunk == r.numChunks-1
	clen := r.encChunk
	if last {
		clen = r.lastCipherLen
	}

	if _, err := io.ReadFull(r.src, r.inBuf[:clen]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// The span ran out before the chunks the range needs: the caller
			// fetched too few bytes (or the wrong span).
			return r.fail(ErrTruncated)
		}
		return r.fail(fmt.Errorf("aesstream: read chunk %d: %w", r.nextChunk, err))
	}

	nonce := streamNonce(r.base, uint32(r.nextChunk), last)
	plain, err := r.aead.Open(r.outBuf[:0], nonce[:], r.inBuf[:clen], r.aad)
	if err != nil {
		return r.fail(ErrCorrupted)
	}

	// Trim the decrypted chunk to the part the caller asked for: drop the
	// leading bytes before the range start (only the first chunk), then cap
	// to the bytes still owed.
	if r.onFirst {
		plain = plain[r.skipFirst:]
		r.onFirst = false
	}
	if int64(len(plain)) > r.remaining {
		plain = plain[:r.remaining]
	}

	r.unread = plain
	r.remaining -= int64(len(plain))
	r.nextChunk++
	if r.remaining == 0 {
		// The range ends within this chunk; the next Read drains r.unread
		// and then sees io.EOF.
		r.err = io.EOF
	}
	return nil
}

// fail records err as the terminal state and returns it.
func (r *SpanReader) fail(err error) error {
	r.err = err
	return err
}

// OpenSpan decrypts the plaintext byte range [off, off+length) from a single
// contiguous ciphertext span and returns exactly those bytes. span carries
// the ciphertext range CiphertextRange reports for the same arguments (see
// NewSpanReader); OpenSpan is the one-shot convenience over SpanReader for
// callers that want the range in one buffer.
//
// length is clamped to the bytes available from off, and like
// SpanReader.Read it reports tampering, reordering or a short span as a
// non-nil error, in which case the returned bytes must be discarded.
func OpenSpan(cfg Config, span io.Reader, ciphertextSize, off, length int64) ([]byte, error) {
	r, err := NewSpanReader(span, ciphertextSize, cfg, off, length)
	if err != nil {
		return nil, err
	}
	// OpenSpan buffers the whole range in memory. On a 64-bit platform
	// r.Len() always fits an int (it is bounded by the plaintext length),
	// but on a 32-bit build a multi-GiB range would overflow the int that
	// make wants and panic with "makeslice: len out of range" — return a
	// clear error instead, and steer such callers to the streaming reader.
	if r.Len() > math.MaxInt {
		return nil, fmt.Errorf("aesstream: range of %d bytes is too large to buffer; use NewSpanReader to stream", r.Len())
	}
	buf := make([]byte, r.Len())
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
