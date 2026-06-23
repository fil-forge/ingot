package aesstream

import (
	"crypto/cipher"
	"fmt"
	"io"
)

// Range-decryption errors, in addition to the decryption sentinels in
// aesstream.go.
var (
	// ErrCiphertextSize is returned when a declared ciphertext length cannot
	// be a structurally valid stream: it is shorter than a single (empty)
	// chunk, or its trailing chunk is too short to hold an authentication
	// tag. It is a structural complaint about the length the caller supplied,
	// distinct from ErrCorrupted (a chunk that failed authentication).
	ErrCiphertextSize = fmt.Errorf("aesstream: ciphertext length is not a valid stream (must be >= %d bytes with a tag-sized final chunk)", TagSize)

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

// RangeReader decrypts an arbitrary plaintext byte range of a chunked
// AES-256-GCM STREAM without reading the whole ciphertext. Given the
// stream's total ciphertext length it computes which fixed-size chunks
// overlap the requested range and fetches only those chunks' bytes from
// the underlying io.ReaderAt, decrypting and trimming them to yield
// exactly the requested plaintext bytes.
//
// Because the total ciphertext length pins down which chunk is final, the
// final-chunk nonce flag is known up front — a RangeReader never needs the
// streaming Reader's last-chunk retry, and reading a range that excludes
// the final chunk fetches none of the trailing bytes.
//
// A RangeReader implements io.Reader, emitting the requested range in
// order with O(chunkSize) memory; it reads each overlapping chunk from the
// source exactly once, on demand. Like Reader, any non-EOF error means the
// returned plaintext is incomplete and must be discarded (a tampered,
// reordered or truncated chunk yields ErrCorrupted, not partial output for
// that chunk). A clean io.EOF means the whole requested range was emitted.
//
// A RangeReader is not safe for concurrent use.
type RangeReader struct {
	aead      cipher.AEAD
	src       io.ReaderAt
	base      [BaseNonceSize]byte
	aad       []byte
	chunkSize int

	encChunk      int64 // chunkSize + TagSize: a full ciphertext chunk
	numChunks     int64 // total chunks in the stream
	lastCipherLen int64 // byte length of the final ciphertext chunk

	total     int64 // total plaintext bytes this reader will emit
	remaining int64 // plaintext bytes not yet decrypted into unread
	nextChunk int64 // index of the next chunk to fetch
	skipFirst int   // bytes to drop from the front of the first fetched chunk
	onFirst   bool  // nextChunk is still the first chunk of the range

	inBuf  []byte // raw ciphertext of the current chunk, cap encChunk
	outBuf []byte // plaintext backing array, cap chunkSize
	unread []byte // decrypted-but-unread plaintext, trimmed to the range
	err    error  // sticky terminal state (io.EOF on clean end)
}

// NewRangeReader returns a RangeReader that yields the plaintext bytes
// [off, off+length) of the stream whose complete ciphertext is
// ciphertextSize bytes long and is readable at any offset from src. cfg
// must match the Writer that produced the stream, including the chunk size.
//
// length is clamped to the bytes available from off, so a length that runs
// past the end of the plaintext simply yields the remaining bytes. off may
// equal the plaintext length (an empty read) but a larger off, or a
// negative off or length, returns ErrRange. An off or length is validated
// against the plaintext size derived from ciphertextSize, which must itself
// be a structurally valid stream length (else ErrCiphertextSize).
func NewRangeReader(src io.ReaderAt, ciphertextSize int64, cfg Config, off, length int64) (*RangeReader, error) {
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

	r := &RangeReader{
		aead:          aead,
		src:           src,
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
		// Nothing to emit: report EOF immediately and fetch no ciphertext.
		r.err = io.EOF
	}
	return r, nil
}

// Len returns the number of plaintext bytes this reader will emit in total
// — the requested length clamped to the bytes available from the offset.
// It is fixed at construction and does not change as bytes are read.
func (r *RangeReader) Len() int64 { return r.total }

// ChunkSize returns the plaintext chunk size in effect.
func (r *RangeReader) ChunkSize() int { return r.chunkSize }

// Read implements io.Reader. It returns the next bytes of the requested
// range, io.EOF once the whole range has been emitted, or one of the
// package's decryption errors.
func (r *RangeReader) Read(p []byte) (int, error) {
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
	if err := r.nextRangeChunk(); err != nil {
		return 0, err
	}
	n := copy(p, r.unread)
	r.unread = r.unread[n:]
	return n, nil
}

// nextRangeChunk fetches, authenticates and decrypts the next overlapping
// chunk, trims it to the requested range, and stores the result in
// r.unread. It records the terminal state in r.err (io.EOF once the range
// is exhausted) and returns a non-nil error only on a fatal condition.
func (r *RangeReader) nextRangeChunk() error {
	// Determine the on-wire length and final-chunk flag from the known
	// stream geometry — no probing, no retry.
	last := r.nextChunk == r.numChunks-1
	clen := r.encChunk
	if last {
		clen = r.lastCipherLen
	}

	if err := r.readChunkAt(r.inBuf[:clen], r.nextChunk*r.encChunk); err != nil {
		return r.fail(err)
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

// readChunkAt fills p with the ciphertext chunk at byte offset off. A read
// that returns fewer than len(p) bytes means the source is shorter than
// ciphertextSize claimed; io.EOF at the exact end of a full read is not an
// error (per the io.ReaderAt contract).
func (r *RangeReader) readChunkAt(p []byte, off int64) error {
	n, err := r.src.ReadAt(p, off)
	if n == len(p) {
		return nil
	}
	if err == nil || err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return fmt.Errorf("aesstream: read chunk %d: %w", r.nextChunk, err)
}

// fail records err as the terminal state and returns it.
func (r *RangeReader) fail(err error) error {
	r.err = err
	return err
}

// OpenRange decrypts the plaintext byte range [off, off+length) of a
// complete ciphertext stream and returns exactly those bytes. The stream's
// total ciphertext length is ciphertextSize and its bytes are read at the
// required offsets from src; only the chunks overlapping the range are
// fetched. It is a convenience wrapper over RangeReader for callers that
// want the range in one buffer.
//
// length is clamped to the bytes available from off (see NewRangeReader),
// and like RangeReader.Read it reports tampering, reordering or truncation
// as a non-nil error, in which case the returned bytes must be discarded.
func OpenRange(cfg Config, src io.ReaderAt, ciphertextSize, off, length int64) ([]byte, error) {
	r, err := NewRangeReader(src, ciphertextSize, cfg, off, length)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, r.Len())
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
