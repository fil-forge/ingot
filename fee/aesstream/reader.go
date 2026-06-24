package aesstream

import (
	"crypto/cipher"
	"fmt"
	"io"
)

// Reader decrypts a chunked AES-256-GCM STREAM produced by a Writer. It
// reads ciphertext from an underlying io.Reader and yields plaintext.
//
// Each chunk's plaintext is released as soon as that chunk authenticates,
// so a truncated stream may return some leading plaintext followed by an
// error. Any non-EOF error means the plaintext is incomplete and must be
// discarded; only a clean io.EOF means the whole stream was intact (its
// final chunk was reached and no trailing bytes followed).
//
// A Reader is not safe for concurrent use.
type Reader struct {
	aead      cipher.AEAD
	source    io.Reader
	base      [BaseNonceSize]byte
	aad       []byte
	chunkSize int
	encChunk  int // chunkSize + TagSize: a full ciphertext chunk

	inBuf  []byte // raw ciphertext of the current chunk, cap encChunk
	outBuf []byte // plaintext backing array, cap chunkSize
	unread []byte // decrypted-but-unread plaintext, a slice of outBuf

	index     uint64 // index of the next chunk to read
	maxChunks uint64 // overridable in tests; defaults to MaxChunks
	err       error  // sticky terminal state (io.EOF on clean end)
}

// NewReader returns a Reader that decrypts the stream from src under cfg.
// It validates cfg and returns the matching sentinel error if the key,
// base nonce or chunk size is invalid. cfg must match the Writer that
// produced the stream, including the chunk size.
func NewReader(src io.Reader, cfg Config) (*Reader, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	aead, err := newGCM(cfg.Key)
	if err != nil {
		return nil, err
	}
	chunkSize := cfg.effectiveChunkSize()
	// Copy AAD so the Reader doesn't alias caller-owned memory: it is
	// authenticated on every chunk, so a later mutation or reuse of the
	// caller's slice must not change this stream's authentication.
	r := &Reader{
		aead:      aead,
		source:    src,
		aad:       append([]byte(nil), cfg.AAD...),
		chunkSize: chunkSize,
		encChunk:  chunkSize + TagSize,
		inBuf:     make([]byte, chunkSize+TagSize),
		outBuf:    make([]byte, 0, chunkSize),
		maxChunks: MaxChunks,
	}
	copy(r.base[:], cfg.BaseNonce)
	return r, nil
}

// Read implements io.Reader. It returns decrypted plaintext, io.EOF at
// the clean end of the stream, or one of the package's decryption errors.
func (r *Reader) Read(p []byte) (int, error) {
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
	if err := r.nextChunk(); err != nil {
		return 0, err
	}
	n := copy(p, r.unread)
	r.unread = r.unread[n:]
	if n == 0 && r.err != nil {
		// The final chunk carried no plaintext (empty input): surface the
		// terminal state now rather than returning (0, nil) forever.
		return 0, r.err
	}
	return n, nil
}

// nextChunk reads, authenticates and decrypts the next ciphertext chunk
// into r.unread. On the final chunk it also sets r.err (io.EOF on a clean
// end, or ErrTrailingData if extra bytes follow). It returns a non-nil
// error only for a fatal condition, which it also records in r.err.
func (r *Reader) nextChunk() error {
	if r.index >= r.maxChunks {
		return r.fail(ErrTooManyChunks)
	}

	n, rerr := io.ReadFull(r.source, r.inBuf[:r.encChunk])
	mustBeLast := false
	switch rerr {
	case nil:
		// A full-size chunk. It is tentatively not the last; if the tag
		// fails below openChunk retries it as the last chunk.
	case io.ErrUnexpectedEOF:
		// A short read: only the final chunk may be shorter than a full
		// chunk, so this must be the last one.
		mustBeLast = true
	case io.EOF:
		// EOF means the source was at the end already and read zero bytes. If the
		// previous chunk was marked as a final chunk, we wouldn't have kept
		// reading, so it must not have been. So if we're here, we ran out of input
		// before we saw a chunk marked final, which means the input was truncated.
		return r.fail(ErrTruncated)
	default:
		return r.fail(fmt.Errorf("aesstream: read chunk %d: %w", r.index, rerr))
	}
	if n < TagSize {
		// Too few bytes to even hold the tag: a malformed/truncated chunk.
		return r.fail(ErrCorrupted)
	}

	plaintext, wasLast, err := r.openChunk(r.index, r.inBuf[:n], mustBeLast)
	if err != nil {
		return r.fail(err)
	}

	r.unread = plaintext
	r.index++

	if wasLast {
		// The stream should end exactly here; any further byte is trailing
		// data and makes the whole stream invalid.
		r.err = io.EOF
		var b [1]byte
		switch _, e := io.ReadFull(r.source, b[:]); e {
		case io.EOF:
			// clean end
		case nil:
			r.err = ErrTrailingData
		default:
			r.err = e
		}
	}
	return nil
}

// openChunk authenticates and decrypts the ciphertext of the chunk at the
// given index. mustBeLast is set when the caller already knows this is the
// final chunk (it came from a short read): the chunk is opened only with
// the last flag. Otherwise the chunk could be a middle chunk or a full-size
// final chunk, so it is tried as non-last first and retried as last on
// failure (the writer marks a full-size final chunk the same way it marks a
// short one). It returns the plaintext and whether the chunk was in fact the
// last.
func (r *Reader) openChunk(index uint64, ciphertext []byte, mustBeLast bool) ([]byte, bool, error) {
	wasLast := mustBeLast
	nonce := streamNonce(r.base, uint32(index), wasLast)
	out, err := r.aead.Open(r.outBuf[:0], nonce[:], ciphertext, r.aad)
	if err != nil && !mustBeLast {
		// A full-size chunk that fails as non-last must be the full-size
		// final chunk; retry with the last flag set.
		wasLast = true
		nonce = streamNonce(r.base, uint32(index), wasLast)
		out, err = r.aead.Open(r.outBuf[:0], nonce[:], ciphertext, r.aad)
	}
	if err != nil {
		return nil, false, ErrCorrupted
	}
	return out, wasLast, nil
}

// ChunkSize returns the plaintext chunk size in effect.
func (r *Reader) ChunkSize() int { return r.chunkSize }

// fail records err as the terminal state and returns it.
func (r *Reader) fail(err error) error {
	r.err = err
	return err
}
