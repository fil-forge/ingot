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
	last := false
	switch rerr {
	case nil:
		// A full-size chunk. It is tentatively not the last; if the tag
		// fails below we retry as the last chunk (see openChunk).
	case io.ErrUnexpectedEOF:
		// A short read: only the final chunk may be shorter than a full
		// chunk, so this must be the last one.
		last = true
	case io.EOF:
		// End of stream on a chunk boundary. A well-formed stream always
		// terminates inside a chunk flagged last (after which we stop
		// reading), so reaching here means the stream was cut short.
		return r.fail(ErrTruncated)
	default:
		return r.fail(fmt.Errorf("aesstream: read chunk %d: %w", r.index, rerr))
	}
	if n < TagSize {
		// Too few bytes to even hold the tag: a malformed/truncated chunk.
		return r.fail(ErrCorrupted)
	}

	plaintext, last, err := r.openChunk(r.inBuf[:n], last)
	if err != nil {
		return r.fail(err)
	}

	r.unread = plaintext
	r.index++

	if last {
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

// openChunk authenticates and decrypts one chunk's ciphertext at the
// current index. last is the caller's initial guess; for a full-size
// chunk that fails as a non-last chunk, openChunk retries it as the last
// chunk (the writer marks a full-size final chunk the same way it marks a
// short one). It returns the plaintext and whether the chunk was the last.
func (r *Reader) openChunk(ciphertext []byte, last bool) ([]byte, bool, error) {
	nonce := streamNonce(r.base, uint32(r.index), last)
	out, err := r.aead.Open(r.outBuf[:0], nonce[:], ciphertext, r.aad)
	if err != nil && !last {
		last = true
		nonce = streamNonce(r.base, uint32(r.index), last)
		out, err = r.aead.Open(r.outBuf[:0], nonce[:], ciphertext, r.aad)
	}
	if err != nil {
		return nil, false, ErrCorrupted
	}
	return out, last, nil
}

// ChunkSize returns the plaintext chunk size in effect.
func (r *Reader) ChunkSize() int { return r.chunkSize }

// fail records err as the terminal state and returns it.
func (r *Reader) fail(err error) error {
	r.err = err
	return err
}
