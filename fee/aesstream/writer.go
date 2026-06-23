package aesstream

import (
	"crypto/cipher"
	"errors"
	"io"
)

// errWriteAfterClose is returned by Write once Close has been called.
var errWriteAfterClose = errors.New("aesstream: write after close")

// Writer encrypts a plaintext stream into the chunked AES-256-GCM STREAM
// format and writes the ciphertext to an underlying io.Writer.
//
// Writes are buffered into ChunkSize-sized plaintext chunks. A full chunk
// is sealed and flushed only once more plaintext arrives — so the final
// (possibly partial, possibly empty) chunk can be marked as last. Close
// flushes that final chunk and MUST be called for the ciphertext to be
// complete; a Writer that is never closed produces a truncated stream
// that will fail to decrypt. Close does not close the underlying writer.
//
// A Writer is not safe for concurrent use.
type Writer struct {
	dst       io.Writer
	aead      cipher.AEAD
	base      [BaseNonceSize]byte
	aad       []byte
	chunkSize int

	buf     []byte // pending plaintext, len 0..chunkSize, cap chunkSize
	sealBuf []byte // scratch for one sealed chunk, cap chunkSize+TagSize

	index     uint64 // index of the next chunk to flush
	maxChunks uint64 // overridable in tests; defaults to MaxChunks
	closed    bool
	err       error // sticky: once set, all operations fail with it
}

// NewWriter returns a Writer that encrypts under cfg and writes
// ciphertext to dst. It validates cfg (see Config) and returns the
// matching sentinel error if the key, base nonce or chunk size is invalid.
func NewWriter(dst io.Writer, cfg Config) (*Writer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	aead, err := newGCM(cfg.Key)
	if err != nil {
		return nil, err
	}
	chunkSize := cfg.effectiveChunkSize()
	w := &Writer{
		dst:       dst,
		aead:      aead,
		aad:       cfg.AAD,
		chunkSize: chunkSize,
		buf:       make([]byte, 0, chunkSize),
		sealBuf:   make([]byte, 0, chunkSize+TagSize),
		maxChunks: MaxChunks,
	}
	copy(w.base[:], cfg.BaseNonce)
	return w, nil
}

// Write buffers and encrypts p. It returns the number of bytes consumed
// from p (always len(p) unless an error occurred) and the first error.
// Once an error is returned it is sticky: every later Write and Close
// returns the same error.
func (w *Writer) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.closed {
		return 0, errWriteAfterClose
	}

	total := 0
	for len(p) > 0 {
		// Only flush a full buffer here, where we know more plaintext
		// follows: that keeps the full chunk eligible to be the last one
		// until Close decides otherwise.
		if len(w.buf) == w.chunkSize {
			if err := w.flush(false); err != nil {
				w.err = err
				return total, err
			}
		}
		n := copy(w.buf[len(w.buf):w.chunkSize], p)
		w.buf = w.buf[:len(w.buf)+n]
		p = p[n:]
		total += n
	}
	return total, nil
}

// Close flushes the final chunk — marked as the last chunk — and reports
// any error. It is idempotent: calling it again returns the same result
// without writing more data. Close does not close the underlying writer.
func (w *Writer) Close() error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.flush(true); err != nil {
		w.err = err
		return err
	}
	return nil
}

// ChunkSize returns the plaintext chunk size in effect.
func (w *Writer) ChunkSize() int { return w.chunkSize }

// flush seals the buffered plaintext as chunk w.index and writes it to
// dst, then advances the index and clears the buffer. last sets the
// final-chunk nonce flag.
func (w *Writer) flush(last bool) error {
	if w.index >= w.maxChunks {
		return ErrTooManyChunks
	}
	nonce := streamNonce(w.base, uint32(w.index), last)
	// Seal appends the ciphertext+tag into sealBuf's backing array, which
	// is sized for one chunk, so no allocation happens in steady state.
	w.sealBuf = w.aead.Seal(w.sealBuf[:0], nonce[:], w.buf, w.aad)
	if err := writeAll(w.dst, w.sealBuf); err != nil {
		return err
	}
	w.index++
	w.buf = w.buf[:0]
	return nil
}

// writeAll writes all of p, looping over partial writes.
func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		p = p[n:]
	}
	return nil
}
