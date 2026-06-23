// Package aesstream implements the chunked AES-256-GCM STREAM body
// cipher used by the FilOne File Encryption Envelope (FEE).
//
// FEE encrypts an object segment by splitting its plaintext into
// fixed-size chunks and sealing each chunk independently with
// AES-256-GCM under a single per-segment content-encryption key (CEK).
// Per-chunk uniqueness and ordering come entirely from the nonce, which
// is built from a random per-segment base nonce, the chunk's zero-based
// index, and a flag that marks the final chunk:
//
//	nonce[12] = baseNonce[7] || chunkIndex[4, big-endian] || lastFlag[1]
//
// lastFlag is 0x01 on the final chunk and 0x00 on every other chunk.
// This is the STREAM construction of Hoang, Reyhanitabar, Rogaway and
// Vizár, with the nonce layout fixed by the FEE spec (see the FilOne
// encryption RFC and filecoin-project/FIPs discussion #1253). The
// io.Reader/io.Writer wrapping, the last-chunk look-ahead and the
// truncation/reorder detection follow the same idioms as
// filippo.io/age's internal/stream — age uses ChaCha20-Poly1305 and an
// 11-byte counter, so it is not wire-compatible, but the control flow is
// the same.
//
// This package is deliberately just the body cipher. It takes the CEK,
// the 7-byte base nonce and the additional authenticated data (AAD) as
// inputs and produces (Writer) or consumes (Reader) the raw ciphertext
// byte stream. The COSE_Encrypt envelope that carries the base nonce in
// its `iv` header and the wrapped CEKs, the generation of that base
// nonce, and the key-wrapping itself all live in the parent fee package.
//
// # Chunk framing
//
// Each ciphertext chunk is the AES-GCM sealing of one plaintext chunk:
// up to ChunkSize plaintext bytes followed by a 16-byte tag. Every chunk
// but the last is exactly ChunkSize plaintext; the last chunk holds the
// remainder (1..ChunkSize bytes), or is empty for empty input. The
// empty plaintext is encoded as a single empty final chunk (just the
// 16-byte tag), so the ciphertext stream is never zero-length.
//
// The chunk index is a 4-byte counter, so a single stream holds at most
// MaxChunks chunks (~1 PiB at the default chunk size); exceeding that is
// an error rather than a silent counter wrap.
//
// # Security and streaming semantics
//
// Decryption is streaming: each chunk's plaintext is released to the
// caller as soon as that chunk authenticates, before later chunks (or
// the end of the stream) have been seen. Tampering, reordering,
// truncation and bit flips are always detected, but a truncated stream
// may yield some leading plaintext followed by an error rather than no
// output at all. Callers MUST therefore treat any non-EOF error from
// Reader.Read (or any non-nil error from Open) as "the plaintext is
// incomplete and must be discarded": only a clean io.EOF means the whole
// segment was received intact.
package aesstream

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	// KeySize is the required CEK length in bytes: AES-256.
	KeySize = 32

	// BaseNonceSize is the required base-nonce (IV) length in bytes. The
	// 12-byte GCM nonce is baseNonce(7) || chunkIndex(4, BE) || lastFlag(1).
	BaseNonceSize = 7

	// NonceSize is the AES-GCM nonce length in bytes.
	NonceSize = 12

	// TagSize is the AES-GCM authentication tag length in bytes, and the
	// per-chunk ciphertext overhead.
	TagSize = 16

	// DefaultChunkSize is the plaintext chunk size used when Config.ChunkSize
	// is zero: 256 KiB, the FEE interoperability baseline.
	DefaultChunkSize = 256 * 1024

	// MinChunkSize and MaxChunkSize bound an explicitly configured chunk
	// size, per the FEE spec.
	MinChunkSize = 4 * 1024
	MaxChunkSize = 16 * 1024 * 1024

	// MaxChunks is the number of chunks a single stream may contain. The
	// chunk index is a uint32, so valid indices run 0 .. MaxChunks-1.
	MaxChunks = 1 << 32
)

// last-chunk flag values for the final nonce byte.
const (
	flagNotLast byte = 0x00
	flagLast    byte = 0x01
)

// Validation and decryption errors. The decryption sentinels are
// intentionally coarse: revealing precisely why authentication failed
// would leak a tampering oracle, so callers get one of a small set of
// "the bytes are not an intact ciphertext" errors.
var (
	// ErrKeySize is returned when the CEK is not KeySize bytes.
	ErrKeySize = fmt.Errorf("aesstream: key must be %d bytes", KeySize)

	// ErrBaseNonceSize is returned when the base nonce is not BaseNonceSize bytes.
	ErrBaseNonceSize = fmt.Errorf("aesstream: base nonce must be %d bytes", BaseNonceSize)

	// ErrChunkSize is returned when an explicit chunk size is out of range.
	ErrChunkSize = fmt.Errorf("aesstream: chunk size must be between %d and %d bytes", MinChunkSize, MaxChunkSize)

	// ErrTooManyChunks is returned when a stream would exceed MaxChunks.
	ErrTooManyChunks = errors.New("aesstream: stream exceeds maximum chunk count")

	// ErrCorrupted is returned when a ciphertext chunk fails authentication:
	// the bytes were tampered with, reordered, truncated mid-chunk, or
	// sealed under a different key, base nonce or AAD.
	ErrCorrupted = errors.New("aesstream: chunk failed authentication; data is corrupted, reordered, or tampered with")

	// ErrTruncated is returned when the ciphertext ends on a chunk boundary
	// without a chunk having been marked final — the stream was cut short.
	ErrTruncated = errors.New("aesstream: ciphertext truncated before final chunk")

	// ErrTrailingData is returned when extra bytes follow a full-size final
	// chunk. (Bytes appended after a short final chunk are instead pulled
	// into that chunk and fail authentication as ErrCorrupted.)
	ErrTrailingData = errors.New("aesstream: trailing data after final chunk")
)

// Config holds the parameters shared by a Writer and the Reader that
// decrypts its output. A Writer and Reader round-trip only when their
// Key, BaseNonce, AAD and effective chunk size all match.
type Config struct {
	// Key is the 32-byte AES-256 content-encryption key (CEK). Required.
	Key []byte

	// BaseNonce is the 7-byte base nonce (the envelope's `iv`). Required.
	// It must be unique per (Key) — the fee/envelope layer generates a
	// fresh random one per segment; see NewBaseNonce.
	BaseNonce []byte

	// AAD is additional authenticated data bound to every chunk (the COSE
	// Enc_structure, in FEE). It is authenticated but not encrypted, and
	// may be nil. The same value must be supplied to encrypt and decrypt.
	AAD []byte

	// ChunkSize is the plaintext chunk size in bytes. Zero selects
	// DefaultChunkSize; any other value must be in [MinChunkSize, MaxChunkSize].
	ChunkSize int
}

// effectiveChunkSize resolves ChunkSize, applying the default.
func (c Config) effectiveChunkSize() int {
	if c.ChunkSize == 0 {
		return DefaultChunkSize
	}
	return c.ChunkSize
}

// validate checks the key, base nonce and chunk size. It returns the
// matching sentinel error (usable with errors.Is) on the first problem.
func (c Config) validate() error {
	if len(c.Key) != KeySize {
		return ErrKeySize
	}
	if len(c.BaseNonce) != BaseNonceSize {
		return ErrBaseNonceSize
	}
	if c.ChunkSize != 0 && (c.ChunkSize < MinChunkSize || c.ChunkSize > MaxChunkSize) {
		return ErrChunkSize
	}
	return nil
}

// newGCM builds the AES-256-GCM AEAD from a validated key.
func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		// aes.NewCipher only fails on a bad key length, which validate
		// already rejected; treat anything here as a key-size problem.
		return nil, ErrKeySize
	}
	return cipher.NewGCM(block)
}

// streamNonce builds the 12-byte GCM nonce for one chunk:
// base(7) || index(4, big-endian) || lastFlag(1).
func streamNonce(base [BaseNonceSize]byte, index uint32, last bool) [NonceSize]byte {
	var n [NonceSize]byte
	copy(n[:BaseNonceSize], base[:])
	binary.BigEndian.PutUint32(n[BaseNonceSize:BaseNonceSize+4], index)
	if last {
		n[NonceSize-1] = flagLast
	} else {
		n[NonceSize-1] = flagNotLast
	}
	return n
}

// NewBaseNonce returns a fresh random BaseNonceSize-byte base nonce read
// from crypto/rand, for callers that don't already have an envelope IV.
func NewBaseNonce() ([]byte, error) {
	b := make([]byte, BaseNonceSize)
	if _, err := rand.Read(b); err != nil {
		return nil, fmt.Errorf("aesstream: read base nonce: %w", err)
	}
	return b, nil
}

// EncryptedSize returns the total ciphertext length, in bytes, that
// encrypting plaintextLen plaintext bytes with the given chunk size
// produces. A zero chunkSize selects DefaultChunkSize. It returns the
// plaintext length plus one tag per chunk; empty input still yields one
// (empty) chunk and therefore TagSize bytes.
func EncryptedSize(plaintextLen int64, chunkSize int) int64 {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	chunks := (plaintextLen + int64(chunkSize) - 1) / int64(chunkSize)
	if chunks < 1 {
		chunks = 1
	}
	return plaintextLen + chunks*TagSize
}

// Seal encrypts plaintext in a single call and returns the complete
// ciphertext stream. It is a convenience wrapper over Writer for callers
// that already hold the whole plaintext in memory; streaming callers
// should use NewWriter to keep memory bounded.
func Seal(cfg Config, plaintext []byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(int(EncryptedSize(int64(len(plaintext)), cfg.effectiveChunkSize())))
	w, err := NewWriter(&buf, cfg)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(plaintext); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Open decrypts a complete ciphertext stream in a single call and
// returns the plaintext. It is a convenience wrapper over Reader; like
// Reader it reports tampering, truncation, reordering and trailing data
// as a non-nil error, in which case the returned bytes must be discarded.
func Open(cfg Config, ciphertext []byte) ([]byte, error) {
	r, err := NewReader(bytes.NewReader(ciphertext), cfg)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
}
