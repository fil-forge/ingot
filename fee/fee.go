// Package fee composes the FEE (FilOne File Encryption Envelope) primitives
// into a single, small public API for encrypting and decrypting whole objects.
//
// The cryptographic building blocks each live in a sub-package and are
// deliberately unaware of one another:
//
//   - fee/cose      — the COSE_Encrypt (tag 96) / COSE_Encrypt0 (tag 16)
//     envelope with a detached payload, and the Enc_structure that
//     authenticates the protected header as AEAD additional data (AAD).
//   - fee/aesstream — the chunked AES-256-GCM-STREAM body cipher: it seals the
//     plaintext under a per-object content-encryption key (CEK) and a random
//     base nonce.
//   - fee/ecdhkw    — the ECDH-ES+A256KW key wrap over X25519: it encrypts the
//     CEK to a recipient's X25519 public key.
//   - fee/aeskw     — RFC 3394 AES Key Wrap (A256KW): it wraps the CEK directly
//     under a symmetric key-encryption key (KEK).
//
// This package sequences them so callers do not have to. [Encrypt] generates a
// fresh CEK, seals the plaintext with the STREAM body cipher, wraps the CEK to
// each [Recipient], and encodes the COSE envelope; [Decrypt] reverses the
// process, locating the recipient that a [RecipientUnwrapper] holds the key for,
// recovering the CEK, and streaming out the plaintext.
//
// # Recipients and content-encryption keys
//
// The body is sealed under a single content-encryption key (CEK). Two concerns
// are independent: which algorithm wraps the CEK, and how the CEK reaches the
// decryptor.
//
// A CEK can be carried in the envelope as one or more COSE_Recipient entries
// (a COSE_Encrypt, tag 96), each keyed by a caller-supplied key id (kid —
// opaque to this package, e.g. a DID verification method ID). Two wrap
// algorithms are available and may be mixed in one envelope; on decrypt the
// caller never selects the algorithm, the recipient's COSE header does:
//
//   - ECDH-ES+A256KW to an X25519 public key: [NewECDHESRecipient] /
//     [NewECDHESUnwrapper].
//   - A256KW under a symmetric KEK: [NewA256KWRecipient] / [NewA256KWUnwrapper].
//
// Alternatively the CEK can be managed out of band — generated or unwrapped by a
// custody service and handed to this package directly. [EncryptWithCEK] seals
// under a caller-provided CEK, and with no recipients it emits a recipient-less
// COSE_Encrypt0 (tag 16); [DecryptWithCEK] decrypts with a caller-provided CEK,
// accepting either tag and ignoring any recipients.
//
// # Wire format
//
// The blob is the detached-payload convention: the encoded COSE envelope
// immediately followed by the STREAM ciphertext (envelope || ciphertext). The
// body protected header pins the FEE envelope type ([EnvelopeType]) and the body
// algorithm (chunked AES-256-GCM-STREAM); the body unprotected header carries the
// STREAM base nonce in the COSE iv parameter, the plaintext chunk size, and —
// when the plaintext length is known — the chunk count. These conventions and
// their private-use label values match the foc-encryption reference and the FEE
// cross-implementation vectors (see fee/vectors).
//
// # Streaming
//
// Both directions stream with O(chunk size) memory. [Encrypt] returns an
// io.ReadCloser over envelope||ciphertext, produced as the plaintext is read;
// [Decrypt] reads only the (small) envelope header up front and streams the
// detached ciphertext from its source on demand. Neither buffers the whole
// object.
//
// # Scope
//
// This package covers full-object encrypt/decrypt only. Range-based decryption
// is a separate primitive in fee/aesstream, keyed off the ciphertext length and
// chunk size rather than the envelope's chunk count; a higher-level range API is
// tracked separately. This package adds no cryptography of its own.
package fee

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"github.com/fil-forge/ingot/fee/aesstream"
	"github.com/fil-forge/ingot/fee/cose"
)

// EnvelopeType is the COSE "typ" (RFC 9596, header label 16) that every FEE
// envelope is pinned to. [Encrypt] writes it into the body protected header and
// [Decrypt] requires it on decode, so a blob that is not a FEE envelope is
// rejected before any key material is touched. Its value matches the
// foc-encryption reference.
const EnvelopeType = "application/vnd.foc-envelope+cose"

// algChunkedAES256GCMStream is the FEE private-use COSE algorithm id for the
// chunked AES-256-GCM-STREAM body cipher (fee/aesstream). It is the body
// protected header's alg value; [Decrypt] checks it so an envelope sealed with a
// different body cipher is refused rather than mis-decrypted.
const algChunkedAES256GCMStream int64 = -65793

// FEE private-use COSE header labels carried in the body unprotected header,
// matching the foc-encryption reference (src/cose/headers.ts).
const (
	// labelChunkSize holds the STREAM plaintext chunk size, in bytes.
	labelChunkSize int64 = -65790
	// labelChunkCount holds the number of STREAM chunks. It is emitted only when
	// the plaintext length is known ([WithContentLength]); it is advisory
	// metadata for range/seek consumers and is not required to decrypt.
	labelChunkCount int64 = -65791
)

// Sentinel errors. Decrypt failures that originate in a sub-package are wrapped
// rather than replaced, so errors.Is still matches the sub-package sentinel
// (e.g. cose.ErrMalformed, aeskw.ErrIntegrity, aesstream.ErrCorrupted) in
// addition to the fee-level classification here.
var (
	// ErrNoRecipients means Encrypt was called with no recipients. Encrypt
	// wraps a freshly generated CEK, so it needs at least one recipient to be
	// recoverable; use EncryptWithCEK for a recipient-less (external-CEK) envelope.
	ErrNoRecipients = errors.New("fee: at least one recipient is required")

	// ErrNoMatchingRecipient means no recipient entry in the envelope carries a
	// kid equal to the unwrapper's key id, so there is no wrapped CEK for this
	// unwrapper to recover.
	ErrNoMatchingRecipient = errors.New("fee: no envelope recipient matches the unwrapper's key id")

	// ErrNoRecipientsInEnvelope means Decrypt was given a recipient-less
	// COSE_Encrypt0 (tag 16) envelope; recover it with DecryptWithCEK instead.
	ErrNoRecipientsInEnvelope = errors.New("fee: envelope carries no recipients; use DecryptWithCEK")

	// ErrUnsupportedBodyAlg means the envelope's body algorithm header is absent
	// or is not the FEE chunked AES-256-GCM-STREAM cipher.
	ErrUnsupportedBodyAlg = errors.New("fee: unsupported body algorithm")

	// ErrUnsupportedRecipientAlg means a matched recipient's key-wrap algorithm
	// header does not match the unwrapper that was asked to recover it (e.g. an
	// ECDH-ES unwrapper matched against an A256KW recipient).
	ErrUnsupportedRecipientAlg = errors.New("fee: unsupported recipient key-wrap algorithm")

	// ErrMalformedEnvelope means a required body header was missing or had the
	// wrong type, or a declared parameter was out of range.
	ErrMalformedEnvelope = errors.New("fee: malformed FEE envelope")

	// ErrNilUnwrapper means Decrypt was given a nil RecipientUnwrapper.
	ErrNilUnwrapper = errors.New("fee: nil recipient unwrapper")

	// ErrInvalidCEK means a caller-provided content-encryption key (see
	// EncryptWithCEK / DecryptWithCEK) was not the required AES-256 key length.
	ErrInvalidCEK = errors.New("fee: content-encryption key must be 32 bytes")

	// ErrContentLengthMismatch means the plaintext length declared via
	// WithContentLength did not match the number of bytes actually read; it
	// surfaces from the returned reader, and the envelope's chunk count (already
	// written) is not to be trusted.
	ErrContentLengthMismatch = errors.New("fee: plaintext length did not match the declared content length")
)

// encryptConfig holds the resolved, optional Encrypt parameters.
type encryptConfig struct {
	chunkSize     int
	contentLength int64 // < 0 means unknown (chunk count omitted)
}

// EncryptOption configures [Encrypt] and [EncryptWithCEK].
type EncryptOption func(*encryptConfig)

// WithChunkSize sets the STREAM plaintext chunk size, in bytes. A value of 0
// (or an unset option) selects [aesstream.DefaultChunkSize] (256 KiB); any other
// value must be in [aesstream.MinChunkSize, aesstream.MaxChunkSize]. The chosen
// size is recorded in the envelope, so [Decrypt] recovers it.
func WithChunkSize(n int) EncryptOption {
	return func(c *encryptConfig) { c.chunkSize = n }
}

// WithContentLength declares the total plaintext length in bytes. When set, the
// envelope records the chunk count (advisory metadata that lets a range/seek
// consumer plan fetches from the header alone), and the returned reader fails
// with [ErrContentLengthMismatch] if the plaintext turns out to be a different
// length. When unset, the chunk count is omitted — the object still decrypts,
// and range decryption derives the geometry from the ciphertext length instead.
func WithContentLength(n int64) EncryptOption {
	return func(c *encryptConfig) { c.contentLength = n }
}

// Encrypt seals plaintext into a FEE envelope (a COSE_Encrypt, tag 96) addressed
// to recipients and returns a reader over the wire blob: the encoded envelope
// immediately followed by the detached STREAM ciphertext (envelope||ciphertext).
//
// It generates a fresh content-encryption key (CEK) and base nonce, wraps the
// CEK to each recipient, and streams the plaintext through the chunked
// AES-256-GCM-STREAM body cipher, so both plaintext and ciphertext flow with
// O(chunk size) memory. The same CEK is wrapped to every recipient, so any one
// of them can recover the object. recipients must be non-empty (the generated
// CEK would otherwise be unrecoverable); a nil or invalid recipient is reported
// before any plaintext is read. To seal under a CEK you already hold, or a
// recipient-less envelope, use [EncryptWithCEK].
//
// Encryption runs in a background goroutine that feeds the returned reader, so a
// caller MUST either read it to EOF or Close it: Close aborts the goroutine. An
// encryption failure surfaces as a non-EOF error from the reader's Read.
func Encrypt(plaintext io.Reader, recipients []Recipient, opts ...EncryptOption) (io.ReadCloser, error) {
	if len(recipients) == 0 {
		return nil, ErrNoRecipients
	}
	cek := make([]byte, aesstream.KeySize)
	if _, err := rand.Read(cek); err != nil {
		return nil, fmt.Errorf("fee: generating content-encryption key: %w", err)
	}
	// encryptStream copies the CEK into the body cipher and wraps it to the
	// recipients (synchronously, before it returns), so our generated copy can be
	// wiped once it returns — on every path.
	defer zero(cek)
	return encryptStream(plaintext, cek, recipients, opts...)
}

// EncryptWithCEK is [Encrypt] with a caller-provided content-encryption key
// instead of a freshly generated one — for when the CEK is managed out of band
// (derived deterministically, or issued by a custody service). cek must be 32
// bytes (AES-256).
//
// With one or more recipients it produces a COSE_Encrypt (tag 96) that also
// carries the wrapped CEK; with no recipients it produces a recipient-less
// COSE_Encrypt0 (tag 16). Pair it with [DecryptWithCEK] to recover without an
// in-envelope unwrap.
//
// The caller retains ownership of cek: it is copied into the body cipher (and
// wrapped to any recipients) but neither retained nor wiped by this call.
func EncryptWithCEK(plaintext io.Reader, cek []byte, recipients []Recipient, opts ...EncryptOption) (io.ReadCloser, error) {
	if len(cek) != aesstream.KeySize {
		return nil, fmt.Errorf("%w, got %d", ErrInvalidCEK, len(cek))
	}
	return encryptStream(plaintext, cek, recipients, opts...)
}

// encryptStream is the shared core of Encrypt and EncryptWithCEK: it seals
// plaintext under cek and returns a streaming reader over envelope||ciphertext.
// With no recipients it emits a COSE_Encrypt0 (tag 16); otherwise a COSE_Encrypt
// (tag 96). It does not modify or wipe cek.
//
// It also does not retain cek past its own return: the recipient wraps and the
// aesstream.NewWriter that internalizes the CEK (into a GCM AEAD) both run
// synchronously before encryptStream returns, so a caller may wipe cek as soon
// as it returns — even though the returned reader has not been read and its
// background encryption goroutine is still running. That goroutine works from
// the writer's internalized key, never from the cek slice.
func encryptStream(plaintext io.Reader, cek []byte, recipients []Recipient, opts ...EncryptOption) (io.ReadCloser, error) {
	if plaintext == nil {
		return nil, errors.New("fee: nil plaintext reader")
	}
	for i, r := range recipients {
		if r == nil {
			return nil, fmt.Errorf("fee: recipient %d is nil", i)
		}
		if err := r.validate(); err != nil {
			return nil, fmt.Errorf("fee: recipient %d: %w", i, err)
		}
	}

	cfg := encryptConfig{chunkSize: aesstream.DefaultChunkSize, contentLength: -1}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.chunkSize == 0 {
		cfg.chunkSize = aesstream.DefaultChunkSize
	}
	if cfg.chunkSize < aesstream.MinChunkSize || cfg.chunkSize > aesstream.MaxChunkSize {
		return nil, fmt.Errorf("%w: chunk size %d out of range [%d, %d]",
			ErrMalformedEnvelope, cfg.chunkSize, aesstream.MinChunkSize, aesstream.MaxChunkSize)
	}

	baseNonce, err := aesstream.NewBaseNonce()
	if err != nil {
		return nil, fmt.Errorf("fee: generating base nonce: %w", err)
	}

	// The body header is fixed before encryption: the algorithm and envelope
	// type are protected (authenticated through the Enc_structure AAD); the base
	// nonce, chunk size and (when the length is known) chunk count ride in the
	// unprotected header.
	unprotected := cose.Header{}.
		Set(cose.HeaderLabelIV, baseNonce).
		Set(labelChunkSize, int64(cfg.chunkSize))
	if cfg.contentLength >= 0 {
		unprotected.Set(labelChunkCount, int64(chunkCountFor(cfg.contentLength, int64(cfg.chunkSize))))
	}
	headers := cose.Headers{
		Protected: cose.Header{}.
			Set(cose.HeaderLabelAlg, algChunkedAES256GCMStream).
			Set(cose.HeaderLabelType, EnvelopeType),
		Unprotected: unprotected,
	}

	// Wrap the CEK to each recipient (none → a recipient-less envelope). All the
	// fallible header work happens here, before the pipe is created, so an error
	// returns before the pipe exists and can never orphan a PipeReader/PipeWriter
	// pair.
	entries := make([]*cose.Recipient, len(recipients))
	for i, r := range recipients {
		entry, werr := r.wrap(cek)
		if werr != nil {
			return nil, werr
		}
		entries[i] = entry
	}

	// One envelope drives both the AAD and the encoded header. Recipient presence
	// alone selects the form — a COSE_Encrypt (tag 96) when present, a
	// recipient-less COSE_Encrypt0 (tag 16) when not — and the Enc_structure
	// context tracks it. The AAD binds the protected header into every STREAM
	// chunk.
	env := &cose.Envelope{Headers: headers, Recipients: entries}
	aad, err := env.EncStructure(nil)
	if err != nil {
		return nil, fmt.Errorf("fee: building envelope AAD: %w", err)
	}
	header, err := env.Encode()
	if err != nil {
		return nil, fmt.Errorf("fee: encoding envelope: %w", err)
	}

	// The body cipher streams into a pipe that the returned reader drains. Create
	// it only now that every fallible step above has succeeded; the one remaining
	// fallible call (NewWriter) closes both ends on error, so no pipe is left
	// dangling on any error path.
	pr, pw := io.Pipe()
	w, err := aesstream.NewWriter(pw, aesstream.Config{
		Key:       cek,
		BaseNonce: baseNonce,
		AAD:       aad,
		ChunkSize: cfg.chunkSize,
	})
	if err != nil {
		_ = pw.Close()
		_ = pr.Close()
		return nil, fmt.Errorf("fee: initializing body cipher: %w", err)
	}

	declaredLen := cfg.contentLength
	go func() {
		n, cerr := io.Copy(w, plaintext)
		if cerr == nil {
			cerr = w.Close() // emit the final chunk
		}
		if cerr == nil && declaredLen >= 0 && n != declaredLen {
			cerr = fmt.Errorf("%w: declared %d, got %d", ErrContentLengthMismatch, declaredLen, n)
		}
		// A nil error closes the pipe with io.EOF (clean end); otherwise the
		// error surfaces from the reader's Read.
		_ = pw.CloseWithError(cerr)
	}()

	return &encryptReader{
		body: io.MultiReader(bytes.NewReader(header), pr),
		pr:   pr,
	}, nil
}

// chunkCountFor reports how many STREAM chunks a plaintext of nPlain bytes
// produces at the given chunk size. Empty input is one (empty) final chunk.
func chunkCountFor(nPlain, chunkSize int64) int64 {
	if nPlain <= 0 {
		return 1
	}
	return (nPlain + chunkSize - 1) / chunkSize
}

// encryptReader is the io.ReadCloser returned by [Encrypt] / [EncryptWithCEK].
// Read serves the envelope header and then the streamed ciphertext; Close aborts
// the background encryption goroutine by closing the pipe, so it is safe to
// abandon a partial read.
type encryptReader struct {
	body io.Reader      // io.MultiReader(header, pipe reader)
	pr   *io.PipeReader // closing it stops the encryption goroutine
}

func (e *encryptReader) Read(p []byte) (int, error) { return e.body.Read(p) }

func (e *encryptReader) Close() error { return e.pr.Close() }

// Decrypt recovers the plaintext from a FEE COSE_Encrypt (tag 96) envelope read
// from src.
//
// It decodes the envelope header from src (requiring the FEE typ), finds the
// recipient whose kid matches unwrap's key id, recovers the CEK through unwrap,
// and returns a streaming reader over the decrypted plaintext. Only the header
// is read up front; the detached ciphertext is streamed from src on demand.
//
// Decryption is streaming: a non-EOF error from the returned reader (see
// fee/aesstream) means the plaintext is incomplete and must be discarded.
//
// If the envelope carries no recipients (a COSE_Encrypt0), Decrypt returns
// [ErrNoRecipientsInEnvelope] — use [DecryptWithCEK]. If no recipient kid matches
// unwrap, it returns [ErrNoMatchingRecipient] without attempting an unwrap. If
// the matched recipient's wrapped CEK cannot be recovered (e.g. the wrong key),
// the unwrap error is returned and no plaintext reader is produced.
func Decrypt(src io.Reader, unwrap RecipientUnwrapper) (io.Reader, error) {
	if src == nil {
		return nil, errors.New("fee: nil envelope reader")
	}
	if unwrap == nil {
		return nil, ErrNilUnwrapper
	}

	env, ciphertext, err := cose.DecodeReader(src, cose.WithExpectedType(EnvelopeType))
	if err != nil {
		return nil, fmt.Errorf("fee: decoding envelope: %w", err)
	}
	if len(env.Recipients) == 0 {
		return nil, ErrNoRecipientsInEnvelope
	}

	match, err := matchRecipient(env.Recipients, unwrap.keyID())
	if err != nil {
		return nil, err
	}
	cek, err := unwrap.unwrap(match)
	if err != nil {
		return nil, err
	}
	// The recovered CEK is ours; wipe it once openStream has copied it into the
	// body cipher (synchronously, before it returns).
	defer zero(cek)

	return openStream(env, ciphertext, cek)
}

// DecryptWithCEK is [Decrypt] with a caller-provided content-encryption key
// instead of one recovered from an in-envelope recipient — for when the CEK was
// obtained out of band (e.g. unwrapped by a custody service). It accepts either
// a COSE_Encrypt (tag 96) or a recipient-less COSE_Encrypt0 (tag 16); any
// recipients are ignored. cek must be 32 bytes (AES-256).
//
// The caller retains ownership of cek: it is copied into the body cipher but
// neither retained nor wiped by this call.
func DecryptWithCEK(src io.Reader, cek []byte) (io.Reader, error) {
	if src == nil {
		return nil, errors.New("fee: nil envelope reader")
	}
	if len(cek) != aesstream.KeySize {
		return nil, fmt.Errorf("%w, got %d", ErrInvalidCEK, len(cek))
	}
	env, ciphertext, err := cose.DecodeReader(src, cose.WithExpectedType(EnvelopeType))
	if err != nil {
		return nil, fmt.Errorf("fee: decoding envelope: %w", err)
	}
	return openStream(env, ciphertext, cek)
}

// openStream is the shared core of Decrypt and DecryptWithCEK: given a decoded
// envelope, its detached ciphertext stream, and the content-encryption key, it
// validates the body parameters and returns the streaming plaintext reader.
//
// It does not retain cek past its own return: aesstream.NewReader internalizes
// the CEK (into a GCM AEAD) synchronously before openStream returns, so a caller
// may wipe cek as soon as it returns — even though the returned reader decrypts
// lazily on later reads, which work from the internalized key, never the cek
// slice.
func openStream(env *cose.Envelope, ciphertext io.Reader, cek []byte) (io.Reader, error) {
	alg, ok := env.Headers.Protected.Int(cose.HeaderLabelAlg)
	if !ok {
		return nil, fmt.Errorf("%w: body algorithm header missing or not an integer", ErrUnsupportedBodyAlg)
	}
	if alg != algChunkedAES256GCMStream {
		return nil, fmt.Errorf("%w: body algorithm %d is not chunked AES-256-GCM-STREAM", ErrUnsupportedBodyAlg, alg)
	}

	baseNonce, ok := env.Headers.Unprotected.Bytes(cose.HeaderLabelIV)
	if !ok {
		return nil, fmt.Errorf("%w: missing iv (base nonce)", ErrMalformedEnvelope)
	}

	// Self-describing chunk size; an envelope that omits it is read at the FEE
	// default, but a header present with a non-integer value is a malformed
	// envelope rather than a silent fallback. (The chunk count, when present, is
	// advisory and not needed to decrypt.)
	chunkSize := int64(aesstream.DefaultChunkSize)
	if env.Headers.Unprotected.Has(labelChunkSize) {
		n, ok := env.Headers.Unprotected.Int(labelChunkSize)
		if !ok {
			return nil, fmt.Errorf("%w: chunk-size header is present but not an integer", ErrMalformedEnvelope)
		}
		chunkSize = n
	}
	if chunkSize < int64(aesstream.MinChunkSize) || chunkSize > int64(aesstream.MaxChunkSize) {
		return nil, fmt.Errorf("%w: declared chunk size %d out of range [%d, %d]",
			ErrMalformedEnvelope, chunkSize, aesstream.MinChunkSize, aesstream.MaxChunkSize)
	}

	// The decrypt-side AAD is rebuilt from the decoded envelope, using the
	// Enc_structure context that matches its tag — byte-identical to the value
	// the encoder bound into every chunk.
	aad, err := env.EncStructure(nil)
	if err != nil {
		return nil, fmt.Errorf("fee: building envelope AAD: %w", err)
	}

	r, err := aesstream.NewReader(ciphertext, aesstream.Config{
		Key:       cek,
		BaseNonce: baseNonce,
		AAD:       aad,
		ChunkSize: int(chunkSize),
	})
	if err != nil {
		return nil, fmt.Errorf("fee: initializing body cipher: %w", err)
	}
	return r, nil
}

// matchRecipient returns the first recipient whose kid equals want. A recipient
// without a kid header never matches. An empty want, or no match, yields
// ErrNoMatchingRecipient.
func matchRecipient(recipients []*cose.Recipient, want []byte) (*cose.Recipient, error) {
	if len(want) == 0 {
		return nil, ErrNoMatchingRecipient
	}
	for _, r := range recipients {
		if kid, ok := r.Headers.Unprotected.Bytes(cose.HeaderLabelKID); ok && bytes.Equal(kid, want) {
			return r, nil
		}
	}
	return nil, ErrNoMatchingRecipient
}

// zero overwrites b, a best-effort wipe of the CEK once it has been copied into
// the body cipher (and, on encrypt, wrapped to every recipient).
func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
