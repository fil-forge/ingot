package fee_test

import (
	"bytes"
	"crypto/ecdh"
	"crypto/rand"
	"io"
	"strconv"
	"testing"
	"testing/iotest"

	"github.com/fil-forge/ingot/fee"
	"github.com/fil-forge/ingot/fee/aeskw"
	"github.com/fil-forge/ingot/fee/aesstream"
	"github.com/fil-forge/ingot/fee/cose"
	"github.com/stretchr/testify/require"
)

// FEE wire constants mirrored for white-box assertions. They MUST match the
// unexported values in fee.go (and the foc-encryption reference): the body
// algorithm and the private-use unprotected header labels carrying the STREAM
// geometry. Tests read them straight off a produced envelope with the cose
// package, so a drift in either place surfaces here.
const (
	algChunkedStream = int64(-65793)
	labelChunkSize   = int64(-65790)
	labelChunkCount  = int64(-65791)
)

// COSE_Key parameter labels/values for the X25519 ephemeral key (RFC 9053 §7.1),
// used to assert and to hand-build the self-describing ephemeral key.
const (
	coseKeyKty    = int64(1)
	coseKeyCrv    = int64(-1)
	coseKeyX      = int64(-2)
	coseKtyOKP    = int64(1)
	coseCrvX25519 = int64(4)
)

// ecdhKID and a256kwKID are opaque recipient key ids. In the app that will use
// this library a kid is a DID verification method ID; the library treats it as
// arbitrary bytes, so the tests use readable placeholders.
var (
	ecdhKID   = []byte("did:key:z6MkExampleRecipient#key-1")
	a256kwKID = []byte("did:example:custody#kek-1")
)

// newX25519Key returns a fresh X25519 keypair for an ECDH-ES recipient.
// Round-trip tests do not need determinism, so keys are generated per test.
func newX25519Key(t *testing.T) *ecdh.PrivateKey {
	t.Helper()
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	require.NoError(t, err)
	return priv
}

// newKEK returns a fresh 32-byte (A256KW) key-encryption key.
func newKEK(t *testing.T) []byte {
	t.Helper()
	kek := make([]byte, 32)
	_, err := rand.Read(kek)
	require.NoError(t, err)
	return kek
}

// newCEK returns a fresh 32-byte (AES-256) content-encryption key.
func newCEK(t *testing.T) []byte {
	t.Helper()
	cek := make([]byte, aesstream.KeySize)
	_, err := rand.Read(cek)
	require.NoError(t, err)
	return cek
}

// patternBytes returns n deterministic bytes whose period (251, prime) does not
// align to any power-of-two chunk size, so chunk boundaries fall on varying
// byte values.
func patternBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// encrypt runs fee.Encrypt over plaintext and reads the streamed
// envelope||ciphertext into a single blob, so tests can inspect or corrupt it.
// An Encrypt setup error (e.g. an invalid recipient) is returned directly.
func encrypt(t *testing.T, plaintext []byte, recipients []fee.Recipient, opts ...fee.EncryptOption) ([]byte, error) {
	t.Helper()
	r, err := fee.Encrypt(bytes.NewReader(plaintext), recipients, opts...)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// decryptAll decrypts blob and reads the whole plaintext, asserting a clean
// stream (EOF, no decryption error).
func decryptAll(t *testing.T, blob []byte, u fee.RecipientUnwrapper) []byte {
	t.Helper()
	r, err := fee.Decrypt(bytes.NewReader(blob), u)
	require.NoError(t, err)
	plaintext, err := io.ReadAll(r)
	require.NoError(t, err)
	return plaintext
}

// TestRoundTripECDHES exercises the ECDH-ES+A256KW recipient path across a range
// of sizes — empty, sub-chunk, chunk boundaries and multi-chunk — at a small
// chunk size so the multi-chunk STREAM path runs cheaply.
func TestRoundTripECDHES(t *testing.T) {
	const chunk = aesstream.MinChunkSize // 4 KiB

	sizes := []int{0, 1, 100, chunk - 1, chunk, chunk + 1, 3*chunk + 7}
	for _, n := range sizes {
		n := n
		t.Run(name(n), func(t *testing.T) {
			key := newX25519Key(t)
			plaintext := patternBytes(n)

			blob, err := encrypt(t, plaintext,
				[]fee.Recipient{fee.NewECDHESRecipient(ecdhKID, key.PublicKey())},
				fee.WithChunkSize(chunk),
			)
			require.NoError(t, err)

			got := decryptAll(t, blob, fee.NewECDHESUnwrapper(ecdhKID, key))
			require.Equal(t, plaintext, got)
		})
	}
}

// TestRoundTripA256KW exercises the A256KW recipient path (under a raw KEK),
// including a multi-chunk body, at the default chunk size to confirm the default
// round-trips without WithChunkSize.
func TestRoundTripA256KW(t *testing.T) {
	kek := newKEK(t)
	plaintext := patternBytes(300_000) // > 256 KiB default chunk: multiple chunks

	blob, err := encrypt(t, plaintext,
		[]fee.Recipient{fee.NewA256KWRecipient(a256kwKID, kek)},
	)
	require.NoError(t, err)

	got := decryptAll(t, blob, fee.NewA256KWUnwrapper(a256kwKID, kek))
	require.Equal(t, plaintext, got)
}

// TestRoundTripMixedRecipients is a central acceptance criterion: one Encrypt
// call with both an ECDH-ES and an A256KW recipient yields an envelope that
// either recipient can open on its own, recovering the same plaintext — and the
// caller never selects a wrap algorithm.
func TestRoundTripMixedRecipients(t *testing.T) {
	key := newX25519Key(t)
	kek := newKEK(t)
	plaintext := patternBytes(20_000)

	blob, err := encrypt(t, plaintext,
		[]fee.Recipient{
			fee.NewECDHESRecipient(ecdhKID, key.PublicKey()),
			fee.NewA256KWRecipient(a256kwKID, kek),
		},
		fee.WithChunkSize(aesstream.MinChunkSize),
	)
	require.NoError(t, err)

	t.Run("ECDH-ES opens", func(t *testing.T) {
		require.Equal(t, plaintext, decryptAll(t, blob, fee.NewECDHESUnwrapper(ecdhKID, key)))
	})
	t.Run("A256KW opens", func(t *testing.T) {
		require.Equal(t, plaintext, decryptAll(t, blob, fee.NewA256KWUnwrapper(a256kwKID, kek)))
	})
}

// TestExternalCEK exercises the externally-provided-CEK path with a recipient
// still present: EncryptWithCEK seals under a caller-held CEK (producing a tag-96
// COSE_Encrypt that also carries the wrapped CEK) and DecryptWithCEK recovers
// with it, ignoring recipients. It also cross-checks that the in-envelope
// recipient unwraps to the very CEK that was used, and that a wrong CEK fails.
func TestExternalCEK(t *testing.T) {
	cek := newCEK(t)
	kek := newKEK(t)
	plaintext := patternBytes(3*aesstream.MinChunkSize + 5)

	// Seal under the caller's CEK; still carry an A256KW recipient so the CEK is
	// recoverable in-envelope too.
	enc, err := fee.EncryptWithCEK(bytes.NewReader(plaintext), cek,
		[]fee.Recipient{fee.NewA256KWRecipient(a256kwKID, kek)},
		fee.WithChunkSize(aesstream.MinChunkSize),
	)
	require.NoError(t, err)
	defer enc.Close()
	blob, err := io.ReadAll(enc)
	require.NoError(t, err)

	// With a recipient present it is a tag-96 COSE_Encrypt.
	tag, err := cose.PeekTag(blob)
	require.NoError(t, err)
	require.Equal(t, cose.TagCOSEEncrypt, tag)

	t.Run("DecryptWithCEK recovers", func(t *testing.T) {
		dr, err := fee.DecryptWithCEK(bytes.NewReader(blob), cek)
		require.NoError(t, err)
		got, err := io.ReadAll(dr)
		require.NoError(t, err)
		require.Equal(t, plaintext, got)
	})

	t.Run("in-envelope recipient unwraps the same CEK", func(t *testing.T) {
		require.Equal(t, plaintext, decryptAll(t, blob, fee.NewA256KWUnwrapper(a256kwKID, kek)))
	})

	t.Run("wrong CEK fails on read", func(t *testing.T) {
		dr, err := fee.DecryptWithCEK(bytes.NewReader(blob), newCEK(t))
		require.NoError(t, err, "the CEK is not authenticated until the body is read")
		_, err = io.ReadAll(dr)
		require.ErrorIs(t, err, aesstream.ErrCorrupted)
	})
}

// TestExternalCEKNoRecipients exercises the recipient-less path: EncryptWithCEK
// with no recipients emits a COSE_Encrypt0 (tag 16) that DecryptWithCEK recovers,
// while the in-envelope Decrypt refuses it (there is no wrapped CEK to unwrap).
func TestExternalCEKNoRecipients(t *testing.T) {
	cek := newCEK(t)
	plaintext := patternBytes(2*aesstream.MinChunkSize + 3)

	enc, err := fee.EncryptWithCEK(bytes.NewReader(plaintext), cek, nil,
		fee.WithChunkSize(aesstream.MinChunkSize),
	)
	require.NoError(t, err)
	defer enc.Close()
	blob, err := io.ReadAll(enc)
	require.NoError(t, err)

	// A recipient-less envelope is a COSE_Encrypt0 (tag 16).
	tag, err := cose.PeekTag(blob)
	require.NoError(t, err)
	require.Equal(t, cose.TagCOSEEncrypt0, tag)

	t.Run("DecryptWithCEK recovers", func(t *testing.T) {
		dr, err := fee.DecryptWithCEK(bytes.NewReader(blob), cek)
		require.NoError(t, err)
		got, err := io.ReadAll(dr)
		require.NoError(t, err)
		require.Equal(t, plaintext, got)
	})

	t.Run("Decrypt refuses a recipient-less envelope", func(t *testing.T) {
		r, err := fee.Decrypt(bytes.NewReader(blob), fee.NewA256KWUnwrapper(a256kwKID, newKEK(t)))
		require.Nil(t, r)
		require.ErrorIs(t, err, fee.ErrNoRecipientsInEnvelope)
	})
}

// TestExternalCEKInvalidLength confirms both external-CEK entry points reject a
// CEK that is not 32 bytes.
func TestExternalCEKInvalidLength(t *testing.T) {
	t.Run("encrypt", func(t *testing.T) {
		_, err := fee.EncryptWithCEK(bytes.NewReader(patternBytes(10)), make([]byte, 16),
			[]fee.Recipient{fee.NewA256KWRecipient(a256kwKID, newKEK(t))})
		require.ErrorIs(t, err, fee.ErrInvalidCEK)
	})
	t.Run("decrypt", func(t *testing.T) {
		r, err := fee.DecryptWithCEK(bytes.NewReader([]byte{0xd8, 0x60}), make([]byte, 16))
		require.Nil(t, r)
		require.ErrorIs(t, err, fee.ErrInvalidCEK)
	})
}

// TestStreamingRoundTrip pipes Encrypt's reader straight into Decrypt with no
// intermediate []byte, so the whole object flows reader→reader. This exercises
// cose.DecodeReader parsing the header off a live stream and the ciphertext
// being streamed from the same source.
func TestStreamingRoundTrip(t *testing.T) {
	key := newX25519Key(t)
	plaintext := patternBytes(3*aesstream.MinChunkSize + 123)

	enc, err := fee.Encrypt(
		bytes.NewReader(plaintext),
		[]fee.Recipient{fee.NewECDHESRecipient(ecdhKID, key.PublicKey())},
		fee.WithChunkSize(aesstream.MinChunkSize),
	)
	require.NoError(t, err)
	defer enc.Close()

	dec, err := fee.Decrypt(enc, fee.NewECDHESUnwrapper(ecdhKID, key))
	require.NoError(t, err)

	got, err := io.ReadAll(dec)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

// TestDecryptFromFragmentedReader decrypts an envelope whose bytes arrive one at
// a time, confirming the streaming header decode reassembles correctly when the
// detached ciphertext is split between the decoder's read-ahead buffer and the
// source reader.
func TestDecryptFromFragmentedReader(t *testing.T) {
	key := newX25519Key(t)
	plaintext := patternBytes(2*aesstream.MinChunkSize + 7)
	blob, err := encrypt(t, plaintext,
		[]fee.Recipient{fee.NewECDHESRecipient(ecdhKID, key.PublicKey())},
		fee.WithChunkSize(aesstream.MinChunkSize),
	)
	require.NoError(t, err)

	dec, err := fee.Decrypt(iotest.OneByteReader(bytes.NewReader(blob)), fee.NewECDHESUnwrapper(ecdhKID, key))
	require.NoError(t, err)
	got, err := io.ReadAll(dec)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

// TestEncryptReaderCloseReleases confirms the reader can be closed after a
// partial read without deadlocking, and that reads after close fail rather than
// return the full ciphertext (the background encryption goroutine is aborted).
func TestEncryptReaderCloseReleases(t *testing.T) {
	key := newX25519Key(t)
	// Large enough that the encryption goroutine would block on the pipe if the
	// reader were abandoned without Close.
	enc, err := fee.Encrypt(
		bytes.NewReader(patternBytes(10*aesstream.MinChunkSize)),
		[]fee.Recipient{fee.NewECDHESRecipient(ecdhKID, key.PublicKey())},
		fee.WithChunkSize(aesstream.MinChunkSize),
	)
	require.NoError(t, err)

	buf := make([]byte, 16)
	_, _ = enc.Read(buf) // consume a little
	require.NoError(t, enc.Close())

	// After Close the stream is aborted; draining the rest must error, not hang.
	_, err = io.ReadAll(enc)
	require.Error(t, err)
}

// TestDecryptKidMismatch covers the explicit acceptance criterion: an unwrapper
// whose key id matches no recipient yields ErrNoMatchingRecipient rather than a
// panic or a silent failure. It checks both recipient kinds.
func TestDecryptKidMismatch(t *testing.T) {
	plaintext := patternBytes(5000)

	t.Run("ECDH-ES unwrapper, wrong kid", func(t *testing.T) {
		key := newX25519Key(t)
		blob, err := encrypt(t, plaintext,
			[]fee.Recipient{fee.NewECDHESRecipient(ecdhKID, key.PublicKey())},
			fee.WithChunkSize(aesstream.MinChunkSize),
		)
		require.NoError(t, err)

		r, err := fee.Decrypt(bytes.NewReader(blob), fee.NewECDHESUnwrapper([]byte("other-kid"), key))
		require.Nil(t, r)
		require.ErrorIs(t, err, fee.ErrNoMatchingRecipient)
	})

	t.Run("A256KW unwrapper, wrong kid", func(t *testing.T) {
		kek := newKEK(t)
		blob, err := encrypt(t, plaintext,
			[]fee.Recipient{fee.NewA256KWRecipient([]byte("kid-a"), kek)},
			fee.WithChunkSize(aesstream.MinChunkSize),
		)
		require.NoError(t, err)

		r, err := fee.Decrypt(bytes.NewReader(blob), fee.NewA256KWUnwrapper([]byte("kid-b"), kek))
		require.Nil(t, r)
		require.ErrorIs(t, err, fee.ErrNoMatchingRecipient)
	})
}

// TestDecryptWrongECDHKey confirms that when the kid matches but the private key
// is wrong, the ECDH-ES unwrap fails with the AES-KW integrity error and no
// plaintext reader is produced — decryption is never reached.
func TestDecryptWrongECDHKey(t *testing.T) {
	key := newX25519Key(t)
	blob, err := encrypt(t, patternBytes(9000),
		[]fee.Recipient{fee.NewECDHESRecipient(ecdhKID, key.PublicKey())},
		fee.WithChunkSize(aesstream.MinChunkSize),
	)
	require.NoError(t, err)

	// Same kid, different private key: the entry matches by kid, but the ECDH
	// agreement derives the wrong KEK and the AES-KW integrity check fails.
	wrong := newX25519Key(t)
	r, err := fee.Decrypt(bytes.NewReader(blob), fee.NewECDHESUnwrapper(ecdhKID, wrong))
	require.Nil(t, r)
	require.ErrorIs(t, err, aeskw.ErrIntegrity)
	require.NotErrorIs(t, err, aesstream.ErrCorrupted, "STREAM decryption must not be attempted")
}

// TestDecryptWrongA256KWKEK confirms that when the kid matches but the KEK is
// wrong, the unwrap fails with the AES-KW integrity error and no plaintext
// reader is produced — decryption is never reached.
func TestDecryptWrongA256KWKEK(t *testing.T) {
	blob, err := encrypt(t, patternBytes(9000),
		[]fee.Recipient{fee.NewA256KWRecipient(a256kwKID, newKEK(t))},
		fee.WithChunkSize(aesstream.MinChunkSize),
	)
	require.NoError(t, err)

	r, err := fee.Decrypt(bytes.NewReader(blob), fee.NewA256KWUnwrapper(a256kwKID, newKEK(t)))
	require.Nil(t, r)
	require.ErrorIs(t, err, aeskw.ErrIntegrity)
	require.NotErrorIs(t, err, aesstream.ErrCorrupted, "STREAM decryption must not be attempted")
}

// TestDecryptAlgMismatch confirms the unwrapper rejects a matched recipient whose
// key-wrap algorithm is not the one it handles. An A256KW unwrapper is pointed at
// an ECDH-ES recipient by reusing its kid, so the entry matches by kid but
// carries ECDH-ES+A256KW, not A256KW.
func TestDecryptAlgMismatch(t *testing.T) {
	key := newX25519Key(t)
	blob, err := encrypt(t, patternBytes(5000),
		[]fee.Recipient{fee.NewECDHESRecipient(ecdhKID, key.PublicKey())},
		fee.WithChunkSize(aesstream.MinChunkSize),
	)
	require.NoError(t, err)

	// Same kid as the ECDH-ES entry, but an A256KW unwrapper.
	r, err := fee.Decrypt(bytes.NewReader(blob), fee.NewA256KWUnwrapper(ecdhKID, newKEK(t)))
	require.Nil(t, r)
	require.ErrorIs(t, err, fee.ErrUnsupportedRecipientAlg)
}

// TestDecryptCorruptedProtectedHeader confirms that flipping a byte in the
// protected-header bytes makes Decrypt fail at decode (wrapping cose.ErrMalformed)
// rather than yielding a reader that decrypts to garbage.
func TestDecryptCorruptedProtectedHeader(t *testing.T) {
	key := newX25519Key(t)
	blob, err := encrypt(t, patternBytes(8000),
		[]fee.Recipient{fee.NewECDHESRecipient(ecdhKID, key.PublicKey())},
		fee.WithChunkSize(aesstream.MinChunkSize),
	)
	require.NoError(t, err)

	// Locate the protected-header bytes via a clean decode, then flip the first
	// byte (the CBOR map head) so the protected bytes are no longer a map.
	env, _, err := cose.Decode(blob, cose.WithExpectedType(fee.EnvelopeType))
	require.NoError(t, err)
	require.NotEmpty(t, env.Headers.RawProtected)
	off := bytes.Index(blob, env.Headers.RawProtected)
	require.GreaterOrEqual(t, off, 0)

	corrupted := bytes.Clone(blob)
	corrupted[off] ^= 0xFF

	r, err := fee.Decrypt(bytes.NewReader(corrupted), fee.NewECDHESUnwrapper(ecdhKID, key))
	require.Nil(t, r)
	require.ErrorIs(t, err, cose.ErrMalformed)
}

// TestDecryptTamperedCiphertext confirms that flipping a byte in the detached
// ciphertext is detected when the plaintext is read: the stream reader returns
// aesstream.ErrCorrupted.
func TestDecryptTamperedCiphertext(t *testing.T) {
	key := newX25519Key(t)
	plaintext := patternBytes(3 * aesstream.MinChunkSize)
	blob, err := encrypt(t, plaintext,
		[]fee.Recipient{fee.NewECDHESRecipient(ecdhKID, key.PublicKey())},
		fee.WithChunkSize(aesstream.MinChunkSize),
	)
	require.NoError(t, err)

	// Flip the final byte (inside the detached ciphertext, well past the
	// envelope header).
	corrupted := bytes.Clone(blob)
	corrupted[len(corrupted)-1] ^= 0xFF

	r, err := fee.Decrypt(bytes.NewReader(corrupted), fee.NewECDHESUnwrapper(ecdhKID, key))
	require.NoError(t, err, "decode + unwrap still succeed; tampering is in the body")
	_, err = io.ReadAll(r)
	require.ErrorIs(t, err, aesstream.ErrCorrupted)
}

// TestDecryptWrongEnvelopeType confirms a blob that is not a FEE envelope (wrong
// COSE typ) is refused before any key material is used.
func TestDecryptWrongEnvelopeType(t *testing.T) {
	// A well-formed COSE_Encrypt with a non-FEE typ and a single recipient.
	other := &cose.Envelope{
		Headers: cose.Headers{
			Protected: cose.Header{}.Set(cose.HeaderLabelType, "application/not-fee"),
		},
		Recipients: []*cose.Recipient{{
			Headers:    cose.Headers{Unprotected: cose.Header{}.Set(cose.HeaderLabelKID, []byte("k"))},
			Ciphertext: []byte("xxxxxxxxxxxxxxxxxxxxxxxx"),
		}},
	}
	blob, err := other.Encode()
	require.NoError(t, err)

	r, err := fee.Decrypt(bytes.NewReader(blob), fee.NewA256KWUnwrapper([]byte("k"), newKEK(t)))
	require.Nil(t, r)
	require.ErrorIs(t, err, cose.ErrUnexpectedType)
}

// TestEncryptNoRecipients confirms Encrypt rejects an empty recipient set.
func TestEncryptNoRecipients(t *testing.T) {
	_, err := fee.Encrypt(bytes.NewReader(patternBytes(10)), nil)
	require.ErrorIs(t, err, fee.ErrNoRecipients)
}

// TestEncryptNilRecipient confirms a nil entry in the recipient slice is an
// error rather than a panic.
func TestEncryptNilRecipient(t *testing.T) {
	_, err := fee.Encrypt(bytes.NewReader(patternBytes(10)), []fee.Recipient{nil})
	require.Error(t, err)
}

// TestEncryptChunkSizeOutOfRange confirms an explicit chunk size below the body
// cipher's minimum is rejected as an invalid argument — aesstream.ErrChunkSize,
// the sentinel the body cipher itself uses — not as a malformed envelope.
func TestEncryptChunkSizeOutOfRange(t *testing.T) {
	key := newX25519Key(t)
	_, err := fee.Encrypt(
		bytes.NewReader(patternBytes(10)),
		[]fee.Recipient{fee.NewECDHESRecipient(ecdhKID, key.PublicKey())},
		fee.WithChunkSize(aesstream.MinChunkSize-1),
	)
	require.ErrorIs(t, err, aesstream.ErrChunkSize)
	require.NotErrorIs(t, err, fee.ErrMalformedEnvelope)
}

// TestDecryptNilArgs confirms nil src or nil unwrapper are reported, not
// dereferenced.
func TestDecryptNilArgs(t *testing.T) {
	t.Run("nil unwrapper", func(t *testing.T) {
		r, err := fee.Decrypt(bytes.NewReader([]byte{0xd8, 0x60}), nil)
		require.Nil(t, r)
		require.ErrorIs(t, err, fee.ErrNilUnwrapper)
	})
	t.Run("nil source", func(t *testing.T) {
		r, err := fee.Decrypt(nil, fee.NewA256KWUnwrapper([]byte("k"), newKEK(t)))
		require.Nil(t, r)
		require.Error(t, err)
	})
}

// TestEnvelopeWireConventions decodes a produced envelope with the cose package
// directly and asserts the FEE wire conventions matching the foc-encryption
// reference: the protected header pins the envelope type and the STREAM body
// algorithm; the unprotected header carries the base nonce in the iv and the
// chunk size in the private-use label; and the recipient entry carries the
// expected algorithm, kid and a self-describing COSE_Key ephemeral key.
func TestEnvelopeWireConventions(t *testing.T) {
	key := newX25519Key(t)
	const chunk = aesstream.MinChunkSize

	blob, err := encrypt(t, patternBytes(2000),
		[]fee.Recipient{fee.NewECDHESRecipient(ecdhKID, key.PublicKey())},
		fee.WithChunkSize(chunk),
	)
	require.NoError(t, err)

	env, ciphertext, err := cose.Decode(blob, cose.WithExpectedType(fee.EnvelopeType))
	require.NoError(t, err)

	// Protected header: typ and body algorithm are authenticated via the AAD.
	typ, ok := env.Headers.Protected.Text(cose.HeaderLabelType)
	require.True(t, ok)
	require.Equal(t, fee.EnvelopeType, typ)

	alg, ok := env.Headers.Protected.Int(cose.HeaderLabelAlg)
	require.True(t, ok)
	require.Equal(t, algChunkedStream, alg, "chunked AES-256-GCM-STREAM private-use alg")

	// Unprotected header: base nonce (iv) and chunk size in the private-use label.
	iv, ok := env.Headers.Unprotected.Bytes(cose.HeaderLabelIV)
	require.True(t, ok)
	require.Len(t, iv, aesstream.BaseNonceSize)

	cs, ok := env.Headers.Unprotected.Int(labelChunkSize)
	require.True(t, ok, "chunk size recorded in the unprotected private-use label")
	require.Equal(t, int64(chunk), cs)

	// Recipient: algorithm, kid, and a self-describing COSE_Key ephemeral key.
	require.Len(t, env.Recipients, 1)
	rcpt := env.Recipients[0]
	ralg, ok := rcpt.Headers.Protected.Int(cose.HeaderLabelAlg)
	require.True(t, ok)
	require.Equal(t, cose.AlgECDHESA256KW, ralg)
	kid, ok := rcpt.Headers.Unprotected.Bytes(cose.HeaderLabelKID)
	require.True(t, ok)
	require.Equal(t, ecdhKID, kid)

	ephRaw, ok := rcpt.Headers.Unprotected.Get(cose.HeaderLabelEphemeralKey)
	require.True(t, ok, "ECDH-ES recipient carries an ephemeral key")
	eph, ok := ephRaw.(map[any]any)
	require.True(t, ok, "ephemeral key is a self-describing COSE_Key map")
	require.Equal(t, coseKtyOKP, eph[coseKeyKty], "kty OKP")
	require.Equal(t, coseCrvX25519, eph[coseKeyCrv], "crv X25519")
	x, ok := eph[coseKeyX].([]byte)
	require.True(t, ok, "ephemeral key x is a byte string")
	require.Len(t, x, 32, "X25519 public key is 32 bytes")

	// The detached ciphertext is non-empty and decrypts back through the API.
	require.NotEmpty(t, ciphertext)
}

// TestContentLengthChunkCount confirms the chunk count is written to the
// unprotected header only when the plaintext length is declared, matching the
// reference's advisory chunkCount, and that the envelope decrypts either way.
func TestContentLengthChunkCount(t *testing.T) {
	key := newX25519Key(t)
	const chunk = aesstream.MinChunkSize
	plaintext := patternBytes(3*chunk + 10) // 4 chunks

	t.Run("present and correct when content length declared", func(t *testing.T) {
		blob, err := encrypt(t, plaintext,
			[]fee.Recipient{fee.NewECDHESRecipient(ecdhKID, key.PublicKey())},
			fee.WithChunkSize(chunk), fee.WithContentLength(int64(len(plaintext))),
		)
		require.NoError(t, err)

		env, _, err := cose.Decode(blob, cose.WithExpectedType(fee.EnvelopeType))
		require.NoError(t, err)
		cc, ok := env.Headers.Unprotected.Int(labelChunkCount)
		require.True(t, ok, "chunk count present when content length declared")
		require.Equal(t, int64(4), cc)

		require.Equal(t, plaintext, decryptAll(t, blob, fee.NewECDHESUnwrapper(ecdhKID, key)))
	})

	t.Run("omitted when content length unknown", func(t *testing.T) {
		blob, err := encrypt(t, plaintext,
			[]fee.Recipient{fee.NewECDHESRecipient(ecdhKID, key.PublicKey())},
			fee.WithChunkSize(chunk),
		)
		require.NoError(t, err)

		env, _, err := cose.Decode(blob, cose.WithExpectedType(fee.EnvelopeType))
		require.NoError(t, err)
		require.False(t, env.Headers.Unprotected.Has(labelChunkCount),
			"chunk count omitted when content length unknown")

		require.Equal(t, plaintext, decryptAll(t, blob, fee.NewECDHESUnwrapper(ecdhKID, key)))
	})
}

// TestContentLengthMismatch confirms that when the declared content length does
// not match the plaintext actually streamed, the returned reader surfaces
// ErrContentLengthMismatch (the already-written chunk count cannot be trusted).
func TestContentLengthMismatch(t *testing.T) {
	key := newX25519Key(t)
	// Multiple chunks, so some full chunks are emitted before the final chunk is
	// withheld on the mismatch.
	plaintext := patternBytes(2*aesstream.MinChunkSize + 7)

	enc, err := fee.Encrypt(bytes.NewReader(plaintext),
		[]fee.Recipient{fee.NewECDHESRecipient(ecdhKID, key.PublicKey())},
		fee.WithChunkSize(aesstream.MinChunkSize),
		fee.WithContentLength(int64(len(plaintext)+1)), // declare one byte too many
	)
	require.NoError(t, err)
	defer enc.Close()

	// The mismatch surfaces as a non-EOF error from the reader...
	blob, err := io.ReadAll(enc)
	require.ErrorIs(t, err, fee.ErrContentLengthMismatch)

	// ...and because the final chunk was withheld, the bytes produced so far do
	// not decrypt cleanly: a caller that ignored the error and stored the blob
	// gets a truncated, unauthenticatable object rather than a valid-but-
	// mislabeled one. The header still decodes and the CEK still unwraps; the
	// failure is in the truncated body.
	r, derr := fee.Decrypt(bytes.NewReader(blob), fee.NewECDHESUnwrapper(ecdhKID, key))
	require.NoError(t, derr)
	_, derr = io.ReadAll(r)
	require.ErrorIs(t, derr, aesstream.ErrTruncated)
}

// TestChunkSizeIsSelfDescribing confirms an envelope sealed with a non-default
// chunk size decrypts without the caller passing that size to Decrypt — proving
// it travels in the envelope.
func TestChunkSizeIsSelfDescribing(t *testing.T) {
	key := newX25519Key(t)
	plaintext := patternBytes(5 * 8192)

	blob, err := encrypt(t, plaintext,
		[]fee.Recipient{fee.NewECDHESRecipient(ecdhKID, key.PublicKey())},
		fee.WithChunkSize(8192),
	)
	require.NoError(t, err)

	// Decrypt is given only the blob and the unwrapper; it must recover the
	// 8 KiB chunk size from the envelope to frame the ciphertext correctly.
	require.Equal(t, plaintext, decryptAll(t, blob, fee.NewECDHESUnwrapper(ecdhKID, key)))
}

// TestDecryptStreamsIncrementally confirms the returned reader is a real stream:
// reading one byte at a time reassembles the exact plaintext.
func TestDecryptStreamsIncrementally(t *testing.T) {
	key := newX25519Key(t)
	plaintext := patternBytes(2*aesstream.MinChunkSize + 13)
	blob, err := encrypt(t, plaintext,
		[]fee.Recipient{fee.NewECDHESRecipient(ecdhKID, key.PublicKey())},
		fee.WithChunkSize(aesstream.MinChunkSize),
	)
	require.NoError(t, err)

	r, err := fee.Decrypt(bytes.NewReader(blob), fee.NewECDHESUnwrapper(ecdhKID, key))
	require.NoError(t, err)

	var got bytes.Buffer
	buf := make([]byte, 1)
	for {
		n, err := r.Read(buf)
		got.Write(buf[:n])
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
	require.Equal(t, plaintext, got.Bytes())
}

// TestEncryptInvalidRecipient confirms a malformed recipient is rejected up
// front (before the plaintext is sealed), not deep inside the wrap step.
func TestEncryptInvalidRecipient(t *testing.T) {
	key := newX25519Key(t)
	t.Run("ECDH-ES nil public key", func(t *testing.T) {
		_, err := fee.Encrypt(bytes.NewReader(patternBytes(10)),
			[]fee.Recipient{fee.NewECDHESRecipient(ecdhKID, nil)})
		require.Error(t, err)
	})
	t.Run("ECDH-ES empty kid", func(t *testing.T) {
		_, err := fee.Encrypt(bytes.NewReader(patternBytes(10)),
			[]fee.Recipient{fee.NewECDHESRecipient(nil, key.PublicKey())})
		require.Error(t, err)
	})
	t.Run("A256KW empty kid", func(t *testing.T) {
		_, err := fee.Encrypt(bytes.NewReader(patternBytes(10)),
			[]fee.Recipient{fee.NewA256KWRecipient(nil, newKEK(t))})
		require.Error(t, err)
	})
	t.Run("A256KW wrong KEK length", func(t *testing.T) {
		_, err := fee.Encrypt(bytes.NewReader(patternBytes(10)),
			[]fee.Recipient{fee.NewA256KWRecipient(a256kwKID, make([]byte, 16))})
		require.Error(t, err)
	})
}

// TestDecryptMalformedChunkSizeHeader confirms that a chunk-size header which is
// present but not an integer is rejected as a malformed envelope, rather than
// silently falling back to the default size. The recipient carries a real
// wrapped CEK so the unwrap succeeds and Decrypt reaches the chunk-size check.
func TestDecryptMalformedChunkSizeHeader(t *testing.T) {
	kek := newKEK(t)
	cek := newCEK(t)
	wrappedCEK, err := aeskw.Wrap(kek, cek)
	require.NoError(t, err)

	// Hand-build an envelope whose chunk-size header (FEE private-use label, in
	// the unprotected header) is present but holds a string instead of an integer.
	env := &cose.Envelope{
		Headers: cose.Headers{
			Protected: cose.Header{}.
				Set(cose.HeaderLabelAlg, algChunkedStream).
				Set(cose.HeaderLabelType, fee.EnvelopeType),
			Unprotected: cose.Header{}.
				Set(cose.HeaderLabelIV, make([]byte, aesstream.BaseNonceSize)).
				Set(labelChunkSize, "not-an-integer"),
		},
		Recipients: []*cose.Recipient{{
			Headers: cose.Headers{
				Protected:   cose.Header{}.Set(cose.HeaderLabelAlg, cose.AlgA256KW),
				Unprotected: cose.Header{}.Set(cose.HeaderLabelKID, a256kwKID),
			},
			Ciphertext: wrappedCEK,
		}},
	}
	blob, err := env.Encode()
	require.NoError(t, err)

	r, err := fee.Decrypt(bytes.NewReader(blob), fee.NewA256KWUnwrapper(a256kwKID, kek))
	require.Nil(t, r)
	require.ErrorIs(t, err, fee.ErrMalformedEnvelope)
}

// TestDecryptMalformedEphemeralKey confirms an ECDH-ES recipient whose ephemeral
// key is not a valid X25519 COSE_Key is rejected as a malformed envelope, before
// any ECDH is attempted. Each case hand-builds a recipient the unwrapper matches
// by kid and algorithm, so decode + match succeed and the ephemeral-key decode
// is what fails.
func TestDecryptMalformedEphemeralKey(t *testing.T) {
	key := newX25519Key(t)
	goodX := key.PublicKey().Bytes()

	cases := []struct {
		name string
		eph  any
	}{
		{"not a COSE_Key map", []byte("raw-ephemeral-bytes-not-a-map!!!")},
		{"wrong key type", map[any]any{coseKeyKty: int64(2), coseKeyCrv: coseCrvX25519, coseKeyX: goodX}},
		{"wrong curve", map[any]any{coseKeyKty: coseKtyOKP, coseKeyCrv: int64(6), coseKeyX: goodX}},
		{"missing x coordinate", map[any]any{coseKeyKty: coseKtyOKP, coseKeyCrv: coseCrvX25519}},
		{"x not a byte string", map[any]any{coseKeyKty: coseKtyOKP, coseKeyCrv: coseCrvX25519, coseKeyX: int64(7)}},
		{"x wrong length", map[any]any{coseKeyKty: coseKtyOKP, coseKeyCrv: coseCrvX25519, coseKeyX: []byte("short")}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			env := &cose.Envelope{
				Headers: cose.Headers{
					Protected: cose.Header{}.
						Set(cose.HeaderLabelAlg, algChunkedStream).
						Set(cose.HeaderLabelType, fee.EnvelopeType),
					Unprotected: cose.Header{}.
						Set(cose.HeaderLabelIV, make([]byte, aesstream.BaseNonceSize)).
						Set(labelChunkSize, int64(aesstream.MinChunkSize)),
				},
				Recipients: []*cose.Recipient{{
					Headers: cose.Headers{
						Protected: cose.Header{}.Set(cose.HeaderLabelAlg, cose.AlgECDHESA256KW),
						Unprotected: cose.Header{}.
							Set(cose.HeaderLabelKID, ecdhKID).
							Set(cose.HeaderLabelEphemeralKey, tc.eph),
					},
					Ciphertext: make([]byte, 40), // arbitrary; the unwrap is never reached
				}},
			}
			blob, err := env.Encode()
			require.NoError(t, err)

			r, err := fee.Decrypt(bytes.NewReader(blob), fee.NewECDHESUnwrapper(ecdhKID, key))
			require.Nil(t, r)
			require.ErrorIs(t, err, fee.ErrMalformedEnvelope)
		})
	}
}

// name renders a byte count for a subtest name.
func name(n int) string {
	if n == 0 {
		return "empty"
	}
	return "n=" + strconv.Itoa(n)
}
