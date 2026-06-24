package ecdhkw

import (
	"bytes"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/hex"
	"testing"

	"github.com/fil-forge/ingot/fee/aeskw"
	"github.com/stretchr/testify/require"
)

func newRecipient(t *testing.T) *ecdh.PrivateKey {
	t.Helper()
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	require.NoError(t, err, "generate recipient key")
	return priv
}

// AC: "I can wrap a CEK to an X25519 public key and unwrap it with the
// corresponding private key, recovering the original CEK." Checked across the
// valid CEK sizes (16/24/32; FilOne uses 32).
func TestWrapUnwrapRoundTrip(t *testing.T) {
	recipient := newRecipient(t)
	for _, size := range []int{16, 24, 32} {
		cek := make([]byte, size)
		_, err := rand.Read(cek)
		require.NoError(t, err, "rand cek")

		w, err := Wrap(recipient.PublicKey(), cek)
		require.NoErrorf(t, err, "Wrap(%d-byte CEK)", size)
		require.Equal(t, ecdh.X25519(), w.EphemeralPublicKey.Curve(), "ephemeral key is not X25519")
		require.Lenf(t, w.WrappedCEK, size+8, "wrapped CEK length")

		got, err := Unwrap(recipient, w)
		require.NoErrorf(t, err, "Unwrap(%d-byte CEK)", size)
		require.Equalf(t, cek, got, "round-trip mismatch for %d-byte CEK", size)
	}
}

// AC: "When I attempt to unwrap with the wrong private key, unwrap returns an
// error." The wrong key derives a different KEK, so AES-KW's integrity check
// fails; the error wraps aeskw.ErrIntegrity.
func TestUnwrapWrongPrivateKey(t *testing.T) {
	recipient := newRecipient(t)
	wrongKey := newRecipient(t)
	cek := bytes.Repeat([]byte{0x5A}, 32)

	w, err := Wrap(recipient.PublicKey(), cek)
	require.NoError(t, err, "Wrap")

	got, err := Unwrap(wrongKey, w)
	require.ErrorIs(t, err, aeskw.ErrIntegrity, "Unwrap with wrong key should wrap aeskw.ErrIntegrity")
	require.Nil(t, got)
}

// AC: "When I wrap the same CEK twice to the same public key, the two wrapped
// outputs differ — the ephemeral sender key is fresh each time." Both the
// ephemeral public key and the wrapped bytes must differ, and both copies must
// still unwrap to the original CEK.
func TestWrapFreshEphemeralPerCall(t *testing.T) {
	recipient := newRecipient(t)
	cek := bytes.Repeat([]byte{0xC3}, 32)

	first, err := Wrap(recipient.PublicKey(), cek)
	require.NoError(t, err, "first Wrap")
	second, err := Wrap(recipient.PublicKey(), cek)
	require.NoError(t, err, "second Wrap")

	require.NotEqual(t, first.EphemeralPublicKey.Bytes(), second.EphemeralPublicKey.Bytes(),
		"ephemeral public keys are identical across wraps; expected a fresh key each time")
	require.NotEqual(t, first.WrappedCEK, second.WrappedCEK,
		"wrapped CEKs are identical across wraps; expected different output")

	// Both independently recover the same plaintext CEK.
	for i, w := range []*Wrapped{first, second} {
		got, err := Unwrap(recipient, w)
		require.NoErrorf(t, err, "Unwrap copy %d", i)
		require.Equalf(t, cek, got, "Unwrap copy %d mismatch", i)
	}
}

// A wrap is bound to its ephemeral key: swapping in a different ephemeral
// public key (even a valid one) breaks the derivation and fails the unwrap.
func TestUnwrapTamperedEphemeral(t *testing.T) {
	recipient := newRecipient(t)
	cek := bytes.Repeat([]byte{0x11}, 32)

	w, err := Wrap(recipient.PublicKey(), cek)
	require.NoError(t, err, "Wrap")
	other, err := ecdh.X25519().GenerateKey(rand.Reader)
	require.NoError(t, err, "generate other key")
	w.EphemeralPublicKey = other.PublicKey()

	_, err = Unwrap(recipient, w)
	require.ErrorIs(t, err, aeskw.ErrIntegrity, "Unwrap with swapped ephemeral key")
}

// A low-order ephemeral point would force the ECDH shared secret into a small
// subgroup; crypto/ecdh rejects it, and Unwrap must surface that as an error
// rather than deriving a KEK from a degenerate secret.
func TestUnwrapLowOrderEphemeral(t *testing.T) {
	recipient := newRecipient(t)
	// The all-zero u-coordinate is a classic X25519 low-order point. It is a
	// valid 32-byte public key to construct, but ECDH against it fails.
	lowOrder, err := ecdh.X25519().NewPublicKey(make([]byte, 32))
	require.NoError(t, err, "construct low-order point")
	w := &Wrapped{EphemeralPublicKey: lowOrder, WrappedCEK: make([]byte, 40)}
	_, err = Unwrap(recipient, w)
	require.Error(t, err, "Unwrap with low-order ephemeral point should fail")
}

// A wrap is bound to its wrapped bytes: flipping any bit fails the unwrap.
func TestUnwrapTamperedCEK(t *testing.T) {
	recipient := newRecipient(t)
	cek := bytes.Repeat([]byte{0x22}, 32)

	w, err := Wrap(recipient.PublicKey(), cek)
	require.NoError(t, err, "Wrap")
	for i := range w.WrappedCEK {
		tampered := &Wrapped{
			EphemeralPublicKey: w.EphemeralPublicKey,
			WrappedCEK:         bytes.Clone(w.WrappedCEK),
		}
		tampered.WrappedCEK[i] ^= 0x01
		_, err = Unwrap(recipient, tampered)
		require.ErrorIsf(t, err, aeskw.ErrIntegrity, "tampering wrapped byte %d not detected", i)
	}
}

func TestWrapInputValidation(t *testing.T) {
	recipient := newRecipient(t)
	p256, err := ecdh.P256().GenerateKey(rand.Reader)
	require.NoError(t, err, "generate P256 key")

	tests := []struct {
		name string
		pub  *ecdh.PublicKey
		cek  []byte
	}{
		{"nil public key", nil, make([]byte, 32)},
		{"wrong curve", p256.PublicKey(), make([]byte, 32)},
		{"cek too short", recipient.PublicKey(), make([]byte, 8)},
		{"cek not block-aligned", recipient.PublicKey(), make([]byte, 20)},
		{"nil cek", recipient.PublicKey(), nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Wrap(tc.pub, tc.cek)
			require.Error(t, err)
		})
	}
}

func TestUnwrapInputValidation(t *testing.T) {
	recipient := newRecipient(t)
	p256, err := ecdh.P256().GenerateKey(rand.Reader)
	require.NoError(t, err, "generate P256 key")
	valid, err := Wrap(recipient.PublicKey(), make([]byte, 32))
	require.NoError(t, err, "Wrap")

	t.Run("nil private key", func(t *testing.T) {
		_, err := Unwrap(nil, valid)
		require.Error(t, err)
	})
	t.Run("nil wrapped", func(t *testing.T) {
		_, err := Unwrap(recipient, nil)
		require.Error(t, err)
	})
	t.Run("nil ephemeral key", func(t *testing.T) {
		_, err := Unwrap(recipient, &Wrapped{WrappedCEK: valid.WrappedCEK})
		require.Error(t, err)
	})
	t.Run("ephemeral wrong curve", func(t *testing.T) {
		w := &Wrapped{EphemeralPublicKey: p256.PublicKey(), WrappedCEK: valid.WrappedCEK}
		_, err := Unwrap(recipient, w)
		require.Error(t, err)
	})
}

// TestKnownAnswerVector pins the full ECDH-ES+A256KW construction to fixed
// bytes. With a fixed recipient private key, a fixed ephemeral private key
// (injected through wrapWithEphemeral), and a fixed CEK, the ephemeral public
// key and wrapped CEK are fully determined. This is the gold vector for the
// cross-implementation tests (foc-encryption / FIL-473): any other
// implementation of the same scheme must reproduce these bytes, and any change
// to our KDF context, curve handling, or key wrap will break this test.
//
// All values use the standard RFC 9053 §5.2 context (AlgorithmID = A256KW,
// keyDataLength = 256, empty PartyU/PartyV/protected) documented in kdf.go.
func TestKnownAnswerVector(t *testing.T) {
	const (
		recipientPrivHex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
		ephemeralSeedHex = "a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
		cekHex           = "00112233445566778899aabbccddeeff000102030405060708090a0b0c0d0e0f"

		wantRecipientPubHex = "07a37cbc142093c8b755dc1b10e86cb426374ad16aa853ed0bdfc0b2b86d1c7c"
		wantEphemeralPubHex = "605a725d2a4adfeeb1a29e17edd621c1b7593ee8cdbc44ac6c4ab6e2f805d23c"
		wantWrappedCEKHex   = "1433cdb050fc4ab1ccb616c395a81908c001fcfa7fb865366a3ef8db0af45c7c4305ae7de7080007"
	)

	recipPrivBytes := mustDecode(t, recipientPrivHex)
	ephPrivBytes := mustDecode(t, ephemeralSeedHex)
	cek := mustDecode(t, cekHex)

	priv, err := ecdh.X25519().NewPrivateKey(recipPrivBytes)
	require.NoError(t, err, "recipient private key")
	ephemeral, err := ecdh.X25519().NewPrivateKey(ephPrivBytes)
	require.NoError(t, err, "ephemeral private key")

	// The recipient public key derived from the fixed scalar is itself part of
	// the vector — record it so a cross-impl test starts from the same key.
	require.Equal(t, wantRecipientPubHex, hex.EncodeToString(priv.PublicKey().Bytes()), "recipient public key")

	w, err := wrapWithEphemeral(ephemeral, priv.PublicKey(), cek)
	require.NoError(t, err, "wrap")
	require.Equal(t, wantEphemeralPubHex, hex.EncodeToString(w.EphemeralPublicKey.Bytes()), "ephemeral public key")
	require.Equal(t, wantWrappedCEKHex, hex.EncodeToString(w.WrappedCEK), "wrapped CEK")

	// The vector must also round-trip back to the original CEK.
	back, err := Unwrap(priv, w)
	require.NoError(t, err, "unwrap")
	require.Equal(t, cek, back, "round-trip")

	// A fixed ephemeral key makes the wrap deterministic — wrapping again with
	// the same ephemeral key yields byte-identical output.
	again, err := wrapWithEphemeral(ephemeral, priv.PublicKey(), cek)
	require.NoError(t, err, "second wrap")
	require.Equal(t, w.WrappedCEK, again.WrappedCEK, "wrap not deterministic for a fixed ephemeral key")
}

func mustDecode(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoErrorf(t, err, "bad hex %q", s)
	return b
}
