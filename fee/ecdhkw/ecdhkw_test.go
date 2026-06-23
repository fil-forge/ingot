package ecdhkw

import (
	"bytes"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/fil-forge/ingot/fee/aeskw"
)

func newRecipient(t *testing.T) *ecdh.PrivateKey {
	t.Helper()
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate recipient key: %v", err)
	}
	return priv
}

// AC: "I can wrap a CEK to an X25519 public key and unwrap it with the
// corresponding private key, recovering the original CEK." Checked across the
// valid CEK sizes (16/24/32; FilOne uses 32).
func TestWrapUnwrapRoundTrip(t *testing.T) {
	recipient := newRecipient(t)
	for _, size := range []int{16, 24, 32} {
		cek := make([]byte, size)
		if _, err := rand.Read(cek); err != nil {
			t.Fatalf("rand cek: %v", err)
		}

		w, err := Wrap(recipient.PublicKey(), cek)
		if err != nil {
			t.Fatalf("Wrap(%d-byte CEK): %v", size, err)
		}
		if w.EphemeralPublicKey.Curve() != ecdh.X25519() {
			t.Fatalf("ephemeral key is not X25519")
		}
		if len(w.WrappedCEK) != size+8 {
			t.Fatalf("wrapped CEK length = %d, want %d", len(w.WrappedCEK), size+8)
		}

		got, err := Unwrap(recipient, w)
		if err != nil {
			t.Fatalf("Unwrap(%d-byte CEK): %v", size, err)
		}
		if !bytes.Equal(got, cek) {
			t.Fatalf("round-trip mismatch for %d-byte CEK:\n got %X\nwant %X", size, got, cek)
		}
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
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}

	got, err := Unwrap(wrongKey, w)
	if err == nil {
		t.Fatalf("Unwrap with wrong key succeeded, want error")
	}
	if !errors.Is(err, aeskw.ErrIntegrity) {
		t.Fatalf("Unwrap with wrong key: err = %v, want it to wrap aeskw.ErrIntegrity", err)
	}
	if got != nil {
		t.Fatalf("Unwrap with wrong key returned %X, want nil", got)
	}
}

// AC: "When I wrap the same CEK twice to the same public key, the two wrapped
// outputs differ — the ephemeral sender key is fresh each time." Both the
// ephemeral public key and the wrapped bytes must differ, and both copies must
// still unwrap to the original CEK.
func TestWrapFreshEphemeralPerCall(t *testing.T) {
	recipient := newRecipient(t)
	cek := bytes.Repeat([]byte{0xC3}, 32)

	first, err := Wrap(recipient.PublicKey(), cek)
	if err != nil {
		t.Fatalf("first Wrap: %v", err)
	}
	second, err := Wrap(recipient.PublicKey(), cek)
	if err != nil {
		t.Fatalf("second Wrap: %v", err)
	}

	if bytes.Equal(first.EphemeralPublicKey.Bytes(), second.EphemeralPublicKey.Bytes()) {
		t.Fatalf("ephemeral public keys are identical across wraps; expected fresh key each time")
	}
	if bytes.Equal(first.WrappedCEK, second.WrappedCEK) {
		t.Fatalf("wrapped CEKs are identical across wraps; expected different output")
	}

	// Both independently recover the same plaintext CEK.
	for i, w := range []*Wrapped{first, second} {
		got, err := Unwrap(recipient, w)
		if err != nil {
			t.Fatalf("Unwrap copy %d: %v", i, err)
		}
		if !bytes.Equal(got, cek) {
			t.Fatalf("Unwrap copy %d mismatch: got %X want %X", i, got, cek)
		}
	}
}

// A wrap is bound to its ephemeral key: swapping in a different ephemeral
// public key (even a valid one) breaks the derivation and fails the unwrap.
func TestUnwrapTamperedEphemeral(t *testing.T) {
	recipient := newRecipient(t)
	cek := bytes.Repeat([]byte{0x11}, 32)

	w, err := Wrap(recipient.PublicKey(), cek)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	other, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate other key: %v", err)
	}
	w.EphemeralPublicKey = other.PublicKey()

	if _, err := Unwrap(recipient, w); !errors.Is(err, aeskw.ErrIntegrity) {
		t.Fatalf("Unwrap with swapped ephemeral key: err = %v, want aeskw.ErrIntegrity", err)
	}
}

// A low-order ephemeral point would force the ECDH shared secret into a small
// subgroup; crypto/ecdh rejects it, and Unwrap must surface that as an error
// rather than deriving a KEK from a degenerate secret.
func TestUnwrapLowOrderEphemeral(t *testing.T) {
	recipient := newRecipient(t)
	// The all-zero u-coordinate is a classic X25519 low-order point. It is a
	// valid 32-byte public key to construct, but ECDH against it fails.
	lowOrder, err := ecdh.X25519().NewPublicKey(make([]byte, 32))
	if err != nil {
		t.Fatalf("construct low-order point: %v", err)
	}
	w := &Wrapped{EphemeralPublicKey: lowOrder, WrappedCEK: make([]byte, 40)}
	if _, err := Unwrap(recipient, w); err == nil {
		t.Fatalf("Unwrap with low-order ephemeral point succeeded, want error")
	}
}

// A wrap is bound to its wrapped bytes: flipping any bit fails the unwrap.
func TestUnwrapTamperedCEK(t *testing.T) {
	recipient := newRecipient(t)
	cek := bytes.Repeat([]byte{0x22}, 32)

	w, err := Wrap(recipient.PublicKey(), cek)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	for i := range w.WrappedCEK {
		tampered := &Wrapped{
			EphemeralPublicKey: w.EphemeralPublicKey,
			WrappedCEK:         bytes.Clone(w.WrappedCEK),
		}
		tampered.WrappedCEK[i] ^= 0x01
		if _, err := Unwrap(recipient, tampered); !errors.Is(err, aeskw.ErrIntegrity) {
			t.Fatalf("tampering wrapped byte %d not detected: err = %v", i, err)
		}
	}
}

func TestWrapInputValidation(t *testing.T) {
	recipient := newRecipient(t)
	p256, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate P256 key: %v", err)
	}

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
			if _, err := Wrap(tc.pub, tc.cek); err == nil {
				t.Fatalf("Wrap = nil error, want error")
			}
		})
	}
}

func TestUnwrapInputValidation(t *testing.T) {
	recipient := newRecipient(t)
	p256, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate P256 key: %v", err)
	}
	valid, err := Wrap(recipient.PublicKey(), make([]byte, 32))
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}

	t.Run("nil private key", func(t *testing.T) {
		if _, err := Unwrap(nil, valid); err == nil {
			t.Fatalf("want error")
		}
	})
	t.Run("nil wrapped", func(t *testing.T) {
		if _, err := Unwrap(recipient, nil); err == nil {
			t.Fatalf("want error")
		}
	})
	t.Run("nil ephemeral key", func(t *testing.T) {
		if _, err := Unwrap(recipient, &Wrapped{WrappedCEK: valid.WrappedCEK}); err == nil {
			t.Fatalf("want error")
		}
	})
	t.Run("ephemeral wrong curve", func(t *testing.T) {
		w := &Wrapped{EphemeralPublicKey: p256.PublicKey(), WrappedCEK: valid.WrappedCEK}
		if _, err := Unwrap(recipient, w); err == nil {
			t.Fatalf("want error")
		}
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
	if err != nil {
		t.Fatalf("recipient private key: %v", err)
	}
	ephemeral, err := ecdh.X25519().NewPrivateKey(ephPrivBytes)
	if err != nil {
		t.Fatalf("ephemeral private key: %v", err)
	}

	// The recipient public key derived from the fixed scalar is itself part of
	// the vector — record it so a cross-impl test starts from the same key.
	if got := hex.EncodeToString(priv.PublicKey().Bytes()); got != wantRecipientPubHex {
		t.Fatalf("recipient public key:\n got %s\nwant %s", got, wantRecipientPubHex)
	}

	w, err := wrapWithEphemeral(ephemeral, priv.PublicKey(), cek)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	if got := hex.EncodeToString(w.EphemeralPublicKey.Bytes()); got != wantEphemeralPubHex {
		t.Fatalf("ephemeral public key:\n got %s\nwant %s", got, wantEphemeralPubHex)
	}
	if got := hex.EncodeToString(w.WrappedCEK); got != wantWrappedCEKHex {
		t.Fatalf("wrapped CEK:\n got %s\nwant %s", got, wantWrappedCEKHex)
	}

	// The vector must also round-trip back to the original CEK.
	back, err := Unwrap(priv, w)
	if err != nil {
		t.Fatalf("unwrap: %v", err)
	}
	if !bytes.Equal(back, cek) {
		t.Fatalf("round-trip:\n got %X\nwant %X", back, cek)
	}

	// A fixed ephemeral key makes the wrap deterministic — wrapping again with
	// the same ephemeral key yields byte-identical output.
	again, err := wrapWithEphemeral(ephemeral, priv.PublicKey(), cek)
	if err != nil {
		t.Fatalf("second wrap: %v", err)
	}
	if !bytes.Equal(again.WrappedCEK, w.WrappedCEK) {
		t.Fatalf("wrap not deterministic for a fixed ephemeral key")
	}
}

func mustDecode(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}
