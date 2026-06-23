package aeskw

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}

// rfc3394Vectors are the known-answer test vectors from RFC 3394 §4
// (https://www.rfc-editor.org/rfc/rfc3394#section-4). They exercise every KEK
// length (128/192/256) against 128/192/256-bit key data.
var rfc3394Vectors = []struct {
	name    string
	kek     string
	keyData string
	wrapped string
}{
	{
		name:    "4.1 wrap 128-bit data with 128-bit KEK",
		kek:     "000102030405060708090A0B0C0D0E0F",
		keyData: "00112233445566778899AABBCCDDEEFF",
		wrapped: "1FA68B0A8112B447AEF34BD8FB5A7B829D3E862371D2CFE5",
	},
	{
		name:    "4.2 wrap 128-bit data with 192-bit KEK",
		kek:     "000102030405060708090A0B0C0D0E0F1011121314151617",
		keyData: "00112233445566778899AABBCCDDEEFF",
		wrapped: "96778B25AE6CA435F92B5B97C050AED2468AB8A17AD84E5D",
	},
	{
		name:    "4.3 wrap 128-bit data with 256-bit KEK",
		kek:     "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F",
		keyData: "00112233445566778899AABBCCDDEEFF",
		wrapped: "64E8C3F9CE0F5BA263E9777905818A2A93C8191E7D6E8AE7",
	},
	{
		name:    "4.4 wrap 192-bit data with 192-bit KEK",
		kek:     "000102030405060708090A0B0C0D0E0F1011121314151617",
		keyData: "00112233445566778899AABBCCDDEEFF0001020304050607",
		wrapped: "031D33264E15D33268F24EC260743EDCE1C6C7DDEE725A936BA814915C6762D2",
	},
	{
		name:    "4.5 wrap 192-bit data with 256-bit KEK",
		kek:     "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F",
		keyData: "00112233445566778899AABBCCDDEEFF0001020304050607",
		wrapped: "A8F9BC1612C68B3FF6E6F4FBE30E71E4769C8B80A32CB8958CD5D17D6B254DA1",
	},
	{
		name:    "4.6 wrap 256-bit data with 256-bit KEK",
		kek:     "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F",
		keyData: "00112233445566778899AABBCCDDEEFF000102030405060708090A0B0C0D0E0F",
		wrapped: "28C9F404C4B810F4CBCCB35CFB87F8263F5786E2D80ED326CBC7F0E71A99F43BFB988B9B7A02DD21",
	},
}

func TestRFC3394Vectors(t *testing.T) {
	for _, v := range rfc3394Vectors {
		t.Run(v.name, func(t *testing.T) {
			kek := mustHex(t, v.kek)
			keyData := mustHex(t, v.keyData)
			want := mustHex(t, v.wrapped)

			got, err := Wrap(kek, keyData)
			if err != nil {
				t.Fatalf("Wrap: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("Wrap mismatch:\n got %X\nwant %X", got, want)
			}

			back, err := Unwrap(kek, want)
			if err != nil {
				t.Fatalf("Unwrap: %v", err)
			}
			if !bytes.Equal(back, keyData) {
				t.Fatalf("Unwrap mismatch:\n got %X\nwant %X", back, keyData)
			}
		})
	}
}

func TestRoundTrip256(t *testing.T) {
	kek := mustHex(t, "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F")
	cek := mustHex(t, "fedcba98765432100123456789abcdeffedcba98765432100123456789abcdef")

	wrapped, err := Wrap(kek, cek)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	if len(wrapped) != len(cek)+8 {
		t.Fatalf("wrapped length = %d, want %d", len(wrapped), len(cek)+8)
	}
	back, err := Unwrap(kek, wrapped)
	if err != nil {
		t.Fatalf("Unwrap: %v", err)
	}
	if !bytes.Equal(back, cek) {
		t.Fatalf("round-trip mismatch: got %X want %X", back, cek)
	}
}

func TestUnwrapWrongKEK(t *testing.T) {
	kek := mustHex(t, "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F")
	wrongKEK := mustHex(t, "ff0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F")
	cek := mustHex(t, "00112233445566778899AABBCCDDEEFF000102030405060708090A0B0C0D0E0F")

	wrapped, err := Wrap(kek, cek)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	got, err := Unwrap(wrongKEK, wrapped)
	if !errors.Is(err, ErrIntegrity) {
		t.Fatalf("Unwrap with wrong KEK: err = %v, want ErrIntegrity", err)
	}
	if got != nil {
		t.Fatalf("Unwrap with wrong KEK returned %X, want nil", got)
	}
}

func TestUnwrapTampered(t *testing.T) {
	kek := mustHex(t, "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F")
	cek := mustHex(t, "00112233445566778899AABBCCDDEEFF000102030405060708090A0B0C0D0E0F")

	wrapped, err := Wrap(kek, cek)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	// Flip a bit in each byte position in turn; every mutation must be caught.
	for i := range wrapped {
		tampered := bytes.Clone(wrapped)
		tampered[i] ^= 0x01
		if _, err := Unwrap(kek, tampered); !errors.Is(err, ErrIntegrity) {
			t.Fatalf("tampering byte %d not detected: err = %v", i, err)
		}
	}
}

func TestWrapInputValidation(t *testing.T) {
	goodKEK := make([]byte, 32)
	tests := []struct {
		name    string
		kek     []byte
		keyData []byte
	}{
		{"short KEK", make([]byte, 8), make([]byte, 16)},
		{"odd KEK", make([]byte, 31), make([]byte, 16)},
		{"key data too short", goodKEK, make([]byte, 8)},
		{"key data not block-aligned", goodKEK, make([]byte, 20)},
		{"empty key data", goodKEK, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Wrap(tc.kek, tc.keyData); err == nil {
				t.Fatalf("Wrap(%d-byte KEK, %d-byte data) = nil error, want error", len(tc.kek), len(tc.keyData))
			}
		})
	}
}

func TestUnwrapInputValidation(t *testing.T) {
	goodKEK := make([]byte, 32)
	tests := []struct {
		name    string
		kek     []byte
		wrapped []byte
	}{
		{"bad KEK", make([]byte, 8), make([]byte, 24)},
		{"wrapped too short", goodKEK, make([]byte, 16)},
		{"wrapped not block-aligned", goodKEK, make([]byte, 28)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Unwrap(tc.kek, tc.wrapped); err == nil {
				t.Fatalf("Unwrap = nil error, want error")
			}
		})
	}
}

// TestInputsNotMutated guards the documented promise that Wrap and Unwrap leave
// the caller's slices untouched (both copy into private working buffers).
func TestInputsNotMutated(t *testing.T) {
	kek := mustHex(t, "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F")
	cek := mustHex(t, "00112233445566778899AABBCCDDEEFF000102030405060708090A0B0C0D0E0F")
	cekCopy := bytes.Clone(cek)

	wrapped, err := Wrap(kek, cek)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	if !bytes.Equal(cek, cekCopy) {
		t.Fatalf("Wrap mutated its keyData argument")
	}

	wrappedCopy := bytes.Clone(wrapped)
	if _, err := Unwrap(kek, wrapped); err != nil {
		t.Fatalf("Unwrap: %v", err)
	}
	if !bytes.Equal(wrapped, wrappedCopy) {
		t.Fatalf("Unwrap mutated its wrapped argument")
	}
}
