package tenantkey

import (
	"crypto/ecdh"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"testing"
)

// The RFC 7748 §6.1 X25519 public key ("Alice") and its Multikey form: the
// same vector hilt's wrap-key encoder pins, so the two ends cannot drift.
const (
	rfc7748AlicePub = "8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a"
	rfc7748AliceKID = "z6LSkdrX4EvewpktHBjvNxRDogPdC5iVF8LT3LPKefGAgi89"
)

func TestDecodePublicKey_Vector(t *testing.T) {
	pub, err := DecodePublicKey(rfc7748AliceKID)
	if err != nil {
		t.Fatalf("DecodePublicKey: %v", err)
	}
	if got := hex.EncodeToString(pub.Bytes()); got != rfc7748AlicePub {
		t.Fatalf("decoded key = %s, want %s", got, rfc7748AlicePub)
	}
	if got := EncodePublicKey(pub); got != rfc7748AliceKID {
		t.Fatalf("EncodePublicKey = %s, want %s", got, rfc7748AliceKID)
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	kid := EncodePublicKey(priv.PublicKey())
	back, err := DecodePublicKey(kid)
	if err != nil {
		t.Fatalf("DecodePublicKey(%s): %v", kid, err)
	}
	if !back.Equal(priv.PublicKey()) {
		t.Fatal("round trip changed the key")
	}
}

func TestDecodePublicKey_RejectsOtherKeyTypes(t *testing.T) {
	// An ed25519 did:key identifier (multicodec ed25519-pub 0xed).
	const ed25519 = "z6MkjFRxLLGdBqQSLkZbVnuwUFiomK8eGBkPtim9ETvP7vec"
	if _, err := DecodePublicKey(ed25519); !errors.Is(err, ErrNotX25519) {
		t.Fatalf("ed25519 key: err = %v, want ErrNotX25519", err)
	}
	if _, err := DecodePublicKey("not-multibase"); err == nil {
		t.Fatal("garbage decoded without error")
	}
	// Right codec, wrong length.
	if _, err := DecodePublicKey("z6LS"); !errors.Is(err, ErrNotX25519) && err == nil {
		t.Fatal("truncated key decoded without error")
	}
}
