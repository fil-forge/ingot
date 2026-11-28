package tenantkey

import (
	"crypto/ecdh"
	"crypto/rand"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// The RFC 7748 §6.1 X25519 public key ("Alice") and its Multikey form: the
// same vector hilt's wrap-key encoder pins, so the two ends cannot drift.
const (
	rfc7748AlicePub = "8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a"
	rfc7748AliceKID = "z6LSkdrX4EvewpktHBjvNxRDogPdC5iVF8LT3LPKefGAgi89"
)

func TestDecodePublicKey_Vector(t *testing.T) {
	pub, err := DecodePublicKey(rfc7748AliceKID)
	require.NoError(t, err, "DecodePublicKey")
	got := hex.EncodeToString(pub.Bytes())
	require.Equal(t, rfc7748AlicePub, got, "decoded key")
	require.Equal(t, rfc7748AliceKID, EncodePublicKey(pub), "EncodePublicKey")
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	require.NoError(t, err)
	kid := EncodePublicKey(priv.PublicKey())
	back, err := DecodePublicKey(kid)
	require.NoError(t, err, "DecodePublicKey(%s)", kid)
	require.True(t, back.Equal(priv.PublicKey()), "round trip changed the key")
}

func TestDecodePublicKey_RejectsOtherKeyTypes(t *testing.T) {
	// An ed25519 did:key identifier (multicodec ed25519-pub 0xed).
	const ed25519 = "z6MkjFRxLLGdBqQSLkZbVnuwUFiomK8eGBkPtim9ETvP7vec"
	_, err := DecodePublicKey(ed25519)
	require.ErrorIs(t, err, ErrNotX25519, "ed25519 key")
	_, err = DecodePublicKey("not-multibase")
	require.Error(t, err, "garbage decoded without error")
	// Right codec, wrong length.
	_, err = DecodePublicKey("z6LS")
	require.Error(t, err, "truncated key decoded without error")
}
