package tenantkey

import (
	"crypto/ecdh"
	"fmt"

	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
)

// DecodePublicKey decodes an X25519 public key from its Multikey form: the
// multibase encoding of the x25519-pub multicodec varint followed by the 32
// raw key bytes (the same string that identifies the key's did:key). Any
// other key type is ErrNotX25519.
func DecodePublicKey(multikey string) (*ecdh.PublicKey, error) {
	_, data, err := multibase.Decode(multikey)
	if err != nil {
		return nil, fmt.Errorf("tenantkey: decode multibase: %w", err)
	}
	code, n, err := varint.FromUvarint(data)
	if err != nil {
		return nil, fmt.Errorf("tenantkey: read multicodec: %w", err)
	}
	if multicodec.Code(code) != multicodec.X25519Pub {
		return nil, fmt.Errorf("%w: multicodec %s", ErrNotX25519, multicodec.Code(code))
	}
	pub, err := ecdh.X25519().NewPublicKey(data[n:])
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotX25519, err)
	}
	return pub, nil
}

// EncodePublicKey is the inverse of DecodePublicKey: the canonical Multikey
// fingerprint of pub (base58btc), which is the recipient kid.
func EncodePublicKey(pub *ecdh.PublicKey) string {
	raw := pub.Bytes()
	n := varint.UvarintSize(uint64(multicodec.X25519Pub))
	tagged := make([]byte, n+len(raw))
	varint.PutUvarint(tagged, uint64(multicodec.X25519Pub))
	copy(tagged[n:], raw)
	s, err := multibase.Encode(multibase.Base58BTC, tagged)
	if err != nil {
		// Base58BTC is a registered encoding; Encode fails only on an unknown one.
		panic("tenantkey: base58btc encode: " + err.Error())
	}
	return s
}
