// Package aeskw implements the AES Key Wrap algorithm (AES-KW) from RFC 3394.
//
// AES-KW encrypts key material — itself a symmetric key, the "key data" — under
// a key-encryption key (KEK) using only the AES block cipher. It needs no nonce
// and no associated data, so the output is a deterministic function of the KEK
// and the key data, and it carries a built-in integrity check: Unwrap recovers
// a fixed initial value and rejects the input if that value is wrong, which is
// what happens when the KEK is wrong or the wrapped bytes were tampered with.
//
// The KEK length selects the COSE/JOSE variant: a 32-byte KEK is "A256KW". RFC
// 3394 wraps key data that is a whole number of 64-bit blocks, at least two
// (16 bytes); that range covers AES-128/192/256 content keys.
//
// This package implements only the base RFC 3394 mode, which uses the fixed
// default initial value of §2.2.3.1 and so requires the key data to be a whole
// number of 64-bit blocks. RFC 3394 §2.2.3.2 already anticipates "Alternative
// Initial Values" for cases the default does not cover — explicitly including
// key data that "may not always be a multiple of 64 bits" — but leaves their
// concrete definition to future publications. RFC 5649 is one such: it defines
// an alternative initial value carrying a message-length indicator, yielding a
// padded key wrap for key data of any length. This package implements neither
// the alternative initial value nor RFC 5649 padding.
//
// References:
//   - RFC 3394 (AES Key Wrap): https://www.rfc-editor.org/rfc/rfc3394
//   - RFC 5649 (AES Key Wrap with Padding): https://www.rfc-editor.org/rfc/rfc5649
//
// This is a shared primitive. The tenant-recipient wrap (ECDH-ES+A256KW, in
// the sibling fee/ecdhkw package) derives its KEK ephemerally per message and
// feeds it here; the region wrap takes a KEK straight from a key provider.
// Both paths call Wrap and Unwrap identically.
package aeskw

import (
	"crypto/aes"
	"crypto/subtle"
	"errors"
	"fmt"
)

// defaultIV is the RFC 3394 §2.2.3.1 default initial value
// (https://www.rfc-editor.org/rfc/rfc3394#section-2.2.3.1): a 64-bit constant
// prepended to the key data before wrapping. Unwrap recovers these bytes from
// the leading block and compares them against this constant to detect an
// incorrect KEK or a corrupted wrapped key. It is the only initial value this
// package supports (see the package doc on RFC 3394 §2.2.3.2 alternative IVs).
var defaultIV = [8]byte{0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6}

// rounds is the fixed number of passes the RFC 3394 algorithm makes over the
// key data — six, in both the wrap (§2.2.1) and unwrap (§2.2.2) directions.
const rounds = 6

// ErrIntegrity is returned by Unwrap when the recovered integrity value does
// not match the RFC 3394 initial value. It signals a wrong KEK or a tampered
// wrapped key; the (meaningless) recovered bytes are never returned alongside
// it. Callers can match it with errors.Is.
var ErrIntegrity = errors.New("aeskw: integrity check failed")

// Wrap encrypts keyData under kek with AES Key Wrap (RFC 3394) and returns the
// wrapped key, which is exactly 8 bytes longer than keyData. The caller's
// keyData slice is not modified.
//
// kek must be a valid AES key length — 16, 24, or 32 bytes; 32 bytes is A256KW.
// keyData must be a positive multiple of 8 bytes and at least 16 bytes.
func Wrap(kek, keyData []byte) ([]byte, error) {
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, fmt.Errorf("aeskw: invalid KEK: %w", err)
	}
	if len(keyData) < 16 || len(keyData)%8 != 0 {
		return nil, fmt.Errorf("aeskw: key data must be a multiple of 8 bytes and at least 16, got %d", len(keyData))
	}

	n := len(keyData) / 8

	// a is the 64-bit integrity register; r holds the n data blocks, mutated
	// in place across the six rounds. Neither aliases the caller's keyData.
	var a [8]byte
	copy(a[:], defaultIV[:])
	r := make([]byte, len(keyData))
	copy(r, keyData)

	// RFC 3394 performs 6n block encryptions in total (the spec writes this as
	// the single index t = 1..s, s = 6n). The nested loop is the equivalent
	// form: rounds (6) passes over all n blocks, with t = n*j + i.
	var buf [16]byte // AES input/output block: A || R[i]
	for j := 0; j < rounds; j++ {
		for i := 1; i <= n; i++ {
			copy(buf[:8], a[:])
			copy(buf[8:], r[(i-1)*8:i*8])
			block.Encrypt(buf[:], buf[:])

			// A = MSB(64, B) XOR t, where t = n*j + i; R[i] = LSB(64, B).
			copy(a[:], buf[:8])
			xorT(a[:], uint64(n*j+i))
			copy(r[(i-1)*8:i*8], buf[8:])
		}
	}

	out := make([]byte, 8+len(keyData))
	copy(out[:8], a[:])
	copy(out[8:], r)
	return out, nil
}

// Unwrap reverses Wrap: it decrypts wrapped under kek and returns the original
// key data, which is exactly 8 bytes shorter than wrapped. It returns
// ErrIntegrity if the integrity check fails (a wrong KEK or corrupted input).
// The caller's wrapped slice is not modified.
func Unwrap(kek, wrapped []byte) ([]byte, error) {
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, fmt.Errorf("aeskw: invalid KEK: %w", err)
	}
	if len(wrapped) < 24 || len(wrapped)%8 != 0 {
		return nil, fmt.Errorf("aeskw wrapped key must be a multiple of 8 bytes and at least 24, got %d", len(wrapped))
	}

	// n counts data blocks; the wrapped key is the integrity block plus n.
	n := len(wrapped)/8 - 1

	var a [8]byte
	copy(a[:], wrapped[:8])
	r := make([]byte, len(wrapped)-8)
	copy(r, wrapped[8:])

	var buf [16]byte
	for j := rounds - 1; j >= 0; j-- {
		for i := n; i >= 1; i-- {
			// B = AES^-1(K, (A XOR t) || R[i]), where t = n*j + i.
			copy(buf[:8], a[:])
			xorT(buf[:8], uint64(n*j+i))
			copy(buf[8:], r[(i-1)*8:i*8])
			block.Decrypt(buf[:], buf[:])

			copy(a[:], buf[:8])
			copy(r[(i-1)*8:i*8], buf[8:])
		}
	}

	// Constant-time compare so a wrong KEK can't be distinguished by timing.
	if subtle.ConstantTimeCompare(a[:], defaultIV[:]) != 1 {
		return nil, ErrIntegrity
	}
	return r, nil
}

// xorT XORs the 64-bit big-endian round counter t into the first eight bytes
// of b in place.
func xorT(b []byte, t uint64) {
	b[0] ^= byte(t >> 56)
	b[1] ^= byte(t >> 48)
	b[2] ^= byte(t >> 40)
	b[3] ^= byte(t >> 32)
	b[4] ^= byte(t >> 24)
	b[5] ^= byte(t >> 16)
	b[6] ^= byte(t >> 8)
	b[7] ^= byte(t)
}
