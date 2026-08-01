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
// The RFC 3394 wrap and unwrap themselves are delegated to go-jose's josecipher
// (a widely used implementation; RFC 7518 §4.4), which uses the same §2.2.3.1
// default IV. This package contributes the input validation, the ErrIntegrity
// sentinel, and a stable API over it.
//
// This is a shared primitive. The ECDH-ES+A256KW wrap (in the sibling
// fee/ecdhkw package) derives its KEK ephemerally per message and feeds it
// here; a direct A256KW recipient takes a KEK straight from a key provider.
// Both paths call Wrap and Unwrap identically.
package aeskw

import (
	"crypto/aes"
	"errors"
	"fmt"

	josecipher "github.com/go-jose/go-jose/v4/cipher"
)

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
	wrapped, err := josecipher.KeyWrap(block, keyData)
	if err != nil {
		return nil, fmt.Errorf("aeskw: %w", err)
	}
	return wrapped, nil
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
	keyData, err := josecipher.KeyUnwrap(block, wrapped)
	if err != nil {
		// After the length checks above, the only failure josecipher.KeyUnwrap
		// can report is the RFC 3394 integrity check (a wrong KEK or tampered
		// input); surface it as this package's sentinel.
		return nil, ErrIntegrity
	}
	return keyData, nil
}
