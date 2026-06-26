package ecdhkw

import (
	"crypto/sha256"
)

// This file implements the key derivation half of ECDH-ES: turning the raw
// X25519 shared secret into the A256KW key-encryption key. Two pieces are
// involved, both pinned here so the byte layout is unambiguous for the
// cross-implementation test vectors (foc-encryption / FIL-473):
//
//  1. concatKDF — the SHA-256 single-step KDF of NIST SP 800-56A §5.8.1, which
//     RFC 9053 §5.1 adopts for COSE ECDH key agreement.
//  2. kdfContext — the COSE_KDF_Context structure of RFC 9053 §5.2, CBOR-encoded
//     and fed to the KDF as the "other info" that binds the derived key to its
//     algorithm and length.
//
// The CBOR is hand-written rather than pulled from a codec: the structure is a
// fixed four-element array of small integers and two short byte strings, so a
// dozen lines of deterministic, shortest-form encoding is clearer and lighter
// than a dependency, and it leaves no room for a codec to pick a non-canonical
// encoding that would diverge from another implementation.

// concatKDF derives keyLen bytes from the shared secret z and the COSE_KDF
// context otherInfo, using the SHA-256 single-step KDF (NIST SP 800-56A
// §5.8.1): the concatenation of a big-endian 32-bit counter, z, and otherInfo
// is hashed once per output block, and the blocks are concatenated and
// truncated to keyLen.
//
// For A256KW the output is 32 bytes and SHA-256 produces 32, so exactly one
// round runs; the loop is written generally so the derivation is correct for
// any output length.
func concatKDF(z, otherInfo []byte, keyLen int) []byte {
	out := make([]byte, 0, ((keyLen+sha256.Size-1)/sha256.Size)*sha256.Size)
	h := sha256.New()
	var counter [4]byte
	for i := uint32(1); len(out) < keyLen; i++ {
		counter[0] = byte(i >> 24)
		counter[1] = byte(i >> 16)
		counter[2] = byte(i >> 8)
		counter[3] = byte(i)

		h.Reset()
		h.Write(counter[:])
		h.Write(z)
		h.Write(otherInfo)
		out = h.Sum(out)
	}
	return out[:keyLen]
}

// kdfContext builds the CBOR-encoded COSE_KDF_Context (RFC 9053 §5.2) for a
// key-agreement-with-key-wrap derivation:
//
//	[ algID, PartyUInfo, PartyVInfo, [ keyDataLenBits, protected ] ]
//
// where PartyUInfo and PartyVInfo are each the empty PartyInfo [nil, nil, nil]
// (no identity, nonce, or other data exchanged at this layer), and SuppPubInfo
// carries the derived-key length in bits plus the protected-header byte string.
// The optional SuppPrivInfo trailer is omitted.
//
// The wrap layer has no COSE protected header of its own, so callers pass a
// zero-length protected slice; algID is the COSE identifier of the algorithm
// the derived key feeds (A256KW), which binds the key to its purpose.
func kdfContext(algID int64, keyDataLenBits uint64, protected []byte) []byte {
	var b []byte
	b = cborHead(b, cborArray, 4) // [algID, PartyU, PartyV, SuppPubInfo]
	b = cborInt(b, algID)

	// PartyUInfo and PartyVInfo: the empty PartyInfo, three null slots.
	for k := 0; k < 2; k++ {
		b = cborHead(b, cborArray, 3)
		b = append(b, cborNull, cborNull, cborNull)
	}

	// SuppPubInfo: [keyDataLength (bits), protected].
	b = cborHead(b, cborArray, 2)
	b = cborHead(b, cborUint, keyDataLenBits)
	b = cborHead(b, cborBytes, uint64(len(protected)))
	b = append(b, protected...)
	return b
}

// CBOR major types (high three bits of the initial byte) and the simple value
// used here. Only the subset the context needs is defined.
const (
	cborUint  byte = 0 // major type 0: unsigned integer
	cborNint  byte = 1 // major type 1: negative integer
	cborBytes byte = 2 // major type 2: byte string
	cborArray byte = 4 // major type 4: array
	cborNull  byte = 0xf6
)

// cborHead appends a CBOR head: the major type in the high three bits followed
// by arg, encoded in the shortest form (canonical / deterministic encoding).
func cborHead(b []byte, major byte, arg uint64) []byte {
	m := major << 5
	switch {
	case arg < 24:
		return append(b, m|byte(arg))
	case arg < 1<<8:
		return append(b, m|24, byte(arg))
	case arg < 1<<16:
		return append(b, m|25, byte(arg>>8), byte(arg))
	case arg < 1<<32:
		return append(b, m|26, byte(arg>>24), byte(arg>>16), byte(arg>>8), byte(arg))
	default:
		return append(b, m|27,
			byte(arg>>56), byte(arg>>48), byte(arg>>40), byte(arg>>32),
			byte(arg>>24), byte(arg>>16), byte(arg>>8), byte(arg))
	}
}

// cborInt appends a CBOR integer, choosing the unsigned (major 0) or negative
// (major 1) encoding. A negative n is stored as the argument -1-n.
func cborInt(b []byte, n int64) []byte {
	if n < 0 {
		return cborHead(b, cborNint, uint64(-1-n))
	}
	return cborHead(b, cborUint, uint64(n))
}
