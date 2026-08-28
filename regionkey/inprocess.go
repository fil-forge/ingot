package regionkey

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
)

// KEKLen is the required length in bytes of a region KEK: 32 bytes (AES-256).
const KEKLen = 32

// gcmNonceLen is the standard 12-byte AES-GCM nonce length. A wrap generates
// a fresh random nonce and carries it as the first bytes of the wrapped
// ciphertext.
const gcmNonceLen = 12

// InProcessProvider is the in-process region [Provider] for tests and
// development: AES-256-GCM under a per-binding subkey, mirroring the
// semantics of the production secrets-manager wrap (OpenBao transit
// aes256-gcm96 with derived=true, context bound to (space, blob digest)) so
// code exercised against it sees the same properties — most importantly that
// a wrap authenticates only against its own binding. It holds raw KEK bytes in
// ordinary process memory, which a production provider never does: per the
// regional security RFC, real key custody belongs to the secrets manager,
// which implements [Provider] directly and never exports a key. That is also
// why there is no key-custody seam here — this provider is the only
// implementation that would ever consume one.
//
// The provider keeps every key version it has ever held (archive-don't-
// destroy): [InProcessProvider.Rotate] makes a new version current for new
// wraps while existing wraps keep unwrapping under theirs — the encryption
// RFC's rotation-readiness groundwork. Every binding resolves to the single
// current key; the region-key cardinality decision (FIL-572) is deferred.
type InProcessProvider struct {
	mu      sync.RWMutex
	current KeyVersion
	keys    map[KeyVersion][]byte
}

var _ Provider = (*InProcessProvider)(nil)

// NewInProcessProvider returns a provider holding kek as the current region
// key under version. kek must be exactly [KEKLen] bytes and version must be
// non-empty. kek is copied; the caller's slice is not retained.
func NewInProcessProvider(version KeyVersion, kek []byte) (*InProcessProvider, error) {
	if err := validateKey(version, kek); err != nil {
		return nil, err
	}
	return &InProcessProvider{
		current: version,
		keys:    map[KeyVersion][]byte{version: bytes.Clone(kek)},
	}, nil
}

// Rotate makes kek, under version, the current key for new wraps and archives
// the previous version so existing wraps still unwrap — the archive-don't-
// destroy rotation the RFC's groundwork calls for. It rejects a version the
// provider already holds, and the same invalid inputs the constructor does.
func (p *InProcessProvider) Rotate(version KeyVersion, kek []byte) error {
	if err := validateKey(version, kek); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.keys[version]; ok {
		return fmt.Errorf("regionkey: key version %q already exists", version)
	}
	p.keys[version] = bytes.Clone(kek)
	p.current = version
	return nil
}

// validateKey enforces the constructor/Rotate input contract: a non-empty
// version naming a KEKLen-byte key.
func validateKey(version KeyVersion, kek []byte) error {
	if version == "" {
		return errors.New("regionkey: key version must not be empty")
	}
	if len(kek) != KEKLen {
		return fmt.Errorf("regionkey: region KEK must be %d bytes (AES-256), got %d", KEKLen, len(kek))
	}
	return nil
}

// Wrap implements [Provider.Wrap]: it seals cek with AES-256-GCM under the
// binding-derived subkey of the current region KEK, prepends the fresh random
// nonce, and tags the result with the KEK's version.
func (p *InProcessProvider) Wrap(_ context.Context, binding BindingContext, cek []byte) (WrappedKey, error) {
	p.mu.RLock()
	version := p.current
	kek := p.keys[version]
	p.mu.RUnlock()

	aead, err := boundAEAD(kek, binding)
	if err != nil {
		return WrappedKey{}, err
	}
	nonce := make([]byte, gcmNonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return WrappedKey{}, fmt.Errorf("regionkey: generating nonce: %w", err)
	}
	return WrappedKey{Version: version, Ciphertext: aead.Seal(nonce, nonce, cek, nil)}, nil
}

// Unwrap implements [Provider.Unwrap]: it derives the same binding-bound
// subkey from the KEK named by wrapped.Version and opens the ciphertext. A
// wrong key, a wrong binding, and tampered bytes all fail authentication and
// surface as an error wrapping [ErrAuthentication]; a version the provider
// does not hold surfaces as [ErrUnknownVersion].
func (p *InProcessProvider) Unwrap(_ context.Context, binding BindingContext, wrapped WrappedKey) ([]byte, error) {
	p.mu.RLock()
	kek, ok := p.keys[wrapped.Version]
	p.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("regionkey: fetching KEK version %q: %w", wrapped.Version, ErrUnknownVersion)
	}

	aead, err := boundAEAD(kek, binding)
	if err != nil {
		return nil, err
	}
	if len(wrapped.Ciphertext) < gcmNonceLen {
		return nil, fmt.Errorf("regionkey: wrapped CEK shorter than a nonce: %w", ErrAuthentication)
	}
	nonce, sealed := wrapped.Ciphertext[:gcmNonceLen], wrapped.Ciphertext[gcmNonceLen:]
	cek, err := aead.Open(nil, nonce, sealed, nil)
	if err != nil {
		return nil, fmt.Errorf("regionkey: unwrapping CEK: %w", ErrAuthentication)
	}
	return cek, nil
}

// boundAEAD builds the AES-256-GCM AEAD for one (KEK, binding) pair. The
// subkey is HKDF-SHA256(kek, info=binding), so every binding gets its own GCM
// key — the software analogue of OpenBao transit's derived=true context
// binding, and what makes a wrong-binding unwrap an authentication failure.
// The KEK length check restates the constructor/Rotate invariant as a cheap
// guard.
func boundAEAD(kek []byte, binding BindingContext) (cipher.AEAD, error) {
	if len(kek) != KEKLen {
		return nil, fmt.Errorf("regionkey: KEK is %d bytes, want %d", len(kek), KEKLen)
	}
	subkey, err := hkdf.Key(sha256.New, kek, nil, string(bindingBytes(binding)), KEKLen)
	if err != nil {
		return nil, fmt.Errorf("regionkey: deriving binding subkey: %w", err)
	}
	block, err := aes.NewCipher(subkey)
	if err != nil {
		return nil, fmt.Errorf("regionkey: building cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("regionkey: building GCM: %w", err)
	}
	return aead, nil
}
