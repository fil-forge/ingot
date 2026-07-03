package regionkey

import (
	"context"
	"errors"
	"fmt"
)

// KEKLen is the required length in bytes of a region KEK: 32 bytes (256 bits),
// i.e. A256KW.
const KEKLen = 32

// StaticKEKSource is an in-memory [KEKSource] holding exactly one region KEK for
// all scopes. It is the v1 "single region key" choice — the region-key
// cardinality decision (FIL-572) is deferred — and the in-memory single-key
// implementation the issue calls for. Swapping it for a Vault-backed or
// multi-key source changes nothing in [SoftwareProvider] or Ingot.
//
// The KEK is kept in its own locked buffer for the lifetime of the source;
// CurrentKEK and KEKAt hand out locked copies so that a caller destroying its
// copy after one operation never touches the retained key. Call [StaticKEKSource.Close]
// at shutdown to zero and unlock the retained key.
type StaticKEKSource struct {
	version KeyVersion
	kek     *KEK
}

var _ KEKSource = (*StaticKEKSource)(nil)

// NewStaticKEKSource builds a source holding kek under version. kek must be
// exactly [KEKLen] bytes and version must be non-empty. kek is copied into a
// locked buffer; the caller's slice is not retained and may be zeroed by the
// caller afterward.
func NewStaticKEKSource(version KeyVersion, kek []byte) (*StaticKEKSource, error) {
	if version == "" {
		return nil, errors.New("regionkey: key version must not be empty")
	}
	if len(kek) != KEKLen {
		return nil, fmt.Errorf("regionkey: region KEK must be %d bytes (A256KW), got %d", KEKLen, len(kek))
	}
	locked, err := NewKEK(kek)
	if err != nil {
		return nil, err
	}
	return &StaticKEKSource{version: version, kek: locked}, nil
}

// CurrentKEK implements [KEKSource.CurrentKEK]. Every scope resolves to the one
// retained key, since v1 holds a single region-wide KEK.
func (s *StaticKEKSource) CurrentKEK(ctx context.Context, scope Scope) (KeyVersion, *KEK, error) {
	copied, err := s.copyKEK()
	if err != nil {
		return "", nil, err
	}
	return s.version, copied, nil
}

// KEKAt implements [KEKSource.KEKAt]. It returns the retained key for the one
// version it holds and [ErrUnknownVersion] for any other — which is how an
// unwrap of data wrapped by a different region (a different key version) is
// reported, distinct from a same-version wrong-key unwrap that fails the
// A256KW integrity check.
func (s *StaticKEKSource) KEKAt(ctx context.Context, scope Scope, version KeyVersion) (*KEK, error) {
	if version != s.version {
		// The Provider wrapping this error already names the requested version.
		return nil, ErrUnknownVersion
	}
	return s.copyKEK()
}

// Close zeroes and unlocks the retained KEK and marks the source unusable; a
// subsequent CurrentKEK or KEKAt returns an error rather than handing out a
// wiped key. It is idempotent and nil-safe (a zero-value source Closes cleanly).
func (s *StaticKEKSource) Close() error {
	if s.kek != nil {
		s.kek.Destroy()
		s.kek = nil
	}
	return nil
}

// copyKEK returns a fresh locked KEK holding a copy of the retained key, for a
// caller to use in a single operation and then Destroy. It errors if the source
// has been closed (or was never initialized), so a use-after-Close cannot
// silently wrap under a zeroed key.
func (s *StaticKEKSource) copyKEK() (*KEK, error) {
	if s.kek == nil {
		return nil, errors.New("regionkey: KEK source is closed")
	}
	return NewKEK(s.kek.Bytes())
}
