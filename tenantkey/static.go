package tenantkey

import (
	"context"
	"crypto/ecdh"
)

// Static is a Source that always returns one wrap key, whatever the request:
// the harness for tests and single-tenant development stacks.
type Static struct {
	KID string
	Pub *ecdh.PublicKey
}

var _ Source = (*Static)(nil)

// NewStatic returns a Static source for pub, with the canonical fingerprint
// as its kid.
func NewStatic(pub *ecdh.PublicKey) *Static {
	return &Static{KID: EncodePublicKey(pub), Pub: pub}
}

// WrapKey implements Source.
func (s *Static) WrapKey(context.Context) (string, *ecdh.PublicKey, error) {
	return s.KID, s.Pub, nil
}
