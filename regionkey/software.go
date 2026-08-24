package regionkey

import (
	"context"
	"fmt"

	"github.com/filecoin-project/go-fee/aeskw"
)

// SoftwareProvider is the v1 region [Provider]: it performs A256KW in process,
// importing the KEK from a [KEKSource] into a locked [KEK] buffer only for the
// duration of a single wrap or unwrap and zeroing it immediately afterward. It
// is the software stand-in for a future non-exportable PKCS#11/HSM provider,
// necessary because A256KW is an HSM/PKCS#11 primitive that Vault Transit and
// most software KMS backends do not offer natively.
//
// How many physical region KEKs exist is entirely the KEKSource's business, so
// swapping single-key custody for a per-bucket or per-tenant source (FIL-572)
// needs no change here or in Ingot.
type SoftwareProvider struct {
	source KEKSource
}

var _ Provider = (*SoftwareProvider)(nil)

// NewSoftwareProvider returns a SoftwareProvider that draws region KEKs from
// source.
func NewSoftwareProvider(source KEKSource) *SoftwareProvider {
	return &SoftwareProvider{source: source}
}

// Wrap implements [Provider.Wrap]. It fetches the current region KEK for scope,
// wraps cek under it with A256KW, and tags the result with the KEK's version.
// The KEK is held in a locked buffer only for this call and zeroed before Wrap
// returns, on both the success and error paths.
func (p *SoftwareProvider) Wrap(ctx context.Context, scope Scope, cek []byte) (WrappedKey, error) {
	if p.source == nil {
		return WrappedKey{}, fmt.Errorf("regionkey: provider has no KEK source")
	}
	version, kek, err := p.source.CurrentKEK(ctx, scope)
	if err != nil {
		return WrappedKey{}, fmt.Errorf("regionkey: fetching current KEK: %w", err)
	}
	// A well-behaved source returns a non-nil KEK on the no-error path; guard
	// against a misbehaving custom KEKSource so the deferred Destroy below
	// cannot nil-panic.
	if kek == nil {
		return WrappedKey{}, fmt.Errorf("regionkey: KEK source returned a nil KEK")
	}
	// Zero and unlock the imported KEK the instant this call returns, whatever
	// the outcome below.
	defer kek.Destroy()

	if version == "" {
		return WrappedKey{}, fmt.Errorf("regionkey: KEK source returned an empty key version")
	}

	wrapped, err := aeskw.Wrap(kek.Bytes(), cek)
	if err != nil {
		return WrappedKey{}, fmt.Errorf("regionkey: wrapping CEK: %w", err)
	}
	return WrappedKey{Version: version, Ciphertext: wrapped}, nil
}

// Unwrap implements [Provider.Unwrap]. It fetches the region KEK named by
// wrapped.Version for scope and A256KW-unwraps the CEK under it. As in Wrap the
// KEK is locked for the call only and zeroed before returning. A wrong-key or
// tampered wrap surfaces as an error wrapping fee/aeskw.ErrIntegrity; a missing
// version surfaces as [ErrUnknownVersion].
func (p *SoftwareProvider) Unwrap(ctx context.Context, scope Scope, wrapped WrappedKey) ([]byte, error) {
	if p.source == nil {
		return nil, fmt.Errorf("regionkey: provider has no KEK source")
	}
	kek, err := p.source.KEKAt(ctx, scope, wrapped.Version)
	if err != nil {
		return nil, fmt.Errorf("regionkey: fetching KEK version %q: %w", wrapped.Version, err)
	}
	if kek == nil {
		return nil, fmt.Errorf("regionkey: KEK source returned a nil KEK for version %q", wrapped.Version)
	}
	defer kek.Destroy()

	cek, err := aeskw.Unwrap(kek.Bytes(), wrapped.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("regionkey: unwrapping CEK: %w", err)
	}
	return cek, nil
}
