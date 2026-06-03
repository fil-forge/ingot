// Package tokenstore persists the UCAN delegations + attestations ingot
// holds over its space — the proof chains the forge edge-client presents
// to sprue.
//
// Carried copy of github.com/fil-forge/guppy/pkg/tokenstore (ingot cannot
// import guppy: guppy embeds ingot → import cycle). Keep in sync with
// upstream; see DESIGN_NOTES.md §B for the shared-library plan.
package tokenstore

import (
	"context"
	"iter"

	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
)

// Store is a ucanlib.ProofStore that additionally accepts new
// delegations/attestations/receipts and supports enumeration + reset.
type Store interface {
	ucanlib.ProofStore
	// AddInvocations adds the given invocations to the store.
	AddInvocations(ctx context.Context, invocations ...ucan.Invocation) error
	// AddDelegations adds the given delegations to the store.
	AddDelegations(ctx context.Context, delegations ...ucan.Delegation) error
	// AddReceipts adds the given receipts to the store.
	AddReceipts(ctx context.Context, receipts ...ucan.Receipt) error
	// ListDelegations returns a sequence of delegations matching the given criteria.
	ListDelegations(ctx context.Context, aud did.DID, cmd ucan.Command, sub did.DID) iter.Seq2[ucan.Delegation, error]
	// Delegations returns all delegations held by the store.
	Delegations(ctx context.Context) ([]ucan.Delegation, error)
	// Reset clears all data in the store.
	Reset(ctx context.Context) error
}
