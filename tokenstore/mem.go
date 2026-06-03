// Carried copy of github.com/fil-forge/guppy/pkg/tokenstore/mem.go.
package tokenstore

import (
	"context"
	"iter"
	"sync"

	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/ipfs/go-cid"
)

type MemStore struct {
	invs  map[cid.Cid]ucan.Invocation
	rcpts map[cid.Cid]ucan.Receipt
	dlgs  map[cid.Cid]ucan.Delegation
	mu    sync.RWMutex
}

var _ Store = (*MemStore)(nil)

// NewMemStore creates a new token store backed by an in-memory map.
func NewMemStore() *MemStore {
	return &MemStore{
		invs:  map[cid.Cid]ucan.Invocation{},
		rcpts: map[cid.Cid]ucan.Receipt{},
		dlgs:  map[cid.Cid]ucan.Delegation{},
	}
}

func (ms *MemStore) AddDelegations(ctx context.Context, delegations ...ucan.Delegation) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for _, dlg := range delegations {
		ms.dlgs[dlg.Link()] = dlg
	}
	return nil
}

func (ms *MemStore) AddInvocations(ctx context.Context, invocations ...ucan.Invocation) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for _, inv := range invocations {
		ms.invs[inv.Link()] = inv
	}
	return nil
}

func (ms *MemStore) AddReceipts(ctx context.Context, receipts ...ucan.Receipt) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for _, rcpt := range receipts {
		ms.rcpts[rcpt.Link()] = rcpt
	}
	return nil
}

func (ms *MemStore) ProofChain(ctx context.Context, aud did.DID, cmd ucan.Command, sub did.DID) ([]ucan.Delegation, []cid.Cid, error) {
	return ucanlib.ProofChain(ctx, ms.matchDelegations, aud, cmd, sub)
}

func (ms *MemStore) ProofAttestations(ctx context.Context, proofs []ucan.Delegation, authority did.DID) ([]ucan.Invocation, error) {
	return ucanlib.ProofAttestations(ctx, ms.listInvocations, proofs, authority)
}

func (ms *MemStore) ListDelegations(ctx context.Context, aud did.DID, cmd ucan.Command, sub did.DID) iter.Seq2[ucan.Delegation, error] {
	return func(yield func(ucan.Delegation, error) bool) {
		ms.mu.RLock()
		defer ms.mu.RUnlock()
		for _, d := range ms.dlgs {
			if d.Audience() == aud && d.Command() == cmd && d.Subject() == sub {
				if !yield(d, nil) {
					return
				}
			}
		}
	}
}

func (ms *MemStore) Delegations(ctx context.Context) ([]ucan.Delegation, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	dlgs := make([]ucan.Delegation, 0, len(ms.dlgs))
	for _, d := range ms.dlgs {
		dlgs = append(dlgs, d)
	}
	return dlgs, nil
}

func (ms *MemStore) matchDelegations(ctx context.Context, aud did.DID, cmd ucan.Command, sub did.DID) iter.Seq2[ucan.Delegation, error] {
	return ucanlib.NewDelegationMatcher(ms.ListDelegations)(ctx, aud, cmd, sub)
}

func (ms *MemStore) listInvocations(ctx context.Context, aud did.DID, cmd ucan.Command, sub did.DID) iter.Seq2[ucan.Invocation, error] {
	return func(yield func(ucan.Invocation, error) bool) {
		ms.mu.RLock()
		defer ms.mu.RUnlock()
		for _, d := range ms.invs {
			if d.Audience() == aud && d.Command() == cmd && d.Subject() == sub {
				if !yield(d, nil) {
					return
				}
			}
		}
	}
}

func (ms *MemStore) Reset(ctx context.Context) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.invs = map[cid.Cid]ucan.Invocation{}
	ms.rcpts = map[cid.Cid]ucan.Receipt{}
	ms.dlgs = map[cid.Cid]ucan.Delegation{}
	return nil
}
