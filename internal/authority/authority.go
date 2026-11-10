// Package authority finds the UCAN authority Ingot's queued tasks make their
// network calls with, after the request that queued them has gone.
//
// A task reads, in order: a store a request lent for the space while it waits
// on the task (Resolver.Lend), the chain the task carries, captured from the
// request that queued it (Resolver.Capture), and then the space's captured
// authority: the borrowable store its latest body upload left, which the
// uploader keeps for an hour. A lend is in this process only, so a task run
// elsewhere, or after the lend ends, goes on to the chain it carries.
package authority

import (
	"context"
	"errors"
	"fmt"
	"sync"

	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/ipfs/go-cid"

	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/tokenstore"
)

// Capturer encodes and renews chains: the uploader's half of the lookup,
// which reads a request's store, or the space's captured authority when the
// context carries none.
type Capturer interface {
	// CaptureAuthority encodes the chain authorizing cmd on space.
	CaptureAuthority(ctx context.Context, space did.DID, cmd ucan.Command) ([]byte, error)
	// PrepareAuthority returns stored while it is usable, and otherwise a
	// chain captured afresh, reporting whether it is a new one.
	PrepareAuthority(ctx context.Context, space did.DID, cmd ucan.Command, stored []byte) ([]byte, bool, error)
}

// Resolver holds the lends, and finds a task's authority.
type Resolver struct {
	capture Capturer
	mu      sync.Mutex
	lent    map[string]*lend
}

// lend is one space's lent store and how many lends hold it.
type lend struct {
	store ucanlib.ProofStore
	n     int
}

// New returns a Resolver that captures and renews chains with c.
func New(c Capturer) *Resolver {
	return &Resolver{capture: c}
}

// Capture encodes the chain authorizing cmd on space from the request in ctx,
// for a task to carry.
func (r *Resolver) Capture(ctx context.Context, space did.DID, cmd ucan.Command) ([]byte, error) {
	return r.capture.CaptureAuthority(ctx, space, cmd)
}

// Lend lends ctx's proof store for space until the returned func is called,
// when tasks may borrow it (reqscope.BorrowableProofStore). Of two lends for
// one space, the later store is read.
func (r *Resolver) Lend(ctx context.Context, space did.DID) func() {
	store, ok := reqscope.BorrowableProofStore(ctx)
	if !ok {
		return func() {}
	}
	key := space.String()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lent == nil {
		r.lent = map[string]*lend{}
	}
	l, ok := r.lent[key]
	if !ok {
		l = &lend{}
		r.lent[key] = l
	}
	l.store = store
	l.n++
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if l.n--; l.n == 0 && r.lent[key] == l {
			delete(r.lent, key)
		}
	}
}

// lentStore returns the store lent for space, if any.
func (r *Resolver) lentStore(space did.DID) (ucanlib.ProofStore, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	l, ok := r.lent[space.String()]
	if !ok {
		return nil, false
	}
	return l.store, true
}

// WithProofs returns ctx carrying a proof store that reads the store lent for
// space, then the encoded chains in stored, so the network calls made with it
// read them in that order. A ctx that carries a store already reads it in
// place of the lent one. With neither a store nor a chain, ctx is returned as
// is, and the calls read the space's captured authority.
func (r *Resolver) WithProofs(ctx context.Context, space did.DID, stored ...[]byte) (context.Context, error) {
	var stores proofStores
	if live, ok := reqscope.ProofStore(ctx); ok && live != nil {
		stores = append(stores, live)
	} else if lent, ok := r.lentStore(space); ok {
		stores = append(stores, lent)
	}
	carried := tokenstore.NewMemStore()
	found := false
	for _, proofs := range stored {
		if len(proofs) == 0 {
			continue
		}
		ct, err := container.Decode(proofs)
		if err != nil {
			return nil, fmt.Errorf("authority: decode a stored chain: %w", err)
		}
		if dlgs := ct.Delegations(); len(dlgs) > 0 {
			if err := carried.AddDelegations(ctx, dlgs...); err != nil {
				return nil, err
			}
			found = true
		}
	}
	if found {
		stores = append(stores, carried)
	}
	switch len(stores) {
	case 0:
		return ctx, nil
	case 1:
		return context.WithValue(ctx, reqscope.ProofStoreKey(), stores[0]), nil
	}
	return context.WithValue(ctx, reqscope.ProofStoreKey(), stores), nil
}

// Prepare returns the encoded chain to send a queued change for cmd on space
// with: the lent store's, then stored while it is usable, then one renewed
// from the space's captured authority. It reports whether the chain differs
// from stored, so the caller can keep it.
func (r *Resolver) Prepare(ctx context.Context, space did.DID, cmd ucan.Command, stored []byte) ([]byte, bool, error) {
	if lent, ok := r.lentStore(space); ok {
		proofs, err := r.capture.CaptureAuthority(context.WithValue(ctx, reqscope.ProofStoreKey(), lent), space, cmd)
		if err == nil {
			return proofs, true, nil
		}
	}
	return r.capture.PrepareAuthority(ctx, space, cmd, stored)
}

// proofStores reads each store in turn, and returns the first chain found.
type proofStores []ucanlib.ProofStore

func (ps proofStores) ProofChain(ctx context.Context, aud did.DID, cmd ucan.Command, sub did.DID) ([]ucan.Delegation, []cid.Cid, error) {
	var errs []error
	for _, p := range ps {
		dlgs, links, err := p.ProofChain(ctx, aud, cmd, sub)
		if err == nil && len(dlgs) > 0 {
			return dlgs, links, nil
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	return nil, nil, errors.Join(errs...)
}
