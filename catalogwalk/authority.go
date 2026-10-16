// Package catalogwalk measures how much of each bucket's catalog is live by
// walking it from the bucket's root: whatever else the bucket holds on Forge
// is dead. Most of a catalog lives in segments whose local files are gone,
// so the walk reads it from Forge, which needs retrieval authority for the
// bucket's space. A background job has none of its own, so the walk borrows
// the authority of a recent request in that space (Authority), and a bucket
// is walked only while some is held.
package catalogwalk

import (
	"context"
	"sync"
	"time"

	contentcmds "github.com/fil-forge/libforge/commands/content"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"

	"github.com/fil-forge/ingot/internal/reqscope"
)

// borrowTTL is how long a request's proof store may be borrowed after the
// last read in its space, as the catalog ship borrows a write's.
const borrowTTL = time.Hour

// recaptureEvery bounds how often reads in a space renew its capture, so
// most reads cost a lookup rather than a proof check and a write.
const recaptureEvery = time.Minute

// dropper is a proof store that can report having been dropped for a
// revocation (iam.DelegationCache).
type dropper interface {
	Dropped() bool
}

// usable reports whether store may be held past its request: one that can
// report a revocation's drop and hasn't been dropped. A store that can't
// report it is never held.
func usable(store ucanlib.ProofStore) bool {
	d, ok := store.(dropper)
	return ok && !d.Dropped()
}

// Authority holds, for each space, the proof store of the latest request that
// read in it, so a walk can read that space's catalog from Forge for up to
// borrowTTL afterwards. It takes only a store the IAM service marked
// borrowable (reqscope.Borrowable), the access key's own cached store, and
// only one that can prove /content/retrieve on the space to the agent. A
// revocation of any delegation in the store drops it from the IAM caches,
// and a dropped store is neither taken nor lent.
type Authority struct {
	agent  did.DID // the audience of the agent's retrieval chains
	mu     sync.RWMutex
	stores map[string]borrowed // by space DID
	now    func() time.Time
}

// borrowed is one space's captured store and when it was captured.
type borrowed struct {
	store ucanlib.ProofStore
	at    time.Time
}

// NewAuthority returns an Authority holding nothing, for retrieval by agent.
func NewAuthority(agent did.DID) *Authority {
	return &Authority{agent: agent, stores: map[string]borrowed{}, now: time.Now}
}

// Capture records the proof store of ctx's request for space. It is a
// blockstore.Layered OnRead hook, so it sees every read a request makes,
// whichever tier serves it. A context with no borrowable store (a background
// job's, a walk's own) is ignored, and so is a store that can't prove
// retrieval in space.
func (a *Authority) Capture(ctx context.Context, space did.DID) {
	if !reqscope.Borrowable(ctx) {
		return
	}
	store, ok := reqscope.ProofStore(ctx)
	if !ok || store == nil || !usable(store) {
		return
	}
	key, now := space.String(), a.now()
	a.mu.RLock()
	b, held := a.stores[key]
	a.mu.RUnlock()
	if held && now.Sub(b.at) < recaptureEvery {
		return
	}
	if !a.proves(ctx, store, space) {
		return
	}
	a.mu.Lock()
	a.stores[key] = borrowed{store: store, at: now}
	a.mu.Unlock()
}

// Borrow returns ctx carrying the store captured for space, if it was
// captured within borrowTTL and hasn't been dropped since; ok is false
// otherwise. The returned context is not borrowable in turn.
func (a *Authority) Borrow(ctx context.Context, space did.DID) (context.Context, bool) {
	key := space.String()
	a.mu.RLock()
	b, held := a.stores[key]
	a.mu.RUnlock()
	if !held {
		return ctx, false
	}
	if !a.lendable(b) {
		a.mu.Lock()
		if cur, ok := a.stores[key]; ok && cur.at.Equal(b.at) {
			delete(a.stores, key)
		}
		a.mu.Unlock()
		return ctx, false
	}
	ctx = context.WithValue(ctx, reqscope.ProofStoreKey(), b.store)
	return context.WithValue(ctx, reqscope.BorrowableKey(), false), true
}

// Holds reports whether a lendable store is held for space that still
// proves /content/retrieve on it: the chain a store held when captured can
// expire while it is held.
func (a *Authority) Holds(space did.DID) bool {
	ctx, ok := a.Borrow(context.Background(), space)
	if !ok {
		return false
	}
	store, _ := reqscope.ProofStore(ctx)
	return a.proves(ctx, store, space)
}

// proves reports whether store holds a /content/retrieve chain from space to
// the agent, the chain a retrieval in space presents.
func (a *Authority) proves(ctx context.Context, store ucanlib.ProofStore, space did.DID) bool {
	chain, _, err := store.ProofChain(ctx, a.agent, contentcmds.Retrieve.Command, space)
	return err == nil && len(chain) > 0
}

// Prune forgets every store that can no longer be lent, so a space that
// stops reading, or whose bucket is never walked, doesn't keep its store.
func (a *Authority) Prune() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for key, b := range a.stores {
		if !a.lendable(b) {
			delete(a.stores, key)
		}
	}
}

// lendable reports whether b was captured within borrowTTL and its store
// hasn't been dropped since.
func (a *Authority) lendable(b borrowed) bool {
	return a.now().Sub(b.at) < borrowTTL && usable(b.store)
}
