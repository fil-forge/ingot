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

	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"

	"github.com/fil-forge/ingot/internal/reqscope"
)

// borrowTTL is how long a request's proof store may be borrowed after the
// last read in its space, as the catalog ship borrows a write's.
const borrowTTL = time.Hour

// recaptureEvery bounds how often reads in a space renew its capture, so
// most reads cost a lookup rather than a write.
const recaptureEvery = time.Minute

// Authority holds, for each space, the proof store of the latest request that
// read in it, so a walk can read that space's catalog from Forge for up to
// borrowTTL afterwards. It takes only a store the IAM service marked
// borrowable (reqscope.Borrowable): the access key's own cached store. A
// revocation clears that store's delegations, so a borrowed store loses them
// too.
type Authority struct {
	mu     sync.RWMutex
	stores map[string]borrowed // by space DID
	now    func() time.Time
}

// borrowed is one space's captured store and when it was captured.
type borrowed struct {
	store ucanlib.ProofStore
	at    time.Time
}

// NewAuthority returns an Authority holding nothing.
func NewAuthority() *Authority {
	return &Authority{stores: map[string]borrowed{}, now: time.Now}
}

// Capture records the proof store of ctx's request for space, if it is
// borrowable. It is a blockstore.Layered OnRead hook, so it sees every read a
// request makes, whichever tier serves it. A context with no borrowable store
// (a background job's, a walk's own) is ignored.
func (a *Authority) Capture(ctx context.Context, space did.DID) {
	if !reqscope.Borrowable(ctx) {
		return
	}
	store, ok := reqscope.ProofStore(ctx)
	if !ok || store == nil {
		return
	}
	key, now := space.String(), a.now()
	a.mu.RLock()
	b, held := a.stores[key]
	a.mu.RUnlock()
	if held && now.Sub(b.at) < recaptureEvery {
		return
	}
	a.mu.Lock()
	a.stores[key] = borrowed{store: store, at: now}
	a.mu.Unlock()
}

// Borrow returns ctx carrying the store captured for space, if it was
// captured within borrowTTL; ok is false otherwise.
func (a *Authority) Borrow(ctx context.Context, space did.DID) (context.Context, bool) {
	key := space.String()
	a.mu.RLock()
	b, held := a.stores[key]
	a.mu.RUnlock()
	if !held {
		return ctx, false
	}
	if a.now().Sub(b.at) >= borrowTTL {
		a.mu.Lock()
		if cur, ok := a.stores[key]; ok && cur.at.Equal(b.at) {
			delete(a.stores, key)
		}
		a.mu.Unlock()
		return ctx, false
	}
	return context.WithValue(ctx, reqscope.ProofStoreKey(), b.store), true
}

// Holds reports whether a store captured within borrowTTL is held for space.
func (a *Authority) Holds(space did.DID) bool {
	_, ok := a.Borrow(context.Background(), space)
	return ok
}
