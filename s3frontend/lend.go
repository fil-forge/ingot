package s3frontend

import (
	"context"
	"sync"

	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"

	"github.com/fil-forge/ingot/internal/reqscope"
)

// A request that waits on queued work it does not run itself, such as a
// bucket delete waiting on its space's releases, lends its own authority for
// the space while it waits. The workers that run the work read the lent store
// before the authority the tasks captured, which may have lapsed. A lend is in
// this process only: a task run elsewhere, or after the lend ends, falls back
// to its captured authority.

// lends holds the stores lent for each space.
type lends struct {
	mu sync.Mutex
	m  map[string]*lend
}

// lend is one space's lent store and how many lends hold it.
type lend struct {
	store ucanlib.ProofStore
	n     int
}

// lendAuthority lends ctx's proof store for space until the returned func is
// called, when background work may borrow it (reqscope.BorrowableProofStore).
// Of two lends for one space, the later store is read.
func (b *Backend) lendAuthority(ctx context.Context, space did.DID) func() {
	store, ok := reqscope.BorrowableProofStore(ctx)
	if !ok {
		return func() {}
	}
	key := space.String()
	b.lent.mu.Lock()
	defer b.lent.mu.Unlock()
	if b.lent.m == nil {
		b.lent.m = map[string]*lend{}
	}
	l, ok := b.lent.m[key]
	if !ok {
		l = &lend{}
		b.lent.m[key] = l
	}
	l.store = store
	l.n++
	return func() {
		b.lent.mu.Lock()
		defer b.lent.mu.Unlock()
		if l.n--; l.n == 0 && b.lent.m[key] == l {
			delete(b.lent.m, key)
		}
	}
}

// withLentAuthority returns ctx carrying the store lent for space, if any, as
// its request's store. A ctx that carries one already is returned as is.
func (b *Backend) withLentAuthority(ctx context.Context, space did.DID) context.Context {
	if _, ok := reqscope.ProofStore(ctx); ok {
		return ctx
	}
	b.lent.mu.Lock()
	l, ok := b.lent.m[space.String()]
	var store ucanlib.ProofStore
	if ok {
		store = l.store
	}
	b.lent.mu.Unlock()
	if store == nil {
		return ctx
	}
	return context.WithValue(ctx, reqscope.ProofStoreKey(), store)
}
