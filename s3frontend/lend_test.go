package s3frontend

import (
	"context"
	"testing"

	"github.com/fil-forge/libforge/testutil"

	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/tokenstore"
)

// A borrowable request store lent for a space reaches work run without a
// request for that space, and only while the lend lasts; another space's
// work, and a store that may not be borrowed, get nothing.
func TestALentStoreReachesWorkForItsSpace(t *testing.T) {
	b := &Backend{}
	space, other := testutil.RandomDID(t), testutil.RandomDID(t)
	store := tokenstore.NewMemStore()
	req := context.WithValue(context.Background(), reqscope.ProofStoreKey(), store)

	b.lendAuthority(req, space)() // not borrowable: nothing lent
	if _, ok := reqscope.ProofStore(b.withLentAuthority(context.Background(), space)); ok {
		t.Fatal("a store that may not be borrowed was lent")
	}

	end := b.lendAuthority(context.WithValue(req, reqscope.BorrowableKey(), true), space)
	got, ok := reqscope.ProofStore(b.withLentAuthority(context.Background(), space))
	if !ok || got != store {
		t.Fatal("work for the space did not read the lent store")
	}
	if _, ok := reqscope.ProofStore(b.withLentAuthority(context.Background(), other)); ok {
		t.Fatal("work for another space read the lent store")
	}
	end()
	if _, ok := reqscope.ProofStore(b.withLentAuthority(context.Background(), space)); ok {
		t.Fatal("the store stayed lent after the lend ended")
	}
}
