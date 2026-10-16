package catalogwalk

import (
	"context"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/iam"
	"github.com/fil-forge/ingot/internal/reqscope"
)

// borrowable returns a context carrying store, marked as the IAM layer marks
// a request's own key store.
func borrowable(store any) context.Context {
	ctx := context.WithValue(context.Background(), reqscope.ProofStoreKey(), store)
	return context.WithValue(ctx, reqscope.BorrowableKey(), true)
}

// TestAuthorityBorrowsBorrowableStores: only a request marked borrowable
// lends its store, only one that can prove retrieval in the space, for that
// space only and for borrowTTL after its last read; a borrowed context lends
// nothing in turn.
func TestAuthorityBorrowsBorrowableStores(t *testing.T) {
	f := newFixture(t, Config{})
	a := f.auth
	store := f.keys.For(f.key)
	other, err := did.Parse("did:web:other.example")
	require.NoError(t, err)

	a.Capture(context.WithValue(context.Background(), reqscope.ProofStoreKey(), store), f.space)
	require.False(t, a.Holds(f.space), "captured a store not marked borrowable")

	a.Capture(borrowable(iam.NewDelegationCache(nil)), f.space)
	require.False(t, a.Holds(f.space), "captured a store that can't prove retrieval")

	a.Capture(borrowable(store), other)
	require.False(t, a.Holds(other), "captured a store for a space it can't retrieve in")

	a.Capture(borrowable(store), f.space)
	require.True(t, a.Holds(f.space))
	borrowed, ok := a.Borrow(context.Background(), f.space)
	require.True(t, ok)
	got, ok := reqscope.ProofStore(borrowed)
	require.True(t, ok)
	require.Same(t, store, got)
	require.False(t, reqscope.Borrowable(borrowed))

	fresh := NewAuthority(f.auth.agent)
	fresh.Capture(context.WithValue(borrowed, reqscope.BorrowableKey(), reqscope.Borrowable(borrowed)), f.space)
	require.False(t, fresh.Holds(f.space), "a borrowed context lent its store again")

	f.clock = f.clock.Add(borrowTTL - time.Second)
	require.True(t, a.Holds(f.space))
	f.clock = f.clock.Add(time.Second)
	require.False(t, a.Holds(f.space), "lent past borrowTTL")
}

// TestAuthorityDropsRevokedStores: once a revocation drops the access key's
// store, the store is no longer lent, though the key keeps reading under it
// for less than recaptureEvery, and a store already dropped isn't taken.
func TestAuthorityDropsRevokedStores(t *testing.T) {
	f := newFixture(t, Config{})
	held := f.keys.For(f.key)
	f.request()
	require.True(t, f.auth.Holds(f.space))

	require.Len(t, f.keys.InvalidateHolders(f.grant.Link()), 1)
	require.False(t, f.auth.Holds(f.space), "lent a store a revocation dropped")
	_, ok := f.auth.Borrow(context.Background(), f.space)
	require.False(t, ok)

	f.auth.Capture(borrowable(held), f.space)
	require.False(t, f.auth.Holds(f.space), "took a store a revocation dropped")
}

// TestAuthorityRenewsOnLaterReads: reads keep renewing a space's capture, at
// most every recaptureEvery, so steady traffic keeps the store lent.
func TestAuthorityRenewsOnLaterReads(t *testing.T) {
	f := newFixture(t, Config{})
	a := f.auth
	f.request()
	captured := a.stores[f.space.String()].at
	f.clock = f.clock.Add(recaptureEvery / 2)
	f.request()
	require.Equal(t, captured, a.stores[f.space.String()].at, "renewed within recaptureEvery")
	f.clock = f.clock.Add(recaptureEvery)
	f.request()
	require.Equal(t, f.clock, a.stores[f.space.String()].at)

	f.clock = f.clock.Add(borrowTTL - time.Second)
	require.True(t, a.Holds(f.space))
}

// TestAuthorityPrune: Prune forgets stores that can no longer be lent and
// keeps the rest.
func TestAuthorityPrune(t *testing.T) {
	f := newFixture(t, Config{})
	f.request()
	f.auth.Prune()
	require.Len(t, f.auth.stores, 1)
	f.clock = f.clock.Add(borrowTTL)
	f.auth.Prune()
	require.Empty(t, f.auth.stores)
}
