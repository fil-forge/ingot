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

// TestAuthorityBorrowsBorrowableStores: only a request marked borrowable
// lends its store, the store is lent for its own space only and for
// borrowTTL after the space's last read, and a borrowed context lends
// nothing in turn.
func TestAuthorityBorrowsBorrowableStores(t *testing.T) {
	clock := time.Unix(1_000_000, 0)
	a := NewAuthority()
	a.now = func() time.Time { return clock }
	space, err := did.Parse("did:web:lender.example")
	require.NoError(t, err)
	other, err := did.Parse("did:web:other.example")
	require.NoError(t, err)

	store := iam.NewDelegationCache(nil)
	ctx := context.WithValue(context.Background(), reqscope.ProofStoreKey(), store)
	a.Capture(ctx, space)
	require.False(t, a.Holds(space), "captured a store not marked borrowable")

	a.Capture(context.WithValue(ctx, reqscope.BorrowableKey(), true), space)
	require.True(t, a.Holds(space))
	require.False(t, a.Holds(other))
	borrowed, ok := a.Borrow(context.Background(), space)
	require.True(t, ok)
	got, ok := reqscope.ProofStore(borrowed)
	require.True(t, ok)
	require.Same(t, store, got)

	fresh := NewAuthority()
	fresh.Capture(borrowed, space)
	require.False(t, fresh.Holds(space), "a borrowed context lent its store again")

	clock = clock.Add(borrowTTL - time.Second)
	require.True(t, a.Holds(space))
	clock = clock.Add(time.Second)
	require.False(t, a.Holds(space), "lent past borrowTTL")
}

// TestAuthorityRenewsOnLaterReads: reads keep renewing a space's capture, at
// most every recaptureEvery, so steady traffic keeps the store lent.
func TestAuthorityRenewsOnLaterReads(t *testing.T) {
	clock := time.Unix(1_000_000, 0)
	a := NewAuthority()
	a.now = func() time.Time { return clock }
	space, err := did.Parse("did:web:renew.example")
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), reqscope.ProofStoreKey(), iam.NewDelegationCache(nil))
	ctx = context.WithValue(ctx, reqscope.BorrowableKey(), true)

	a.Capture(ctx, space)
	captured := a.stores[space.String()].at
	clock = clock.Add(recaptureEvery / 2)
	a.Capture(ctx, space)
	require.Equal(t, captured, a.stores[space.String()].at, "renewed within recaptureEvery")
	clock = clock.Add(recaptureEvery)
	a.Capture(ctx, space)
	require.Equal(t, clock, a.stores[space.String()].at)

	clock = clock.Add(borrowTTL - time.Second)
	require.True(t, a.Holds(space))
}
