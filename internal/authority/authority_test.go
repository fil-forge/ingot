package authority

import (
	"context"
	"errors"
	"testing"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/libforge/testutil"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/tokenstore"
)

// A borrowable request store lent for a space reaches work run without a
// request for that space, and only while the lend lasts; another space's
// work, and a store that may not be borrowed, get nothing.
func TestALentStoreReachesWorkForItsSpace(t *testing.T) {
	r := New(nil)
	space, other := testutil.RandomDID(t), testutil.RandomDID(t)
	store := tokenstore.NewMemStore()
	req := context.WithValue(context.Background(), reqscope.ProofStoreKey(), store)
	lent := func(space did.DID) (ucanlib.ProofStore, bool) {
		ctx, err := r.WithProofs(context.Background(), space)
		require.NoError(t, err, "WithProofs")
		return reqscope.ProofStore(ctx)
	}

	r.Lend(req, space)() // not borrowable: nothing lent
	_, ok := lent(space)
	require.False(t, ok, "a store that may not be borrowed was lent")

	end := r.Lend(context.WithValue(req, reqscope.BorrowableKey(), true), space)
	got, ok := lent(space)
	require.True(t, ok, "work for the space did not read the lent store")
	require.Same(t, store, got, "work for the space did not read the lent store")
	_, ok = lent(other)
	require.False(t, ok, "work for another space read the lent store")
	end()
	_, ok = lent(space)
	require.False(t, ok, "the store stayed lent after the lend ended")
}

// chainStore answers every lookup with its chain, or err.
type chainStore struct {
	links []cid.Cid
	err   error
}

func (s chainStore) ProofChain(context.Context, did.DID, ucan.Command, did.DID) ([]ucan.Delegation, []cid.Cid, error) {
	if s.err != nil {
		return nil, nil, s.err
	}
	return make([]ucan.Delegation, len(s.links)), s.links, nil
}

// TestProofStoresReadsInTurn: the first store with a chain answers, so a lent
// store's chains win over a task's stored ones, and the stored ones answer
// for a command the lent store holds none for.
func TestProofStoresReadsInTurn(t *testing.T) {
	lent := []cid.Cid{link(t, "lent")}
	stored := []cid.Cid{link(t, "stored")}
	for name, tc := range map[string]struct {
		stores []ucanlib.ProofStore
		want   []cid.Cid
	}{
		"the lent chain first":              {[]ucanlib.ProofStore{chainStore{links: lent}, chainStore{links: stored}}, lent},
		"the stored chain when it has none": {[]ucanlib.ProofStore{chainStore{err: errors.New("no chain")}, chainStore{links: stored}}, stored},
		"the stored chain when it is empty": {[]ucanlib.ProofStore{chainStore{}, chainStore{links: stored}}, stored},
	} {
		t.Run(name, func(t *testing.T) {
			_, links, err := proofStores(tc.stores).ProofChain(t.Context(), did.Undef, blobcmds.Remove.Command, did.Undef)
			require.NoError(t, err, "ProofChain")
			require.Len(t, links, 1, "ProofChain")
			require.Equal(t, tc.want[0], links[0], "ProofChain")
		})
	}
	_, _, err := (proofStores{chainStore{err: errors.New("none")}}).ProofChain(t.Context(), did.Undef, blobcmds.Remove.Command, did.Undef)
	require.Error(t, err, "ProofChain with no chain anywhere succeeded")
}

// capturer captures from whichever store ctx carries, encoding it as the
// store itself so a test can see which one answered, and keeps a stored
// chain unless told it is spent.
type capturer struct {
	spent bool
}

func (capturer) CaptureAuthority(ctx context.Context, _ did.DID, _ ucan.Command) ([]byte, error) {
	store, ok := reqscope.ProofStore(ctx)
	if !ok {
		return nil, errors.New("no store")
	}
	name, ok := store.(namedStore)
	if !ok {
		return nil, errors.New("unnamed store")
	}
	return []byte(name), nil
}

func (c capturer) PrepareAuthority(_ context.Context, _ did.DID, _ ucan.Command, stored []byte) ([]byte, bool, error) {
	if c.spent {
		return []byte("renewed"), true, nil
	}
	return stored, false, nil
}

// namedStore is a proof store the capturer encodes as its name.
type namedStore string

func (namedStore) ProofChain(context.Context, did.DID, ucan.Command, did.DID) ([]ucan.Delegation, []cid.Cid, error) {
	return nil, nil, nil
}

// TestPrepareReadsTheLentStoreFirst: a queued change goes out on the store
// lent for its space ahead of its stored chain, on the stored chain while it
// is usable when nothing is lent, and on a renewed one once it is spent.
func TestPrepareReadsTheLentStoreFirst(t *testing.T) {
	space := testutil.RandomDID(t)
	lending := context.WithValue(context.WithValue(context.Background(), reqscope.ProofStoreKey(), namedStore("lent")), reqscope.BorrowableKey(), true)
	for name, tc := range map[string]struct {
		lend  bool
		spent bool
		want  string
		fresh bool
	}{
		"the lent store over a usable stored chain":    {lend: true, want: "lent", fresh: true},
		"the stored chain with nothing lent":           {want: "stored"},
		"a renewed chain once the stored one is spent": {spent: true, want: "renewed", fresh: true},
	} {
		t.Run(name, func(t *testing.T) {
			r := New(capturer{spent: tc.spent})
			if tc.lend {
				defer r.Lend(lending, space)()
			}
			got, fresh, err := r.Prepare(t.Context(), space, blobcmds.Remove.Command, []byte("stored"))
			require.NoError(t, err, "Prepare = %q, %v, %v", got, fresh, err)
			require.Equal(t, tc.want, string(got), "Prepare = %q, %v, %v", got, fresh, err)
			require.Equal(t, tc.fresh, fresh, "Prepare = %q, %v, %v", got, fresh, err)
		})
	}
}

func link(t *testing.T, s string) cid.Cid {
	t.Helper()
	h, err := multihash.Sum([]byte(s), multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, h)
}
