package forgeclient

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/fil-forge/ingot/tokenstore"
	blobcmds "github.com/fil-forge/libforge/commands/blob"
	contentcmds "github.com/fil-forge/libforge/commands/content"
	indexcmds "github.com/fil-forge/libforge/commands/index"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// proofQuery is one ProofChain lookup the recorder observed.
type proofQuery struct {
	aud did.DID
	cmd ucan.Command
	sub did.DID
}

// recordingProofStore records every ProofChain call and returns a fixed
// result, so a test can assert which store a client call consulted.
type recordingProofStore struct {
	mu    sync.Mutex
	calls []proofQuery
	err   error // returned from every ProofChain call
}

func (r *recordingProofStore) ProofChain(_ context.Context, aud did.DID, cmd ucan.Command, sub did.DID) ([]ucan.Delegation, []cid.Cid, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, proofQuery{aud, cmd, sub})
	return nil, nil, r.err
}

func (r *recordingProofStore) queries() []proofQuery {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]proofQuery(nil), r.calls...)
}

// tokenStoreWithBlobAdd returns a token store already holding a space→agent
// /blob/add chain, so that if a client call fell back to the token store
// (instead of the override) it would find a chain and proceed past ProofChain
// — letting the override test distinguish the two by the sentinel error.
func tokenStoreWithBlobAdd(t *testing.T, ctx context.Context, space, agent ucan.Issuer) tokenstore.Store {
	t.Helper()
	ts := tokenstore.NewMemStore()
	dlg, err := blobcmds.Add.Delegate(space, agent.DID(), space.DID(), delegation.WithNoExpiration())
	require.NoError(t, err)
	require.NoError(t, ts.AddDelegations(ctx, dlg))
	return ts
}

func TestBlobAddUsesProofStoreOverride(t *testing.T) {
	ctx := context.Background()
	agent, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	space, err := ed25519.GenerateIssuer()
	require.NoError(t, err)

	// The token store can satisfy /blob/add on its own; the override must win.
	c := newAccountsTestClient(t, agent, tokenStoreWithBlobAdd(t, ctx, space, agent))

	rec := &recordingProofStore{err: errors.New("sentinel-override-used")}
	_, err = c.BlobAdd(ctx, space.DID(), bytes.NewReader([]byte("hello")),
		WithProofStore(rec))

	// The override returned the sentinel, so BlobAdd fails building the chain —
	// proving it consulted rec, not the (satisfiable) token store.
	require.ErrorContains(t, err, "sentinel-override-used")

	qs := rec.queries()
	require.Len(t, qs, 1)
	require.Equal(t, agent.DID(), qs[0].aud)
	require.Equal(t, blobcmds.Add.Command, qs[0].cmd)
	require.Equal(t, space.DID(), qs[0].sub)
}

func TestIndexAddUsesProofStoreOverride(t *testing.T) {
	ctx := context.Background()
	agent, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	space, err := ed25519.GenerateIssuer()
	require.NoError(t, err)

	c := newAccountsTestClient(t, agent, tokenstore.NewMemStore())

	// Return empty (non-error) chains so IndexAdd runs both ProofChain lookups
	// before failing at the network step (which we ignore).
	rec := &recordingProofStore{}
	digest, err := multihash.Sum([]byte("index-bytes"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	indexCID := cid.NewCidV1(cid.Raw, digest)

	_, _ = indexCID, c.IndexAdd(ctx, space.DID(), indexCID, WithProofStore(rec))

	qs := rec.queries()
	require.Len(t, qs, 2, "IndexAdd builds content/retrieve then index/add chains")
	require.Equal(t, contentcmds.Retrieve.Command, qs[0].cmd)
	require.Equal(t, indexcmds.Add.Command, qs[1].cmd)
	for _, q := range qs {
		require.Equal(t, agent.DID(), q.aud)
		require.Equal(t, space.DID(), q.sub)
	}
}
