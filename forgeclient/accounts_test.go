package forgeclient

import (
	"context"
	"net/url"
	"testing"

	"github.com/fil-forge/ingot/tokenstore"
	"github.com/fil-forge/libforge/attestation/didmailto"
	"github.com/fil-forge/ucantone/absentee"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/command"
	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/stretchr/testify/require"
)

func newAccountsTestClient(t *testing.T, agent ucan.Issuer, store tokenstore.Store) *Client {
	t.Helper()
	svc, err := ed25519.GenerateIssuer()
	require.NoError(t, err, "generate service issuer")
	u, err := url.Parse("http://127.0.0.1:1")
	require.NoError(t, err, "parse url")
	c, err := New(agent, svc.DID(), *u, WithTokenStore(store))
	require.NoError(t, err, "new client")
	return c
}

// accountGrant builds an account→agent delegation issued by an absentee
// (did:mailto) signer, shaped like the delegations login stores (audience =
// agent, subject = did.Undef, command = Top).
func accountGrant(t *testing.T, account did.DID, agent ucan.Issuer) ucan.Delegation {
	t.Helper()
	dlg, err := delegation.Delegate(absentee.From(account), agent.DID(), did.Undef, command.Top(), delegation.WithNoExpiration())
	require.NoError(t, err, "delegate account→agent")
	return dlg
}

func TestAccounts(t *testing.T) {
	ctx := context.Background()
	agent, err := ed25519.GenerateIssuer()
	require.NoError(t, err, "generate agent")

	alice, err := didmailto.New("alice@example.com")
	require.NoError(t, err, "didmailto alice")
	bob, err := didmailto.New("bob@example.com")
	require.NoError(t, err, "didmailto bob")

	t.Run("empty store returns no accounts", func(t *testing.T) {
		c := newAccountsTestClient(t, agent, tokenstore.NewMemStore())
		got, err := c.Accounts(ctx)
		require.NoError(t, err, "Accounts")
		require.Empty(t, got)
	})

	t.Run("non-mailto issuer is filtered out", func(t *testing.T) {
		other, err := ed25519.GenerateIssuer()
		require.NoError(t, err, "generate other")
		keyGrant, err := delegation.Delegate(other, agent.DID(), did.Undef, command.Top(), delegation.WithNoExpiration())
		require.NoError(t, err, "delegate key→agent")
		store := tokenstore.NewMemStore()
		require.NoError(t, store.AddDelegations(ctx, keyGrant), "add")
		c := newAccountsTestClient(t, agent, store)
		got, err := c.Accounts(ctx)
		require.NoError(t, err, "Accounts")
		require.Empty(t, got, "did:key issuer should be filtered out")
	})

	t.Run("returns distinct mailto accounts, sorted", func(t *testing.T) {
		store := tokenstore.NewMemStore()
		// bob then alice (out of order); alice twice (dedup).
		require.NoError(t, store.AddDelegations(ctx,
			accountGrant(t, bob, agent),
			accountGrant(t, alice, agent),
			accountGrant(t, alice, agent),
		), "add")
		c := newAccountsTestClient(t, agent, store)
		got, err := c.Accounts(ctx)
		require.NoError(t, err, "Accounts")
		want := []did.DID{alice, bob}
		if alice.String() > bob.String() {
			want = []did.DID{bob, alice}
		}
		require.Len(t, got, len(want))
		for i := range want {
			require.Equal(t, want[i], got[i], "account[%d]", i)
		}
	})
}
