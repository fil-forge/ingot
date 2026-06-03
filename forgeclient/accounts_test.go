package forgeclient

import (
	"context"
	"net/url"
	"testing"

	"github.com/fil-forge/ingot/tokenstore"
	"github.com/fil-forge/libforge/didmailto"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/principal/absentee"
	"github.com/fil-forge/ucantone/principal/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/command"
	"github.com/fil-forge/ucantone/ucan/delegation"
)

func newAccountsTestClient(t *testing.T, agent ucan.Signer, store tokenstore.Store) *Client {
	t.Helper()
	svc, err := ed25519.Generate()
	if err != nil {
		t.Fatalf("generate service signer: %v", err)
	}
	u, err := url.Parse("http://127.0.0.1:1")
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	c, err := New(agent, svc.DID(), *u, WithTokenStore(store))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return c
}

// accountGrant builds an account→agent delegation issued by an absentee
// (did:mailto) signer, shaped like the delegations login stores (audience =
// agent, subject = did.Undef, command = Top).
func accountGrant(t *testing.T, account did.DID, agent ucan.Signer) ucan.Delegation {
	t.Helper()
	dlg, err := delegation.Delegate(absentee.From(account), agent.DID(), did.Undef, command.Top(), delegation.WithNoExpiration())
	if err != nil {
		t.Fatalf("delegate account→agent: %v", err)
	}
	return dlg
}

func TestAccounts(t *testing.T) {
	ctx := context.Background()
	agent, err := ed25519.Generate()
	if err != nil {
		t.Fatalf("generate agent: %v", err)
	}

	alice, err := didmailto.New("alice@example.com")
	if err != nil {
		t.Fatalf("didmailto alice: %v", err)
	}
	bob, err := didmailto.New("bob@example.com")
	if err != nil {
		t.Fatalf("didmailto bob: %v", err)
	}

	t.Run("empty store returns no accounts", func(t *testing.T) {
		c := newAccountsTestClient(t, agent, tokenstore.NewMemStore())
		got, err := c.Accounts(ctx)
		if err != nil {
			t.Fatalf("Accounts: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("want 0 accounts, got %v", got)
		}
	})

	t.Run("non-mailto issuer is filtered out", func(t *testing.T) {
		other, err := ed25519.Generate()
		if err != nil {
			t.Fatalf("generate other: %v", err)
		}
		keyGrant, err := delegation.Delegate(other, agent.DID(), did.Undef, command.Top(), delegation.WithNoExpiration())
		if err != nil {
			t.Fatalf("delegate key→agent: %v", err)
		}
		store := tokenstore.NewMemStore()
		if err := store.AddDelegations(ctx, keyGrant); err != nil {
			t.Fatalf("add: %v", err)
		}
		c := newAccountsTestClient(t, agent, store)
		got, err := c.Accounts(ctx)
		if err != nil {
			t.Fatalf("Accounts: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("did:key issuer should be filtered out, got %v", got)
		}
	})

	t.Run("returns distinct mailto accounts, sorted", func(t *testing.T) {
		store := tokenstore.NewMemStore()
		// bob then alice (out of order); alice twice (dedup).
		if err := store.AddDelegations(ctx,
			accountGrant(t, bob, agent),
			accountGrant(t, alice, agent),
			accountGrant(t, alice, agent),
		); err != nil {
			t.Fatalf("add: %v", err)
		}
		c := newAccountsTestClient(t, agent, store)
		got, err := c.Accounts(ctx)
		if err != nil {
			t.Fatalf("Accounts: %v", err)
		}
		want := []did.DID{alice, bob}
		if alice.String() > bob.String() {
			want = []did.DID{bob, alice}
		}
		if len(got) != len(want) {
			t.Fatalf("want %d accounts %v, got %d %v", len(want), want, len(got), got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("account[%d]: want %s, got %s", i, want[i], got[i])
			}
		}
	})
}
