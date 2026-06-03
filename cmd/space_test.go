package cmd

import (
	"testing"

	"github.com/fil-forge/libforge/didmailto"
	"github.com/fil-forge/ucantone/did"
)

func mustAccount(t *testing.T, email string) did.DID {
	t.Helper()
	d, err := didmailto.New(email)
	if err != nil {
		t.Fatalf("didmailto.New(%q): %v", email, err)
	}
	return d
}

func TestParseAccount(t *testing.T) {
	alice := mustAccount(t, "alice@example.com")

	t.Run("plain email", func(t *testing.T) {
		got, err := parseAccount("alice@example.com")
		if err != nil {
			t.Fatalf("parseAccount: %v", err)
		}
		if got != alice {
			t.Fatalf("want %s, got %s", alice, got)
		}
	})

	t.Run("did:mailto passthrough", func(t *testing.T) {
		got, err := parseAccount(alice.String())
		if err != nil {
			t.Fatalf("parseAccount: %v", err)
		}
		if got != alice {
			t.Fatalf("want %s, got %s", alice, got)
		}
	})

	t.Run("garbage errors", func(t *testing.T) {
		if _, err := parseAccount("not-an-email"); err == nil {
			t.Fatalf("expected error for %q", "not-an-email")
		}
	})
}

func TestPickAccount(t *testing.T) {
	alice := mustAccount(t, "alice@example.com")
	bob := mustAccount(t, "bob@example.com")

	t.Run("no flag, zero accounts -> error", func(t *testing.T) {
		if _, err := pickAccount(nil, "", "provision-to"); err == nil {
			t.Fatal("expected error when no accounts are logged in")
		}
	})

	t.Run("no flag, one account -> that account", func(t *testing.T) {
		got, err := pickAccount([]did.DID{alice}, "", "provision-to")
		if err != nil {
			t.Fatalf("pickAccount: %v", err)
		}
		if got != alice {
			t.Fatalf("want %s, got %s", alice, got)
		}
	})

	t.Run("no flag, many accounts -> error", func(t *testing.T) {
		if _, err := pickAccount([]did.DID{alice, bob}, "", "provision-to"); err == nil {
			t.Fatal("expected error when multiple accounts are logged in")
		}
	})

	t.Run("explicit flag in the set -> that account", func(t *testing.T) {
		got, err := pickAccount([]did.DID{alice, bob}, "bob@example.com", "grant-to")
		if err != nil {
			t.Fatalf("pickAccount: %v", err)
		}
		if got != bob {
			t.Fatalf("want %s, got %s", bob, got)
		}
	})

	t.Run("explicit flag (did:mailto) in the set -> that account", func(t *testing.T) {
		got, err := pickAccount([]did.DID{alice}, alice.String(), "grant-to")
		if err != nil {
			t.Fatalf("pickAccount: %v", err)
		}
		if got != alice {
			t.Fatalf("want %s, got %s", alice, got)
		}
	})

	t.Run("explicit flag not in the set -> error", func(t *testing.T) {
		if _, err := pickAccount([]did.DID{alice}, "bob@example.com", "provision-to"); err == nil {
			t.Fatal("expected error when the named account is not logged in")
		}
	})

	t.Run("explicit garbage flag -> error", func(t *testing.T) {
		if _, err := pickAccount([]did.DID{alice}, "not-an-email", "provision-to"); err == nil {
			t.Fatal("expected error for an unparseable account")
		}
	})
}
