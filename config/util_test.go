package config_test

import (
	"os"
	"path/filepath"
	"testing"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/fil-forge/ucantone/ucan/delegation"

	"github.com/fil-forge/ingot/config"
)

// mintProofsContainer builds a container holding one space→agent /blob/add
// delegation, returning the container and the delegation for later comparison.
func mintProofsContainer(t *testing.T) (*container.Container, ucan.Delegation) {
	t.Helper()
	space, err := ed25519.GenerateIssuer()
	if err != nil {
		t.Fatalf("generate space: %v", err)
	}
	agent, err := ed25519.GenerateIssuer()
	if err != nil {
		t.Fatalf("generate agent: %v", err)
	}
	d, err := blobcmds.Add.Delegate(space, agent.DID(), space.DID(), delegation.WithNoExpiration())
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	return container.New(container.WithDelegations(d)), d
}

// assertHoldsDelegation asserts ct decoded to exactly the minted delegation.
func assertHoldsDelegation(t *testing.T, ct *container.Container, want ucan.Delegation) {
	t.Helper()
	dlgs := ct.Delegations()
	if len(dlgs) != 1 {
		t.Fatalf("expected 1 delegation, got %d", len(dlgs))
	}
	if got := dlgs[0].Command(); got != want.Command() {
		t.Fatalf("expected command %s, got %s", want.Command(), got)
	}
	if got := dlgs[0].Issuer(); got != want.Issuer() {
		t.Fatalf("expected issuer %s, got %s", want.Issuer(), got)
	}
}

// TestLoadProofsContainer covers the two accepted forms of a proofs config
// value — a path to a file holding a UCAN container, and the string-encoded
// container itself — across the raw and base64 codecs.
func TestLoadProofsContainer(t *testing.T) {
	ct, want := mintProofsContainer(t)

	rawBytes, err := container.Encode(container.Raw, ct)
	if err != nil {
		t.Fatalf("encode raw: %v", err)
	}
	b64Bytes, err := container.Encode(container.Base64, ct)
	if err != nil {
		t.Fatalf("encode base64: %v", err)
	}

	t.Run("raw file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "proofs.cbor")
		if err := os.WriteFile(path, rawBytes, 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		got, err := config.LoadProofsContainer(path)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		assertHoldsDelegation(t, got, want)
	})

	t.Run("base64 file with trailing newline", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "proofs.b64")
		if err := os.WriteFile(path, append(b64Bytes, '\n'), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		got, err := config.LoadProofsContainer(path)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		assertHoldsDelegation(t, got, want)
	})

	t.Run("inline string", func(t *testing.T) {
		got, err := config.LoadProofsContainer(string(b64Bytes))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		assertHoldsDelegation(t, got, want)
	})

	t.Run("missing file / undecodable value errors", func(t *testing.T) {
		if _, err := config.LoadProofsContainer("/nonexistent/proofs.cbor"); err == nil {
			t.Fatal("expected error for value that is neither a file nor a container")
		}
	})
}
