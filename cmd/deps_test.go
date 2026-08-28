package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fil-forge/libforge/identity"
	"github.com/fil-forge/ucantone/multikey/ed25519"

	"github.com/fil-forge/ingot/config"
)

// writeAgentKey generates an ed25519 key, writes it as PEM under t.TempDir()
// and returns the path plus the key's did:key.
func writeAgentKey(t *testing.T) (string, string) {
	t.Helper()
	signer, err := ed25519.Generate()
	if err != nil {
		t.Fatalf("generate agent key: %v", err)
	}
	pemBytes, err := identity.EncodeSignerToPEM(signer)
	if err != nil {
		t.Fatalf("encode agent key: %v", err)
	}
	keyFile := filepath.Join(t.TempDir(), "agent.pem")
	if err := os.WriteFile(keyFile, pemBytes, 0o600); err != nil {
		t.Fatalf("write agent key: %v", err)
	}
	return keyFile, signer.KeyDID().String()
}

// TestLoadAgentIdentity_DIDWeb: with identity.service_id set, the agent issues
// as the did:web while signing with the PEM key (the key DID is preserved for
// the DID document's verification method).
func TestLoadAgentIdentity_DIDWeb(t *testing.T) {
	keyFile, keyDID := writeAgentKey(t)
	id, err := loadAgentIdentity(config.IdentityConfig{KeyFile: keyFile, ServiceID: "did:web:ingot.test"})
	if err != nil {
		t.Fatalf("loadAgentIdentity: %v", err)
	}
	if got := id.DID().String(); got != "did:web:ingot.test" {
		t.Fatalf("DID = %s, want did:web:ingot.test", got)
	}
	if got := id.KeyDID().String(); got != keyDID {
		t.Fatalf("KeyDID = %s, want %s", got, keyDID)
	}
}

// TestLoadAgentIdentity_DIDKey: without a service DID the agent identifies by
// the key's own did:key.
func TestLoadAgentIdentity_DIDKey(t *testing.T) {
	keyFile, keyDID := writeAgentKey(t)
	id, err := loadAgentIdentity(config.IdentityConfig{KeyFile: keyFile})
	if err != nil {
		t.Fatalf("loadAgentIdentity: %v", err)
	}
	if got := id.DID().String(); got != keyDID {
		t.Fatalf("DID = %s, want the key DID %s", got, keyDID)
	}
}

// TestLoadAgentIdentity_BadServiceID: a malformed service DID is rejected at
// load time rather than producing an issuer nobody can resolve.
func TestLoadAgentIdentity_BadServiceID(t *testing.T) {
	keyFile, _ := writeAgentKey(t)
	if _, err := loadAgentIdentity(config.IdentityConfig{KeyFile: keyFile, ServiceID: "ingot.test"}); err == nil {
		t.Fatal("expected an error for a service id that is not a DID")
	}
}
