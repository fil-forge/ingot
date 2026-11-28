package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fil-forge/libforge/identity"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/config"
)

// writeAgentKey generates an ed25519 key, writes it as PEM under t.TempDir()
// and returns the path plus the key's did:key.
func writeAgentKey(t *testing.T) (string, string) {
	t.Helper()
	signer, err := ed25519.Generate()
	require.NoError(t, err, "generate agent key")
	pemBytes, err := identity.EncodeSignerToPEM(signer)
	require.NoError(t, err, "encode agent key")
	keyFile := filepath.Join(t.TempDir(), "agent.pem")
	require.NoError(t, os.WriteFile(keyFile, pemBytes, 0o600), "write agent key")
	return keyFile, signer.KeyDID().String()
}

// TestLoadAgentIdentity_DIDWeb: with identity.service_id set, the agent issues
// as the did:web while signing with the PEM key (the key DID is preserved for
// the DID document's verification method).
func TestLoadAgentIdentity_DIDWeb(t *testing.T) {
	keyFile, keyDID := writeAgentKey(t)
	id, err := loadAgentIdentity(config.IdentityConfig{KeyFile: keyFile, ServiceID: "did:web:ingot.test"})
	require.NoError(t, err, "loadAgentIdentity")
	require.Equal(t, "did:web:ingot.test", id.DID().String(), "DID")
	require.Equal(t, keyDID, id.KeyDID().String(), "KeyDID")
}

// TestLoadAgentIdentity_DIDKey: without a service DID the agent identifies by
// the key's own did:key.
func TestLoadAgentIdentity_DIDKey(t *testing.T) {
	keyFile, keyDID := writeAgentKey(t)
	id, err := loadAgentIdentity(config.IdentityConfig{KeyFile: keyFile})
	require.NoError(t, err, "loadAgentIdentity")
	got := id.DID().String()
	require.Equal(t, keyDID, got, "DID")
}

// TestLoadAgentIdentity_BadServiceID: a malformed service DID is rejected at
// load time rather than producing an issuer nobody can resolve.
func TestLoadAgentIdentity_BadServiceID(t *testing.T) {
	keyFile, _ := writeAgentKey(t)
	_, err := loadAgentIdentity(config.IdentityConfig{KeyFile: keyFile, ServiceID: "ingot.test"})
	require.Error(t, err, "expected an error for a service id that is not a DID")
}
