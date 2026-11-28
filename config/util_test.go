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
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/config"
)

// mintProofsContainer builds a container holding one space→agent /blob/add
// delegation, returning the container and the delegation for later comparison.
func mintProofsContainer(t *testing.T) (*container.Container, ucan.Delegation) {
	t.Helper()
	space, err := ed25519.GenerateIssuer()
	require.NoError(t, err, "generate space")
	agent, err := ed25519.GenerateIssuer()
	require.NoError(t, err, "generate agent")
	d, err := blobcmds.Add.Delegate(space, agent.DID(), space.DID(), delegation.WithNoExpiration())
	require.NoError(t, err, "delegate")
	return container.New(container.WithDelegations(d)), d
}

// assertHoldsDelegation asserts ct decoded to exactly the minted delegation.
func assertHoldsDelegation(t *testing.T, ct *container.Container, want ucan.Delegation) {
	t.Helper()
	dlgs := ct.Delegations()
	require.Len(t, dlgs, 1)
	require.Equal(t, want.Command(), dlgs[0].Command(), "command")
	require.Equal(t, want.Issuer(), dlgs[0].Issuer(), "issuer")
}

// TestLoadProofsContainer covers the two accepted forms of a proofs config
// value — a path to a file holding a UCAN container, and the string-encoded
// container itself — across the raw and base64 codecs.
func TestLoadProofsContainer(t *testing.T) {
	ct, want := mintProofsContainer(t)

	rawBytes, err := container.Encode(container.Raw, ct)
	require.NoError(t, err, "encode raw")
	b64Bytes, err := container.Encode(container.Base64, ct)
	require.NoError(t, err, "encode base64")

	t.Run("raw file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "proofs.cbor")
		require.NoError(t, os.WriteFile(path, rawBytes, 0o600), "write")
		got, err := config.LoadProofsContainer(path)
		require.NoError(t, err, "load")
		assertHoldsDelegation(t, got, want)
	})

	t.Run("base64 file with trailing newline", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "proofs.b64")
		require.NoError(t, os.WriteFile(path, append(b64Bytes, '\n'), 0o600), "write")
		got, err := config.LoadProofsContainer(path)
		require.NoError(t, err, "load")
		assertHoldsDelegation(t, got, want)
	})

	t.Run("inline string", func(t *testing.T) {
		got, err := config.LoadProofsContainer(string(b64Bytes))
		require.NoError(t, err, "load")
		assertHoldsDelegation(t, got, want)
	})

	t.Run("missing file / undecodable value errors", func(t *testing.T) {
		_, err := config.LoadProofsContainer("/nonexistent/proofs.cbor")
		require.Error(t, err, "expected error for value that is neither a file nor a container")
	})
}
