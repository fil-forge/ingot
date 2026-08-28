package regionkey_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/openbao/openbao/api/v2"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/regionkey"
)

// TestOpenBaoProvider_Live exercises OpenBaoProvider against a real OpenBao —
// the derived-context binding, tamper rejection, and version behavior none of
// the httptest fakes can prove. Skipped unless INGOT_TEST_BAO_ADDR names a
// reachable dev-mode server (token from INGOT_TEST_BAO_TOKEN, default "root"):
//
//	docker run --rm -d -p 8200:8200 -e BAO_DEV_ROOT_TOKEN_ID=root openbao/openbao
//	INGOT_TEST_BAO_ADDR=http://127.0.0.1:8200 \
//	  GOWORK=off go test ./regionkey/ -run Live -v
func TestOpenBaoProvider_Live(t *testing.T) {
	addr := os.Getenv("INGOT_TEST_BAO_ADDR")
	if addr == "" {
		t.Skip("set INGOT_TEST_BAO_ADDR to run the live OpenBao provider test")
	}
	token := os.Getenv("INGOT_TEST_BAO_TOKEN")
	if token == "" {
		token = "root"
	}
	ctx := context.Background()

	cfg := api.DefaultConfig()
	cfg.Address = addr
	client, err := api.NewClient(cfg)
	require.NoError(t, err, "NewClient")
	client.SetToken(token)

	// A test-owned transit mount, recreated fresh each run.
	const mount = "ingot-test-transit"
	const keyName = "region-kek"
	_ = client.Sys().UnmountWithContext(ctx, mount) // drop any previous run's mount
	require.NoError(t, client.Sys().MountWithContext(ctx, mount, &api.MountInput{Type: "transit"}), "mount transit")
	t.Cleanup(func() { _ = client.Sys().UnmountWithContext(ctx, mount) })

	// The key exactly as the RFC provisions it: AES-256-GCM, derived contexts.
	_, err = client.Logical().WriteWithContext(ctx, mount+"/keys/"+keyName, map[string]interface{}{
		"type":    "aes256-gcm96",
		"derived": true,
	})
	require.NoError(t, err, "create transit key")

	p, err := regionkey.NewOpenBaoProvider(client, mount, keyName)
	require.NoError(t, err, "NewOpenBaoProvider")

	binding := testBinding(t, "blob-1")
	cek := randKey(t, 32)

	t.Run("round trip", func(t *testing.T) {
		wrapped, err := p.Wrap(ctx, binding, cek)
		require.NoError(t, err, "Wrap")
		require.True(t, strings.HasPrefix(string(wrapped.Ciphertext), "vault:v1:"),
			"transit ciphertext self-describes its version: %s", wrapped.Ciphertext)
		require.Equal(t, regionkey.KeyVersion("v1"), wrapped.Version)

		got, err := p.Unwrap(ctx, binding, wrapped)
		require.NoError(t, err, "Unwrap")
		require.Equal(t, cek, got)
	})

	t.Run("transplant fails authentication", func(t *testing.T) {
		wrapped, err := p.Wrap(ctx, binding, cek)
		require.NoError(t, err, "Wrap")

		otherDigest := binding
		otherDigest.Digest = digestOf(t, []byte("blob-2"))
		_, err = p.Unwrap(ctx, otherDigest, wrapped)
		require.ErrorIs(t, err, regionkey.ErrAuthentication, "a different digest must fail authentication")

		otherSpace := testBinding(t, "blob-1") // same blob, different space
		_, err = p.Unwrap(ctx, otherSpace, wrapped)
		require.ErrorIs(t, err, regionkey.ErrAuthentication, "a different space must fail authentication")
	})

	t.Run("tampered ciphertext fails authentication", func(t *testing.T) {
		wrapped, err := p.Wrap(ctx, binding, cek)
		require.NoError(t, err, "Wrap")
		wrapped.Ciphertext[len(wrapped.Ciphertext)-1] ^= 0x01
		_, err = p.Unwrap(ctx, binding, wrapped)
		require.ErrorIs(t, err, regionkey.ErrAuthentication)
	})

	t.Run("rotation archives old versions until policy retires them", func(t *testing.T) {
		wrappedV1, err := p.Wrap(ctx, binding, cek)
		require.NoError(t, err, "Wrap under v1")

		_, err = client.Logical().WriteWithContext(ctx, mount+"/keys/"+keyName+"/rotate", nil)
		require.NoError(t, err, "rotate key")

		wrappedV2, err := p.Wrap(ctx, binding, cek)
		require.NoError(t, err, "Wrap after rotation")
		require.Equal(t, regionkey.KeyVersion("v2"), wrappedV2.Version, "new wraps record the rotated version")

		// Archive-don't-destroy: the v1 wrap still unwraps after rotation.
		got, err := p.Unwrap(ctx, binding, wrappedV1)
		require.NoError(t, err, "Unwrap of pre-rotation wrap")
		require.Equal(t, cek, got)

		// Retiring v1 via min_decryption_version makes the old wrap an
		// unknown version — the state a rewrap campaign must clear before
		// the policy advances.
		_, err = client.Logical().WriteWithContext(ctx, mount+"/keys/"+keyName+"/config", map[string]interface{}{
			"min_decryption_version": 2,
		})
		require.NoError(t, err, "advance min_decryption_version")
		_, err = p.Unwrap(ctx, binding, wrappedV1)
		require.ErrorIs(t, err, regionkey.ErrUnknownVersion, "a retired version must surface as unknown")

		// The current version is unaffected.
		got, err = p.Unwrap(ctx, binding, wrappedV2)
		require.NoError(t, err)
		require.Equal(t, cek, got)
	})
}
