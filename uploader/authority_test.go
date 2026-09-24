package uploader

import (
	"testing"
	"time"

	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/command"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/stretchr/testify/require"
)

// encodeChain builds the container a queued registration carries, from
// delegations with the given expiries (nil meaning none).
func encodeChain(t *testing.T, expiries ...*time.Time) []byte {
	t.Helper()
	issuer, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	audience, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	subject, err := ed25519.GenerateIssuer()
	require.NoError(t, err)

	dlgs := make([]ucan.Delegation, 0, len(expiries))
	for _, exp := range expiries {
		opts := []delegation.Option{delegation.WithNoExpiration()}
		if exp != nil {
			opts = []delegation.Option{delegation.WithExpiration(ucan.UnixTimestamp(exp.Unix()))}
		}
		d, err := delegation.Delegate(issuer, audience.DID(), subject.DID(),
			command.MustParse("/upload/add"), opts...)
		require.NoError(t, err)
		dlgs = append(dlgs, d)
	}
	encoded, err := container.Encode(container.RawGzip, container.New(container.WithDelegations(dlgs...)))
	require.NoError(t, err)
	return encoded
}

// TestAuthorityUsable covers what decides whether a queued change goes out on
// the chain it stored or waits for a renewed one. Hilt expires its delegation
// to the gateway at the next UTC midnight, so this is the check that keeps a
// change queued overnight from being sent with authority the upload service
// will refuse.
func TestAuthorityUsable(t *testing.T) {
	now := time.Now()
	soon := now.Add(time.Minute)
	later := now.Add(time.Hour)
	past := now.Add(-time.Minute)

	t.Run("a chain with life left is usable", func(t *testing.T) {
		require.True(t, authorityUsable(encodeChain(t, &later), now))
	})

	t.Run("an expired chain is not", func(t *testing.T) {
		require.False(t, authorityUsable(encodeChain(t, &past), now))
	})

	t.Run("the earliest expiry decides", func(t *testing.T) {
		// A chain is only as good as its shortest-lived link.
		require.False(t, authorityUsable(encodeChain(t, &later, &past), now))
	})

	t.Run("a chain expiring inside the margin is renewed early", func(t *testing.T) {
		// Checked against the instant the caller cares about, not now, so a
		// batch is not spent on authority that lapses in flight.
		require.True(t, authorityUsable(encodeChain(t, &soon), now))
		require.False(t, authorityUsable(encodeChain(t, &soon), now.Add(authorityRenewMargin)))
	})

	t.Run("delegations with no expiry never lapse", func(t *testing.T) {
		require.True(t, authorityUsable(encodeChain(t, nil), now.AddDate(1, 0, 0)))
	})

	t.Run("an empty or undecodable chain is not usable", func(t *testing.T) {
		empty, err := container.Encode(container.RawGzip, container.New())
		require.NoError(t, err)
		require.False(t, authorityUsable(empty, now), "nothing to send with")
		require.False(t, authorityUsable([]byte("not a container"), now))
	})
}
