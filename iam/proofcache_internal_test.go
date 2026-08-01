package iam

import (
	"context"
	"testing"
	"time"

	contentcmds "github.com/fil-forge/libforge/commands/content"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/stretchr/testify/require"
)

// mintDelegation issues a single /content/retrieve delegation between fresh
// principals, with the given options.
func mintDelegation(t *testing.T, opts ...delegation.Option) ucan.Delegation {
	t.Helper()
	iss, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	aud, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	dlg, err := contentcmds.Retrieve.Delegate(iss, aud.DID(), iss.DID(), opts...)
	require.NoError(t, err)
	return dlg
}

// indexSize reports the number of (key, link) pairs currently indexed.
func (d *DelegationCache) indexSize() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	n := 0
	for _, byLink := range d.index {
		n += len(byLink)
	}
	return n
}

func TestIndexTracksAddAndLookup(t *testing.T) {
	d := NewDelegationCache()
	dlg := mintDelegation(t, delegation.WithNoExpiration())
	d.Add(dlg)

	require.Equal(t, 1, d.indexSize())

	// The exact-key lookup yields the delegation.
	var got int
	for range d.listDelegations(context.Background(), dlg.Audience(), dlg.Command(), dlg.Subject()) {
		got++
	}
	require.Equal(t, 1, got)

	// A different key yields nothing (and never scans).
	other := mintDelegation(t, delegation.WithNoExpiration())
	for range d.listDelegations(context.Background(), other.Audience(), other.Command(), other.Subject()) {
		t.Fatal("unexpected yield for unindexed key")
	}
}

// TestExpiredEntryIsLiveCheckedBeforeJanitor: an expired-but-unswept entry
// stays in the index (janitor hasn't run) but must not yield from lookups.
func TestExpiredEntryIsLiveCheckedBeforeJanitor(t *testing.T) {
	d := NewDelegationCache()
	dlg := mintDelegation(t, delegation.WithExpiration(ucan.Now()+1))
	d.Add(dlg)
	require.Equal(t, 1, d.indexSize())

	time.Sleep(1500 * time.Millisecond) // past expiry, well before any janitor

	require.Equal(t, 1, d.indexSize(), "janitor has not swept; index still holds the entry")
	for range d.listDelegations(context.Background(), dlg.Audience(), dlg.Command(), dlg.Subject()) {
		t.Fatal("expired delegation must not yield")
	}

	// The janitor's sweep (run directly here) prunes the index via OnEvicted.
	d.data.DeleteExpired()
	require.Equal(t, 0, d.indexSize(), "eviction must prune the index")
	require.Empty(t, d.index, "empty inner maps must be dropped")
}

func TestExplicitDeletePrunesIndex(t *testing.T) {
	d := NewDelegationCache()
	dlg := mintDelegation(t, delegation.WithNoExpiration())
	d.Add(dlg)
	require.Equal(t, 1, d.indexSize())

	d.data.Delete(dlg.Link().String())
	require.Equal(t, 0, d.indexSize())
}

// TestEvictionReAddRace: the OnEvicted handler can run after a fresh Add of
// the same CID (janitor evicts → Add re-inserts → handler fires with the
// stale value). The handler must keep the index entry when the cache holds a
// live entry again.
func TestEvictionReAddRace(t *testing.T) {
	d := NewDelegationCache()
	dlg := mintDelegation(t, delegation.WithNoExpiration())
	d.Add(dlg)

	// Simulate the interleaving: the entry is live in the cache (re-added)
	// when the stale eviction callback runs.
	d.removeFromIndex(dlg.Link().String(), dlg)
	require.Equal(t, 1, d.indexSize(), "handler must not prune an entry the cache holds live")

	// And the normal path still prunes once the cache entry is really gone.
	d.data.Delete(dlg.Link().String())
	require.Equal(t, 0, d.indexSize())
}
