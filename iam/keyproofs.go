package iam

import (
	"sync"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/ipfs/go-cid"
	gocache "github.com/patrickmn/go-cache"
)

// KeyProofs holds one [DelegationCache] per access key, so proof chains are
// isolated by key: a store contains only the delegations Hilt issued for
// that key's requests, and [DelegationCache.ProofChain] therefore cannot
// assemble a chain that crosses into another key's delegations. This is what
// lets the local fast path trust a chain, and what scopes an onward Forge
// retrieval to the access key that made the request.
//
// Per-key caches are held in a TTL cache so an idle key's cache is dropped
// and garbage-collected (go-cache stops its janitor goroutine via a
// finalizer), bounding memory and goroutines to recently-active keys. The
// TTL is refreshed on every access, so an active key is never evicted
// mid-use; a caller already holding a *DelegationCache is unaffected by
// eviction regardless.
type KeyProofs struct {
	mu    sync.Mutex
	byKey *gocache.Cache // access-key DID string → *DelegationCache
}

// keyProofsIdleTTL is how long a key's cache survives with no access before
// it is evicted (and its janitor reclaimed). Comfortably longer than Hilt's
// delegation lifetimes (≤ next UTC midnight), so an in-use key's store never
// disappears out from under it.
const keyProofsIdleTTL = 24 * time.Hour

// NewKeyProofs returns an empty per-key proof store registry.
func NewKeyProofs() *KeyProofs {
	return &KeyProofs{byKey: gocache.New(keyProofsIdleTTL, keyProofsIdleTTL)}
}

// For returns key's proof store, creating it on first use. The idle TTL is
// refreshed so the store stays live while the key is active.
func (k *KeyProofs) For(key did.DID) *DelegationCache {
	id := key.String()
	k.mu.Lock()
	defer k.mu.Unlock()
	if v, ok := k.byKey.Get(id); ok {
		dc := v.(*DelegationCache)
		k.byKey.Set(id, dc, gocache.DefaultExpiration) // refresh idle window
		return dc
	}
	dc := NewDelegationCache()
	k.byKey.Set(id, dc, gocache.DefaultExpiration)
	return dc
}

// Deposit adds delegations to key's proof store.
func (k *KeyProofs) Deposit(key did.DID, dlgs ...ucan.Delegation) {
	k.For(key).Add(dlgs...)
}

// InvalidateHolders drops every per-key proof store holding the delegation
// with CID link, returning the affected access-key DIDs. Dropping the whole
// store rather than the one entry is deliberate: a revoked delegation is a
// hop in every chain the local fast path could assemble for that key, so no
// partial state is worth keeping — the key's next request falls through to
// Hilt, which re-authorizes (or, for a deleted key, refuses). A caller
// already holding the dropped *DelegationCache (an in-flight request) is
// unaffected; the next For(key) starts a fresh empty store.
func (k *KeyProofs) InvalidateHolders(link cid.Cid) []did.DID {
	k.mu.Lock()
	defer k.mu.Unlock()
	var affected []did.DID
	// Items() already skips expired entries; expired stores are gone anyway.
	for id, item := range k.byKey.Items() {
		dc, ok := item.Object.(*DelegationCache)
		if !ok || !dc.Contains(link) {
			continue
		}
		key, err := did.Parse(id)
		if err != nil {
			continue // unreachable: keys are For(did).String()
		}
		k.byKey.Delete(id)
		affected = append(affected, key)
	}
	return affected
}
