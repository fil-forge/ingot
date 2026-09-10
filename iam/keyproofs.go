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
// Keys are also indexed by the (tenant, principal) pair Hilt reports for
// them, so a principal invalidation from the firehose can drop every store
// belonging to that principal without knowing which keys they are. See
// [KeyProofs.Bind] and [KeyProofs.InvalidatePrincipal].
type KeyProofs struct {
	mu    sync.Mutex
	byKey *gocache.Cache // access-key DID string → *DelegationCache

	// The principal index has its own mutex, deliberately not mu: go-cache
	// calls the byKey eviction hook synchronously from Delete, and
	// InvalidateHolders deletes while holding mu, so a hook that took mu
	// would deadlock.
	pmu          sync.Mutex
	byPrincipal  map[PrincipalRef]map[string]struct{} // → set of access-key DID strings
	keyPrincipal map[string]PrincipalRef              // access-key DID string → its pair
}

// PrincipalRef identifies a principal: a userId is unique only within its
// tenant, so the tenant is part of the identity. It is what Hilt names in an
// authorize result and what a firehose principal event carries.
type PrincipalRef struct {
	Tenant    did.DID
	Principal string
}

// Defined reports whether the reference names a principal. An undefined
// tenant or an empty userId is not a principal and never indexes anything.
func (r PrincipalRef) Defined() bool {
	return r.Tenant.Defined() && r.Principal != ""
}

// keyProofsIdleTTL is how long a key's cache survives with no access before
// it is evicted (and its janitor reclaimed). Comfortably longer than Hilt's
// delegation lifetimes (≤ next UTC midnight), so an in-use key's store never
// disappears out from under it.
const keyProofsIdleTTL = 24 * time.Hour

// NewKeyProofs returns an empty per-key proof store registry.
func NewKeyProofs() *KeyProofs {
	k := &KeyProofs{
		byKey:        gocache.New(keyProofsIdleTTL, keyProofsIdleTTL),
		byPrincipal:  map[PrincipalRef]map[string]struct{}{},
		keyPrincipal: map[string]PrincipalRef{},
	}
	k.byKey.OnEvicted(k.unbind)
	return k
}

// Bind records that key belongs to a principal, so
// [KeyProofs.InvalidatePrincipal] can find it. Idempotent: re-binding the
// same pair changes nothing. A key that moves to another principal (Hilt
// reporting a different pair) is re-indexed under the new one.
func (k *KeyProofs) Bind(key did.DID, ref PrincipalRef) {
	if !ref.Defined() || !key.Defined() {
		return
	}
	id := key.String()
	k.pmu.Lock()
	defer k.pmu.Unlock()
	if cur, ok := k.keyPrincipal[id]; ok {
		if cur == ref {
			return
		}
		k.detachLocked(id, cur)
	}
	k.keyPrincipal[id] = ref
	ids, ok := k.byPrincipal[ref]
	if !ok {
		ids = map[string]struct{}{}
		k.byPrincipal[ref] = ids
	}
	ids[id] = struct{}{}
}

// InvalidatePrincipal drops the proof store of every access key bound to
// ref, returning the affected access-key DIDs. It mirrors
// [KeyProofs.InvalidateHolders]: the whole store goes, so the key's next
// request falls through to Hilt, which re-authorizes it against the
// principal's current access (or refuses). Idempotent — the bindings go
// with the stores, so a re-delivered event affects nothing — and a pair
// nothing is bound to is a no-op.
func (k *KeyProofs) InvalidatePrincipal(ref PrincipalRef) []did.DID {
	if !ref.Defined() {
		return nil
	}
	// Snapshot the bound keys and release the index lock before deleting:
	// the delete runs the eviction hook, which takes this same lock.
	k.pmu.Lock()
	ids := make([]string, 0, len(k.byPrincipal[ref]))
	for id := range k.byPrincipal[ref] {
		ids = append(ids, id)
	}
	k.pmu.Unlock()

	var affected []did.DID
	for _, id := range ids {
		key, err := did.Parse(id)
		if err != nil {
			continue // unreachable: ids are Bind(did, …).String()
		}
		k.mu.Lock()
		k.byKey.Delete(id) // prunes the binding via unbind, when a store exists
		k.mu.Unlock()
		// A key whose store had already been evicted leaves no store to
		// delete and so no hook to run; drop its binding here so the pair
		// stays idempotent. The liveness guard in unbind keeps a store that
		// a concurrent request re-created (and re-bound) after the delete.
		k.unbind(id, nil)
		affected = append(affected, key)
	}
	return affected
}

// unbind is the byKey eviction hook: a dropped store takes its principal
// binding with it. Guarded against the re-add race the same way
// [DelegationCache.removeFromIndex] is — an idle eviction that raced a
// fresh For(key) must not unbind the live store. The liveness check runs
// under pmu so a Bind that lands between the eviction and this hook cannot
// be undone: Bind takes pmu too, so either it ran before the check (and the
// store reads live) or it runs after the detach (and re-indexes the key).
func (k *KeyProofs) unbind(id string, _ any) {
	k.pmu.Lock()
	defer k.pmu.Unlock()
	if _, live := k.byKey.Get(id); live {
		return
	}
	if ref, ok := k.keyPrincipal[id]; ok {
		k.detachLocked(id, ref)
	}
}

// detachLocked removes id from ref's key set and forgets its binding. The
// caller holds pmu.
func (k *KeyProofs) detachLocked(id string, ref PrincipalRef) {
	delete(k.keyPrincipal, id)
	ids, ok := k.byPrincipal[ref]
	if !ok {
		return
	}
	delete(ids, id)
	if len(ids) == 0 {
		delete(k.byPrincipal, ref)
	}
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
