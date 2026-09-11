package iam

import (
	"context"
	"iter"
	"sync"
	"time"

	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/ipfs/go-cid"
	gocache "github.com/patrickmn/go-cache"
)

// DelegationCache is a TTL cache of Hilt-issued delegations, keyed by
// delegation CID. Each entry lives exactly as long as the delegation itself
// (Hilt's /s3/request/authorize re-delegations carry a ≤24h expiry; chains
// from /s3/bucket/info may carry none and are kept until process restart).
//
// It implements [ucanlib.ProofStore], so the network read tier
// (blockstore.Forge) resolves its per-space /content/retrieve proof chains
// from here: the Service deposits delegations as requests are authorized,
// and retrieval consumes them.
//
// Lookups go through an (audience, command, subject) index rather than a
// cache scan — chain assembly probes once per command variation × subject
// per hop, so the lookup path must be cheap. The index tracks the cache:
// Add inserts into both; go-cache's OnEvicted hook (explicit deletes and
// the janitor's expiry sweep) prunes the index. Between a delegation
// expiring and the janitor sweeping it, the index may still reference it —
// lookups re-check liveness against the expiry-aware cache, so the janitor
// only bounds memory, never correctness.
// It also holds the effective S3 action set Hilt reported for this access
// key, per bucket (see [DelegationCache.PutPermissions] and
// [DelegationCache.Permits]). The set lives here because a chain probe alone
// cannot enforce a policy: several S3 actions map to the same Forge
// commands — s3:GetObject and s3:ListBucket both need /content/retrieve, and
// s3:PutObject's commands are a superset of s3:AbortMultipartUpload's — so a
// key granted one action would otherwise satisfy the probe for the others.
// Holding the set in the same per-key store means an invalidation drops the
// chains and the set together.
type DelegationCache struct {
	data *gocache.Cache

	mu    sync.RWMutex
	index map[indexKey]map[string]ucan.Delegation // → CID string → delegation

	perms *gocache.Cache // bucket DID string → map[string]struct{} of S3 actions
}

// indexKey is the exact-match probe the delegation matcher uses. Powerline
// delegations index under sub = did.Undef — precisely the key the matcher
// probes for them.
type indexKey struct {
	aud did.DID
	cmd ucan.Command
	sub did.DID
}

// Compile-time assertion: the retrieval path holds this as its proof store.
var _ ucanlib.ProofStore = (*DelegationCache)(nil)

// cacheJanitorInterval is how often expired entries are swept out of the
// cache and, via OnEvicted, the index. Lookups already skip expired entries;
// the janitor just bounds memory.
const cacheJanitorInterval = 10 * time.Minute

// NewDelegationCache returns an empty cache.
func NewDelegationCache() *DelegationCache {
	d := &DelegationCache{
		data:  gocache.New(gocache.NoExpiration, cacheJanitorInterval),
		index: map[indexKey]map[string]ucan.Delegation{},
		perms: gocache.New(gocache.NoExpiration, cacheJanitorInterval),
	}
	d.data.OnEvicted(d.removeFromIndex)
	return d
}

// Add caches each delegation for its own lifetime: TTL = expiration - now,
// no expiration → cached until restart, already expired → skipped.
func (d *DelegationCache) Add(dlgs ...ucan.Delegation) {
	now := ucan.Now()
	for _, dlg := range dlgs {
		ttl := gocache.NoExpiration
		if exp := dlg.Expiration(); exp != nil {
			remaining := time.Duration(int64(*exp)-int64(now)) * time.Second
			if remaining <= 0 {
				continue
			}
			ttl = remaining
		}
		link := dlg.Link().String()
		// Cache first, then index: the index must never reference a CID
		// the cache doesn't hold.
		d.data.Set(link, dlg, ttl)
		key := keyOf(dlg)
		d.mu.Lock()
		byLink, ok := d.index[key]
		if !ok {
			byLink = map[string]ucan.Delegation{}
			d.index[key] = byLink
		}
		byLink[link] = dlg
		d.mu.Unlock()
	}
}

// removeFromIndex is the go-cache OnEvicted hook: it prunes the evicted
// delegation from the index. go-cache invokes it after releasing its own
// mutex (on explicit Delete and on the janitor's expiry sweep), so taking
// the index lock here cannot deadlock.
func (d *DelegationCache) removeFromIndex(link string, v any) {
	dlg, ok := v.(ucan.Delegation)
	if !ok {
		return
	}
	// Re-add race guard: a fresh Add of the same CID may have landed
	// between the eviction and this handler running; if the cache holds a
	// live entry again, the index entry is current — keep it.
	if _, live := d.data.Get(link); live {
		return
	}
	key := keyOf(dlg)
	d.mu.Lock()
	defer d.mu.Unlock()
	if byLink, ok := d.index[key]; ok {
		delete(byLink, link)
		if len(byLink) == 0 {
			delete(d.index, key)
		}
	}
}

func keyOf(dlg ucan.Delegation) indexKey {
	return indexKey{aud: dlg.Audience(), cmd: dlg.Command(), sub: dlg.Subject()}
}

// Contains reports whether the cache holds a live (unexpired) delegation
// with this CID. Revocation processing uses it to find which access keys'
// stores a revoked delegation participates in.
func (d *DelegationCache) Contains(link cid.Cid) bool {
	_, ok := d.data.Get(link.String())
	return ok
}

// ProofChain assembles the root-first delegation chain granting cmd over sub
// to aud, from the cached delegations. See [ucanlib.ProofChain].
func (d *DelegationCache) ProofChain(ctx context.Context, aud did.DID, cmd ucan.Command, sub did.DID) ([]ucan.Delegation, []cid.Cid, error) {
	return ucanlib.ProofChain(ctx, d.matchDelegations, aud, cmd, sub)
}

func (d *DelegationCache) matchDelegations(ctx context.Context, aud did.DID, cmd ucan.Command, sub did.DID) iter.Seq2[ucan.Delegation, error] {
	return ucanlib.NewDelegationMatcher(d.listDelegations)(ctx, aud, cmd, sub)
}

// listDelegations yields cached, unexpired delegations exactly matching
// (aud, cmd, sub) — the same exact-match contract as tokenstore.MemStore;
// command variations and powerline subjects are the matcher's job. The
// index resolves the key in one lookup; each hit is then re-checked against
// the expiry-aware cache so an expired-but-unswept entry never yields.
func (d *DelegationCache) listDelegations(_ context.Context, aud did.DID, cmd ucan.Command, sub did.DID) iter.Seq2[ucan.Delegation, error] {
	d.mu.RLock()
	byLink := d.index[indexKey{aud: aud, cmd: cmd, sub: sub}]
	links := make([]string, 0, len(byLink))
	dlgs := make([]ucan.Delegation, 0, len(byLink))
	for link, dlg := range byLink {
		links = append(links, link)
		dlgs = append(dlgs, dlg)
	}
	d.mu.RUnlock()

	return func(yield func(ucan.Delegation, error) bool) {
		for i, dlg := range dlgs {
			if _, live := d.data.Get(links[i]); !live {
				continue // expired, janitor hasn't swept yet
			}
			if !yield(dlg, nil) {
				return
			}
		}
	}
}

// PutPermissions caches the effective S3 action set Hilt reported for an
// access key on one bucket, keyed by the bucket's DID and expiring after
// ttl. A nil actions slice means Hilt reported no set at all and nothing is
// cached, so [DelegationCache.Permits] keeps reporting the bucket unknown
// and requests for it keep going to Hilt. An empty but non-nil slice is a
// real answer — the key may do nothing on this bucket — and is cached as
// such, denying every action.
//
// The set lives in the same per-access-key store as the chains, so
// [KeyProofs.InvalidateHolders] drops both together and a stale set can
// never outlive the proofs it was issued with.
func (d *DelegationCache) PutPermissions(bucket did.DID, ttl time.Duration, actions []string) {
	if ttl <= 0 || !bucket.Defined() || actions == nil {
		return
	}
	set := make(map[string]struct{}, len(actions))
	for _, a := range actions {
		set[a] = struct{}{}
	}
	d.perms.Set(bucket.String(), set, ttl)
}

// Permits reports whether the cached action set for bucket contains action.
// known is false when no live set is cached for that bucket — the caller
// must then ask Hilt rather than decide locally. Because the set is keyed by
// the bucket DID Hilt named, a bucket whose local registry space disagrees
// with Hilt's answer simply reports unknown and takes the Hilt path.
func (d *DelegationCache) Permits(bucket did.DID, action string) (allowed, known bool) {
	if !bucket.Defined() {
		return false, false
	}
	v, ok := d.perms.Get(bucket.String())
	if !ok {
		return false, false
	}
	set, ok := v.(map[string]struct{})
	if !ok {
		return false, false
	}
	_, allowed = set[action]
	return allowed, true
}
