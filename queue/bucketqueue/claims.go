package bucketqueue

import (
	"context"
	"errors"
	"fmt"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/queue/spaceuploadqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
)

// The reference index (blob_refs) is written by the commit: a version's
// claims on its body digests land, and the discarded versions' claims drop,
// with the root's advance (registry.Registry.RootCommitWrites).
// The diff is the crux of safe dedup + delete:
//
//   - a digest in new but not old gains a claim (newly referenced);
//   - each generation's claim rows are keyed by a per-generation id
//     (claimVersionID), so racing writers never touch one shared row;
//   - the new generation's claims and the superseded generation's drops
//     commit with the root or not at all: a failed commit leaves no claim
//     behind and drops none, and a crash can neither leave a version
//     unclaimed nor a discarded one claimed;
//   - a drop of the space's last claim on a digest queues the blob's
//     release in the same transaction (queue-space-shard.release); the
//     release re-checks the claim count when it runs, so every remaining
//     interleave converges.

// claimVersionID names a generation's blob_refs rows: the version's ULID
// token for versioned buckets, or "null#<seq>" for a null version — unique
// per commit, so unversioned generations never share a claim row.
func claimVersionID(versionID string, seq uint64) string {
	if versionID == registry.NullVersionID {
		return fmt.Sprintf("null#%d", seq)
	}
	return versionID
}

// addClaims records one claim per DEDUPLICATED digest for a new generation,
// for the commit to carry with the root's advance. Each claim takes over the write's pending reference to its
// blob, from refs by string(digest), so a release of the blob, which counts
// the reference or the claim, never finds neither. A digest with no
// reference is a Complete's, whose session's parts reference it until the
// teardown that follows the commit.
func (q *Queue) addClaims(tx *bucketop.Tx, st *registry.State, key, claimID string, digests []multihash.Multihash, refs map[string]taskrunner.TaskID) {
	var claims []registry.BlobClaim
	for k, d := range digestSet(digests) {
		claims = append(claims, registry.BlobClaim{Digest: d, Bucket: st.Name, ObjectKey: key, VersionID: claimID, Space: st.Space, Ref: refs[k]})
	}
	tx.ClaimBlobs(claims)
}

// dropClaims records a superseded/deleted generation's claims for the commit
// to drop with the root's advance; the drop of a space's last claim
// on a digest queues the blob's release, due the release grace after the
// commit.
func (q *Queue) dropClaims(sc *commitScope, st *registry.State, key, claimID string, digests []multihash.Multihash) {
	if len(digests) == 0 {
		return
	}
	var drops []registry.ClaimDrop
	for _, d := range digestSet(digests) {
		drops = append(drops, registry.ClaimDrop{Digest: d, Bucket: st.Name, ObjectKey: key, VersionID: claimID, Space: st.Space})
	}
	sc.tx.DropClaims(drops, q.releaseGrace)
}

// bodyDigests returns the digests of a stored body's blobs in order, loading
// its extents through store.
func bodyDigests(ctx context.Context, store blockstore.Reader, space did.DID, body msbucket.Body) ([]multihash.Multihash, error) {
	digests, err := body.Index(store, space).Digests(ctx)
	if err != nil {
		return nil, fmt.Errorf("body digests: %w", err)
	}
	return digests, nil
}

// digestSet maps each distinct digest (by its bytes) to one representative
// multihash, so callers can both test membership and recover the digest while
// processing every digest exactly once.
func digestSet(ds []multihash.Multihash) map[string]multihash.Multihash {
	s := make(map[string]multihash.Multihash, len(ds))
	for _, d := range ds {
		s[string(d)] = d
	}
	return s
}

// applyDeleteKey permanently removes one key — the unversioned bucket's
// delete: it drops the key's value (its manifest; a leaf never occurs
// on a purely-unversioned bucket, but is handled for completeness) and
// releases the (single, null) version's body blobs through the reference
// index. Missing keys (and an empty bucket) are idempotent no-ops. pre,
// when non-nil, gates the delete on If-Match / size / mod-time under the
// lock.
func (q *Queue) applyDeleteKey(ctx context.Context, sc *commitScope, key string, pre *DeletePreconditions) (cid.Cid, error) {
	tx := sc.tx
	return func() (cid.Cid, error) {
		// Empty bucket: nothing to delete. Returning cid.Undef leaves the
		// root as it is — the equivalent of "no-op success."
		if !tx.State().Root.Defined() {
			return cid.Undef, nil
		}
		t := tx.LoadTree()

		// Load the value + manifest being removed so the body blobs can be
		// released through the reference index.
		valCid, gerr := t.Get(ctx, key)
		if errors.Is(gerr, mst.ErrNotFound) {
			return cid.Undef, nil // idempotent DELETE: missing key isn't an error
		}
		if gerr != nil {
			return cid.Undef, fmt.Errorf("mst get: %w", gerr)
		}
		var val msbucket.ObjectValue
		if err := tx.Get(ctx, tx.State().Space, valCid, &val); err != nil {
			return cid.Undef, fmt.Errorf("load value: %w", err)
		}
		oldMf := val.Manifest
		// A manifest-valued key's block is the manifest itself; a leaf key
		// names it. Either way this is the root the space counted.
		oldManifest := valCid
		if val.Leaf != nil {
			var em msbucket.EnvelopedManifest
			if err := tx.Get(ctx, tx.State().Space, val.Leaf.Current.Manifest, &em); err != nil {
				return cid.Undef, fmt.Errorf("load manifest: %w", err)
			}
			oldMf = em.Manifest
			oldManifest = val.Leaf.Current.Manifest
		}

		// Preconditions (If-Match / size / mod-time) against the version
		// being removed.
		if pre != nil {
			if err := pre.check(oldMf); err != nil {
				return cid.Undef, err
			}
		}

		t2, err := t.Delete(ctx, key)
		if err != nil {
			return cid.Undef, fmt.Errorf("mst delete: %w", err)
		}
		// A manifest-valued key's block is the manifest itself — one candidate
		// covers it; a leaf key contributes the leaf block and its current
		// manifest.
		if val.Leaf != nil {
			tx.AddGCCandidate(val.Leaf.Current.Manifest)
		}
		tx.AddGCCandidate(valCid)
		oldDigests, err := bodyDigests(ctx, tx, tx.State().Space, oldMf.Body)
		if err != nil {
			return cid.Undef, err
		}
		// The removed version's claims drop with the commit; each last-claim
		// drop queues a release.
		oldVersionID := oldMf.VersionID
		if oldVersionID == "" {
			oldVersionID = registry.NullVersionID
		}
		q.dropClaims(sc, tx.State(), key, claimVersionID(oldVersionID, oldMf.Seq), oldDigests)
		// The key is gone, so the space stops counting it.
		q.register(sc, tx.State(), key, oldManifest, spaceuploadqueue.Remove)
		return t2.GetPointer(ctx, tx)
	}()
}
