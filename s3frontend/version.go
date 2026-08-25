package s3frontend

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/oklog/ulid/v2"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/registry"
)

// This file implements the per-key version-tree design of
// docs/s3-versioning.md: the version_id token (§3), the two-form value
// (manifest vs leaf under the value union, §2.1) and prev-tree helpers,
// version resolution for reads (§6.1), the write rule (§5), and
// version-scoped delete (§7.2).

// mintVersionID renders a numbered version's client id: a ULID whose 48-bit
// timestamp is the mint time and whose 80-bit entropy is 16 zero bits followed
// by the big-endian seq. Uniqueness within a bucket comes from seq alone; the
// timestamp is cosmetic (AWS-shaped ids).
func mintVersionID(seq uint64) string {
	var id ulid.ULID
	_ = id.SetTime(ulid.Timestamp(time.Now()))
	binary.BigEndian.PutUint64(id[8:], seq)
	return id.String()
}

// versionIDKind classifies a client-supplied versionId per the §3 grammar.
type versionIDKind int

const (
	versionKindCurrent versionIDKind = iota // absent: resolve the current version
	versionKindNull                         // the literal "null"
	versionKindToken                        // a well-formed ULID token (seq = locator hint)
	versionKindInvalid                      // malformed → 400 InvalidArgument
)

// classifyVersionID parses a versionId. For versionKindToken, seq is the
// candidate ordinal extracted from the token's low 64 entropy bits — a locator
// hint only: resolution must confirm the stored VersionID equals the token
// before treating the version as found.
func classifyVersionID(versionID string) (kind versionIDKind, seq uint64) {
	if versionID == "" {
		return versionKindCurrent, 0
	}
	if versionID == registry.NullVersionID {
		return versionKindNull, 0
	}
	id, err := ulid.ParseStrict(versionID)
	if err != nil {
		return versionKindInvalid, 0
	}
	return versionKindToken, binary.BigEndian.Uint64(id[8:])
}

// errInvalidVersionID returns the 400 InvalidArgument S3 error for a malformed
// versionId, matching versitygw's grammar (anything that is neither "null" nor
// ULID-parseable).
func errInvalidVersionID(versionID string) error {
	return s3err.GetInvalidArgumentErr(s3err.InvalidArgVersionId, versionID)
}

// revSeqKey renders a prev-tree key: the fixed-width lowercase hex of the
// bitwise-inverted seq, so the forward-only MST walk yields versions
// newest-first (larger seq → smaller key). The fixed width also makes
// key+"\x01" a strictly-greater seek bound (see the ListObjectVersions
// resumption walk).
func revSeqKey(seq uint64) string {
	return fmt.Sprintf("%016x", ^seq)
}

// mstEmpty reports whether a (possibly just-mutated, in-memory) MST has no
// leaves.
func mstEmpty(ctx context.Context, t *mst.MerkleSearchTree) (bool, error) {
	found := false
	err := t.WalkLeavesFrom(ctx, "", func(string, cid.Cid) error {
		found = true
		return mst.ErrStopWalk
	})
	if err != nil {
		return false, err
	}
	return !found, nil
}

// prevHead returns the newest entry of a prev tree (the first leaf of the
// newest-first walk). ok is false when the tree is empty.
func prevHead(ctx context.Context, t *mst.MerkleSearchTree) (mfCid cid.Cid, ok bool, err error) {
	werr := t.WalkLeavesFrom(ctx, "", func(_ string, v cid.Cid) error {
		mfCid, ok = v, true
		return mst.ErrStopWalk
	})
	if werr != nil {
		return cid.Undef, false, werr
	}
	return mfCid, ok, nil
}

// resolvedVersion is the outcome of resolving (bucket, key, versionId) through
// the key's value block: the version's node identity, its manifest, and
// whether it is the key's current version. leaf is nil for a
// manifest-valued (single-version) key (§2.1).
type resolvedVersion struct {
	st       *registry.State
	leaf     *msbucket.ObjectLeaf
	node     msbucket.VersionNode
	mf       *msbucket.ObjectManifest
	isLatest bool
}

// manifestVersionID returns a manifest-valued key's version id for matching and
// blob_refs rows: the null sentinel when the block predates versioning
// (VersionID "", which §10 rules out but the fallback keeps harmless).
func manifestVersionID(mf *msbucket.ObjectManifest) string {
	if mf.VersionID == "" {
		return registry.NullVersionID
	}
	return mf.VersionID
}

// versioned reports whether the bucket has a versioning configuration, i.e.
// whether version ids belong in responses (§4.3).
func (rv *resolvedVersion) versioned() bool {
	return rv.st.Versioning.Configured()
}

// resolveVersion implements §6.1: registry → top MST → value block → for a
// manifest-valued key the manifest answers every versionId class; for a leaf key,
// (current | null | prev-tree seek). Maps missing bucket/key to the S3
// errors; a versionId that is well-formed but names no live version is
// ErrNoSuchVersion; a malformed one is 400 InvalidArgument. Delete-marker
// semantics (404 vs 405) are the caller's — the marker's manifest is
// returned like any other version's.
func (b *Backend) resolveVersion(ctx context.Context, bucketName, key, versionID string) (*resolvedVersion, error) {
	st, err := b.reg.Get(ctx, bucketName)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return nil, err
	}
	// Bucket existence outranks versionId validation: a malformed id against a
	// missing bucket reports NoSuchBucket.
	kind, seq := classifyVersionID(versionID)
	if kind == versionKindInvalid {
		return nil, errInvalidVersionID(versionID)
	}
	if !st.Root.Defined() {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	t := mst.LoadMST(b.read, st.Space, st.Root)
	valCid, err := t.Get(ctx, key)
	if errors.Is(err, mst.ErrNotFound) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if err != nil {
		return nil, fmt.Errorf("s3frontend: mst get: %w", err)
	}
	rv := &resolvedVersion{st: st}
	var val msbucket.ObjectValue
	if err := b.read.Get(ctx, st.Space, valCid, &val); err != nil {
		return nil, fmt.Errorf("s3frontend: value get: %w", err)
	}

	if val.Manifest != nil {
		// Manifest-valued key (§2.1): it answers every versionId class.
		rv.mf = val.Manifest
		rv.node = msbucket.VersionNode{Seq: val.Manifest.Seq, VersionID: manifestVersionID(val.Manifest), Manifest: valCid}
		rv.isLatest = true
		switch kind {
		case versionKindNull:
			if rv.node.VersionID != registry.NullVersionID {
				return nil, s3err.GetNoSuchVersionErr(key, versionID)
			}
		case versionKindToken:
			if val.Manifest.VersionID != versionID {
				return nil, s3err.GetNoSuchVersionErr(key, versionID)
			}
		}
		return rv, nil
	}
	rv.leaf = val.Leaf

	// Locate the requested version: the current node, or a prev-tree seek.
	switch kind {
	case versionKindCurrent:
		rv.node, rv.isLatest = rv.leaf.Current, true
	case versionKindNull:
		if rv.leaf.Current.VersionID == registry.NullVersionID {
			rv.node, rv.isLatest = rv.leaf.Current, true
			break
		}
		if rv.leaf.NullSeq == 0 {
			return nil, s3err.GetNoSuchVersionErr(key, versionID)
		}
		seq = rv.leaf.NullSeq
		fallthrough
	case versionKindToken:
		if kind == versionKindToken && rv.leaf.Current.Seq == seq {
			if rv.leaf.Current.VersionID != versionID {
				return nil, s3err.GetNoSuchVersionErr(key, versionID)
			}
			rv.node, rv.isLatest = rv.leaf.Current, true
			break
		}
		if rv.leaf.Prev == nil {
			return nil, s3err.GetNoSuchVersionErr(key, versionID)
		}
		pt := mst.LoadMST(b.read, st.Space, *rv.leaf.Prev)
		mfCid, gerr := pt.Get(ctx, revSeqKey(seq))
		if errors.Is(gerr, mst.ErrNotFound) {
			return nil, s3err.GetNoSuchVersionErr(key, versionID)
		}
		if gerr != nil {
			return nil, fmt.Errorf("s3frontend: prev get: %w", gerr)
		}
		rv.node = msbucket.VersionNode{Seq: seq, Manifest: mfCid}
	}

	var em msbucket.EnvelopedManifest
	if err := b.read.Get(ctx, st.Space, rv.node.Manifest, &em); err != nil {
		return nil, fmt.Errorf("s3frontend: manifest get: %w", err)
	}
	rv.mf = em.Manifest
	if !rv.isLatest {
		// A prev entry's id lives on its manifest (the tree key only carries
		// the seq). Confirm a token before trusting the locator hint (§3).
		rv.node.VersionID = rv.mf.VersionID
		if kind == versionKindToken && rv.mf.VersionID != versionID {
			return nil, s3err.GetNoSuchVersionErr(key, versionID)
		}
	}
	return rv, nil
}

// discardedVersion records a version permanently removed by a commit, for the
// post-commit reference-index release (§8).
type discardedVersion struct {
	versionID string
	digests   []multihash.Multihash
}

// commitVersion runs the §5 write rule for one new version (a PutObject /
// CopyObject / CompleteMultipartUpload manifest, or a delete marker): it
// allocates the seq under the per-bucket critical section, stamps mf with
// Seq/VersionID, supersedes the current version per the bucket's versioning
// state, splices the rebuilt value (the manifest itself until the key's
// first retained supersession creates its leaf, §2.1/§5.2), and — after the
// commit is durable — reconciles the reference index (claims for the new
// version; releases for discarded ones). preCheck, when non-nil, runs under
// the lock against the superseded current manifest (nil when the key is new)
// before anything is written, so conditional mutations are race-safe.
//
// initState, when non-nil, is the new version's creation-time lock state
// (the x-amz-object-lock-* headers): it is stamped into the key's
// version-state tree in this same commit, forcing the leaf form for a new
// key, so the version and its protections land in one root swap
// (docs/s3-object-lock.md §7).
//
// The returned VersioningState is the bucket state read UNDER the lock — the
// state that actually decided the version's id. Response shaping must gate on
// it, not on the caller's pre-lock snapshot, so a racing PutBucketVersioning
// can't mint an id the response then omits.
func (b *Backend) commitVersion(ctx context.Context, bucketState *registry.State, key string, mf *msbucket.ObjectManifest, initState *msbucket.VersionState, preCheck func(superseded *msbucket.ObjectManifest) error) (msbucket.VersionNode, registry.VersioningState, error) {
	var node msbucket.VersionNode
	var discards []discardedVersion
	var effState registry.VersioningState
	err := b.txns.WithTx(ctx, bucketState.Name, func(ctx context.Context, tx *bucketop.Tx) (cid.Cid, error) {
		discards = nil
		st := tx.State()
		effState = st.Versioning

		// The ordinal is allocated inside the critical section so seq order
		// matches commit order per bucket; a failed commit leaves a harmless gap.
		seq, err := b.reg.AllocVersionSeq(ctx, st.Name)
		if err != nil {
			return cid.Undef, fmt.Errorf("alloc version seq: %w", err)
		}
		vid := registry.NullVersionID
		if st.Versioning == registry.VersioningEnabled {
			vid = mintVersionID(seq)
		}
		mf.Seq, mf.VersionID = seq, vid
		mfCid, err := tx.Put(ctx, msbucket.ManifestValue(mf))
		if err != nil {
			return cid.Undef, fmt.Errorf("manifest put: %w", err)
		}

		t := tx.LoadTree()
		var oldLeaf *msbucket.ObjectLeaf
		var superseded *msbucket.VersionNode // nil when the key is new
		var supersededMf *msbucket.ObjectManifest
		oldValCid, gerr := t.Get(ctx, key)
		switch {
		case gerr == nil:
			var val msbucket.ObjectValue
			if err := tx.Get(ctx, st.Space, oldValCid, &val); err != nil {
				return cid.Undef, fmt.Errorf("load prior value: %w", err)
			}
			if val.Leaf != nil {
				oldLeaf = val.Leaf
				var em msbucket.EnvelopedManifest
				if err := tx.Get(ctx, st.Space, oldLeaf.Current.Manifest, &em); err != nil {
					return cid.Undef, fmt.Errorf("load prior manifest: %w", err)
				}
				supersededMf = em.Manifest
				superseded = &oldLeaf.Current
			} else {
				// Manifest-valued key (§2.1): the value block is the manifest.
				supersededMf = val.Manifest
				superseded = &msbucket.VersionNode{
					Seq:       val.Manifest.Seq,
					VersionID: manifestVersionID(val.Manifest),
					Manifest:  oldValCid,
				}
			}
		case errors.Is(gerr, mst.ErrNotFound):
			// new key — no supersession
		default:
			return cid.Undef, fmt.Errorf("mst get prior: %w", gerr)
		}

		// Race-safe re-check of the caller's preconditions against the current
		// state under the lock.
		if preCheck != nil {
			if err := preCheck(supersededMf); err != nil {
				return cid.Undef, err
			}
		}

		newNode := msbucket.VersionNode{Seq: seq, VersionID: vid, Manifest: mfCid}
		var newLeaf *msbucket.ObjectLeaf // nil → the value stays the manifest
		if oldLeaf != nil {
			// State (the version-state tree) carries across supersession:
			// retained versions keep their lock state (docs/s3-object-lock.md §4.1).
			newLeaf = &msbucket.ObjectLeaf{Current: newNode, NullSeq: oldLeaf.NullSeq, State: oldLeaf.State}
		}
		var prevTree *mst.MerkleSearchTree
		var discardSeqs []uint64 // discarded versions' seqs, for state-entry cleanup (§9)
		retained, evictedNull := false, false
		if superseded != nil {
			if oldLeaf != nil && oldLeaf.Prev != nil {
				prevTree = mst.LoadMST(tx, st.Space, *oldLeaf.Prev)
			}

			// Supersession (§5.2): Enabled retains everything; Suspended and
			// Unversioned replace a null current in place.
			retain := st.Versioning == registry.VersioningEnabled ||
				superseded.VersionID != registry.NullVersionID
			retained = retain
			if retain {
				if newLeaf == nil {
					// First supersession: the key gains its leaf (§5.2)
					// and keeps it from now on (invariant 6).
					newLeaf = &msbucket.ObjectLeaf{Current: newNode}
				}
				if prevTree == nil {
					prevTree = mst.NewEmptyMST(tx, st.Space)
				}
				prevTree, err = prevTree.Add(ctx, revSeqKey(superseded.Seq), superseded.Manifest, -1)
				if err != nil {
					return cid.Undef, fmt.Errorf("prev add: %w", err)
				}
				if superseded.VersionID == registry.NullVersionID {
					newLeaf.NullSeq = superseded.Seq
				}
			} else {
				// Discard (null over null). A manifest-valued key stays one; its value
				// block is the manifest queued for GC just below.
				discards = append(discards, discardedVersion{superseded.VersionID, bodyDigests(supersededMf.Body)})
				discardSeqs = append(discardSeqs, superseded.Seq)
				if err := b.gc.AddGCCandidate(ctx, superseded.Manifest.Bytes(), st.Name); err != nil {
					return cid.Undef, fmt.Errorf("gc candidate: %w", err)
				}
			}

			// Only one null per key: a new null evicts a noncurrent null (§2.4).
			// Only a leaf key can hold one. NullSeq is cleared unconditionally —
			// a stale NullSeq (missing prev entry, or no prev tree at all)
			// self-heals here instead of propagating beside a null Current.
			if newLeaf != nil && vid == registry.NullVersionID && newLeaf.NullSeq != 0 {
				if prevTree != nil {
					nullKey := revSeqKey(newLeaf.NullSeq)
					nullCid, nerr := prevTree.Get(ctx, nullKey)
					switch {
					case nerr == nil:
						var nullEm msbucket.EnvelopedManifest
						if err := tx.Get(ctx, st.Space, nullCid, &nullEm); err != nil {
							return cid.Undef, fmt.Errorf("load prev null manifest: %w", err)
						}
						discards = append(discards, discardedVersion{registry.NullVersionID, bodyDigests(nullEm.Manifest.Body)})
						discardSeqs = append(discardSeqs, newLeaf.NullSeq)
						if prevTree, err = prevTree.Delete(ctx, nullKey); err != nil {
							return cid.Undef, fmt.Errorf("prev delete null: %w", err)
						}
						if err := b.gc.AddGCCandidate(ctx, nullCid.Bytes(), st.Name); err != nil {
							return cid.Undef, fmt.Errorf("gc candidate: %w", err)
						}
						evictedNull = true
					case errors.Is(nerr, mst.ErrNotFound):
						// NullSeq stale (shouldn't happen); nothing to evict.
					default:
						return cid.Undef, fmt.Errorf("prev get null: %w", nerr)
					}
				}
				newLeaf.NullSeq = 0
			}

			// The replaced leaf block is superseded; a manifest-valued key's
			// old block either lives on as a prev entry or was queued for GC
			// on the discard path above.
			if oldLeaf != nil {
				if err := b.gc.AddGCCandidate(ctx, oldValCid.Bytes(), st.Name); err != nil {
					return cid.Undef, fmt.Errorf("gc candidate: %w", err)
				}
			}
		}

		if prevTree != nil {
			// The emptiness walk is only needed when this commit deleted from
			// the tree without inserting (a null eviction with nothing
			// retained); otherwise the tree is provably non-empty — an Add
			// just ran, or the tree was loaded from a non-nil Prev.
			empty := false
			if evictedNull && !retained {
				var eerr error
				if empty, eerr = mstEmpty(ctx, prevTree); eerr != nil {
					return cid.Undef, fmt.Errorf("prev empty check: %w", eerr)
				}
			}
			if !empty {
				proot, err := prevTree.GetPointer(ctx, tx)
				if err != nil {
					return cid.Undef, fmt.Errorf("prev pointer: %w", err)
				}
				newLeaf.Prev = &proot
			}
		}

		// Version-state maintenance (docs/s3-object-lock.md §7, §9): prune
		// the state entries of versions this commit discarded — unreachable
		// for lock state (§5's guard), covered for future tenants — then
		// stamp the new version's creation-time lock state in this same
		// commit. The common write has neither and skips both.
		if newLeaf != nil {
			for _, dseq := range discardSeqs {
				var perr error
				if newLeaf.State, perr = b.pruneVersionState(ctx, tx, st, newLeaf.State, dseq); perr != nil {
					return cid.Undef, perr
				}
			}
		}
		if initState != nil {
			if newLeaf == nil {
				// Creation-time lock state forces the leaf form (§7).
				newLeaf = &msbucket.ObjectLeaf{Current: newNode}
			}
			stateTree := mst.NewEmptyMST(tx, st.Space)
			if newLeaf.State != nil {
				stateTree = mst.LoadMST(tx, st.Space, *newLeaf.State)
			}
			scid, serr := tx.Put(ctx, &msbucket.EnvelopedVersionState{State: initState})
			if serr != nil {
				return cid.Undef, fmt.Errorf("state put: %w", serr)
			}
			if stateTree, serr = stateTree.Add(ctx, revSeqKey(seq), scid, -1); serr != nil {
				return cid.Undef, fmt.Errorf("state add: %w", serr)
			}
			sroot, serr := stateTree.GetPointer(ctx, tx)
			if serr != nil {
				return cid.Undef, fmt.Errorf("state pointer: %w", serr)
			}
			newLeaf.State = &sroot
		}

		// The new value: the enveloped leaf when the key has (or now gains)
		// one, else the manifest CID (§2.1).
		newValCid := mfCid
		if newLeaf != nil {
			newValCid, err = tx.Put(ctx, &msbucket.EnvelopedLeaf{Leaf: newLeaf})
			if err != nil {
				return cid.Undef, fmt.Errorf("leaf put: %w", err)
			}
		}
		var t2 *mst.MerkleSearchTree
		if superseded != nil {
			t2, err = t.Update(ctx, key, newValCid)
		} else {
			t2, err = t.Add(ctx, key, newValCid, -1)
		}
		if err != nil {
			return cid.Undef, fmt.Errorf("mst write: %w", err)
		}
		node = newNode
		return t2.GetPointer(ctx, tx)
	})
	if err != nil {
		return node, effState, mapCommitError(err, "commit version")
	}

	// Reference index, after the commit is durable (§8): claim the new
	// version's blobs; release discarded versions'. A discard sharing the new
	// version's id (null replacing null — the only same-id case) goes through
	// the set-diff so unchanged digests never churn.
	newDigests := bodyDigests(mf.Body)
	var sameIDOld []multihash.Multihash
	var toRemove []multihash.Multihash
	for _, d := range discards {
		if d.versionID == node.VersionID {
			sameIDOld = append(sameIDOld, d.digests...)
			continue
		}
		r, err := b.reconcileClaims(ctx, bucketState, key, d.versionID, d.digests, nil)
		if err != nil {
			return node, effState, fmt.Errorf("s3frontend: commit reconcile: %w", err)
		}
		toRemove = append(toRemove, r...)
	}
	r, err := b.reconcileClaims(ctx, bucketState, key, node.VersionID, sameIDOld, newDigests)
	if err != nil {
		return node, effState, fmt.Errorf("s3frontend: commit reconcile: %w", err)
	}
	toRemove = append(toRemove, r...)
	b.releaseBlobs(ctx, bucketState.Space, toRemove)
	return node, effState, nil
}

// scopedDeleteResult reports a version-scoped delete: whether a version was
// actually removed and whether it was a delete marker (§7.2). An absent
// version is a success no-op (found=false), matching S3/versitygw.
// versioning is the bucket state read under the commit lock, for response
// shaping (§4.3).
type scopedDeleteResult struct {
	found      bool
	wasMarker  bool
	versioning registry.VersioningState
}

// deleteVersionScoped permanently removes one specific version (§7.2): the
// current version (promoting the newest prev entry, or dropping the leaf when
// none remain) or a prev entry. Claims are released after the commit; delete
// markers have none.
func (b *Backend) deleteVersionScoped(ctx context.Context, bucketState *registry.State, key, versionID string, preconds *backend.ObjectDeletePreconditions) (scopedDeleteResult, error) {
	kind, seq := classifyVersionID(versionID)
	switch kind {
	case versionKindInvalid, versionKindCurrent:
		return scopedDeleteResult{}, errInvalidVersionID(versionID)
	}

	var res scopedDeleteResult
	var removed discardedVersion
	err := b.txns.WithTx(ctx, bucketState.Name, func(ctx context.Context, tx *bucketop.Tx) (cid.Cid, error) {
		res, removed = scopedDeleteResult{}, discardedVersion{}
		st := tx.State()
		res.versioning = st.Versioning
		if !st.Root.Defined() {
			return cid.Undef, nil // no-op success: nothing to delete
		}
		t := tx.LoadTree()
		valCid, gerr := t.Get(ctx, key)
		if errors.Is(gerr, mst.ErrNotFound) {
			return cid.Undef, nil // no-op success
		}
		if gerr != nil {
			return cid.Undef, fmt.Errorf("mst get: %w", gerr)
		}
		var val msbucket.ObjectValue
		if err := tx.Get(ctx, st.Space, valCid, &val); err != nil {
			return cid.Undef, fmt.Errorf("load value: %w", err)
		}

		if val.Manifest != nil {
			// Manifest-valued key (§2.1): the single version either matches or the
			// delete is a no-op.
			mf := val.Manifest
			switch kind {
			case versionKindNull:
				if manifestVersionID(mf) != registry.NullVersionID {
					return cid.Undef, nil // no null version: no-op success
				}
			case versionKindToken:
				if mf.VersionID != versionID {
					return cid.Undef, nil // no-op success
				}
			}
			if preconds != nil && !mf.DeleteMarker {
				if err := backend.EvaluateObjectDeletePreconditions(etagOf(mf), time.Unix(mf.Created, 0), mf.Body.Size, *preconds); err != nil {
					return cid.Undef, err
				}
			}
			removed = discardedVersion{versionID: targetVersionID(kind, mf), digests: bodyDigests(mf.Body)}
			res.found, res.wasMarker = true, mf.DeleteMarker
			// The value block is the manifest — one GC candidate covers it.
			if err := b.gc.AddGCCandidate(ctx, valCid.Bytes(), st.Name); err != nil {
				return cid.Undef, fmt.Errorf("gc candidate: %w", err)
			}
			t2, err := t.Delete(ctx, key)
			if err != nil {
				return cid.Undef, fmt.Errorf("mst delete: %w", err)
			}
			return t2.GetPointer(ctx, tx)
		}
		leaf := val.Leaf

		// Locate the target: the current node or a prev-tree entry. A leaf
		// stays a leaf even when this delete leaves one version (invariant 6).
		targetSeq := seq
		isCurrent := false
		switch kind {
		case versionKindNull:
			if leaf.Current.VersionID == registry.NullVersionID {
				isCurrent = true
			} else if leaf.NullSeq != 0 {
				targetSeq = leaf.NullSeq
			} else {
				return cid.Undef, nil // no null version: no-op success
			}
		case versionKindToken:
			isCurrent = leaf.Current.Seq == seq && leaf.Current.VersionID == versionID
		}

		var prevTree *mst.MerkleSearchTree
		if leaf.Prev != nil {
			prevTree = mst.LoadMST(tx, st.Space, *leaf.Prev)
		}

		var targetCid cid.Cid
		var targetEm msbucket.EnvelopedManifest
		if isCurrent {
			targetCid = leaf.Current.Manifest
			if err := tx.Get(ctx, st.Space, targetCid, &targetEm); err != nil {
				return cid.Undef, fmt.Errorf("load manifest: %w", err)
			}
		} else {
			if prevTree == nil {
				return cid.Undef, nil // no-op success
			}
			var err error
			targetCid, err = prevTree.Get(ctx, revSeqKey(targetSeq))
			if errors.Is(err, mst.ErrNotFound) {
				return cid.Undef, nil // no-op success
			}
			if err != nil {
				return cid.Undef, fmt.Errorf("prev get: %w", err)
			}
			if err := tx.Get(ctx, st.Space, targetCid, &targetEm); err != nil {
				return cid.Undef, fmt.Errorf("load manifest: %w", err)
			}
			// A token is only found when the stored id matches it (§3).
			if kind == versionKindToken && targetEm.Manifest.VersionID != versionID {
				return cid.Undef, nil // no-op success
			}
		}
		targetMf := targetEm.Manifest

		// Preconditions (If-Match / size / mod-time) against the version being
		// removed; a marker has no ETag/body to match against.
		if preconds != nil && !targetMf.DeleteMarker {
			if err := backend.EvaluateObjectDeletePreconditions(etagOf(targetMf), time.Unix(targetMf.Created, 0), targetMf.Body.Size, *preconds); err != nil {
				return cid.Undef, err
			}
		}

		removed = discardedVersion{versionID: targetVersionID(kind, targetMf), digests: bodyDigests(targetMf.Body)}
		res.found, res.wasMarker = true, targetMf.DeleteMarker
		if err := b.gc.AddGCCandidate(ctx, targetCid.Bytes(), st.Name); err != nil {
			return cid.Undef, fmt.Errorf("gc candidate: %w", err)
		}
		if err := b.gc.AddGCCandidate(ctx, valCid.Bytes(), st.Name); err != nil {
			return cid.Undef, fmt.Errorf("gc candidate: %w", err)
		}

		// The removed version's state entry goes with it, in this commit
		// (docs/s3-object-lock.md §9). removedSeq covers all three exits.
		removedSeq := targetSeq
		if isCurrent {
			removedSeq = leaf.Current.Seq
		}

		if isCurrent {
			// Promote the newest prev entry, or drop the leaf when none remain.
			var headCid cid.Cid
			var ok bool
			if prevTree != nil {
				var err error
				headCid, ok, err = prevHead(ctx, prevTree)
				if err != nil {
					return cid.Undef, fmt.Errorf("prev head: %w", err)
				}
			}
			if !ok {
				// The key's last version: the leaf and its trees die with it;
				// prune GCs the removed version's state block.
				if _, err := b.pruneVersionState(ctx, tx, st, leaf.State, removedSeq); err != nil {
					return cid.Undef, err
				}
				t2, err := t.Delete(ctx, key)
				if err != nil {
					return cid.Undef, fmt.Errorf("mst delete: %w", err)
				}
				return t2.GetPointer(ctx, tx)
			}
			var headEm msbucket.EnvelopedManifest
			if err := tx.Get(ctx, st.Space, headCid, &headEm); err != nil {
				return cid.Undef, fmt.Errorf("load promoted manifest: %w", err)
			}
			headMf := headEm.Manifest
			var err error
			if prevTree, err = prevTree.Delete(ctx, revSeqKey(headMf.Seq)); err != nil {
				return cid.Undef, fmt.Errorf("prev delete: %w", err)
			}
			newLeaf := msbucket.ObjectLeaf{
				Current: msbucket.VersionNode{Seq: headMf.Seq, VersionID: headMf.VersionID, Manifest: headCid},
				NullSeq: leaf.NullSeq,
			}
			if headMf.Seq == leaf.NullSeq {
				newLeaf.NullSeq = 0 // the null version is current again
			}
			if newLeaf.State, err = b.pruneVersionState(ctx, tx, st, leaf.State, removedSeq); err != nil {
				return cid.Undef, err
			}
			return b.spliceLeaf(ctx, tx, t, key, newLeaf, prevTree)
		}

		// Remove a noncurrent version.
		var err error
		if prevTree, err = prevTree.Delete(ctx, revSeqKey(targetSeq)); err != nil {
			return cid.Undef, fmt.Errorf("prev delete: %w", err)
		}
		newLeaf := msbucket.ObjectLeaf{Current: leaf.Current, NullSeq: leaf.NullSeq}
		if targetSeq == leaf.NullSeq {
			newLeaf.NullSeq = 0
		}
		if newLeaf.State, err = b.pruneVersionState(ctx, tx, st, leaf.State, removedSeq); err != nil {
			return cid.Undef, err
		}
		return b.spliceLeaf(ctx, tx, t, key, newLeaf, prevTree)
	})
	if err != nil {
		return res, mapCommitError(err, "delete version")
	}
	if res.found {
		toRemove, err := b.reconcileClaims(ctx, bucketState, key, removed.versionID, removed.digests, nil)
		if err != nil {
			return res, fmt.Errorf("s3frontend: delete version reconcile: %w", err)
		}
		b.releaseBlobs(ctx, bucketState.Space, toRemove)
	}
	return res, nil
}

// pruneVersionState removes seq's entry from a leaf's version-state tree,
// queueing the removed block for GC, and returns the tree's new root: the
// input root untouched when there was no entry, nil when the tree empties.
// This is the §9 cleanup rule (docs/s3-object-lock.md): a commit that
// permanently removes a version also removes its state entry, in the same
// commit.
func (b *Backend) pruneVersionState(ctx context.Context, tx *bucketop.Tx, st *registry.State, stateRoot *cid.Cid, seq uint64) (*cid.Cid, error) {
	if stateRoot == nil {
		return nil, nil
	}
	t := mst.LoadMST(tx, st.Space, *stateRoot)
	scid, err := t.Get(ctx, revSeqKey(seq))
	if errors.Is(err, mst.ErrNotFound) {
		return stateRoot, nil
	}
	if err != nil {
		return nil, fmt.Errorf("state get: %w", err)
	}
	if err := b.gc.AddGCCandidate(ctx, scid.Bytes(), st.Name); err != nil {
		return nil, fmt.Errorf("gc candidate: %w", err)
	}
	if t, err = t.Delete(ctx, revSeqKey(seq)); err != nil {
		return nil, fmt.Errorf("state delete: %w", err)
	}
	empty, err := mstEmpty(ctx, t)
	if err != nil {
		return nil, fmt.Errorf("state empty check: %w", err)
	}
	if empty {
		return nil, nil
	}
	root, err := t.GetPointer(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("state pointer: %w", err)
	}
	return &root, nil
}

// spliceLeaf finalizes a rebuilt leaf: serializes the (possibly emptied) prev
// tree, writes the leaf block, and updates the top MST at key.
func (b *Backend) spliceLeaf(ctx context.Context, tx *bucketop.Tx, t *mst.MerkleSearchTree, key string, leaf msbucket.ObjectLeaf, prevTree *mst.MerkleSearchTree) (cid.Cid, error) {
	if prevTree != nil {
		empty, err := mstEmpty(ctx, prevTree)
		if err != nil {
			return cid.Undef, fmt.Errorf("prev empty check: %w", err)
		}
		if !empty {
			proot, err := prevTree.GetPointer(ctx, tx)
			if err != nil {
				return cid.Undef, fmt.Errorf("prev pointer: %w", err)
			}
			leaf.Prev = &proot
		}
	}
	leafCid, err := tx.Put(ctx, &msbucket.EnvelopedLeaf{Leaf: &leaf})
	if err != nil {
		return cid.Undef, fmt.Errorf("leaf put: %w", err)
	}
	t2, err := t.Update(ctx, key, leafCid)
	if err != nil {
		return cid.Undef, fmt.Errorf("mst update: %w", err)
	}
	return t2.GetPointer(ctx, tx)
}

// targetVersionID names the version a scoped delete removed for its blob_refs
// rows: "null" for the null version, else the manifest's own id. An empty
// manifest id (a pre-versioning block, which §10 rules out) falls back to the
// null sentinel — those blocks' rows were written under it — and never to
// another version's id.
func targetVersionID(kind versionIDKind, mf *msbucket.ObjectManifest) string {
	if kind == versionKindNull || mf.VersionID == "" {
		return registry.NullVersionID
	}
	return mf.VersionID
}
