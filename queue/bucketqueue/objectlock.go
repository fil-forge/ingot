package bucketqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fil-forge/versitygw/auth"
	"github.com/ipfs/go-cid"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/registry"
)

// applySetAttributes sets the fields set names on the version's state block,
// carrying the rest (docs/s3-object-lock.md §6). Key existence outranks
// lock-enabled, which outranks the version id's grammar, matching posix and
// the pinning conformance cases.
func (q *Queue) applySetAttributes(ctx context.Context, sc *commitScope, key string, set *SetAttributesChange) (cid.Cid, error) {
	tx := sc.tx
	versionID, requireLock := set.VersionID, set.RequireLock
	mutate := func(vs *msbucket.VersionState) {
		if set.SetRetention {
			vs.Retention = set.Retention
		}
		if set.SetLegalHold {
			vs.LegalHold = set.LegalHold
		}
		if set.SetTags {
			vs.Tags = set.Tags
			if len(vs.Tags) == 0 {
				vs.Tags = nil
			}
		}
	}
	return func() (cid.Cid, error) {
		st := tx.State()
		if !st.Root.Defined() {
			return cid.Undef, errNoSuchKey
		}
		t := tx.LoadTree()
		valCid, gerr := t.Get(ctx, key)
		if errors.Is(gerr, mst.ErrNotFound) {
			return cid.Undef, errNoSuchKey
		}
		if gerr != nil {
			return cid.Undef, fmt.Errorf("mst get: %w", gerr)
		}
		if requireLock && !LockEnabled(st) {
			return cid.Undef, errNoObjectLock
		}
		kind, seq := ClassifyVersionID(versionID)
		if kind == VersionKindInvalid {
			return cid.Undef, &ChangeError{Kind: KindInvalidVersionID, VersionID: versionID}
		}
		var val msbucket.ObjectValue
		if err := tx.Get(ctx, st.Space, valCid, &val); err != nil {
			return cid.Undef, fmt.Errorf("load value: %w", err)
		}

		// Locate the target version and its seq; reject markers (§6 step 5).
		var leaf msbucket.ObjectLeaf
		var targetSeq uint64
		if val.Manifest != nil {
			// Manifest-arm key: the single version either matches or the
			// named version is absent; a match upgrades the key to a leaf
			// built from the manifest's own identity fields (§4.1 rule 1).
			mf := val.Manifest
			node := msbucket.VersionNode{Seq: mf.Seq, VersionID: ManifestVersionID(mf), Manifest: valCid}
			switch kind {
			case VersionKindNull:
				if node.VersionID != registry.NullVersionID {
					return cid.Undef, noSuchVersion(key, versionID)
				}
			case VersionKindToken:
				if mf.VersionID != versionID {
					return cid.Undef, noSuchVersion(key, versionID)
				}
			}
			if mf.DeleteMarker {
				return cid.Undef, errDeleteMarker
			}
			leaf = msbucket.ObjectLeaf{Current: node}
			targetSeq = mf.Seq
		} else {
			leaf = *val.Leaf
			targetSeq = seq
			isCurrent := false
			switch kind {
			case VersionKindCurrent:
				isCurrent = true
			case VersionKindNull:
				if leaf.Current.VersionID == registry.NullVersionID {
					isCurrent = true
				} else if leaf.NullSeq != 0 {
					targetSeq = leaf.NullSeq
				} else {
					return cid.Undef, noSuchVersion(key, versionID)
				}
			case VersionKindToken:
				isCurrent = leaf.Current.Seq == seq && leaf.Current.VersionID == versionID
			}
			var targetEm msbucket.EnvelopedManifest
			if isCurrent {
				targetSeq = leaf.Current.Seq
				if err := tx.Get(ctx, st.Space, leaf.Current.Manifest, &targetEm); err != nil {
					return cid.Undef, fmt.Errorf("load manifest: %w", err)
				}
			} else {
				if leaf.Prev == nil {
					return cid.Undef, noSuchVersion(key, versionID)
				}
				pt := mst.LoadMST(tx, st.Space, *leaf.Prev)
				mfCid, err := pt.Get(ctx, RevSeqKey(targetSeq))
				if errors.Is(err, mst.ErrNotFound) {
					return cid.Undef, noSuchVersion(key, versionID)
				}
				if err != nil {
					return cid.Undef, fmt.Errorf("prev get: %w", err)
				}
				if err := tx.Get(ctx, st.Space, mfCid, &targetEm); err != nil {
					return cid.Undef, fmt.Errorf("load manifest: %w", err)
				}
				// A token names a version only when the stored id matches (§3).
				if kind == VersionKindToken && targetEm.Manifest.VersionID != versionID {
					return cid.Undef, noSuchVersion(key, versionID)
				}
			}
			if targetEm.Manifest.DeleteMarker {
				return cid.Undef, errDeleteMarker
			}
		}

		// Read-modify-write the target's state block.
		var stateTree *mst.MerkleSearchTree
		if leaf.State != nil {
			stateTree = mst.LoadMST(tx, st.Space, *leaf.State)
		}
		var vs msbucket.VersionState
		var oldStateCid cid.Cid
		if stateTree != nil {
			scid, err := stateTree.Get(ctx, RevSeqKey(targetSeq))
			switch {
			case err == nil:
				var env msbucket.EnvelopedVersionState
				if err := tx.Get(ctx, st.Space, scid, &env); err != nil {
					return cid.Undef, fmt.Errorf("load version state: %w", err)
				}
				vs = *env.State
				oldStateCid = scid
			case errors.Is(err, mst.ErrNotFound):
			default:
				return cid.Undef, fmt.Errorf("state get: %w", err)
			}
		}
		mutate(&vs)

		if vs.Empty() {
			// Elision (§4.1 rule 3): an empty block is removed, never stored.
			// Lock mutations never produce one; the branch serves operations
			// that unset (tagging's delete).
			if !oldStateCid.Defined() {
				return cid.Undef, nil // nothing stored, nothing to store
			}
			tx.AddGCCandidate(oldStateCid)
			var err error
			if stateTree, err = stateTree.Delete(ctx, RevSeqKey(targetSeq)); err != nil {
				return cid.Undef, fmt.Errorf("state delete: %w", err)
			}
			empty, err := mstEmpty(ctx, stateTree)
			if err != nil {
				return cid.Undef, fmt.Errorf("state empty check: %w", err)
			}
			if empty {
				leaf.State = nil
			} else {
				root, perr := stateTree.GetPointer(ctx, tx)
				if perr != nil {
					return cid.Undef, fmt.Errorf("state pointer: %w", perr)
				}
				leaf.State = &root
			}
		} else {
			newStateCid, err := tx.Put(ctx, &msbucket.EnvelopedVersionState{State: &vs})
			if err != nil {
				return cid.Undef, fmt.Errorf("state put: %w", err)
			}
			if stateTree == nil {
				stateTree = mst.NewEmptyMST(tx, st.Space)
			}
			if oldStateCid.Defined() {
				tx.AddGCCandidate(oldStateCid)
				stateTree, err = stateTree.Update(ctx, RevSeqKey(targetSeq), newStateCid)
			} else {
				stateTree, err = stateTree.Add(ctx, RevSeqKey(targetSeq), newStateCid, -1)
			}
			if err != nil {
				return cid.Undef, fmt.Errorf("state write: %w", err)
			}
			root, perr := stateTree.GetPointer(ctx, tx)
			if perr != nil {
				return cid.Undef, fmt.Errorf("state pointer: %w", perr)
			}
			leaf.State = &root
		}

		// Rewrite the leaf (the replaced value block is superseded) and
		// splice the top MST. Invariant 5 is untouched: no manifest and no
		// prev entry was rewritten.
		tx.AddGCCandidate(valCid)
		newValCid, err := tx.Put(ctx, &msbucket.EnvelopedLeaf{Leaf: &leaf})
		if err != nil {
			return cid.Undef, fmt.Errorf("leaf put: %w", err)
		}
		t2, err := t.Update(ctx, key, newValCid)
		if err != nil {
			return cid.Undef, fmt.Errorf("mst update: %w", err)
		}
		return t2.GetPointer(ctx, tx)
	}()
}

// LockEnabled reports whether the bucket carries an Enabled object-lock
// configuration. The stored document is the controller's own
// auth.BucketLockConfig JSON; a document that does not parse reports disabled,
// which fails toward refusing lock operations, never toward ignoring locks
// (CheckObjectAccess parses the same bytes itself and errors loudly).
func LockEnabled(st *registry.State) bool {
	if len(st.ObjectLockConfig) == 0 {
		return false
	}
	var cfg auth.BucketLockConfig
	if err := json.Unmarshal(st.ObjectLockConfig, &cfg); err != nil {
		return false
	}
	return cfg.Enabled
}
