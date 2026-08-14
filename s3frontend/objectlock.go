package s3frontend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/auth"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/ipfs/go-cid"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/registry"
)

// This file implements the per-version half of S3 Object Lock
// (docs/s3-object-lock.md): the four retention / legal-hold methods over the
// per-key version-state tree, the creation-time stamping helper the write
// paths use (§7), and the Head/Get lock-header echo (§8). WORM enforcement
// itself lives in versitygw's controller (auth.CheckObjectAccess), which
// consumes these methods; the backend only stores state and returns the §2
// sentinels. Bucket-level configuration is in bucket.go.

// bucketLockEnabled reports whether the bucket carries an Enabled object-lock
// configuration. The stored document is the controller's own
// auth.BucketLockConfig JSON; a document that does not parse reports disabled,
// which fails toward refusing lock operations, never toward ignoring locks
// (CheckObjectAccess parses the same bytes itself and errors loudly).
func bucketLockEnabled(st *registry.State) bool {
	if len(st.ObjectLockConfig) == 0 {
		return false
	}
	var cfg auth.BucketLockConfig
	if err := json.Unmarshal(st.ObjectLockConfig, &cfg); err != nil {
		return false
	}
	return cfg.Enabled
}

// lockStateFromHeaders builds the VersionState a version-creating write
// stamps from its x-amz-object-lock-* headers (§7), already validated by the
// controller (mode and date arrive together, future-dated, known enums). Nil
// when no header was supplied. Headers against a bucket without lock enabled
// are the "NoSpaces" variant of the §2 missing-configuration error: every
// creation-time path (PutObject, CopyObject, CreateMultipartUpload) reports
// it, while the four per-version lock methods report the spaced variant —
// the wire messages differ by one space and the conformance suite
// distinguishes them (PutObject_missing_bucket_lock vs
// GetObjectRetention_disabled_lock).
func lockStateFromHeaders(st *registry.State, mode types.ObjectLockMode, retainUntil *time.Time, hold types.ObjectLockLegalHoldStatus) (*msbucket.VersionState, error) {
	// The controller populates the retain-until pointer unconditionally
	// (object-put.go: &objLock.RetainUntilDate), so an absent header arrives
	// as a pointer to the zero time, never as nil.
	if retainUntil != nil && retainUntil.IsZero() {
		retainUntil = nil
	}
	if mode == "" && retainUntil == nil && hold == "" {
		return nil, nil
	}
	if !bucketLockEnabled(st) {
		return nil, s3err.GetAPIError(s3err.ErrMissingObjectLockConfigurationNoSpaces)
	}
	vs := &msbucket.VersionState{}
	if mode != "" {
		ret, err := json.Marshal(types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionMode(mode),
			RetainUntilDate: retainUntil,
		})
		if err != nil {
			return nil, fmt.Errorf("s3frontend: marshal retention: %w", err)
		}
		vs.Retention = ret
	}
	switch hold {
	case types.ObjectLockLegalHoldStatusOn:
		vs.LegalHold = msbucket.LegalHoldOn
	case types.ObjectLockLegalHoldStatusOff:
		vs.LegalHold = msbucket.LegalHoldOff
	}
	if vs.Empty() {
		return nil, nil // never stamp an empty state block (§4.1 rule 3)
	}
	return vs, nil
}

// resolveLockTarget runs the §6 read-side check order for a per-version lock
// operation: bucket, key existence, lock-enabled, then versionId grammar and
// resolution (via resolveVersion) and the delete-marker sentinel. Key
// existence outranks the lock-enabled check — a missing key on a bucket
// without lock is NoSuchKey, never the missing-configuration error
// (GetObjectRetention_non_existing_object pins it, matching posix).
func (b *Backend) resolveLockTarget(ctx context.Context, bucketName, key, versionID string) (*resolvedVersion, error) {
	st, err := b.reg.Get(ctx, bucketName)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return nil, err
	}
	if !st.Root.Defined() {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if _, err := mst.LoadMST(b.read, st.Space, st.Root).Get(ctx, key); err != nil {
		if errors.Is(err, mst.ErrNotFound) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		return nil, fmt.Errorf("s3frontend: mst get: %w", err)
	}
	if !bucketLockEnabled(st) {
		return nil, s3err.GetAPIError(s3err.ErrMissingObjectLockConfiguration)
	}
	rv, err := b.resolveVersion(ctx, bucketName, key, versionID)
	if err != nil {
		return nil, err
	}
	if rv.mf.DeleteMarker {
		return nil, s3err.GetAPIError(s3err.ErrMethodNotAllowed)
	}
	return rv, nil
}

// versionStateOf seeks the resolved version's VersionState block: leaf →
// state tree → block. Nil (with no error) when the key or version carries no
// explicit state — the callers' ErrNoSuchObjectLockConfiguration case.
func (b *Backend) versionStateOf(ctx context.Context, rv *resolvedVersion) (*msbucket.VersionState, error) {
	if rv.leaf == nil || rv.leaf.State == nil {
		return nil, nil
	}
	t := mst.LoadMST(b.read, rv.st.Space, *rv.leaf.State)
	scid, err := t.Get(ctx, revSeqKey(rv.node.Seq))
	if errors.Is(err, mst.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("s3frontend: state get: %w", err)
	}
	var env msbucket.EnvelopedVersionState
	if err := b.read.Get(ctx, rv.st.Space, scid, &env); err != nil {
		return nil, fmt.Errorf("s3frontend: state block get: %w", err)
	}
	return env.State, nil
}

// GetObjectRetention returns the version's stored retention document
// verbatim (§6). An expired retention is returned as stored; expiry is the
// controller's judgment.
func (b *Backend) GetObjectRetention(ctx context.Context, bucket, object, versionId string) ([]byte, error) {
	rv, err := b.resolveLockTarget(ctx, bucket, object, versionId)
	if err != nil {
		return nil, err
	}
	vs, err := b.versionStateOf(ctx, rv)
	if err != nil {
		return nil, err
	}
	if vs == nil || vs.Retention == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchObjectLockConfiguration)
	}
	return vs.Retention, nil
}

// GetObjectLegalHold reports the version's legal-hold status. Never-set is
// the §2 no-such-configuration sentinel; an explicit OFF is &false (§4.1's
// tri-valued hold).
func (b *Backend) GetObjectLegalHold(ctx context.Context, bucket, object, versionId string) (*bool, error) {
	rv, err := b.resolveLockTarget(ctx, bucket, object, versionId)
	if err != nil {
		return nil, err
	}
	vs, err := b.versionStateOf(ctx, rv)
	if err != nil {
		return nil, err
	}
	if vs == nil || vs.LegalHold == msbucket.LegalHoldUnset {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchObjectLockConfiguration)
	}
	on := vs.LegalHold == msbucket.LegalHoldOn
	return &on, nil
}

// PutObjectRetention stores the controller's retention document on the
// resolved version. Mode-transition policy (same-mode replacement,
// COMPLIANCE never weakened, governance bypass) ran in the controller before
// this is called.
func (b *Backend) PutObjectRetention(ctx context.Context, bucket, object, versionId string, retention []byte) error {
	return b.mutateVersionState(ctx, bucket, object, versionId, func(vs *msbucket.VersionState) {
		vs.Retention = retention
	})
}

// PutObjectLegalHold stores the version's legal-hold status.
func (b *Backend) PutObjectLegalHold(ctx context.Context, bucket, object, versionId string, status bool) error {
	hold := msbucket.LegalHoldOff
	if status {
		hold = msbucket.LegalHoldOn
	}
	return b.mutateVersionState(ctx, bucket, object, versionId, func(vs *msbucket.VersionState) {
		vs.LegalHold = hold
	})
}

// mutateVersionState runs one per-version state write (§6): the check order
// under the per-bucket commit lock, a read-modify-write merge of the target
// version's state block (mutate owns its fields and every other field is
// carried, §4.1 rule 3), and the leaf/state-tree/top-MST splice. A
// manifest-arm key upgrades to a leaf on its first state write (§4.1 rule 1);
// an empty merged block is elided rather than stored. The check order runs
// entirely inside the commit (a missing bucket surfaces through
// mapCommitError); key existence outranks lock-enabled, which outranks the
// versionId grammar, matching posix and the pinning conformance cases.
func (b *Backend) mutateVersionState(ctx context.Context, bucketName, key, versionID string, mutate func(*msbucket.VersionState)) error {
	err := b.txns.WithTx(ctx, bucketName, func(ctx context.Context, tx *bucketop.Tx) (cid.Cid, error) {
		st := tx.State()
		if !st.Root.Defined() {
			return cid.Undef, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		t := tx.LoadTree()
		valCid, gerr := t.Get(ctx, key)
		if errors.Is(gerr, mst.ErrNotFound) {
			return cid.Undef, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		if gerr != nil {
			return cid.Undef, fmt.Errorf("mst get: %w", gerr)
		}
		if !bucketLockEnabled(st) {
			return cid.Undef, s3err.GetAPIError(s3err.ErrMissingObjectLockConfiguration)
		}
		kind, seq := classifyVersionID(versionID)
		if kind == versionKindInvalid {
			return cid.Undef, errInvalidVersionID(versionID)
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
			node := msbucket.VersionNode{Seq: mf.Seq, VersionID: manifestVersionID(mf), Manifest: valCid}
			switch kind {
			case versionKindNull:
				if node.VersionID != registry.NullVersionID {
					return cid.Undef, s3err.GetNoSuchVersionErr(key, versionID)
				}
			case versionKindToken:
				if mf.VersionID != versionID {
					return cid.Undef, s3err.GetNoSuchVersionErr(key, versionID)
				}
			}
			if mf.DeleteMarker {
				return cid.Undef, s3err.GetAPIError(s3err.ErrMethodNotAllowed)
			}
			leaf = msbucket.ObjectLeaf{Current: node}
			targetSeq = mf.Seq
		} else {
			leaf = *val.Leaf
			targetSeq = seq
			isCurrent := false
			switch kind {
			case versionKindCurrent:
				isCurrent = true
			case versionKindNull:
				if leaf.Current.VersionID == registry.NullVersionID {
					isCurrent = true
				} else if leaf.NullSeq != 0 {
					targetSeq = leaf.NullSeq
				} else {
					return cid.Undef, s3err.GetNoSuchVersionErr(key, versionID)
				}
			case versionKindToken:
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
					return cid.Undef, s3err.GetNoSuchVersionErr(key, versionID)
				}
				pt := mst.LoadMST(tx, st.Space, *leaf.Prev)
				mfCid, err := pt.Get(ctx, revSeqKey(targetSeq))
				if errors.Is(err, mst.ErrNotFound) {
					return cid.Undef, s3err.GetNoSuchVersionErr(key, versionID)
				}
				if err != nil {
					return cid.Undef, fmt.Errorf("prev get: %w", err)
				}
				if err := tx.Get(ctx, st.Space, mfCid, &targetEm); err != nil {
					return cid.Undef, fmt.Errorf("load manifest: %w", err)
				}
				// A token names a version only when the stored id matches (§3).
				if kind == versionKindToken && targetEm.Manifest.VersionID != versionID {
					return cid.Undef, s3err.GetNoSuchVersionErr(key, versionID)
				}
			}
			if targetEm.Manifest.DeleteMarker {
				return cid.Undef, s3err.GetAPIError(s3err.ErrMethodNotAllowed)
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
			scid, err := stateTree.Get(ctx, revSeqKey(targetSeq))
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
			if err := b.gc.AddGCCandidate(ctx, oldStateCid.Bytes(), st.Name); err != nil {
				return cid.Undef, fmt.Errorf("gc candidate: %w", err)
			}
			var err error
			if stateTree, err = stateTree.Delete(ctx, revSeqKey(targetSeq)); err != nil {
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
				if err := b.gc.AddGCCandidate(ctx, oldStateCid.Bytes(), st.Name); err != nil {
					return cid.Undef, fmt.Errorf("gc candidate: %w", err)
				}
				stateTree, err = stateTree.Update(ctx, revSeqKey(targetSeq), newStateCid)
			} else {
				stateTree, err = stateTree.Add(ctx, revSeqKey(targetSeq), newStateCid, -1)
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
		if err := b.gc.AddGCCandidate(ctx, valCid.Bytes(), st.Name); err != nil {
			return cid.Undef, fmt.Errorf("gc candidate: %w", err)
		}
		newValCid, err := tx.Put(ctx, &msbucket.EnvelopedLeaf{Leaf: &leaf})
		if err != nil {
			return cid.Undef, fmt.Errorf("leaf put: %w", err)
		}
		t2, err := t.Update(ctx, key, newValCid)
		if err != nil {
			return cid.Undef, fmt.Errorf("mst update: %w", err)
		}
		return t2.GetPointer(ctx, tx)
	})
	if err != nil {
		return mapCommitError(err, "put object lock state")
	}
	return nil
}

// lockHeaderFields returns the x-amz-object-lock-* response fields for a
// resolved version (§8): the retention mode and retain-until date from the
// stored document, and the legal-hold status. All zero when the bucket has no
// lock configuration or the version carries no state, so the common read
// path costs nothing.
func (b *Backend) lockHeaderFields(ctx context.Context, rv *resolvedVersion) (types.ObjectLockMode, *time.Time, types.ObjectLockLegalHoldStatus, error) {
	if rv.leaf == nil || rv.leaf.State == nil || !bucketLockEnabled(rv.st) {
		return "", nil, "", nil
	}
	vs, err := b.versionStateOf(ctx, rv)
	if err != nil {
		return "", nil, "", err
	}
	if vs == nil {
		return "", nil, "", nil
	}
	var mode types.ObjectLockMode
	var until *time.Time
	if vs.Retention != nil {
		var ret types.ObjectLockRetention
		if err := json.Unmarshal(vs.Retention, &ret); err != nil {
			return "", nil, "", fmt.Errorf("s3frontend: parse stored retention: %w", err)
		}
		mode = types.ObjectLockMode(ret.Mode)
		until = ret.RetainUntilDate
	}
	var hold types.ObjectLockLegalHoldStatus
	switch vs.LegalHold {
	case msbucket.LegalHoldOn:
		hold = types.ObjectLockLegalHoldStatusOn
	case msbucket.LegalHoldOff:
		hold = types.ObjectLockLegalHoldStatusOff
	}
	return mode, until, hold, nil
}
