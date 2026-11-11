package s3frontend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/s3err"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/queue/bucketqueue"
	"github.com/fil-forge/ingot/registry"
)

// This file implements the per-version half of S3 Object Lock
// (docs/s3-object-lock.md): the four retention / legal-hold methods over the
// per-key version-state tree, the creation-time stamping helper the write
// paths use (§7), and the Head/Get lock-header echo (§8). WORM enforcement
// itself lives in versitygw's controller (auth.CheckObjectAccess), which
// consumes these methods; the backend only stores state and returns the §2
// sentinels. Bucket-level configuration is in bucket.go.

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
	if !bucketqueue.LockEnabled(st) {
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

// resolveStateTarget runs the read-side check order shared by the
// per-version state operations (docs/s3-object-lock.md §6;
// docs/s3-object-tagging.md §2): bucket, key existence, the lock-enabled gate
// when the operation requires it (the lock methods do; tagging has no gate),
// then versionId grammar and resolution (via resolveVersion) and the
// delete-marker sentinel. Call through resolveLockTarget /
// resolveTagTarget, which set the gate for their operation family. Key
// existence outranks the lock-enabled check — a missing key on a bucket
// without lock is NoSuchKey, never the missing-configuration error
// (GetObjectRetention_non_existing_object pins it, matching posix).
func (b *Backend) resolveStateTarget(ctx context.Context, bucketName, key, versionID string, requireLock bool) (*resolvedVersion, error) {
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
	if requireLock && !bucketqueue.LockEnabled(st) {
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

// resolveLockTarget resolves a lock method's target: resolveStateTarget
// with the lock-enabled gate required. The tagging twin is
// resolveTagTarget (objecttag.go).
func (b *Backend) resolveLockTarget(ctx context.Context, bucketName, key, versionID string) (*resolvedVersion, error) {
	return b.resolveStateTarget(ctx, bucketName, key, versionID, true)
}

// versionStateOf seeks the resolved version's VersionState block: leaf →
// state tree → block. Nil (with no error) when the key or version carries no
// explicit state — the callers' ErrNoSuchObjectLockConfiguration case.
func (b *Backend) versionStateOf(ctx context.Context, rv *resolvedVersion) (*msbucket.VersionState, error) {
	if rv.leaf == nil || rv.leaf.State == nil {
		return nil, nil
	}
	t := mst.LoadMST(b.read, rv.st.Space, *rv.leaf.State)
	scid, err := t.Get(ctx, bucketqueue.RevSeqKey(rv.node.Seq))
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

// mutateVersionLock runs a per-version state write for the lock methods:
// mutateVersionState with the lock-enabled gate required. The tagging twin
// is mutateVersionTags (objecttag.go).
func (b *Backend) mutateVersionLock(ctx context.Context, bucketName, key string, set *bucketqueue.SetAttributesChange) error {
	set.RequireLock = true
	return b.mutateVersionState(ctx, bucketName, key, set)
}

// PutObjectRetention stores the controller's retention document on the
// resolved version. Mode-transition policy (same-mode replacement,
// COMPLIANCE never weakened, governance bypass) ran in the controller before
// this is called.
func (b *Backend) PutObjectRetention(ctx context.Context, bucket, object, versionId string, retention []byte) error {
	return b.mutateVersionLock(ctx, bucket, object, &bucketqueue.SetAttributesChange{VersionID: versionId, SetRetention: true, Retention: retention})
}

// PutObjectLegalHold stores the version's legal-hold status.
func (b *Backend) PutObjectLegalHold(ctx context.Context, bucket, object, versionId string, status bool) error {
	hold := msbucket.LegalHoldOff
	if status {
		hold = msbucket.LegalHoldOn
	}
	return b.mutateVersionLock(ctx, bucket, object, &bucketqueue.SetAttributesChange{VersionID: versionId, SetLegalHold: true, LegalHold: hold})
}

// mutateVersionState runs one per-version state write (§6): the check order
// in the bucket's commit, a read-modify-write merge of the target
// version's state block (mutate owns its fields and every other field is
// carried, §4.1 rule 3), and the leaf/state-tree/top-MST splice. A
// manifest-arm key upgrades to a leaf on its first state write (§4.1 rule 1);
// an empty merged block is elided rather than stored. Call through
// mutateVersionLock / mutateVersionTags, which set the gate for their
// operation family. The check order runs
// entirely inside the commit (a missing bucket surfaces through
// mapBucketError); key existence outranks lock-enabled, which outranks the
// versionId grammar, matching posix and the pinning conformance cases.
func (b *Backend) mutateVersionState(ctx context.Context, bucketName, key string, set *bucketqueue.SetAttributesChange) error {
	st, err := b.reg.Get(ctx, bucketName)
	if errors.Is(err, registry.ErrNotFound) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return fmt.Errorf("s3frontend: put object state: %w", err)
	}
	_, _, err = b.commitOne(ctx, st, bucketqueue.KeyChange{Key: key, SetAttributes: set}, "put object lock state")
	return err
}

// stateHeaderFields returns the state-derived response fields for a resolved
// version: the x-amz-object-lock-* trio (docs/s3-object-lock.md §8) and the
// x-amz-tagging-count (docs/s3-object-tagging.md §5, set only when the
// version carries at least one tag). One state-block fetch serves both. The
// lock fields stay gated on the bucket's lock configuration; the tag count is
// not (tagging has no gate). Keys without state skip the fetch, so the common
// read path costs nothing.
func (b *Backend) stateHeaderFields(ctx context.Context, rv *resolvedVersion) (types.ObjectLockMode, *time.Time, types.ObjectLockLegalHoldStatus, *int32, error) {
	if rv.leaf == nil || rv.leaf.State == nil {
		return "", nil, "", nil, nil
	}
	vs, err := b.versionStateOf(ctx, rv)
	if err != nil {
		return "", nil, "", nil, err
	}
	if vs == nil {
		return "", nil, "", nil, nil
	}
	var tagCount *int32
	if n := len(vs.Tags); n > 0 {
		c := int32(n)
		tagCount = &c
	}
	if !bucketqueue.LockEnabled(rv.st) {
		return "", nil, "", tagCount, nil
	}
	var mode types.ObjectLockMode
	var until *time.Time
	if vs.Retention != nil {
		var ret types.ObjectLockRetention
		if err := json.Unmarshal(vs.Retention, &ret); err != nil {
			return "", nil, "", nil, fmt.Errorf("s3frontend: parse stored retention: %w", err)
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
	return mode, until, hold, tagCount, nil
}
