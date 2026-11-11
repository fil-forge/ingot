package s3frontend

import (
	"context"
	"errors"
	"fmt"

	"github.com/fil-forge/versitygw/s3err"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/internal/tracing"
	"github.com/fil-forge/ingot/mst"
	"github.com/fil-forge/ingot/queue/bucketqueue"
	"github.com/fil-forge/ingot/registry"
)

// This file resolves versions for reads over the per-key version tree of
// docs/s3-versioning.md (§6.1). The tree's writes, the write rule (§5) and
// version-scoped delete (§7.2), are queue/bucketqueue's.

// errInvalidVersionID returns the 400 InvalidArgument S3 error for a malformed
// versionId, matching versitygw's grammar (anything that is neither "null" nor
// ULID-parseable).
func errInvalidVersionID(versionID string) error {
	return s3err.GetInvalidArgumentErr(s3err.InvalidArgVersionId, versionID)
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
	return b.resolveVersionIn(ctx, st, key, versionID)
}

// resolveVersionIn is resolveVersion from the bucket state onward, for callers
// that must vet the bucket before any key lookup (the copy paths' source
// bucket). Bucket existence outranks versionId validation: a malformed id
// against a missing bucket reports NoSuchBucket, which the split preserves
// because every caller resolves the bucket first.
func (b *Backend) resolveVersionIn(ctx context.Context, st *registry.State, key, versionID string) (_ *resolvedVersion, err error) {
	ctx, span := tracing.Start(ctx, "tree.lookup")
	defer func() { tracing.End(span, err) }()

	kind, seq := bucketqueue.ClassifyVersionID(versionID)
	if kind == bucketqueue.VersionKindInvalid {
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
		rv.node = msbucket.VersionNode{Seq: val.Manifest.Seq, VersionID: bucketqueue.ManifestVersionID(val.Manifest), Manifest: valCid}
		rv.isLatest = true
		switch kind {
		case bucketqueue.VersionKindNull:
			if rv.node.VersionID != registry.NullVersionID {
				return nil, s3err.GetNoSuchVersionErr(key, versionID)
			}
		case bucketqueue.VersionKindToken:
			if val.Manifest.VersionID != versionID {
				return nil, s3err.GetNoSuchVersionErr(key, versionID)
			}
		}
		return rv, nil
	}
	rv.leaf = val.Leaf

	// Locate the requested version: the current node, or a prev-tree seek.
	switch kind {
	case bucketqueue.VersionKindCurrent:
		rv.node, rv.isLatest = rv.leaf.Current, true
	case bucketqueue.VersionKindNull:
		if rv.leaf.Current.VersionID == registry.NullVersionID {
			rv.node, rv.isLatest = rv.leaf.Current, true
			break
		}
		if rv.leaf.NullSeq == 0 {
			return nil, s3err.GetNoSuchVersionErr(key, versionID)
		}
		seq = rv.leaf.NullSeq
		fallthrough
	case bucketqueue.VersionKindToken:
		if kind == bucketqueue.VersionKindToken && rv.leaf.Current.Seq == seq {
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
		mfCid, gerr := pt.Get(ctx, bucketqueue.RevSeqKey(seq))
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
		if kind == bucketqueue.VersionKindToken && rv.mf.VersionID != versionID {
			return nil, s3err.GetNoSuchVersionErr(key, versionID)
		}
	}
	return rv, nil
}
