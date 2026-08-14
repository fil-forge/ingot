package s3frontend

import (
	"context"

	msbucket "github.com/fil-forge/ingot/bucket"
)

// This file implements S3 object tagging (docs/s3-object-tagging.md): the
// three per-version methods over the version-state tree, as its second
// tenant beside object lock. The controller parses and validates the
// PutObjectTagging XML (tag count, key/value limits) and hands us a clean
// map; the x-amz-tagging creation-time header is stamped by the write paths
// (§4). Tagging has no bucket gate — its wrappers below run the shared
// check order without the lock-enabled step — and absence is a success: a
// version without tags answers the empty set, never a sentinel.

// resolveTagTarget resolves a tagging method's target: resolveStateTarget
// with no bucket gate (docs/s3-object-tagging.md §2).
func (b *Backend) resolveTagTarget(ctx context.Context, bucketName, key, versionID string) (*resolvedVersion, error) {
	return b.resolveStateTarget(ctx, bucketName, key, versionID, false)
}

// mutateVersionTags runs a per-version state write for the tagging methods:
// mutateVersionState with no bucket gate.
func (b *Backend) mutateVersionTags(ctx context.Context, bucketName, key, versionID string, mutate func(*msbucket.VersionState)) error {
	return b.mutateVersionState(ctx, bucketName, key, versionID, false, mutate)
}

// applyTagsIfPresent merges a creation-time tag set into a version's initial
// state (docs/s3-object-tagging.md §4). When tags are present it MUTATES vs
// in place — allocating it when nil — and returns it; when there are no tags
// it returns vs untouched. Callers pass freshly built state, never a shared
// value.
func applyTagsIfPresent(vs *msbucket.VersionState, tags map[string]string) *msbucket.VersionState {
	if len(tags) == 0 {
		return vs
	}
	if vs == nil {
		vs = &msbucket.VersionState{}
	}
	vs.Tags = tags
	return vs
}

// GetObjectTagging returns the resolved version's tag set; the empty map
// when it carries none (the controller renders an empty <TagSet/>).
func (b *Backend) GetObjectTagging(ctx context.Context, bucket, object, versionId string) (map[string]string, error) {
	rv, err := b.resolveTagTarget(ctx, bucket, object, versionId)
	if err != nil {
		return nil, err
	}
	vs, err := b.versionStateOf(ctx, rv)
	if err != nil {
		return nil, err
	}
	if vs == nil || len(vs.Tags) == 0 {
		return map[string]string{}, nil
	}
	return vs.Tags, nil
}

// PutObjectTagging replaces the resolved version's tag set (last-write-wins
// on the Tags field; lock fields are carried, §3). An empty set stores
// nothing — the elision rule removes the entry rather than keeping an empty
// block.
func (b *Backend) PutObjectTagging(ctx context.Context, bucket, object, versionId string, tags map[string]string) error {
	if len(tags) == 0 {
		tags = nil
	}
	return b.mutateVersionTags(ctx, bucket, object, versionId, func(vs *msbucket.VersionState) {
		vs.Tags = tags
	})
}

// DeleteObjectTagging clears the resolved version's tag set. Idempotent: a
// version without tags is a success no-op.
func (b *Backend) DeleteObjectTagging(ctx context.Context, bucket, object, versionId string) error {
	return b.mutateVersionTags(ctx, bucket, object, versionId, func(vs *msbucket.VersionState) {
		vs.Tags = nil
	})
}
