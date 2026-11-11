package bucketqueue

import (
	"strings"
	"time"

	"github.com/google/uuid"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
)

// BucketCommitTask is a queue-bucket.commit task's arguments: the changes to
// apply to the bucket's keys, in order, and the authority the tasks they
// spawn run with.
type BucketCommitTask struct {
	// Bucket is the bucket's name.
	Bucket string
	// Changes are applied in order, each to the root the one before left.
	Changes []KeyChange
	// Release is the authority the releases of dropped claims remove their
	// blobs with.
	Release registry.ReleaseTask
	// RegisterAdd and RegisterRemove are the /upload/add and /upload/remove
	// proofs the upload registrations a commit queues run with; empty when
	// the request held none, and the registration is then skipped.
	RegisterAdd    []byte
	RegisterRemove []byte
}

// KeyChange is one change to one key: exactly one of Put, Delete and
// SetAttributes is set.
type KeyChange struct {
	// Key is the object key.
	Key string
	// Put adds a version: an upload, a copy, a completed multipart upload
	// or a delete marker.
	Put *PutChange
	// Delete removes a version, or the key's current one.
	Delete *DeleteChange
	// SetAttributes sets a version's retention, legal hold or tags.
	SetAttributes *SetAttributesChange
}

// PutChange adds a version of the key, numbered and superseding per the
// bucket's versioning state at the commit.
type PutChange struct {
	// Manifest is the new version's manifest, without its seq, version id
	// or (when HasBlobs is set) blob list, which the commit fills in.
	Manifest *msbucket.ObjectManifest
	// HasBlobs says Blobs lists the body's blobs, which the commit writes
	// as the manifest's blob list; otherwise the manifest's own body is
	// claimed.
	HasBlobs bool
	// Blobs are the body's blobs in order, up to msbucket.MaxBodyBlobs
	// (8,389,632), the most one body holds; the codec's default cap is far
	// lower.
	Blobs []msbucket.BlobRef `cborgen:"maxlen=8389632"`
	// Refs are the write's pending references to the blobs, which the
	// commit's claims take over: at most one per blob.
	Refs []BlobRefTask `cborgen:"maxlen=8389632"`
	// InitState is the version's creation-time state (lock and tags), or
	// nil.
	InitState *msbucket.VersionState
	// Reserved is what a multipart Complete reported of its version before
	// the commit (versionReservation), or nil.
	Reserved *ReservedVersion
	// IfMatch and IfNoneMatch are the write's If-Match and If-None-Match,
	// checked against the version it would supersede.
	HasIfMatch     bool
	IfMatch        string
	HasIfNoneMatch bool
	IfNoneMatch    string
	// DeletePre are a delete marker's preconditions against the version it
	// supersedes; nil for none.
	DeletePre *DeletePreconditions
	// Completes marks a Complete's put, whose If-Match on a key with no
	// object is NoSuchKey rather than PreconditionFailed.
	Completes bool
}

// BlobRefTask names the pending reference a write holds on one blob: the ID
// of the queue-space-shard.add that took it.
type BlobRefTask struct {
	// Digest is the blob's.
	Digest []byte
	// Ref is the add's task ID.
	Ref []byte
}

// ReservedVersion is a reserved seq and the version id minted from it, or,
// with VersionID empty, a Complete that reported no id.
type ReservedVersion struct {
	// Seq is the reserved ordinal, or 0 with no id.
	Seq uint64
	// VersionID is the id reported, or "".
	VersionID string
}

// DeleteChange removes a version of the key.
type DeleteChange struct {
	// VersionID is the version to remove permanently; empty removes the
	// current one as the bucket's versioning state at the commit says: a
	// delete marker when versioning is configured, the key otherwise.
	VersionID string
	// Pre are the delete's preconditions against the version it removes or
	// supersedes; nil for none.
	Pre *DeletePreconditions
	// HasETag requires the key's current version to exist with ETag
	// (DeleteObjects' per-key ETag), or the delete fails with
	// PreconditionFailed.
	HasETag bool
	ETag    string
}

// DeletePreconditions are a delete's If-Match, If-Match-Size and
// If-Match-Last-Modified-Time.
type DeletePreconditions struct {
	// HasIfMatch and IfMatch are If-Match.
	HasIfMatch bool
	IfMatch    string
	// HasSize and Size are If-Match-Size.
	HasSize bool
	Size    int64
	// HasModTime and ModTime (Unix nanoseconds) are
	// If-Match-Last-Modified-Time.
	HasModTime bool
	ModTime    int64
}

// SetAttributesChange sets fields of a version's state, carrying the rest.
type SetAttributesChange struct {
	// VersionID is the version, empty for the current one.
	VersionID string
	// RequireLock refuses the change on a bucket without object lock.
	RequireLock bool
	// SetRetention replaces the retention document with Retention.
	SetRetention bool
	Retention    []byte
	// SetLegalHold replaces the legal hold with LegalHold.
	SetLegalHold bool
	LegalHold    uint8
	// SetTags replaces the tag set with Tags; an empty set clears it.
	SetTags bool
	Tags    map[string]string
}

// BucketCommitResult is a queue-bucket.commit's result.
type BucketCommitResult struct {
	// Versioning is the bucket's versioning state the commit applied its
	// changes under.
	Versioning string
	// Results are the changes' outcomes, in order.
	Results []ChangeResult
}

// ChangeResult is one change's outcome.
type ChangeResult struct {
	// VersionID and Seq are the new or affected version's.
	VersionID string
	Seq       uint64
	// Found is set when a delete removed a version.
	Found bool
	// DeleteMarker is set when a delete added a marker, or removed one.
	DeleteMarker bool
	// Err is the change's own failure; nil when it applied.
	Err *ChangeError
}

// check evaluates the preconditions against the version a delete removes
// or supersedes, as S3 does: If-Match "*" matches any version, and the
// modification time compares to the second.
func (p *DeletePreconditions) check(mf *msbucket.ObjectManifest) error {
	if p.HasIfMatch && p.IfMatch != "*" && p.IfMatch != strings.Trim(mf.QuotedETag(), `"`) {
		return preconditionFailed(ConditionIfMatch)
	}
	if p.HasModTime && time.Unix(0, p.ModTime).Unix() != mf.Created {
		return preconditionFailed(ConditionIfMatchLastModTime)
	}
	if p.HasSize && p.Size != mf.Body.Size {
		return preconditionFailed(ConditionIfMatchSize)
	}
	return nil
}

// refMap is the map of a put's pending references by digest.
func refMap(refs []BlobRefTask) (map[string]taskrunner.TaskID, error) {
	out := make(map[string]taskrunner.TaskID, len(refs))
	for _, r := range refs {
		id, err := uuid.FromBytes(r.Ref)
		if err != nil {
			return nil, err
		}
		out[string(r.Digest)] = id
	}
	return out, nil
}

// putBlobs is a put's blob list, or nil when it claims its manifest's body.
func (p *PutChange) putBlobs() msbucket.BlobRefs {
	if !p.HasBlobs {
		return nil
	}
	if p.Blobs == nil {
		return msbucket.BlobRefs{}
	}
	return msbucket.BlobRefs(p.Blobs)
}

// etagsEqual compares two ETags, quoted or not.
func etagsEqual(a, b string) bool {
	return strings.Trim(a, `"`) == strings.Trim(b, `"`)
}
