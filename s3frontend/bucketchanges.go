package s3frontend

import (
	"errors"
	"time"

	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
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
	// Blobs are the body's blobs in order.
	Blobs []msbucket.BlobRef
	// Refs are the write's pending references to the blobs, which the
	// commit's claims take over.
	Refs []BlobRefTask
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
	// Session is the multipart upload the version completes, or "": the
	// upload must still be completing, and is recorded completed with the
	// commit.
	Session string
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

// DeletePreconditions are DeleteObject's If-Match, If-Match-Size and
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

// ChangeError is a change's failure on its own terms, carried as the S3
// error it answers.
type ChangeError struct {
	// Kind says which error type it rebuilds as: "api",
	// "precondition", "no-such-version", "invalid-argument", "reservation"
	// or "reference-expired".
	Kind string
	// Code, Description and Status are the S3 error's.
	Code        string
	Description string
	Status      int64
	// Key and VersionID are a NoSuchVersion's.
	Key       string
	VersionID string
	// ArgName and ArgValue are an InvalidArgument's.
	ArgName  string
	ArgValue string
	// Condition is a PreconditionFailed's.
	Condition string
}

// changeError classifies err as a change's own failure, or reports false for
// one that fails the whole commit.
func changeError(err error) (*ChangeError, bool) {
	var pf s3err.PreconditionFailedError
	if errors.As(err, &pf) {
		return &ChangeError{Kind: "precondition", Code: pf.Code, Description: pf.Description, Status: int64(pf.HTTPStatusCode), Condition: string(pf.Condition)}, true
	}
	var nsv s3err.NoSuchVersionError
	if errors.As(err, &nsv) {
		return &ChangeError{Kind: "no-such-version", Key: nsv.Key, VersionID: nsv.VersionId}, true
	}
	var inv s3err.InvalidArgumentError
	if errors.As(err, &inv) {
		return &ChangeError{Kind: "invalid-argument", Description: inv.Description, ArgName: inv.ArgumentName, ArgValue: inv.ArgumentValue}, true
	}
	var api s3err.APIError
	if errors.As(err, &api) {
		return &ChangeError{Kind: "api", Code: api.Code, Description: api.Description, Status: int64(api.HTTPStatusCode)}, true
	}
	switch {
	case errors.Is(err, errReservationSuperseded):
		return &ChangeError{Kind: "reservation"}, true
	case errors.Is(err, registry.ErrReferenceExpired):
		return &ChangeError{Kind: "reference-expired"}, true
	}
	return nil, false
}

// err rebuilds the error the change failed with.
func (e *ChangeError) err() error {
	switch e.Kind {
	case "precondition":
		return s3err.PreconditionFailedError{APIError: s3err.APIError{Code: e.Code, Description: e.Description, HTTPStatusCode: int(e.Status)}, Condition: s3err.Condition(e.Condition)}
	case "no-such-version":
		return s3err.GetNoSuchVersionErr(e.Key, e.VersionID)
	case "invalid-argument":
		return s3err.InvalidArgumentError{Description: e.Description, ArgumentName: e.ArgName, ArgumentValue: e.ArgValue}
	case "reservation":
		return errReservationSuperseded
	case "reference-expired":
		return registry.ErrReferenceExpired
	}
	return s3err.APIError{Code: e.Code, Description: e.Description, HTTPStatusCode: int(e.Status)}
}

// deletePreconditions converts S3's delete preconditions for a change;
// nil for none.
func deletePreconditions(p *backend.ObjectDeletePreconditions) *DeletePreconditions {
	if p == nil || (p.IfMatch == nil && p.IfMatchSize == nil && p.IfMatchLastModTime == nil) {
		return nil
	}
	var out DeletePreconditions
	if p.IfMatch != nil {
		out.HasIfMatch, out.IfMatch = true, *p.IfMatch
	}
	if p.IfMatchSize != nil {
		out.HasSize, out.Size = true, *p.IfMatchSize
	}
	if p.IfMatchLastModTime != nil {
		out.HasModTime, out.ModTime = true, p.IfMatchLastModTime.UnixNano()
	}
	return &out
}

// preconditions converts a change's delete preconditions back for
// backend.EvaluateObjectDeletePreconditions.
func (p *DeletePreconditions) preconditions() backend.ObjectDeletePreconditions {
	var out backend.ObjectDeletePreconditions
	if p.HasIfMatch {
		v := p.IfMatch
		out.IfMatch = &v
	}
	if p.HasSize {
		v := p.Size
		out.IfMatchSize = &v
	}
	if p.HasModTime {
		v := time.Unix(0, p.ModTime)
		out.IfMatchLastModTime = &v
	}
	return out
}

// blobRefTasks converts a write's pending references for a change.
func blobRefTasks(refs map[string]taskrunner.TaskID) []BlobRefTask {
	out := make([]BlobRefTask, 0, len(refs))
	for d, id := range refs {
		out = append(out, BlobRefTask{Digest: []byte(d), Ref: append([]byte(nil), id[:]...)})
	}
	return out
}

// refMap is blobRefTasks' inverse.
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
