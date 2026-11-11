package bucketqueue

import (
	"errors"
	"fmt"

	"github.com/fil-forge/ingot/registry"
)

// The errors a change fails with on its own terms, as ChangeError.Unwrap
// reports them. A put whose pending reference to one of its blobs expired
// before the commit unwraps to registry.ErrReferenceExpired.
var (
	// ErrPreconditionFailed is a write's precondition failing against the
	// version it would supersede or remove.
	ErrPreconditionFailed = errors.New("precondition failed")
	// ErrNoSuchKey is a change to a key with no object: an If-Match write,
	// or a state change.
	ErrNoSuchKey = errors.New("no such key")
	// ErrNoSuchVersion is a change naming a version the key does not have.
	ErrNoSuchVersion = errors.New("no such version")
	// ErrInvalidVersionID is a change naming a version by a malformed id.
	ErrInvalidVersionID = errors.New("invalid version id")
	// ErrDeleteMarker is a state change to a delete marker.
	ErrDeleteMarker = errors.New("version is a delete marker")
	// ErrNoObjectLock is a lock change on a bucket without object lock.
	ErrNoObjectLock = errors.New("bucket has no object lock configuration")
	// ErrReservationSuperseded refuses a put that can no longer be what its
	// multipart Complete reported: another write to the key committed with
	// a later seq than the reserved one, or the bucket's versioning changed
	// so the version would be null under a reported id, or numbered with
	// none reported. The report cannot change, so the write fails.
	ErrReservationSuperseded = errors.New("reserved version superseded")
)

// ErrBucketClosing fails a whole commit to a bucket a DeleteBucket has
// closed.
var ErrBucketClosing = errors.New("bucket is closing")

// The kinds of ChangeError, one per error it unwraps to.
const (
	KindPreconditionFailed    = "precondition-failed"
	KindNoSuchKey             = "no-such-key"
	KindNoSuchVersion         = "no-such-version"
	KindInvalidVersionID      = "invalid-version-id"
	KindDeleteMarker          = "delete-marker"
	KindNoObjectLock          = "no-object-lock"
	KindReservationSuperseded = "reservation-superseded"
	KindReferenceExpired      = "reference-expired"
)

var kindErrors = map[string]error{
	KindPreconditionFailed:    ErrPreconditionFailed,
	KindNoSuchKey:             ErrNoSuchKey,
	KindNoSuchVersion:         ErrNoSuchVersion,
	KindInvalidVersionID:      ErrInvalidVersionID,
	KindDeleteMarker:          ErrDeleteMarker,
	KindNoObjectLock:          ErrNoObjectLock,
	KindReservationSuperseded: ErrReservationSuperseded,
	KindReferenceExpired:      registry.ErrReferenceExpired,
}

// The preconditions a KindPreconditionFailed names, by the S3 header that
// carried each.
const (
	ConditionIfMatch            = "If-Match"
	ConditionIfNoneMatch        = "If-None-Match"
	ConditionIfMatchSize        = "If-Match-Size"
	ConditionIfMatchLastModTime = "If-Match-Last-Mod-Time"
)

// ChangeError is a change's failure on its own terms. It is stored in the
// commit's result, so it carries only what the request needs to answer.
type ChangeError struct {
	// Kind is one of the Kind constants.
	Kind string
	// Condition is the failed precondition of a KindPreconditionFailed, one
	// of the Condition constants, or empty when the answer names none.
	Condition string
	// Key is the key a KindNoSuchVersion named.
	Key string
	// VersionID is the version id a KindNoSuchVersion or
	// KindInvalidVersionID named.
	VersionID string
}

// Error describes the failure.
func (e *ChangeError) Error() string {
	switch e.Kind {
	case KindPreconditionFailed:
		if e.Condition != "" {
			return fmt.Sprintf("precondition failed: %s", e.Condition)
		}
	case KindNoSuchVersion:
		return fmt.Sprintf("no such version: %q of %q", e.VersionID, e.Key)
	case KindInvalidVersionID:
		return fmt.Sprintf("invalid version id: %q", e.VersionID)
	}
	if err, ok := kindErrors[e.Kind]; ok {
		return err.Error()
	}
	return "change failed: " + e.Kind
}

// Unwrap returns the Err value for the error's Kind.
func (e *ChangeError) Unwrap() error {
	return kindErrors[e.Kind]
}

// preconditionFailed is a KindPreconditionFailed naming condition.
func preconditionFailed(condition string) *ChangeError {
	return &ChangeError{Kind: KindPreconditionFailed, Condition: condition}
}

// noSuchVersion is a KindNoSuchVersion of key's versionID.
func noSuchVersion(key, versionID string) *ChangeError {
	return &ChangeError{Kind: KindNoSuchVersion, Key: key, VersionID: versionID}
}

// The failures that carry nothing but their kind.
var (
	errNoSuchKey             = &ChangeError{Kind: KindNoSuchKey}
	errDeleteMarker          = &ChangeError{Kind: KindDeleteMarker}
	errNoObjectLock          = &ChangeError{Kind: KindNoObjectLock}
	errReservationSuperseded = &ChangeError{Kind: KindReservationSuperseded}
)

// changeError classifies err as a change's own failure, or reports false for
// one that fails the whole commit.
func changeError(err error) (*ChangeError, bool) {
	var ce *ChangeError
	if errors.As(err, &ce) {
		return ce, true
	}
	if errors.Is(err, registry.ErrReferenceExpired) {
		return &ChangeError{Kind: KindReferenceExpired}, true
	}
	return nil, false
}
