package s3frontend

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/fil-forge/versitygw/s3err"

	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/queue/bucketqueue"
	"github.com/fil-forge/ingot/registry"
)

// Each way a commit or one of its changes fails answers its S3 error, of the
// concrete type versitygw renders without unwrapping.
func TestMapCommitError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"precondition", &bucketqueue.ChangeError{Kind: bucketqueue.KindPreconditionFailed, Condition: bucketqueue.ConditionIfMatch},
			s3err.GetPreconditionFailedErr(s3err.ConditionIfMatch)},
		{"precondition naming none", &bucketqueue.ChangeError{Kind: bucketqueue.KindPreconditionFailed},
			s3err.GetAPIError(s3err.ErrPreconditionFailed)},
		{"no such key", &bucketqueue.ChangeError{Kind: bucketqueue.KindNoSuchKey},
			s3err.GetAPIError(s3err.ErrNoSuchKey)},
		{"no such version", &bucketqueue.ChangeError{Kind: bucketqueue.KindNoSuchVersion, Key: "k", VersionID: "v"},
			s3err.GetNoSuchVersionErr("k", "v")},
		{"invalid version id", &bucketqueue.ChangeError{Kind: bucketqueue.KindInvalidVersionID, VersionID: "bad"},
			errInvalidVersionID("bad")},
		{"delete marker", &bucketqueue.ChangeError{Kind: bucketqueue.KindDeleteMarker},
			s3err.GetAPIError(s3err.ErrMethodNotAllowed)},
		{"no object lock", &bucketqueue.ChangeError{Kind: bucketqueue.KindNoObjectLock},
			s3err.GetAPIError(s3err.ErrMissingObjectLockConfiguration)},
		{"reference expired", &bucketqueue.ChangeError{Kind: bucketqueue.KindReferenceExpired},
			s3err.GetAPIError(s3err.ErrSlowDown)},
		{"reference expired at the append", fmt.Errorf("append: %w", registry.ErrReferenceExpired),
			s3err.GetAPIError(s3err.ErrSlowDown)},
		{"bucket gone", fmt.Errorf("begin: %w", bucketop.ErrBucketNotFound),
			s3err.GetAPIError(s3err.ErrNoSuchBucket)},
		{"bucket closing", bucketqueue.ErrBucketClosing, errBucketClosing},
	} {
		if got := mapCommitError(tc.err, "commit"); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: %#v, want %#v", tc.name, got, tc.want)
		}
	}
}

// A superseded reservation and a storage error answer InternalError, and
// keep their cause for the log and the Complete that checks for it.
func TestMapCommitErrorWrapsTheRest(t *testing.T) {
	for _, err := range []error{
		&bucketqueue.ChangeError{Kind: bucketqueue.KindReservationSuperseded},
		errors.New("disk full"),
	} {
		got := mapCommitError(err, "commit")
		var api s3err.APIError
		if errors.As(got, &api) {
			t.Fatalf("%v answered %s, want an internal error", err, api.Code)
		}
		if !errors.Is(got, err) {
			t.Fatalf("%v lost its cause: %v", err, got)
		}
	}
	if got := mapCommitError(&bucketqueue.ChangeError{Kind: bucketqueue.KindReservationSuperseded}, "commit"); !errors.Is(got, bucketqueue.ErrReservationSuperseded) {
		t.Fatalf("a superseded reservation is not ErrReservationSuperseded: %v", got)
	}
}
