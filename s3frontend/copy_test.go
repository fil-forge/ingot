package s3frontend

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
)

// Every failed copy-source precondition is a 412, including the two the
// shared read-semantics helper reports as 304, and the body names the header
// with S3's x-amz-copy-source- prefix.
func TestEvaluateCopySourcePreconditions(t *testing.T) {
	etag := "abc"
	other := "def"
	mod := time.Unix(1_700_000_000, 0)
	before, after := mod.Add(-time.Hour), mod.Add(time.Hour)

	tests := []struct {
		name string
		pc   backend.PreConditions
		want s3err.Condition // "" = the copy proceeds
	}{
		{"no conditions", backend.PreConditions{}, ""},
		{"if-none-match differs", backend.PreConditions{IfNoneMatch: &other}, ""},
		{"if-none-match matches", backend.PreConditions{IfNoneMatch: &etag}, s3err.ConditionIfNoneMatch},
		{"if-modified-since satisfied", backend.PreConditions{IfModSince: &before}, ""},
		{"if-modified-since unsatisfied", backend.PreConditions{IfModSince: &after}, conditionIfModifiedSince},
		{"if-match matches", backend.PreConditions{IfMatch: &etag}, ""},
		{"if-match differs", backend.PreConditions{IfMatch: &other}, s3err.ConditionIfMatch},
		{"if-unmodified-since satisfied", backend.PreConditions{IfUnmodeSince: &after}, ""},
		{"if-unmodified-since unsatisfied", backend.PreConditions{IfUnmodeSince: &before}, s3err.ConditionIfUnmodifiedSince},
		// if-match holds but if-none-match also matches: the helper's 304 case
		// with both ETag headers present.
		{"if-match and matching if-none-match", backend.PreConditions{IfMatch: &etag, IfNoneMatch: &etag}, s3err.ConditionIfNoneMatch},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := evaluateCopySourcePreconditions(etag, mod, tc.pc)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("got %v, want the copy to proceed", err)
				}
				return
			}
			var pf s3err.PreconditionFailedError
			if !errors.As(err, &pf) {
				t.Fatalf("got %v, want PreconditionFailedError", err)
			}
			if pf.HTTPStatusCode != http.StatusPreconditionFailed {
				t.Fatalf("status = %d, want 412", pf.HTTPStatusCode)
			}
			want := s3err.Condition(copySourceConditionPrefix + string(tc.want))
			if pf.Condition != want {
				t.Fatalf("condition = %q, want %q", pf.Condition, want)
			}
		})
	}
}

// CopyObject surfaces a matched x-amz-copy-source-if-none-match as 412, not as
// GET's 304, and still copies when the header names a different ETag.
func TestCopyObject_IfNoneMatchIs412(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	out := putObjV(t, b, "src", []byte("copy me"))

	// The controller hands the backend the header's ETag with its quotes
	// stripped (utils.ParsePreconditionMatchHeaders); the helper compares
	// against the trimmed form.
	etag := strings.Trim(out.ETag, `"`)

	bucket, dst, src := "bk", "dst", "bk/src"
	_, err := b.CopyObject(context.Background(), s3response.CopyObjectInput{
		Bucket:                &bucket,
		Key:                   &dst,
		CopySource:            &src,
		CopySourceIfNoneMatch: &etag,
	})
	if !errors.Is(err, s3err.GetAPIError(s3err.ErrPreconditionFailed)) {
		t.Fatalf("matching if-none-match: got %v, want PreconditionFailed", err)
	}
	if _, _, gerr := getObjV(t, b, dst, ""); apiErrCode(t, gerr) != "NoSuchKey" {
		t.Fatalf("destination must not exist after a failed precondition: %v", gerr)
	}

	other := "00000000000000000000000000000000"
	if _, err := b.CopyObject(context.Background(), s3response.CopyObjectInput{
		Bucket:                &bucket,
		Key:                   &dst,
		CopySource:            &src,
		CopySourceIfNoneMatch: &other,
	}); err != nil {
		t.Fatalf("non-matching if-none-match: %v", err)
	}
	if _, data, err := getObjV(t, b, dst, ""); err != nil || string(data) != "copy me" {
		t.Fatalf("copied GET = %q, %v", data, err)
	}
}
