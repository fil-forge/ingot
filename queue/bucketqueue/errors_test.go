package bucketqueue

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A change error unwraps to its kind's error, through any wrapping, and
// survives the commit result's codec.
func TestChangeErrorUnwrapsToItsKind(t *testing.T) {
	for kind, want := range kindErrors {
		in := &ChangeError{Kind: kind, Condition: ConditionIfMatch, Key: "k", VersionID: "v"}
		var buf bytes.Buffer
		require.NoError(t, (&BucketCommitResult{Results: []ChangeResult{{Err: in}}}).MarshalCBOR(&buf), "%s: encode", kind)
		var res BucketCommitResult
		require.NoError(t, res.UnmarshalCBOR(&buf), "%s: decode", kind)
		got := res.Results[0].Err
		require.Equal(t, *in, *got, "%s: decoded", kind)
		err := fmt.Errorf("commit: %w", got)
		require.ErrorIs(t, err, want, "%s: %v is not %v", kind, err, want)
	}
}

// A change fails on its own terms only with a change error or an expired
// reference; anything else fails the whole commit.
func TestChangeErrorClassifies(t *testing.T) {
	ce, ok := changeError(fmt.Errorf("put: %w", errNoSuchKey))
	require.True(t, ok, "a wrapped change error")
	require.Equal(t, KindNoSuchKey, ce.Kind, "a wrapped change error")
	ce, ok = changeError(registry.ErrReferenceExpired)
	require.True(t, ok, "an expired reference")
	require.Equal(t, KindReferenceExpired, ce.Kind, "an expired reference")
	_, ok = changeError(errors.New("disk full"))
	require.False(t, ok, "a storage error classified as the change's own")
}

// Delete preconditions compare as S3 does: If-Match "*" matches any version,
// quotes do not matter, and the modification time compares to the second.
func TestDeletePreconditionsCheck(t *testing.T) {
	created := time.Unix(1_700_000_000, 0)
	mf := &msbucket.ObjectManifest{ETag: "abc", Created: created.Unix(), Body: msbucket.Body{Size: 5}}
	for _, tc := range []struct {
		name string
		pre  DeletePreconditions
		want string // the failed condition, or "" to pass
	}{
		{"no conditions", DeletePreconditions{}, ""},
		{"if-match matches", DeletePreconditions{HasIfMatch: true, IfMatch: "abc"}, ""},
		{"if-match star", DeletePreconditions{HasIfMatch: true, IfMatch: "*"}, ""},
		{"if-match differs", DeletePreconditions{HasIfMatch: true, IfMatch: "abd"}, ConditionIfMatch},
		{"mod time within the second", DeletePreconditions{HasModTime: true, ModTime: created.Add(500 * time.Millisecond).UnixNano()}, ""},
		{"mod time differs", DeletePreconditions{HasModTime: true, ModTime: created.Add(time.Second).UnixNano()}, ConditionIfMatchLastModTime},
		{"size matches", DeletePreconditions{HasSize: true, Size: 5}, ""},
		{"size differs", DeletePreconditions{HasSize: true, Size: 6}, ConditionIfMatchSize},
	} {
		err := tc.pre.check(mf)
		if tc.want == "" {
			assert.NoError(t, err, "%s", tc.name)
			continue
		}
		var ce *ChangeError
		assert.True(t, errors.As(err, &ce) && ce.Kind == KindPreconditionFailed && ce.Condition == tc.want,
			"%s: %v, want %s failed", tc.name, err, tc.want)
	}
}

// A put's preconditions check the version it would supersede, a delete
// marker counting as none.
func TestPutPreconditionsCheck(t *testing.T) {
	live := &msbucket.ObjectManifest{ETag: "abc"}
	marker := &msbucket.ObjectManifest{DeleteMarker: true}
	for _, tc := range []struct {
		name       string
		put        PutChange
		superseded *msbucket.ObjectManifest
		want       error  // nil to pass
		condition  string // the failed condition, for ErrPreconditionFailed
	}{
		{"if-none-match on a new key", PutChange{HasIfNoneMatch: true, IfNoneMatch: "*"}, nil, nil, ""},
		{"if-none-match over a marker", PutChange{HasIfNoneMatch: true, IfNoneMatch: "*"}, marker, nil, ""},
		{"if-none-match over an object", PutChange{HasIfNoneMatch: true, IfNoneMatch: "*"}, live, ErrPreconditionFailed, ConditionIfNoneMatch},
		{"if-match on a new key", PutChange{HasIfMatch: true, IfMatch: "abc"}, nil, ErrNoSuchKey, ""},
		{"if-match matches", PutChange{HasIfMatch: true, IfMatch: "abc"}, live, nil, ""},
		{"if-match differs", PutChange{HasIfMatch: true, IfMatch: "abd"}, live, ErrPreconditionFailed, ConditionIfMatch},
		{"complete if-match on a new key", PutChange{Completes: true, HasIfMatch: true, IfMatch: "abc"}, nil, ErrNoSuchKey, ""},
		{"complete if-match quoted", PutChange{Completes: true, HasIfMatch: true, IfMatch: `"abc"`}, live, nil, ""},
		{"complete if-match differs", PutChange{Completes: true, HasIfMatch: true, IfMatch: "abd"}, live, ErrPreconditionFailed, ""},
		{"complete if-none-match over an object", PutChange{Completes: true, HasIfNoneMatch: true, IfNoneMatch: "*"}, live, ErrPreconditionFailed, ""},
		{"marker over a mismatched object", PutChange{DeletePre: &DeletePreconditions{HasIfMatch: true, IfMatch: "abd"}}, live, ErrPreconditionFailed, ConditionIfMatch},
		{"marker over a new key", PutChange{DeletePre: &DeletePreconditions{HasIfMatch: true, IfMatch: "abd"}}, nil, nil, ""},
	} {
		check, err := (&Queue{}).putPreCheck(t.Context(), &tc.put)
		require.NoError(t, err, "%s", tc.name)
		err = check(tc.superseded)
		if !assert.ErrorIs(t, err, tc.want, "%s", tc.name) {
			continue
		}
		var ce *ChangeError
		if tc.want == ErrPreconditionFailed && errors.As(err, &ce) {
			assert.Equal(t, tc.condition, ce.Condition, "%s: condition", tc.name)
		}
	}
}
