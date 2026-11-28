package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/stretchr/testify/require"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/mst"
)

// Versioning tests over the same in-process harness as refindex_test.go,
// covering the docs/s3-versioning.md write rule, resolution, delete-marker,
// and list semantics.

func setVersioning(t *testing.T, b *Backend, status types.BucketVersioningStatus) {
	t.Helper()
	require.NoError(t, b.PutBucketVersioning(context.Background(), "bk", status), "PutBucketVersioning(%s)", status)
}

func putObjV(t *testing.T, b *Backend, key string, data []byte) s3response.PutObjectOutput {
	t.Helper()
	bucket := "bk"
	out, err := b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(data), ContentLength: byteLen(data),
	})
	require.NoError(t, err, "PutObject %s", key)
	return out
}

func getObjV(t *testing.T, b *Backend, key, versionID string) (*s3.GetObjectOutput, []byte, error) {
	t.Helper()
	bucket := "bk"
	in := &s3.GetObjectInput{Bucket: &bucket, Key: &key}
	if versionID != "" {
		in.VersionId = &versionID
	}
	out, err := b.GetObject(context.Background(), in)
	if err != nil {
		return nil, nil, err
	}
	defer out.Body.Close()
	data, rerr := io.ReadAll(out.Body)
	require.NoError(t, rerr, "read body")
	return out, data, nil
}

func deleteObjV(t *testing.T, b *Backend, key, versionID string) (*s3.DeleteObjectOutput, error) {
	t.Helper()
	bucket := "bk"
	in := &s3.DeleteObjectInput{Bucket: &bucket, Key: &key}
	if versionID != "" {
		in.VersionId = &versionID
	}
	return b.DeleteObject(context.Background(), in)
}

func listVersions(t *testing.T, b *Backend, in *s3.ListObjectVersionsInput) s3response.ListVersionsResult {
	t.Helper()
	bucket := "bk"
	if in == nil {
		in = &s3.ListObjectVersionsInput{}
	}
	in.Bucket = &bucket
	res, err := b.ListObjectVersions(context.Background(), in)
	require.NoError(t, err, "ListObjectVersions")
	return res
}

func apiErrCode(t *testing.T, err error) string {
	t.Helper()
	var invErr s3err.InvalidArgumentError
	if errors.As(err, &invErr) {
		return "InvalidArgument"
	}
	var nsvErr s3err.NoSuchVersionError
	if errors.As(err, &nsvErr) {
		return nsvErr.Code
	}
	var ipErr s3err.InvalidPartError
	if errors.As(err, &ipErr) {
		return ipErr.Code
	}
	var apiErr s3err.APIError
	require.ErrorAs(t, err, &apiErr, "not an API error")
	return apiErr.Code
}

func TestVersioning_EnabledPutRetainsVersions(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)

	a, bb := []byte("version A"), []byte("version B")
	out1 := putObjV(t, b, "k1", a)
	out2 := putObjV(t, b, "k1", bb)
	da := blobDigestOf(t, b, "k1", out1.VersionID)
	db := blobDigestOf(t, b, "k1", out2.VersionID)
	require.NotEmpty(t, out1.VersionID, "expected distinct non-empty version ids, got %q / %q", out1.VersionID, out2.VersionID)
	require.NotEmpty(t, out2.VersionID, "expected distinct non-empty version ids, got %q / %q", out1.VersionID, out2.VersionID)
	require.NotEqual(t, out2.VersionID, out1.VersionID, "expected distinct non-empty version ids, got %q / %q", out1.VersionID, out2.VersionID)

	// Current is B; version-scoped read returns A.
	_, data, err := getObjV(t, b, "k1", "")
	require.NoError(t, err, "current GET")
	require.Equal(t, bb, data, "current GET")
	got, data, err := getObjV(t, b, "k1", out1.VersionID)
	require.NoError(t, err, "versioned GET")
	require.Equal(t, a, data, "versioned GET")
	require.NotNil(t, got.VersionId, "versioned GET VersionId")
	require.Equal(t, out1.VersionID, *got.VersionId, "versioned GET VersionId")

	// Overwrite under Enabled releases nothing.
	require.Equal(t, 1, claims(t, mem, da), "claims(A)")
	require.Equal(t, 1, claims(t, mem, db), "claims(B)")
	require.Empty(t, rm.removed, "removed %d blobs", len(rm.removed))

	// ListObjectVersions: newest-first, IsLatest on the head.
	res := listVersions(t, b, nil)
	require.Len(t, res.Versions, 2, "versions=%d markers", len(res.Versions))
	require.Empty(t, res.DeleteMarkers, "versions=%d markers", len(res.Versions))
	require.Equal(t, out2.VersionID, *res.Versions[0].VersionId, "head version")
	require.True(t, *res.Versions[0].IsLatest, "head version latest")
	require.Equal(t, out1.VersionID, *res.Versions[1].VersionId, "second version")
	require.False(t, *res.Versions[1].IsLatest, "second version latest")
}

func TestVersioning_NullVersionRetainedOnEnable(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	a, bb := []byte("pre-versioning"), []byte("post-enable")

	putObjV(t, b, "k1", a) // unversioned → the null version
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	putObjV(t, b, "k1", bb)

	// The null version survives beneath the numbered one.
	_, data, err := getObjV(t, b, "k1", "null")
	require.NoError(t, err, "GET versionId=null = %q, %v", data, err)
	require.Equal(t, a, data, "GET versionId=null = %q, %v", data, err)
	res := listVersions(t, b, nil)
	require.Len(t, res.Versions, 2, "versions")
	require.Equal(t, "null", *res.Versions[1].VersionId, "bottom version")
	require.False(t, *res.Versions[1].IsLatest, "bottom version latest")
}

func TestVersioning_SuspendedReplacesNullInPlace(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	a := []byte("numbered A")
	outA := putObjV(t, b, "k1", a)
	da := blobDigestOf(t, b, "k1", outA.VersionID)

	setVersioning(t, b, types.BucketVersioningStatusSuspended)
	bb, cc := []byte("null B"), []byte("null C")

	outB := putObjV(t, b, "k1", bb)
	// A suspended PUT omits the version id from the response (§4.3); the write
	// still lands as the "null" version, confirmed by resolving it below.
	require.Empty(t, outB.VersionID, "suspended PUT VersionID")
	db := blobDigestOf(t, b, "k1", "null")
	// The numbered version was retained; nothing released yet.
	require.Equal(t, 1, claims(t, mem, da), "claims(A)")
	require.Empty(t, rm.removed, "removed")

	// A second null write replaces the first in place.
	putObjV(t, b, "k1", cc)
	dc := blobDigestOf(t, b, "k1", "null")
	drainReleases(t, b)
	require.Zero(t, claims(t, mem, db), "claims(B)")
	require.Equal(t, 1, rm.removedDigests()[string(db)], "removals(B)")
	require.Equal(t, 1, claims(t, mem, dc), "claims(C)")
	require.Equal(t, 1, claims(t, mem, da), "claims(A)")

	res := listVersions(t, b, nil)
	require.Len(t, res.Versions, 2, "versions")
	require.Equal(t, "null", *res.Versions[0].VersionId, "head")
	require.True(t, *res.Versions[0].IsLatest, "head latest")
	require.Equal(t, outA.VersionID, *res.Versions[1].VersionId, "bottom")
}

func TestVersioning_DeleteMarker(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	data := []byte("to be marked")
	putObjV(t, b, "k1", data)
	d := blobDigestOf(t, b, "k1", "")

	out, err := deleteObjV(t, b, "k1", "")
	require.NoError(t, err, "DeleteObject")
	require.NotNil(t, out.DeleteMarker, "marker response")
	require.True(t, *out.DeleteMarker, "marker response = %+v, want DeleteMarker + VersionId", out)
	require.NotNil(t, out.VersionId, "marker response")
	require.NotEmpty(t, *out.VersionId, "marker response")
	markerID := *out.VersionId

	// Current-is-marker: GET/HEAD 404; scoped GET of the marker 405.
	_, _, err = getObjV(t, b, "k1", "")
	require.Equal(t, "NoSuchKey", apiErrCode(t, err), "GET over marker")
	_, _, err = getObjV(t, b, "k1", markerID)
	require.Equal(t, "MethodNotAllowed", apiErrCode(t, err), "GET marker version")

	// The marked version's data is retained.
	require.Equal(t, 1, claims(t, mem, d), "claims")
	require.Empty(t, rm.removed, "removed")

	// ListObjects hides the key; ListObjectVersions shows marker + version.
	bucket := "bk"
	lres, err := b.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: &bucket})
	require.NoError(t, err, "ListObjectsV2")
	require.Empty(t, lres.Contents, "ListObjects shows %d keys over a marker", len(lres.Contents))
	vres := listVersions(t, b, nil)
	require.Len(t, vres.DeleteMarkers, 1, "markers=%d versions", len(vres.DeleteMarkers))
	require.Len(t, vres.Versions, 1, "markers=%d versions", len(vres.DeleteMarkers))
	require.True(t, *vres.DeleteMarkers[0].IsLatest, "marker entry latest")
	require.Equal(t, markerID, *vres.DeleteMarkers[0].VersionId, "marker entry")

	// Deleting the marker by version id restores the object.
	dout, err := deleteObjV(t, b, "k1", markerID)
	require.NoError(t, err, "delete marker")
	require.NotNil(t, dout.DeleteMarker, "delete-marker response")
	require.True(t, *dout.DeleteMarker, "delete-marker response = %+v, want DeleteMarker + id %q", dout, markerID)
	require.Equal(t, markerID, *dout.VersionId, "delete-marker response")
	_, got, err := getObjV(t, b, "k1", "")
	require.NoError(t, err, "GET after marker removal")
	require.Equal(t, data, got, "GET after marker removal")
}

func TestVersioning_DeleteSpecificVersionPromotes(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	a, bb := []byte("keep me"), []byte("delete me")
	outA := putObjV(t, b, "k1", a)
	outB := putObjV(t, b, "k1", bb)
	da := blobDigestOf(t, b, "k1", outA.VersionID)
	db := blobDigestOf(t, b, "k1", outB.VersionID)

	// Delete the CURRENT version: the older one is promoted.
	_, err := deleteObjV(t, b, "k1", outB.VersionID)
	require.NoError(t, err, "delete current version")
	_, data, err := getObjV(t, b, "k1", "")
	require.NoError(t, err, "GET after promotion")
	require.Equal(t, a, data, "GET after promotion")
	drainReleases(t, b)
	require.Zero(t, claims(t, mem, db), "claims(B)")
	require.Equal(t, 1, rm.removedDigests()[string(db)], "removals(B)")
	res := listVersions(t, b, nil)
	require.Len(t, res.Versions, 1, "after promotion versions")
	require.Equal(t, outA.VersionID, *res.Versions[0].VersionId, "after promotion versions")
	require.True(t, *res.Versions[0].IsLatest, "after promotion versions = %+v, want just %q latest", res.Versions, outA.VersionID)

	// Delete the last version: the key is gone entirely.
	_, err = deleteObjV(t, b, "k1", outA.VersionID)
	require.NoError(t, err, "delete last version")
	_, _, err = getObjV(t, b, "k1", "")
	require.Equal(t, "NoSuchKey", apiErrCode(t, err), "GET after last delete")
	require.Zero(t, claims(t, mem, da), "claims(A)")
	n := len(listVersions(t, b, nil).Versions)
	require.Zero(t, n, "versions after full delete")
}

func TestVersioning_SuspendedDeleteCreatesNullMarker(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	outA := putObjV(t, b, "k1", []byte("numbered"))
	setVersioning(t, b, types.BucketVersioningStatusSuspended)

	// Repeated suspended deletes each replace the null marker in place.
	for i := 0; i < 3; i++ {
		out, err := deleteObjV(t, b, "k1", "")
		require.NoError(t, err, "suspended delete #%d", i)
		require.NotNil(t, out.DeleteMarker, "suspended delete #%d", i)
		require.True(t, *out.DeleteMarker, "suspended delete #%d = %+v, want null marker", i, out)
		require.Equal(t, "null", *out.VersionId, "suspended delete #%d", i)
	}
	res := listVersions(t, b, nil)
	require.Len(t, res.DeleteMarkers, 1, "markers = %+v, want one latest null marker", res.DeleteMarkers)
	require.Equal(t, "null", *res.DeleteMarkers[0].VersionId, "markers = %+v, want one latest null marker", res.DeleteMarkers)
	require.True(t, *res.DeleteMarkers[0].IsLatest, "markers = %+v, want one latest null marker", res.DeleteMarkers)
	require.Len(t, res.Versions, 1, "versions")
	require.Equal(t, outA.VersionID, *res.Versions[0].VersionId, "versions")

	// Removing the null marker restores the numbered version.
	_, err := deleteObjV(t, b, "k1", "null")
	require.NoError(t, err, "delete null marker")
	_, data, err := getObjV(t, b, "k1", "")
	require.NoError(t, err, "GET after null-marker removal")
	require.Equal(t, []byte("numbered"), data, "GET after null-marker removal")
}

func TestVersioning_DeleteMarkerOnMissingKey(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)

	// S3 inserts a marker even when the key does not exist.
	out, err := deleteObjV(t, b, "ghost", "")
	require.NoError(t, err, "delete missing key")
	require.NotNil(t, out.DeleteMarker, "response")
	require.True(t, *out.DeleteMarker, "response = %+v, want a delete marker", out)
	res := listVersions(t, b, nil)
	require.Len(t, res.DeleteMarkers, 1, "markers=%d versions", len(res.DeleteMarkers))
	require.Empty(t, res.Versions, "markers=%d versions", len(res.DeleteMarkers))
}

func TestVersioning_InvalidAndUnknownVersionIDs(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	putObjV(t, b, "k1", []byte("x"))

	_, _, err := getObjV(t, b, "k1", "invalid_version_id")
	require.Equal(t, "InvalidArgument", apiErrCode(t, err), "malformed versionId GET")
	_, _, err = getObjV(t, b, "k1", "01G65Z755AFWAKHE12NY0CQ9FH")
	require.Equal(t, "NoSuchVersion", apiErrCode(t, err), "unknown versionId GET")
	_, err = deleteObjV(t, b, "k1", "not-a-ulid!")
	require.Equal(t, "InvalidArgument", apiErrCode(t, err), "malformed versionId DELETE")
	// A well-formed but absent version deletes as a success no-op.
	_, err = deleteObjV(t, b, "k1", "01G65Z755AFWAKHE12NY0CQ9FH")
	require.NoError(t, err, "unknown versionId DELETE")
	_, _, err = getObjV(t, b, "k1", "")
	require.NoError(t, err, "object must survive the no-op delete")
}

func TestVersioning_UnversionedScopedNullDelete(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	putObjV(t, b, "k1", []byte("plain"))
	_, err := deleteObjV(t, b, "k1", "null")
	require.NoError(t, err, "scoped null delete")
	_, _, err = getObjV(t, b, "k1", "")
	require.Equal(t, "NoSuchKey", apiErrCode(t, err), "GET after scoped null delete")
}

func TestVersioning_CopyFromVersion(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	a := []byte("original content")
	outA := putObjV(t, b, "k1", a)
	putObjV(t, b, "k1", []byte("newer content"))
	da := blobDigestOf(t, b, "k1", outA.VersionID)

	bucket, dstKey := "bk", "k2"
	src := "bk/k1?versionId=" + outA.VersionID
	cout, err := b.CopyObject(context.Background(), s3response.CopyObjectInput{
		Bucket:     &bucket,
		Key:        &dstKey,
		CopySource: &src,
	})
	require.NoError(t, err, "CopyObject from version")
	require.NotNil(t, cout.CopySourceVersionId, "CopySourceVersionId")
	require.Equal(t, outA.VersionID, *cout.CopySourceVersionId, "CopySourceVersionId")
	require.NotNil(t, cout.VersionId, "dest VersionId missing: %+v", cout)
	require.NotEmpty(t, *cout.VersionId, "dest VersionId missing: %+v", cout)
	_, data, err := getObjV(t, b, "k2", "")
	require.NoError(t, err, "copied GET")
	require.Equal(t, a, data, "copied GET")
	// The shared digest now has two claims: k1's old version and k2's new one.
	require.Equal(t, 2, claims(t, mem, da), "claims(A)")
}

func TestVersioning_ListVersionsPagination(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)

	// bar: 3 versions, baz: 2, foo: 2 → 7 entries across keys.
	var want []string // "key/versionId" in expected list order
	perKey := map[string][]string{}
	for _, kv := range []struct {
		key string
		n   int
	}{{"bar", 3}, {"baz", 2}, {"foo", 2}} {
		for i := 0; i < kv.n; i++ {
			out := putObjV(t, b, kv.key, []byte(kv.key+string(rune('a'+i))))
			perKey[kv.key] = append(perKey[kv.key], out.VersionID)
		}
	}
	for _, key := range []string{"bar", "baz", "foo"} {
		ids := perKey[key]
		for i := len(ids) - 1; i >= 0; i-- { // newest-first
			want = append(want, key+"/"+ids[i])
		}
	}

	var got []string
	maxKeys := int32(3)
	in := &s3.ListObjectVersionsInput{MaxKeys: &maxKeys}
	for {
		res := listVersions(t, b, in)
		for _, v := range res.Versions {
			got = append(got, *v.Key+"/"+*v.VersionId)
		}
		if res.IsTruncated == nil || !*res.IsTruncated {
			break
		}
		require.NotNil(t, res.NextKeyMarker, "truncated page missing NextKeyMarker")
		in = &s3.ListObjectVersionsInput{
			MaxKeys:         &maxKeys,
			KeyMarker:       res.NextKeyMarker,
			VersionIdMarker: res.NextVersionIdMarker,
		}
	}
	require.Len(t, got, len(want), "paged entries")
	for i := range want {
		require.Equal(t, want[i], got[i], "entry %d", i)
	}
}

// TestVersioning_ListVersionsExactFit locks the look-ahead on the versions
// path: with 2 keys × 3 versions = 6 entries and MaxKeys=3, page 1 fills
// exactly and IS truncated (key 2 still follows), while page 2 fills exactly
// and is NOT truncated (nothing follows).
func TestVersioning_ListVersionsExactFit(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	for _, key := range []string{"o1", "o2"} {
		for i := 0; i < 3; i++ {
			putObjV(t, b, key, []byte(key+string(rune('a'+i))))
		}
	}

	maxKeys := int32(3)
	res := listVersions(t, b, &s3.ListObjectVersionsInput{MaxKeys: &maxKeys})
	require.Len(t, res.Versions, 3, "page1: versions=%d truncated", len(res.Versions))
	require.NotNil(t, res.IsTruncated, "page1: versions=%d truncated", len(res.Versions))
	require.True(t, *res.IsTruncated, "page1: versions=%d truncated=%v, want 3/true", len(res.Versions), res.IsTruncated)
	require.NotNil(t, res.NextKeyMarker, "page1 missing NextKeyMarker")

	res2 := listVersions(t, b, &s3.ListObjectVersionsInput{
		MaxKeys:         &maxKeys,
		KeyMarker:       res.NextKeyMarker,
		VersionIdMarker: res.NextVersionIdMarker,
	})
	require.Len(t, res2.Versions, 3, "page2 versions")
	require.NotNil(t, res2.IsTruncated, "page2 IsTruncated")
	require.False(t, *res2.IsTruncated, "page2 IsTruncated (exact fit, nothing follows)")
	for _, v := range res2.Versions {
		require.Equal(t, "o2", *v.Key, "page2 key")
	}
}

func TestVersioning_UnversionedResponsesOmitVersionIDs(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	out := putObjV(t, b, "k1", []byte("plain"))
	require.Empty(t, out.VersionID, "unversioned PUT VersionID")
	gout, _, err := getObjV(t, b, "k1", "")
	require.NoError(t, err, "GET")
	require.Nil(t, gout.VersionId, "unversioned GET VersionId")
	dout, err := deleteObjV(t, b, "k1", "")
	require.NoError(t, err, "DELETE")
	require.Nil(t, dout.VersionId, "unversioned DELETE")
	require.Nil(t, dout.DeleteMarker, "unversioned DELETE")
}

func TestVersioning_GetBucketVersioningStates(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	out, err := b.GetBucketVersioning(ctx, "bk")
	require.NoError(t, err, "GetBucketVersioning")
	require.Nil(t, out.Status, "never-configured Status")
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	out, _ = b.GetBucketVersioning(ctx, "bk")
	require.NotNil(t, out.Status, "Status")
	require.Equal(t, types.BucketVersioningStatusEnabled, *out.Status, "Status")
	setVersioning(t, b, types.BucketVersioningStatusSuspended)
	out, _ = b.GetBucketVersioning(ctx, "bk")
	require.NotNil(t, out.Status, "Status")
	require.Equal(t, types.BucketVersioningStatusSuspended, *out.Status, "Status")
	_, err = b.GetBucketVersioning(ctx, "nope")
	require.Equal(t, "NoSuchBucket", apiErrCode(t, err), "missing bucket")
}

func TestVersioning_DeleteObjectsMixed(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	outA := putObjV(t, b, "a", []byte("aaa"))
	putObjV(t, b, "b", []byte("bbb"))

	bucket := "bk"
	ctx := withActions(t, b, bucket, "s3:DeleteObject", "s3:DeleteObjectVersion")
	res, err := b.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: &bucket,
		Delete: &types.Delete{Objects: []types.ObjectIdentifier{
			{Key: strPtr("a"), VersionId: &outA.VersionID}, // scoped: permanent
			{Key: strPtr("b")}, // unscoped: marker
		}},
	})
	require.NoError(t, err, "DeleteObjects")
	require.Empty(t, res.Error, "errors")
	require.Len(t, res.Deleted, 2, "deleted = %d entries", len(res.Deleted))
	byKey := map[string]types.DeletedObject{}
	for _, d := range res.Deleted {
		byKey[*d.Key] = d
	}
	ea := byKey["a"]
	require.NotNil(t, ea.VersionId, "entry \"a\"")
	require.Equal(t, outA.VersionID, *ea.VersionId, "entry \"a\" VersionId")
	require.NotNil(t, ea.DeleteMarker, "entry \"a\"")
	require.False(t, *ea.DeleteMarker, "entry \"a\" DeleteMarker")
	e := byKey["b"]
	require.NotNil(t, e.DeleteMarker, "entry \"b\"")
	require.True(t, *e.DeleteMarker, "entry \"b\" = %+v, want marker fields", e)
	require.NotNil(t, e.DeleteMarkerVersionId, "entry \"b\"")

	// "a" is fully gone (its only version removed); "b" is marker-hidden.
	_, _, err = getObjV(t, b, "a", "")
	require.Equal(t, "NoSuchKey", apiErrCode(t, err), "GET \"a\"")
	_, _, err = getObjV(t, b, "b", "")
	require.Equal(t, "NoSuchKey", apiErrCode(t, err), "GET \"b\"")
	n := len(listVersions(t, b, nil).Versions)
	require.Equal(t, 1, n, "remaining versions = %d, want 1 (b's data version)", n)
}

// TestVersioning_DeleteObjectsNeedsVersionPermission covers the per-entry
// check: an entry naming a version fails with AccessDenied, and leaves the
// version in place, unless the key holds s3:DeleteObjectVersion on the
// bucket. The request's other entries are deleted as usual.
func TestVersioning_DeleteObjectsNeedsVersionPermission(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	outA := putObjV(t, b, "a", []byte("aaa"))
	putObjV(t, b, "b", []byte("bbb"))

	bucket := "bk"
	for name, ctx := range map[string]context.Context{
		"without the permission": withActions(t, b, bucket, "s3:DeleteObject"),
		"with no action set":     context.Background(),
	} {
		t.Run(name, func(t *testing.T) {
			res, err := b.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: &bucket,
				Delete: &types.Delete{Objects: []types.ObjectIdentifier{
					{Key: strPtr("a"), VersionId: &outA.VersionID},
				}},
			})
			require.NoError(t, err, "DeleteObjects")
			require.Empty(t, res.Deleted, "result = %+v, want one AccessDenied error for \"a\"", res)
			require.Len(t, res.Error, 1, "result = %+v, want one AccessDenied error for \"a\"", res)
			require.Equal(t, "AccessDenied", *res.Error[0].Code, "result = %+v, want one AccessDenied error for \"a\"", res)
			require.Equal(t, "a", *res.Error[0].Key, "result = %+v, want one AccessDenied error for \"a\"", res)
			_, _, err = getObjV(t, b, "a", outA.VersionID)
			require.NoError(t, err, "GET refused version of \"a\"")
		})
	}

	res, err := b.DeleteObjects(withActions(t, b, bucket, "s3:DeleteObject"), &s3.DeleteObjectsInput{
		Bucket: &bucket,
		Delete: &types.Delete{Objects: []types.ObjectIdentifier{
			{Key: strPtr("a"), VersionId: &outA.VersionID},
			{Key: strPtr("b")},
		}},
	})
	require.NoError(t, err, "DeleteObjects")
	require.Len(t, res.Error, 1, "result = %+v, want \"a\" refused and \"b\" deleted", res)
	require.Equal(t, "a", *res.Error[0].Key, "result = %+v, want \"a\" refused and \"b\" deleted", res)
	require.Len(t, res.Deleted, 1, "result = %+v, want \"a\" refused and \"b\" deleted", res)
	require.Equal(t, "b", *res.Deleted[0].Key, "result = %+v, want \"a\" refused and \"b\" deleted", res)
}

// actionSet is a reqscope.ActionSet holding one bucket's actions.
type actionSet struct {
	space   string
	actions []string
}

func (a actionSet) Permits(bucket did.DID, action string) (allowed, known bool) {
	if bucket.String() != a.space {
		return false, false
	}
	return slices.Contains(a.actions, action), true
}

// withActions returns a context whose action set grants actions on bucket,
// as the IAM service's would for a key Hilt authorized there.
func withActions(t *testing.T, b *Backend, bucket string, actions ...string) context.Context {
	t.Helper()
	st, err := b.reg.Get(context.Background(), bucket)
	require.NoError(t, err, "registry get %q", bucket)
	return context.WithValue(context.Background(), reqscope.ActionsKey(),
		reqscope.ActionSet(actionSet{space: st.Space.String(), actions: actions}))
}

func strPtr(s string) *string { return &s }

// TestVersioning_NullEvictionFromPrev drives the §5.2 branch where a new null
// write finds the key's null version NONCURRENT (an entry in the prev tree,
// named by NullSeq) and must evict it there: only one null per key.
func TestVersioning_NullEvictionFromPrev(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)

	// The null version starts life unversioned, then is buried under a
	// numbered version: noncurrent, in prev, NullSeq set.
	a := []byte("null A")
	putObjV(t, b, "k1", a)
	da := blobDigestOf(t, b, "k1", "null")
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	bb := []byte("numbered B")
	outB := putObjV(t, b, "k1", bb)
	db := blobDigestOf(t, b, "k1", outB.VersionID)

	// A Suspended write is a new null: it must evict A from the prev tree.
	setVersioning(t, b, types.BucketVersioningStatusSuspended)
	cc := []byte("null C")
	putObjV(t, b, "k1", cc)
	dc := blobDigestOf(t, b, "k1", "null")

	// A's claim is released and its blob removed; B and C stay claimed.
	drainReleases(t, b)
	require.Zero(t, claims(t, mem, da), "claims(A)")
	require.Equal(t, 1, rm.removedDigests()[string(da)], "removals(A)")
	require.Equal(t, 1, claims(t, mem, db), "claims(B)")
	require.Equal(t, 1, claims(t, mem, dc), "claims(C)")

	// The stack is [C null latest, B numbered]; the old null resolves to C.
	res := listVersions(t, b, nil)
	require.Len(t, res.Versions, 2, "versions=%d markers", len(res.Versions))
	require.Empty(t, res.DeleteMarkers, "versions=%d markers", len(res.Versions))
	require.Equal(t, "null", *res.Versions[0].VersionId, "head")
	require.True(t, *res.Versions[0].IsLatest, "head latest")
	require.Equal(t, outB.VersionID, *res.Versions[1].VersionId, "bottom")
	require.False(t, *res.Versions[1].IsLatest, "bottom latest")
	_, data, err := getObjV(t, b, "k1", "null")
	require.NoError(t, err, "GET null = %q, %v", data, err)
	require.Equal(t, cc, data, "GET null = %q, %v", data, err)
}

// getValue decodes the top-MST value block for key: a bare manifest or an
// enveloped leaf (docs/s3-versioning.md §2.1).
func getValue(t *testing.T, b *Backend, key string) msbucket.ObjectValue {
	t.Helper()
	ctx := context.Background()
	st, err := b.reg.Get(ctx, "bk")
	require.NoError(t, err, "registry get")
	valCid, err := mst.LoadMST(b.read, st.Space, st.Root).Get(ctx, key)
	require.NoError(t, err, "mst get %s", key)
	var val msbucket.ObjectValue
	require.NoError(t, b.read.Get(ctx, st.Space, valCid, &val), "value get %s", key)
	return val
}

// TestVersioning_UnversionedKeyStaysBare pins §2.1/§5.2's structural claim for
// buckets that never version: overwrites replace in place and the key's value
// stays a bare manifest — no leaf, no prev tree, ever.
func TestVersioning_UnversionedKeyStaysBare(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	putObjV(t, b, "k1", []byte("one"))
	putObjV(t, b, "k1", []byte("two"))
	putObjV(t, b, "k1", []byte("three"))

	// Behaviorally: a single listed null version.
	res := listVersions(t, b, nil)
	require.Len(t, res.Versions, 1, "versions")
	require.Equal(t, "null", *res.Versions[0].VersionId, "versions")
	// Structurally: the value block is the manifest itself.
	val := getValue(t, b, "k1")
	require.NotNil(t, val.Manifest, "value = {Manifest:%v Leaf:%v}", val.Manifest, val.Leaf)
	require.Nil(t, val.Leaf, "value = {Manifest:%v Leaf:%v}", val.Manifest, val.Leaf)
	require.Equal(t, "null", val.Manifest.VersionID, "manifest VersionID")
}

// TestVersioning_FirstSupersessionCreatesLeaf pins the §5.2 upgrade rule on an
// Enabled bucket: a new key's value is a bare manifest; the first overwrite
// creates the leaf (with the superseded version as its one prev entry); and a
// leaf is never downgraded, even when deletes shrink the key back to one
// version (invariant 6).
func TestVersioning_FirstSupersessionCreatesLeaf(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)

	putObjV(t, b, "k1", []byte("one"))
	require.NotNil(t, getValue(t, b, "k1").Manifest, "single-version Enabled key")

	out2 := putObjV(t, b, "k1", []byte("two"))
	val := getValue(t, b, "k1")
	require.NotNil(t, val.Leaf, "superseded key: value is still a bare manifest")
	require.Equal(t, out2.VersionID, val.Leaf.Current.VersionID, "leaf = {Current:%q Prev:%v}, want current %q with a prev tree", val.Leaf.Current.VersionID, val.Leaf.Prev, out2.VersionID)
	require.NotNil(t, val.Leaf.Prev, "leaf = {Current:%q Prev:%v}, want current %q with a prev tree", val.Leaf.Current.VersionID, val.Leaf.Prev, out2.VersionID)

	// Delete the current version: the old one promotes, and the key keeps its
	// leaf even though only one version remains.
	_, err := deleteObjV(t, b, "k1", out2.VersionID)
	require.NoError(t, err, "delete version")
	val = getValue(t, b, "k1")
	require.NotNil(t, val.Leaf, "post-delete key: value downgraded to a bare manifest")
	require.Nil(t, val.Leaf.Prev, "post-delete leaf.Prev")
	_, data, err := getObjV(t, b, "k1", "")
	require.NoError(t, err, "GET after promotion")
	require.Equal(t, []byte("one"), data, "GET after promotion")
}

// TestVersioning_ListVersionsDelimiter pins §9.2 grouping: every version of a
// key rolled into a CommonPrefix is subsumed by it.
func TestVersioning_ListVersionsDelimiter(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	putObjV(t, b, "a/x", []byte("x1"))
	putObjV(t, b, "a/x", []byte("x2"))
	putObjV(t, b, "a/y", []byte("y1"))
	outB := putObjV(t, b, "b", []byte("b1"))

	delim := "/"
	res := listVersions(t, b, &s3.ListObjectVersionsInput{Delimiter: &delim})
	require.Len(t, res.CommonPrefixes, 1, "common prefixes")
	require.Equal(t, "a/", *res.CommonPrefixes[0].Prefix, "common prefixes")
	require.Len(t, res.Versions, 1, "versions")
	require.Equal(t, "b", *res.Versions[0].Key, "versions")
	require.Equal(t, outB.VersionID, *res.Versions[0].VersionId, "versions")
	require.Empty(t, res.DeleteMarkers, "markers")
}
