package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/mst"
)

// Versioning tests over the same in-process harness as refindex_test.go,
// covering the docs/s3-versioning.md write rule, resolution, delete-marker,
// and list semantics.

func setVersioning(t *testing.T, b *Backend, status types.BucketVersioningStatus) {
	t.Helper()
	if err := b.PutBucketVersioning(context.Background(), "bk", status); err != nil {
		t.Fatalf("PutBucketVersioning(%s): %v", status, err)
	}
}

func putObjV(t *testing.T, b *Backend, key string, data []byte) s3response.PutObjectOutput {
	t.Helper()
	bucket := "bk"
	out, err := b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		t.Fatalf("PutObject %s: %v", key, err)
	}
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
	if rerr != nil {
		t.Fatalf("read body: %v", rerr)
	}
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
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
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
	if !errors.As(err, &apiErr) {
		t.Fatalf("not an API error: %v", err)
	}
	return apiErr.Code
}

func TestVersionID_Classify(t *testing.T) {
	if kind, _ := classifyVersionID(""); kind != versionKindCurrent {
		t.Fatalf("empty → %v, want current", kind)
	}
	if kind, _ := classifyVersionID("null"); kind != versionKindNull {
		t.Fatalf("null → %v, want null", kind)
	}
	// A minted token round-trips to its seq.
	tok := mintVersionID(42)
	kind, seq := classifyVersionID(tok)
	if kind != versionKindToken || seq != 42 {
		t.Fatalf("minted token → (%v, %d), want (token, 42)", kind, seq)
	}
	// A foreign-but-well-formed ULID is a token (resolution rejects it later).
	if kind, _ := classifyVersionID("01G65Z755AFWAKHE12NY0CQ9FH"); kind != versionKindToken {
		t.Fatalf("foreign ULID → %v, want token", kind)
	}
	// Malformed ids are invalid.
	for _, bad := range []string{"invalid_version_id", "../../secret.txt", "abc", "NULL "} {
		if kind, _ := classifyVersionID(bad); kind != versionKindInvalid {
			t.Fatalf("%q → %v, want invalid", bad, kind)
		}
	}
}

func TestRevSeqKey_NewestFirst(t *testing.T) {
	// Larger seq must sort lexicographically smaller (newest-first walk).
	keys := []string{revSeqKey(1), revSeqKey(2), revSeqKey(10), revSeqKey(1 << 40)}
	sorted := append([]string(nil), keys...)
	sort.Strings(sorted)
	for i := range keys {
		if keys[len(keys)-1-i] != sorted[i] {
			t.Fatalf("revSeqKey ordering broken: %v (sorted %v)", keys, sorted)
		}
	}
}

func TestVersioning_EnabledPutRetainsVersions(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)

	a, bb := []byte("version A"), []byte("version B")
	da, db := digestOf(t, a), digestOf(t, bb)
	out1 := putObjV(t, b, "k1", a)
	out2 := putObjV(t, b, "k1", bb)
	if out1.VersionID == "" || out2.VersionID == "" || out1.VersionID == out2.VersionID {
		t.Fatalf("expected distinct non-empty version ids, got %q / %q", out1.VersionID, out2.VersionID)
	}

	// Current is B; version-scoped read returns A.
	if _, data, err := getObjV(t, b, "k1", ""); err != nil || !bytes.Equal(data, bb) {
		t.Fatalf("current GET = %q, %v; want %q", data, err, bb)
	}
	got, data, err := getObjV(t, b, "k1", out1.VersionID)
	if err != nil || !bytes.Equal(data, a) {
		t.Fatalf("versioned GET = %q, %v; want %q", data, err, a)
	}
	if got.VersionId == nil || *got.VersionId != out1.VersionID {
		t.Fatalf("versioned GET VersionId = %v, want %q", got.VersionId, out1.VersionID)
	}

	// Overwrite under Enabled releases nothing.
	if claims(t, mem, da) != 1 || claims(t, mem, db) != 1 {
		t.Fatalf("claims(A)=%d claims(B)=%d, want 1/1", claims(t, mem, da), claims(t, mem, db))
	}
	if len(rm.removed) != 0 {
		t.Fatalf("removed %d blobs, want 0", len(rm.removed))
	}

	// ListObjectVersions: newest-first, IsLatest on the head.
	res := listVersions(t, b, nil)
	if len(res.Versions) != 2 || len(res.DeleteMarkers) != 0 {
		t.Fatalf("versions=%d markers=%d, want 2/0", len(res.Versions), len(res.DeleteMarkers))
	}
	if *res.Versions[0].VersionId != out2.VersionID || !*res.Versions[0].IsLatest {
		t.Fatalf("head version = %q latest=%v, want %q latest", *res.Versions[0].VersionId, *res.Versions[0].IsLatest, out2.VersionID)
	}
	if *res.Versions[1].VersionId != out1.VersionID || *res.Versions[1].IsLatest {
		t.Fatalf("second version = %q latest=%v, want %q non-latest", *res.Versions[1].VersionId, *res.Versions[1].IsLatest, out1.VersionID)
	}
}

func TestVersioning_NullVersionRetainedOnEnable(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	a, bb := []byte("pre-versioning"), []byte("post-enable")

	putObjV(t, b, "k1", a) // unversioned → the null version
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	putObjV(t, b, "k1", bb)

	// The null version survives beneath the numbered one.
	if _, data, err := getObjV(t, b, "k1", "null"); err != nil || !bytes.Equal(data, a) {
		t.Fatalf(`GET versionId=null = %q, %v; want %q`, data, err, a)
	}
	res := listVersions(t, b, nil)
	if len(res.Versions) != 2 {
		t.Fatalf("versions = %d, want 2", len(res.Versions))
	}
	if *res.Versions[1].VersionId != "null" || *res.Versions[1].IsLatest {
		t.Fatalf("bottom version = %q latest=%v, want null non-latest", *res.Versions[1].VersionId, *res.Versions[1].IsLatest)
	}
}

func TestVersioning_SuspendedReplacesNullInPlace(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	a := []byte("numbered A")
	da := digestOf(t, a)
	outA := putObjV(t, b, "k1", a)

	setVersioning(t, b, types.BucketVersioningStatusSuspended)
	bb, cc := []byte("null B"), []byte("null C")
	db, dc := digestOf(t, bb), digestOf(t, cc)

	outB := putObjV(t, b, "k1", bb)
	if outB.VersionID != "null" {
		t.Fatalf("suspended PUT VersionID = %q, want null", outB.VersionID)
	}
	// The numbered version was retained; nothing released yet.
	if claims(t, mem, da) != 1 || len(rm.removed) != 0 {
		t.Fatalf("claims(A)=%d removed=%d, want 1/0", claims(t, mem, da), len(rm.removed))
	}

	// A second null write replaces the first in place.
	putObjV(t, b, "k1", cc)
	if claims(t, mem, db) != 0 || rm.removedDigests()[string(db)] != 1 {
		t.Fatalf("claims(B)=%d removals(B)=%d, want 0/1", claims(t, mem, db), rm.removedDigests()[string(db)])
	}
	if claims(t, mem, dc) != 1 || claims(t, mem, da) != 1 {
		t.Fatalf("claims(C)=%d claims(A)=%d, want 1/1", claims(t, mem, dc), claims(t, mem, da))
	}

	res := listVersions(t, b, nil)
	if len(res.Versions) != 2 {
		t.Fatalf("versions = %d, want 2 (null + numbered)", len(res.Versions))
	}
	if *res.Versions[0].VersionId != "null" || !*res.Versions[0].IsLatest {
		t.Fatalf("head = %q latest=%v, want null latest", *res.Versions[0].VersionId, *res.Versions[0].IsLatest)
	}
	if *res.Versions[1].VersionId != outA.VersionID {
		t.Fatalf("bottom = %q, want %q", *res.Versions[1].VersionId, outA.VersionID)
	}
}

func TestVersioning_DeleteMarker(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	data := []byte("to be marked")
	d := digestOf(t, data)
	putObjV(t, b, "k1", data)

	out, err := deleteObjV(t, b, "k1", "")
	if err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if out.DeleteMarker == nil || !*out.DeleteMarker || out.VersionId == nil || *out.VersionId == "" {
		t.Fatalf("marker response = %+v, want DeleteMarker + VersionId", out)
	}
	markerID := *out.VersionId

	// Current-is-marker: GET/HEAD 404; scoped GET of the marker 405.
	if _, _, err := getObjV(t, b, "k1", ""); apiErrCode(t, err) != "NoSuchKey" {
		t.Fatalf("GET over marker = %v, want NoSuchKey", err)
	}
	if _, _, err := getObjV(t, b, "k1", markerID); apiErrCode(t, err) != "MethodNotAllowed" {
		t.Fatalf("GET marker version = %v, want MethodNotAllowed", err)
	}

	// The marked version's data is retained.
	if claims(t, mem, d) != 1 || len(rm.removed) != 0 {
		t.Fatalf("claims=%d removed=%d, want 1/0", claims(t, mem, d), len(rm.removed))
	}

	// ListObjects hides the key; ListObjectVersions shows marker + version.
	bucket := "bk"
	lres, err := b.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: &bucket})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if len(lres.Contents) != 0 {
		t.Fatalf("ListObjects shows %d keys over a marker, want 0", len(lres.Contents))
	}
	vres := listVersions(t, b, nil)
	if len(vres.DeleteMarkers) != 1 || len(vres.Versions) != 1 {
		t.Fatalf("markers=%d versions=%d, want 1/1", len(vres.DeleteMarkers), len(vres.Versions))
	}
	if !*vres.DeleteMarkers[0].IsLatest || *vres.DeleteMarkers[0].VersionId != markerID {
		t.Fatalf("marker entry = %+v, want latest %q", vres.DeleteMarkers[0], markerID)
	}

	// Deleting the marker by version id restores the object.
	dout, err := deleteObjV(t, b, "k1", markerID)
	if err != nil {
		t.Fatalf("delete marker: %v", err)
	}
	if dout.DeleteMarker == nil || !*dout.DeleteMarker || *dout.VersionId != markerID {
		t.Fatalf("delete-marker response = %+v, want DeleteMarker + id %q", dout, markerID)
	}
	if _, got, err := getObjV(t, b, "k1", ""); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("GET after marker removal = %q, %v; want %q", got, err, data)
	}
}

func TestVersioning_DeleteSpecificVersionPromotes(t *testing.T) {
	b, mem, rm := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	a, bb := []byte("keep me"), []byte("delete me")
	da, db := digestOf(t, a), digestOf(t, bb)
	outA := putObjV(t, b, "k1", a)
	outB := putObjV(t, b, "k1", bb)

	// Delete the CURRENT version: the older one is promoted.
	if _, err := deleteObjV(t, b, "k1", outB.VersionID); err != nil {
		t.Fatalf("delete current version: %v", err)
	}
	if _, data, err := getObjV(t, b, "k1", ""); err != nil || !bytes.Equal(data, a) {
		t.Fatalf("GET after promotion = %q, %v; want %q", data, err, a)
	}
	if claims(t, mem, db) != 0 || rm.removedDigests()[string(db)] != 1 {
		t.Fatalf("claims(B)=%d removals(B)=%d, want 0/1", claims(t, mem, db), rm.removedDigests()[string(db)])
	}
	res := listVersions(t, b, nil)
	if len(res.Versions) != 1 || *res.Versions[0].VersionId != outA.VersionID || !*res.Versions[0].IsLatest {
		t.Fatalf("after promotion versions = %+v, want just %q latest", res.Versions, outA.VersionID)
	}

	// Delete the last version: the key is gone entirely.
	if _, err := deleteObjV(t, b, "k1", outA.VersionID); err != nil {
		t.Fatalf("delete last version: %v", err)
	}
	if _, _, err := getObjV(t, b, "k1", ""); apiErrCode(t, err) != "NoSuchKey" {
		t.Fatalf("GET after last delete = %v, want NoSuchKey", err)
	}
	if claims(t, mem, da) != 0 {
		t.Fatalf("claims(A) = %d, want 0", claims(t, mem, da))
	}
	if n := len(listVersions(t, b, nil).Versions); n != 0 {
		t.Fatalf("versions after full delete = %d, want 0", n)
	}
}

func TestVersioning_SuspendedDeleteCreatesNullMarker(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	outA := putObjV(t, b, "k1", []byte("numbered"))
	setVersioning(t, b, types.BucketVersioningStatusSuspended)

	// Repeated suspended deletes each replace the null marker in place.
	for i := 0; i < 3; i++ {
		out, err := deleteObjV(t, b, "k1", "")
		if err != nil {
			t.Fatalf("suspended delete #%d: %v", i, err)
		}
		if out.DeleteMarker == nil || !*out.DeleteMarker || *out.VersionId != "null" {
			t.Fatalf("suspended delete #%d = %+v, want null marker", i, out)
		}
	}
	res := listVersions(t, b, nil)
	if len(res.DeleteMarkers) != 1 || *res.DeleteMarkers[0].VersionId != "null" || !*res.DeleteMarkers[0].IsLatest {
		t.Fatalf("markers = %+v, want one latest null marker", res.DeleteMarkers)
	}
	if len(res.Versions) != 1 || *res.Versions[0].VersionId != outA.VersionID {
		t.Fatalf("versions = %+v, want just %q", res.Versions, outA.VersionID)
	}

	// Removing the null marker restores the numbered version.
	if _, err := deleteObjV(t, b, "k1", "null"); err != nil {
		t.Fatalf("delete null marker: %v", err)
	}
	if _, data, err := getObjV(t, b, "k1", ""); err != nil || !bytes.Equal(data, []byte("numbered")) {
		t.Fatalf("GET after null-marker removal = %q, %v", data, err)
	}
}

func TestVersioning_DeleteMarkerOnMissingKey(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)

	// S3 inserts a marker even when the key does not exist.
	out, err := deleteObjV(t, b, "ghost", "")
	if err != nil {
		t.Fatalf("delete missing key: %v", err)
	}
	if out.DeleteMarker == nil || !*out.DeleteMarker {
		t.Fatalf("response = %+v, want a delete marker", out)
	}
	res := listVersions(t, b, nil)
	if len(res.DeleteMarkers) != 1 || len(res.Versions) != 0 {
		t.Fatalf("markers=%d versions=%d, want 1/0", len(res.DeleteMarkers), len(res.Versions))
	}
}

func TestVersioning_InvalidAndUnknownVersionIDs(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	putObjV(t, b, "k1", []byte("x"))

	if _, _, err := getObjV(t, b, "k1", "invalid_version_id"); apiErrCode(t, err) != "InvalidArgument" {
		t.Fatalf("malformed versionId GET = %v, want InvalidArgument", err)
	}
	if _, _, err := getObjV(t, b, "k1", "01G65Z755AFWAKHE12NY0CQ9FH"); apiErrCode(t, err) != "NoSuchVersion" {
		t.Fatalf("unknown versionId GET = %v, want NoSuchVersion", err)
	}
	if _, err := deleteObjV(t, b, "k1", "not-a-ulid!"); apiErrCode(t, err) != "InvalidArgument" {
		t.Fatalf("malformed versionId DELETE = %v, want InvalidArgument", err)
	}
	// A well-formed but absent version deletes as a success no-op.
	if _, err := deleteObjV(t, b, "k1", "01G65Z755AFWAKHE12NY0CQ9FH"); err != nil {
		t.Fatalf("unknown versionId DELETE = %v, want no-op success", err)
	}
	if _, _, err := getObjV(t, b, "k1", ""); err != nil {
		t.Fatalf("object must survive the no-op delete: %v", err)
	}
}

func TestVersioning_UnversionedScopedNullDelete(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	putObjV(t, b, "k1", []byte("plain"))
	if _, err := deleteObjV(t, b, "k1", "null"); err != nil {
		t.Fatalf("scoped null delete: %v", err)
	}
	if _, _, err := getObjV(t, b, "k1", ""); apiErrCode(t, err) != "NoSuchKey" {
		t.Fatalf("GET after scoped null delete = %v, want NoSuchKey", err)
	}
}

func TestVersioning_CopyFromVersion(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	a := []byte("original content")
	da := digestOf(t, a)
	outA := putObjV(t, b, "k1", a)
	putObjV(t, b, "k1", []byte("newer content"))

	bucket, dstKey := "bk", "k2"
	src := "bk/k1?versionId=" + outA.VersionID
	cout, err := b.CopyObject(context.Background(), s3response.CopyObjectInput{
		Bucket:     &bucket,
		Key:        &dstKey,
		CopySource: &src,
	})
	if err != nil {
		t.Fatalf("CopyObject from version: %v", err)
	}
	if cout.CopySourceVersionId == nil || *cout.CopySourceVersionId != outA.VersionID {
		t.Fatalf("CopySourceVersionId = %v, want %q", cout.CopySourceVersionId, outA.VersionID)
	}
	if cout.VersionId == nil || *cout.VersionId == "" {
		t.Fatalf("dest VersionId missing: %+v", cout)
	}
	if _, data, err := getObjV(t, b, "k2", ""); err != nil || !bytes.Equal(data, a) {
		t.Fatalf("copied GET = %q, %v; want %q", data, err, a)
	}
	// The shared digest now has two claims: k1's old version and k2's new one.
	if claims(t, mem, da) != 2 {
		t.Fatalf("claims = %d, want 2", claims(t, mem, da))
	}
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
		if res.NextKeyMarker == nil {
			t.Fatalf("truncated page missing NextKeyMarker")
		}
		in = &s3.ListObjectVersionsInput{
			MaxKeys:         &maxKeys,
			KeyMarker:       res.NextKeyMarker,
			VersionIdMarker: res.NextVersionIdMarker,
		}
	}
	if len(got) != len(want) {
		t.Fatalf("paged entries = %d, want %d\n got: %v\nwant: %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entry %d = %q, want %q\n got: %v\nwant: %v", i, got[i], want[i], got, want)
		}
	}
}

func TestVersioning_UnversionedResponsesOmitVersionIDs(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	out := putObjV(t, b, "k1", []byte("plain"))
	if out.VersionID != "" {
		t.Fatalf("unversioned PUT VersionID = %q, want empty", out.VersionID)
	}
	gout, _, err := getObjV(t, b, "k1", "")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if gout.VersionId != nil {
		t.Fatalf("unversioned GET VersionId = %q, want nil", *gout.VersionId)
	}
	dout, err := deleteObjV(t, b, "k1", "")
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	if dout.VersionId != nil || dout.DeleteMarker != nil {
		t.Fatalf("unversioned DELETE = %+v, want no version fields", dout)
	}
}

func TestVersioning_GetBucketVersioningStates(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	ctx := context.Background()

	out, err := b.GetBucketVersioning(ctx, "bk")
	if err != nil {
		t.Fatalf("GetBucketVersioning: %v", err)
	}
	if out.Status != nil {
		t.Fatalf("never-configured Status = %v, want nil", *out.Status)
	}
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	if out, _ = b.GetBucketVersioning(ctx, "bk"); out.Status == nil || *out.Status != types.BucketVersioningStatusEnabled {
		t.Fatalf("Status = %v, want Enabled", out.Status)
	}
	setVersioning(t, b, types.BucketVersioningStatusSuspended)
	if out, _ = b.GetBucketVersioning(ctx, "bk"); out.Status == nil || *out.Status != types.BucketVersioningStatusSuspended {
		t.Fatalf("Status = %v, want Suspended", out.Status)
	}
	if _, err := b.GetBucketVersioning(ctx, "nope"); apiErrCode(t, err) != "NoSuchBucket" {
		t.Fatalf("missing bucket = %v, want NoSuchBucket", err)
	}
}

func TestVersioning_DeleteObjectsMixed(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	outA := putObjV(t, b, "a", []byte("aaa"))
	putObjV(t, b, "b", []byte("bbb"))

	bucket := "bk"
	res, err := b.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
		Bucket: &bucket,
		Delete: &types.Delete{Objects: []types.ObjectIdentifier{
			{Key: strPtr("a"), VersionId: &outA.VersionID}, // scoped: permanent
			{Key: strPtr("b")}, // unscoped: marker
		}},
	})
	if err != nil {
		t.Fatalf("DeleteObjects: %v", err)
	}
	if len(res.Error) != 0 {
		t.Fatalf("errors = %+v, want none", res.Error)
	}
	if len(res.Deleted) != 2 {
		t.Fatalf("deleted = %d entries, want 2", len(res.Deleted))
	}
	byKey := map[string]types.DeletedObject{}
	for _, d := range res.Deleted {
		byKey[*d.Key] = d
	}
	if e := byKey["a"]; e.VersionId == nil || *e.VersionId != outA.VersionID || e.DeleteMarker == nil || *e.DeleteMarker {
		t.Fatalf(`entry "a" = %+v, want scoped VersionId + explicit DeleteMarker=false`, e)
	}
	if e := byKey["b"]; e.DeleteMarker == nil || !*e.DeleteMarker || e.DeleteMarkerVersionId == nil {
		t.Fatalf(`entry "b" = %+v, want marker fields`, e)
	}

	// "a" is fully gone (its only version removed); "b" is marker-hidden.
	if _, _, err := getObjV(t, b, "a", ""); apiErrCode(t, err) != "NoSuchKey" {
		t.Fatalf(`GET "a" = %v, want NoSuchKey`, err)
	}
	if _, _, err := getObjV(t, b, "b", ""); apiErrCode(t, err) != "NoSuchKey" {
		t.Fatalf(`GET "b" = %v, want NoSuchKey`, err)
	}
	if n := len(listVersions(t, b, nil).Versions); n != 1 {
		t.Fatalf("remaining versions = %d, want 1 (b's data version)", n)
	}
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
	da := digestOf(t, a)
	putObjV(t, b, "k1", a)
	setVersioning(t, b, types.BucketVersioningStatusEnabled)
	bb := []byte("numbered B")
	db := digestOf(t, bb)
	outB := putObjV(t, b, "k1", bb)

	// A Suspended write is a new null: it must evict A from the prev tree.
	setVersioning(t, b, types.BucketVersioningStatusSuspended)
	cc := []byte("null C")
	dc := digestOf(t, cc)
	putObjV(t, b, "k1", cc)

	// A's claim is released and its blob removed; B and C stay claimed.
	if claims(t, mem, da) != 0 || rm.removedDigests()[string(da)] != 1 {
		t.Fatalf("claims(A)=%d removals(A)=%d, want 0/1", claims(t, mem, da), rm.removedDigests()[string(da)])
	}
	if claims(t, mem, db) != 1 || claims(t, mem, dc) != 1 {
		t.Fatalf("claims(B)=%d claims(C)=%d, want 1/1", claims(t, mem, db), claims(t, mem, dc))
	}

	// The stack is [C null latest, B numbered]; the old null resolves to C.
	res := listVersions(t, b, nil)
	if len(res.Versions) != 2 || len(res.DeleteMarkers) != 0 {
		t.Fatalf("versions=%d markers=%d, want 2/0", len(res.Versions), len(res.DeleteMarkers))
	}
	if *res.Versions[0].VersionId != "null" || !*res.Versions[0].IsLatest {
		t.Fatalf("head = %q latest=%v, want null latest", *res.Versions[0].VersionId, *res.Versions[0].IsLatest)
	}
	if *res.Versions[1].VersionId != outB.VersionID || *res.Versions[1].IsLatest {
		t.Fatalf("bottom = %q latest=%v, want %q non-latest", *res.Versions[1].VersionId, *res.Versions[1].IsLatest, outB.VersionID)
	}
	if _, data, err := getObjV(t, b, "k1", "null"); err != nil || !bytes.Equal(data, cc) {
		t.Fatalf("GET null = %q, %v; want %q", data, err, cc)
	}
}

// getValue decodes the top-MST value block for key: a bare manifest or an
// enveloped leaf (docs/s3-versioning.md §2.1).
func getValue(t *testing.T, b *Backend, key string) msbucket.ObjectValue {
	t.Helper()
	ctx := context.Background()
	st, err := b.reg.Get(ctx, "bk")
	if err != nil {
		t.Fatalf("registry get: %v", err)
	}
	valCid, err := mst.LoadMST(b.read, st.Space, st.Root).Get(ctx, key)
	if err != nil {
		t.Fatalf("mst get %s: %v", key, err)
	}
	var val msbucket.ObjectValue
	if err := b.read.Get(ctx, st.Space, valCid, &val); err != nil {
		t.Fatalf("value get %s: %v", key, err)
	}
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
	if len(res.Versions) != 1 || *res.Versions[0].VersionId != "null" {
		t.Fatalf("versions = %+v, want one null version", res.Versions)
	}
	// Structurally: the value block is the manifest itself.
	val := getValue(t, b, "k1")
	if val.Manifest == nil || val.Leaf != nil {
		t.Fatalf("value = {Manifest:%v Leaf:%v}, want bare manifest", val.Manifest, val.Leaf)
	}
	if val.Manifest.VersionID != "null" {
		t.Fatalf("manifest VersionID = %q, want null", val.Manifest.VersionID)
	}
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
	if val := getValue(t, b, "k1"); val.Manifest == nil {
		t.Fatalf("single-version Enabled key: value = {Leaf:%v}, want bare manifest", val.Leaf)
	}

	out2 := putObjV(t, b, "k1", []byte("two"))
	val := getValue(t, b, "k1")
	if val.Leaf == nil {
		t.Fatalf("superseded key: value is still a bare manifest, want leaf")
	}
	if val.Leaf.Current.VersionID != out2.VersionID || val.Leaf.Prev == nil {
		t.Fatalf("leaf = {Current:%q Prev:%v}, want current %q with a prev tree", val.Leaf.Current.VersionID, val.Leaf.Prev, out2.VersionID)
	}

	// Delete the current version: the old one promotes, and the key keeps its
	// leaf even though only one version remains.
	if _, err := deleteObjV(t, b, "k1", out2.VersionID); err != nil {
		t.Fatalf("delete version: %v", err)
	}
	val = getValue(t, b, "k1")
	if val.Leaf == nil {
		t.Fatalf("post-delete key: value downgraded to a bare manifest, want leaf (invariant 6)")
	}
	if val.Leaf.Prev != nil {
		t.Fatalf("post-delete leaf.Prev = %v, want nil (single version)", val.Leaf.Prev)
	}
	if _, data, err := getObjV(t, b, "k1", ""); err != nil || !bytes.Equal(data, []byte("one")) {
		t.Fatalf("GET after promotion = %q, %v; want \"one\"", data, err)
	}
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
	if len(res.CommonPrefixes) != 1 || *res.CommonPrefixes[0].Prefix != "a/" {
		t.Fatalf("common prefixes = %+v, want [a/]", res.CommonPrefixes)
	}
	if len(res.Versions) != 1 || *res.Versions[0].Key != "b" || *res.Versions[0].VersionId != outB.VersionID {
		t.Fatalf("versions = %+v, want just b@%s", res.Versions, outB.VersionID)
	}
	if len(res.DeleteMarkers) != 0 {
		t.Fatalf("markers = %+v, want none", res.DeleteMarkers)
	}
}
