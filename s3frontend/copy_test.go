package s3frontend

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/queue/spaceshardqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
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
				require.NoError(t, err, "got %v, want the copy to proceed", err)
				return
			}
			var pf s3err.PreconditionFailedError
			require.ErrorAs(t, err, &pf, "got %v, want PreconditionFailedError", err)
			require.Equal(t, http.StatusPreconditionFailed, pf.HTTPStatusCode, "status")
			want := s3err.Condition(copySourceConditionPrefix + string(tc.want))
			require.Equal(t, want, pf.Condition, "condition")
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
	require.ErrorIs(t, err, s3err.GetAPIError(s3err.ErrPreconditionFailed), "matching if-none-match")
	_, _, gerr := getObjV(t, b, dst, "")
	require.Equal(t, "NoSuchKey", apiErrCode(t, gerr), "destination must not exist after a failed precondition: %v", gerr)

	other := "00000000000000000000000000000000"
	_, err = b.CopyObject(context.Background(), s3response.CopyObjectInput{
		Bucket:                &bucket,
		Key:                   &dst,
		CopySource:            &src,
		CopySourceIfNoneMatch: &other,
	})
	require.NoError(t, err, "non-matching if-none-match")
	_, data, err := getObjV(t, b, dst, "")
	require.NoError(t, err, "copied GET")
	require.Equal(t, "copy me", string(data), "copied GET")
}

// A copy source in another tenant's bucket is AccessDenied before any key
// lookup, so neither the bucket's contents nor the key's existence leaks; a
// nonexistent source bucket stays NoSuchBucket, and a same-tenant source
// reaches the (still unimplemented) cross-space path.
func TestCopyObject_ForeignTenantSourceIsAccessDenied(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	tenantA, tenantB := testutil.RandomDID(t), testutil.RandomDID(t)
	for name, tenant := range map[string]did.DID{"a": tenantA, "a2": tenantA, "b": tenantB} {
		require.NoError(t, mem.Create(ctx, name, testutil.RandomDID(t), registry.CreateState{Tenant: tenant}), "create %s", name)
	}
	srcBucket, srcKey := "a", "obj"
	_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &srcBucket, Key: &srcKey, Body: bytes.NewReader([]byte("secret")), ContentLength: byteLen([]byte("secret"))})
	require.NoError(t, err, "put source")

	copyFrom := func(dst, source string) error {
		key := "copied"
		_, err := b.CopyObject(ctx, s3response.CopyObjectInput{Bucket: &dst, Key: &key, CopySource: &source})
		return err
	}
	require.Equal(t, "AccessDenied", apiErrCode(t, copyFrom("b", "a/obj")), "foreign source with existing key")
	require.Equal(t, "AccessDenied", apiErrCode(t, copyFrom("b", "a/no-such-key")), "foreign source with missing key (not NoSuchKey)")
	require.Equal(t, "NoSuchBucket", apiErrCode(t, copyFrom("b", "no-such-bucket/obj")), "nonexistent source bucket")
	require.NoError(t, copyFrom("a2", "a/obj"), "same-tenant cross-space source: want the copy to proceed")
}

// A copy between spaces re-ingests the source's bytes: the destination reads
// back the same bytes under new digests with their own claims, the source's
// claims are untouched, the ETag is the md5 of the bytes, the source's
// checksum value carries over as a full-object value, and metadata follows
// the directive.
func TestCopyObject_CrossSpaceReingest(t *testing.T) {
	b, mem, _ := newRefTestBackend(t, 64<<10) // 64 KiB blobs: the body spans several
	ctx := context.Background()
	tenant := testutil.RandomDID(t)
	for _, name := range []string{"src-bkt", "dst-bkt"} {
		require.NoError(t, mem.Create(ctx, name, testutil.RandomDID(t), registry.CreateState{Tenant: tenant}))
	}
	srcSt, _ := mem.Get(ctx, "src-bkt")
	dstSt, _ := mem.Get(ctx, "dst-bkt")

	data := bytes.Repeat([]byte("re-ingest me "), 20000) // ~260 KiB
	srcBucket, srcKey, ctype := "src-bkt", "obj", "text/bla"
	_, err := b.PutObject(ctx, s3response.PutObjectInput{
		Bucket: &srcBucket, Key: &srcKey, Body: bytes.NewReader(data), ContentLength: byteLen(data),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256, ContentType: &ctype,
		Metadata: map[string]string{"foo": "bar"},
	})
	require.NoError(t, err)
	srcRv, err := b.resolveVersion(ctx, "src-bkt", "obj", "")
	require.NoError(t, err)

	dstBucket, dstKey, source := "dst-bkt", "copied", "src-bkt/obj"
	out, err := b.CopyObject(ctx, s3response.CopyObjectInput{Bucket: &dstBucket, Key: &dstKey, CopySource: &source})
	require.NoError(t, err, "cross-space copy")
	require.Equal(t, `"`+hex.EncodeToString(md5Sum(data))+`"`, *out.CopyObjectResult.ETag, "copy ETag: want md5 of the bytes")
	require.NotNil(t, out.CopyObjectResult.ChecksumSHA256, "copy checksum = %+v, want the source's SHA256 as FULL_OBJECT", out.CopyObjectResult)
	require.Equal(t, sha256B64(data), *out.CopyObjectResult.ChecksumSHA256, "copy checksum = %+v, want the source's SHA256 as FULL_OBJECT", out.CopyObjectResult)
	require.Equal(t, types.ChecksumTypeFullObject, out.CopyObjectResult.ChecksumType, "copy checksum = %+v, want the source's SHA256 as FULL_OBJECT", out.CopyObjectResult)

	got, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &dstBucket, Key: &dstKey})
	require.NoError(t, err)
	gotBytes, _ := io.ReadAll(got.Body)
	got.Body.Close()
	require.True(t, bytes.Equal(data, gotBytes), "copied object: %d bytes, want %d", len(gotBytes), len(data))
	require.Equal(t, ctype, *got.ContentType, "copied object content type")
	require.Equal(t, "bar", got.Metadata["foo"], "copied object metadata")

	// New blobs under the destination's space, each with one claim there; the
	// source's blobs keep exactly their one claim in the source's space.
	dstRv, err := b.resolveVersion(ctx, "dst-bkt", "copied", "")
	require.NoError(t, err)
	dstBlobs := storedBlobs(t, b, dstRv)
	require.GreaterOrEqual(t, len(dstBlobs), 2, "expected a multi-blob body, got %d blobs", len(dstBlobs))
	srcDigests := map[string]bool{}
	for _, ref := range storedBlobs(t, b, srcRv) {
		srcDigests[string(ref.Digest)] = true
		n, _ := mem.CountReferences(ctx, srcSt.Space, ref.Digest, uuid.Nil)
		require.Equal(t, 1, n, "source blob claims")
	}
	for _, ref := range dstBlobs {
		require.False(t, srcDigests[string(ref.Digest)], "destination pins a source blob %x; a cross-space copy must re-ingest", ref.Digest)
		n, _ := mem.CountReferences(ctx, dstSt.Space, ref.Digest, uuid.Nil)
		require.Equal(t, 1, n, "destination blob claims")
	}

	// REPLACE takes the request's metadata instead of the source's.
	replaced, newType := "replaced", "application/x-new"
	_, err = b.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket: &dstBucket, Key: &replaced, CopySource: &source,
		MetadataDirective: types.MetadataDirectiveReplace, ContentType: &newType, Metadata: map[string]string{"k": "v"},
	})
	require.NoError(t, err)
	head, err := b.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &dstBucket, Key: &replaced})
	require.NoError(t, err, "REPLACE copy head: %v %v %v", err, head.ContentType, head.Metadata)
	require.Equal(t, newType, *head.ContentType, "REPLACE copy head: %v %v %v", err, head.ContentType, head.Metadata)
	require.Equal(t, "v", head.Metadata["k"], "REPLACE copy head: %v %v %v", err, head.ContentType, head.Metadata)
	require.Empty(t, head.Metadata["foo"], "REPLACE copy head: %v %v %v", err, head.ContentType, head.Metadata)
}

// hookCommitCapture is an authority capturer that runs hook once, at the
// capture of /blob/remove authority after skip earlier ones. A same-space
// copy captures it twice: as it starts taking its references to the source's
// blobs, and as its last step before it runs its commit, which is the one to
// hook with skip set to one.
type hookCommitCapture struct {
	uploader.AuthorityCapturer
	hook func()
	skip int
}

func (h *hookCommitCapture) CaptureAuthority(ctx context.Context, space did.DID, cmd ucan.Command) ([]byte, error) {
	if cmd == blobcmds.Remove.Command && h.hook != nil {
		if h.skip > 0 {
			h.skip--
		} else {
			hook := h.hook
			h.hook = nil
			hook()
		}
	}
	return h.AuthorityCapturer.CaptureAuthority(ctx, space, cmd)
}

// TestCopyObject_PinnedSourceDeletedBeforeCommitKeepsItsBlobs: a copy within
// its bucket, so its space, takes a reference to each of the source's blobs
// before its commit. If
// the source is deleted and its blobs' release runs after that and before the
// commit, the release finds the copy's references and does nothing, and the
// copy commits over blobs its provider still holds.
func TestCopyObject_PinnedSourceDeletedBeforeCommitKeepsItsBlobs(t *testing.T) {
	rm := &recordingRemover{}
	reg := &hookCommitCapture{}
	var b *Backend
	b, mem := newDeferredBackend(t, testfakes.NopUploader{}, func(d *Deps) {
		reg.AuthorityCapturer = d.Capturer
		d.Capturer = reg
		d.Remover = rm
	})
	ctx := context.Background()
	space, tenant := testutil.RandomDID(t), testutil.RandomDID(t)
	require.NoError(t, mem.Create(ctx, "src", space, registry.CreateState{Tenant: tenant}))
	srcBucket, srcKey := "src", "obj"
	body := []byte("pin me")
	_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &srcBucket, Key: &srcKey, Body: bytes.NewReader(body), ContentLength: byteLen(body)})
	require.NoError(t, err)
	srcRv, err := b.resolveVersion(ctx, "src", "obj", "")
	require.NoError(t, err)
	digests := storedBlobs(t, b, srcRv).DigestList()

	reg.skip = 1
	reg.hook = func() {
		_, err := b.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &srcBucket, Key: &srcKey})
		assert.NoError(t, err, "DeleteObject during the copy")
		drainReleases(t, b)
	}
	dstBucket, dstKey, source := "src", "copied", "src/obj"
	_, err = b.CopyObject(ctx, s3response.CopyObjectInput{Bucket: &dstBucket, Key: &dstKey, CopySource: &source})
	require.NoError(t, err, "copy whose source was deleted before its commit")
	require.Nil(t, reg.hook, "the source was never deleted")
	for _, d := range digests {
		n := rm.removedDigests()[string(d)]
		require.Zero(t, n, "removals of blob %x the copy references", d)
		rows, _ := mem.CountClaimRows(ctx, space, d)
		require.Equal(t, 1, rows, "claims on blob %x", d)
	}
	got := getFrom(t, b, "src", "copied")
	require.Equal(t, body, got, "the copy does not read back")
}

// TestPinBlobsRefusesABlobTheSpaceNoLongerHolds: a same-space copy's
// reference to a source blob whose release has run, its location gone, fails
// the copy.
func TestPinBlobsRefusesABlobTheSpaceNoLongerHolds(t *testing.T) {
	b, mem := newDeferredBackend(t, testfakes.NopUploader{})
	ctx := context.Background()
	st, err := mem.Get(ctx, "bk")
	require.NoError(t, err)
	var blobs msbucket.BlobRefs
	blobs.Append(mustSum([]byte("released")), 8)
	_, err = b.pinBlobs(ctx, st, blobs)
	require.ErrorIs(t, err, spaceshardqueue.ErrBlobGone, "pinBlobs of an unlocated blob")
}

// A copy of a multipart object is a single-part object on S3: its ETag is the
// md5 of the whole bytes rather than the source's "-N" form, and its checksum
// is a full-object value.
func TestCopyObject_MultipartSourceETag(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	p1 := bytes.Repeat([]byte("p"), 5<<20)
	p2 := bytes.Repeat([]byte("q"), 100)
	id := mpCreate(t, b, "mpsrc", "", "")
	u1, err := mpUploadPart(t, b, "mpsrc", id, 1, p1, nil)
	require.NoError(t, err)
	u2, err := mpUploadPart(t, b, "mpsrc", id, 2, p2, nil)
	require.NoError(t, err)
	n1, n2 := int32(1), int32(2)
	res, err := mpComplete(t, b, "mpsrc", id, []types.CompletedPart{{ETag: u1.ETag, PartNumber: &n1}, {ETag: u2.ETag, PartNumber: &n2}}, nil)
	require.NoError(t, err)
	require.Contains(t, *res.ETag, "-2", "multipart source ETag")

	bucket, key, source := "bk", "mpcopy", "bk/mpsrc"
	out, err := b.CopyObject(context.Background(), s3response.CopyObjectInput{Bucket: &bucket, Key: &key, CopySource: &source})
	require.NoError(t, err, "copy of a multipart object")
	all := append(append([]byte{}, p1...), p2...)
	require.Equal(t, `"`+hex.EncodeToString(md5Sum(all))+`"`, *out.CopyObjectResult.ETag, "copy ETag: want md5 of the bytes")
	require.Equal(t, types.ChecksumTypeFullObject, out.CopyObjectResult.ChecksumType, "copy checksum")
	require.NotNil(t, out.CopyObjectResult.ChecksumCRC64NVME, "copy checksum")
	_, got, err := getObjV(t, b, "mpcopy", "")
	require.NoError(t, err, "copy GET")
	require.True(t, bytes.Equal(all, got), "copy GET: %d bytes, want %d", len(got), len(all))
}

// Buckets whose owner was never recorded carry the same sentinel tenant, which
// must not make them look like one tenant's: a copy between two such buckets
// is refused, while a copy within one of them still proceeds.
func TestCopyObject_UnknownTenantSourceIsAccessDenied(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	for _, name := range []string{"legacy1", "legacy2"} {
		require.NoError(t, mem.Create(ctx, name, testutil.RandomDID(t), registry.CreateState{Tenant: registry.UnknownTenant}), "create %s", name)
	}
	srcBucket, srcKey := "legacy1", "obj"
	_, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &srcBucket, Key: &srcKey, Body: bytes.NewReader([]byte("legacy")), ContentLength: byteLen([]byte("legacy"))})
	require.NoError(t, err, "put source")
	copyFrom := func(dst, source string) error {
		key := "copied"
		_, err := b.CopyObject(ctx, s3response.CopyObjectInput{Bucket: &dst, Key: &key, CopySource: &source})
		return err
	}
	require.Equal(t, "AccessDenied", apiErrCode(t, copyFrom("legacy2", "legacy1/obj")), "copy between two unknown-tenant buckets")
	require.NoError(t, copyFrom("legacy1", "legacy1/obj"), "copy within an unknown-tenant bucket")
}

func TestIsMultipartETag(t *testing.T) {
	for etag, want := range map[string]bool{
		"cce1266ca5dbeb465a0f39ec0d6c8ad5-2":    true,
		`"cce1266ca5dbeb465a0f39ec0d6c8ad5-12"`: true,
		"6eb9fc855f310f9dc251ab2e5dfe179f":      false,
		`"6eb9fc855f310f9dc251ab2e5dfe179f"`:    false,
		"not-an-etag":                           false,
		"6eb9fc855f310f9dc251ab2e5dfe179f-":     false,
		"6eb9fc855f310f9dc251ab2e5dfe179f-2-3":  false,
	} {
		got := isMultipartETag(etag)
		assert.Equal(t, want, got, "isMultipartETag(%q)", etag)
	}
}

// A source written before every object carried a checksum has none to carry
// over; the copy computes the default CRC64NVME over the bytes instead.
func TestCopyObject_SourceWithoutChecksumGetsDefault(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	st, err := mem.Get(ctx, "bk")
	require.NoError(t, err)
	data := []byte("legacy object")
	body, blobs, refs, err := b.ingestBody(ctx, st, bytes.NewReader(data), int64(len(data)), nil, false)
	require.NoError(t, err)
	legacy := &msbucket.ObjectManifest{Key: "legacy", Created: time.Now().Unix(), Body: body, ETag: hex.EncodeToString(body.MD5), ContentType: "application/octet-stream"}
	require.NoError(t, commitPut(ctx, b, st, "legacy", legacy, blobs, refs))

	bucket, key, source := "bk", "copied", "bk/legacy"
	out, err := b.CopyObject(ctx, s3response.CopyObjectInput{Bucket: &bucket, Key: &key, CopySource: &source})
	require.NoError(t, err)
	require.NotNil(t, out.CopyObjectResult.ChecksumCRC64NVME, "copy of a checksum-less source")
	require.Equal(t, types.ChecksumTypeFullObject, out.CopyObjectResult.ChecksumType, "copy of a checksum-less source")
	require.Equal(t, `"`+legacy.ETag+`"`, *out.CopyObjectResult.ETag, "copy ETag")
}

// A source over S3's single-copy ceiling is refused before any byte is read;
// the manifest is committed directly, since no test can afford the bytes.
func TestCopyObject_SourceOverCopyLimit(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	st, err := mem.Get(ctx, "bk")
	require.NoError(t, err)
	huge := &msbucket.ObjectManifest{Key: "huge", Created: time.Now().Unix(), Body: msbucket.Body{Size: maxCopySize + 1}, ETag: "00000000000000000000000000000000"}
	// No blobs: an empty list, so the commit has nothing to read or claim.
	require.NoError(t, commitPut(ctx, b, st, "huge", huge, msbucket.BlobRefs{}, nil))
	bucket, key, source := "bk", "copied", "bk/huge"
	_, err = b.CopyObject(ctx, s3response.CopyObjectInput{Bucket: &bucket, Key: &key, CopySource: &source})
	require.Equal(t, "InvalidRequest", apiErrCode(t, err), "copy of a %d-byte source: %v", maxCopySize+1, err)
}
