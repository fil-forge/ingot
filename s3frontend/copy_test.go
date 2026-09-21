package s3frontend

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"

	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/registry"
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

// A copy source in another tenant's bucket is AccessDenied before any key
// lookup, so neither the bucket's contents nor the key's existence leaks; a
// nonexistent source bucket stays NoSuchBucket, and a same-tenant source
// reaches the (still unimplemented) cross-space path.
func TestCopyObject_ForeignTenantSourceIsAccessDenied(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	tenantA, tenantB := testutil.RandomDID(t), testutil.RandomDID(t)
	for name, tenant := range map[string]did.DID{"a": tenantA, "a2": tenantA, "b": tenantB} {
		if err := mem.Create(ctx, name, testutil.RandomDID(t), registry.CreateState{Tenant: tenant}); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	srcBucket, srcKey := "a", "obj"
	if _, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &srcBucket, Key: &srcKey, Body: bytes.NewReader([]byte("secret"))}); err != nil {
		t.Fatalf("put source: %v", err)
	}

	copyFrom := func(dst, source string) error {
		key := "copied"
		_, err := b.CopyObject(ctx, s3response.CopyObjectInput{Bucket: &dst, Key: &key, CopySource: &source})
		return err
	}
	if err := copyFrom("b", "a/obj"); apiErrCode(t, err) != "AccessDenied" {
		t.Fatalf("foreign source with existing key: %v, want AccessDenied", err)
	}
	if err := copyFrom("b", "a/no-such-key"); apiErrCode(t, err) != "AccessDenied" {
		t.Fatalf("foreign source with missing key: %v, want AccessDenied (not NoSuchKey)", err)
	}
	if err := copyFrom("b", "no-such-bucket/obj"); apiErrCode(t, err) != "NoSuchBucket" {
		t.Fatalf("nonexistent source bucket: %v, want NoSuchBucket", err)
	}
	if err := copyFrom("a2", "a/obj"); err != nil {
		t.Fatalf("same-tenant cross-space source: %v, want the copy to proceed", err)
	}
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
		if err := mem.Create(ctx, name, testutil.RandomDID(t), registry.CreateState{Tenant: tenant}); err != nil {
			t.Fatal(err)
		}
	}
	srcSt, _ := mem.Get(ctx, "src-bkt")
	dstSt, _ := mem.Get(ctx, "dst-bkt")

	data := bytes.Repeat([]byte("re-ingest me "), 20000) // ~260 KiB
	srcBucket, srcKey, ctype := "src-bkt", "obj", "text/bla"
	if _, err := b.PutObject(ctx, s3response.PutObjectInput{
		Bucket: &srcBucket, Key: &srcKey, Body: bytes.NewReader(data),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256, ContentType: &ctype,
		Metadata: map[string]string{"foo": "bar"},
	}); err != nil {
		t.Fatal(err)
	}
	srcRv, err := b.resolveVersion(ctx, "src-bkt", "obj", "")
	if err != nil {
		t.Fatal(err)
	}

	dstBucket, dstKey, source := "dst-bkt", "copied", "src-bkt/obj"
	out, err := b.CopyObject(ctx, s3response.CopyObjectInput{Bucket: &dstBucket, Key: &dstKey, CopySource: &source})
	if err != nil {
		t.Fatalf("cross-space copy: %v", err)
	}
	if want := `"` + hex.EncodeToString(md5Sum(data)) + `"`; *out.CopyObjectResult.ETag != want {
		t.Fatalf("copy ETag = %s, want md5 of the bytes %s", *out.CopyObjectResult.ETag, want)
	}
	if out.CopyObjectResult.ChecksumSHA256 == nil || *out.CopyObjectResult.ChecksumSHA256 != sha256B64(data) || out.CopyObjectResult.ChecksumType != types.ChecksumTypeFullObject {
		t.Fatalf("copy checksum = %+v, want the source's SHA256 as FULL_OBJECT", out.CopyObjectResult)
	}

	got, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &dstBucket, Key: &dstKey})
	if err != nil {
		t.Fatal(err)
	}
	gotBytes, _ := io.ReadAll(got.Body)
	got.Body.Close()
	if !bytes.Equal(gotBytes, data) || *got.ContentType != ctype || got.Metadata["foo"] != "bar" {
		t.Fatalf("copied object: %d bytes, %q, %v", len(gotBytes), *got.ContentType, got.Metadata)
	}

	// New blobs under the destination's space, each with one claim there; the
	// source's blobs keep exactly their one claim in the source's space.
	dstRv, err := b.resolveVersion(ctx, "dst-bkt", "copied", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(dstRv.mf.Body.Blobs) < 2 {
		t.Fatalf("expected a multi-blob body, got %d blobs", len(dstRv.mf.Body.Blobs))
	}
	srcDigests := map[string]bool{}
	for _, ref := range srcRv.mf.Body.Blobs {
		srcDigests[string(ref.Digest)] = true
		if n, _ := mem.CountClaims(ctx, srcSt.Space, ref.Digest); n != 1 {
			t.Fatalf("source blob claims = %d, want 1 (untouched)", n)
		}
	}
	for _, ref := range dstRv.mf.Body.Blobs {
		if srcDigests[string(ref.Digest)] {
			t.Fatalf("destination pins a source blob %x; a cross-space copy must re-ingest", ref.Digest)
		}
		if n, _ := mem.CountClaims(ctx, dstSt.Space, ref.Digest); n != 1 {
			t.Fatalf("destination blob claims = %d, want 1", n)
		}
	}

	// REPLACE takes the request's metadata instead of the source's.
	replaced, newType := "replaced", "application/x-new"
	if _, err := b.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket: &dstBucket, Key: &replaced, CopySource: &source,
		MetadataDirective: types.MetadataDirectiveReplace, ContentType: &newType, Metadata: map[string]string{"k": "v"},
	}); err != nil {
		t.Fatal(err)
	}
	head, err := b.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &dstBucket, Key: &replaced})
	if err != nil || *head.ContentType != newType || head.Metadata["k"] != "v" || head.Metadata["foo"] != "" {
		t.Fatalf("REPLACE copy head: %v %v %v", err, head.ContentType, head.Metadata)
	}
}

// hookAllocSeq is a bucket registry that runs hook once, at the named
// bucket's next version-seq allocation: the first step of a commit, inside
// the destination bucket's lock, after the copy has resolved its source.
type hookAllocSeq struct {
	registry.Registry
	bucket string
	hook   func()
}

func (h *hookAllocSeq) AllocVersionSeq(ctx context.Context, name string) (uint64, error) {
	if name == h.bucket && h.hook != nil {
		hook := h.hook
		h.hook = nil
		hook()
	}
	return h.Registry.AllocVersionSeq(ctx, name)
}

// TestCopyObject_PinnedSourceReleasedBeforeCommitIsNoSuchKey: a same-space
// copy pins the source's body. If the source is deleted and its blobs'
// release runs after the copy resolved the source but before its commit,
// the pin finds no claim to attach to and the copy fails with NoSuchKey
// instead of committing a manifest over released blobs. Nothing is left
// claimed or pending afterwards.
func TestCopyObject_PinnedSourceReleasedBeforeCommitIsNoSuchKey(t *testing.T) {
	rm := &recordingRemover{}
	reg := &hookAllocSeq{bucket: "dst"}
	var b *Backend
	b, mem := newDeferredBackend(t, inmem.NopUploader{}, func(d *Deps) {
		reg.Registry = d.Registry
		d.Registry = reg
		d.Remover = rm
	})
	ctx := context.Background()
	space, tenant := testutil.RandomDID(t), testutil.RandomDID(t)
	for _, name := range []string{"src", "dst"} {
		if err := mem.Create(ctx, name, space, registry.CreateState{Tenant: tenant}); err != nil {
			t.Fatal(err)
		}
	}
	srcBucket, srcKey := "src", "obj"
	if _, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &srcBucket, Key: &srcKey, Body: bytes.NewReader([]byte("pin me"))}); err != nil {
		t.Fatal(err)
	}
	srcRv, err := b.resolveVersion(ctx, "src", "obj", "")
	if err != nil {
		t.Fatal(err)
	}
	digests := bodyDigests(srcRv.mf.Body)

	reg.hook = func() {
		if _, err := b.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &srcBucket, Key: &srcKey}); err != nil {
			t.Errorf("DeleteObject during the copy: %v", err)
		}
		drainReleases(t, b)
	}
	dstBucket, dstKey, source := "dst", "copied", "src/obj"
	_, err = b.CopyObject(ctx, s3response.CopyObjectInput{Bucket: &dstBucket, Key: &dstKey, CopySource: &source})
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "NoSuchKey" {
		t.Fatalf("copy whose source was released before the commit: err = %v, want NoSuchKey", err)
	}
	if reg.hook != nil {
		t.Fatal("the source was never deleted")
	}
	if _, err := b.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &dstBucket, Key: &dstKey}); err == nil {
		t.Fatal("the copy committed a manifest over released blobs")
	}
	for _, d := range digests {
		if n, _ := mem.CountClaims(ctx, space, d); n != 0 {
			t.Fatalf("claims on released blob %x = %d, want 0 (a leaked pin)", d, n)
		}
		if rm.removedDigests()[string(d)] != 1 {
			t.Fatalf("blob %x removed %d times, want once", d, rm.removedDigests()[string(d)])
		}
	}
	if pending, _ := mem.ListReleasesBySpace(ctx, space); len(pending) != 0 {
		t.Fatalf("pending releases after the refused copy = %v, want none", pending)
	}

	// The same copy against a live source pins as before.
	if _, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &srcBucket, Key: &srcKey, Body: bytes.NewReader([]byte("pin me"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.CopyObject(ctx, s3response.CopyObjectInput{Bucket: &dstBucket, Key: &dstKey, CopySource: &source}); err != nil {
		t.Fatalf("copy of a live source: %v", err)
	}
	dstRv, err := b.resolveVersion(ctx, "dst", "copied", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range bodyDigests(dstRv.mf.Body) {
		if n, _ := mem.CountClaims(ctx, space, d); n != 2 {
			t.Fatalf("claims on pinned blob %x = %d, want 2 (source and copy)", d, n)
		}
	}
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
	if err != nil {
		t.Fatal(err)
	}
	u2, err := mpUploadPart(t, b, "mpsrc", id, 2, p2, nil)
	if err != nil {
		t.Fatal(err)
	}
	n1, n2 := int32(1), int32(2)
	res, err := mpComplete(t, b, "mpsrc", id, []types.CompletedPart{{ETag: u1.ETag, PartNumber: &n1}, {ETag: u2.ETag, PartNumber: &n2}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains([]byte(*res.ETag), []byte("-2")) {
		t.Fatalf("multipart source ETag = %s, want a -2 suffix", *res.ETag)
	}

	bucket, key, source := "bk", "mpcopy", "bk/mpsrc"
	out, err := b.CopyObject(context.Background(), s3response.CopyObjectInput{Bucket: &bucket, Key: &key, CopySource: &source})
	if err != nil {
		t.Fatalf("copy of a multipart object: %v", err)
	}
	all := append(append([]byte{}, p1...), p2...)
	if want := `"` + hex.EncodeToString(md5Sum(all)) + `"`; *out.CopyObjectResult.ETag != want {
		t.Fatalf("copy ETag = %s, want md5 of the bytes %s", *out.CopyObjectResult.ETag, want)
	}
	if out.CopyObjectResult.ChecksumType != types.ChecksumTypeFullObject || out.CopyObjectResult.ChecksumCRC64NVME == nil {
		t.Fatalf("copy checksum = %+v, want a FULL_OBJECT CRC64NVME", out.CopyObjectResult)
	}
	if _, got, err := getObjV(t, b, "mpcopy", ""); err != nil || !bytes.Equal(got, all) {
		t.Fatalf("copy GET: %d bytes, %v", len(got), err)
	}
}

// Buckets whose owner was never recorded carry the same sentinel tenant, which
// must not make them look like one tenant's: a copy between two such buckets
// is refused, while a copy within one of them still proceeds.
func TestCopyObject_UnknownTenantSourceIsAccessDenied(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	for _, name := range []string{"legacy1", "legacy2"} {
		if err := mem.Create(ctx, name, testutil.RandomDID(t), registry.CreateState{Tenant: registry.UnknownTenant}); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	srcBucket, srcKey := "legacy1", "obj"
	if _, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &srcBucket, Key: &srcKey, Body: bytes.NewReader([]byte("legacy"))}); err != nil {
		t.Fatalf("put source: %v", err)
	}
	copyFrom := func(dst, source string) error {
		key := "copied"
		_, err := b.CopyObject(ctx, s3response.CopyObjectInput{Bucket: &dst, Key: &key, CopySource: &source})
		return err
	}
	if err := copyFrom("legacy2", "legacy1/obj"); apiErrCode(t, err) != "AccessDenied" {
		t.Fatalf("copy between two unknown-tenant buckets: %v, want AccessDenied", err)
	}
	if err := copyFrom("legacy1", "legacy1/obj"); err != nil {
		t.Fatalf("copy within an unknown-tenant bucket: %v", err)
	}
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
		if got := isMultipartETag(etag); got != want {
			t.Errorf("isMultipartETag(%q) = %v, want %v", etag, got, want)
		}
	}
}

// A source written before every object carried a checksum has none to carry
// over; the copy computes the default CRC64NVME over the bytes instead.
func TestCopyObject_SourceWithoutChecksumGetsDefault(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	st, err := mem.Get(ctx, "bk")
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("legacy object")
	body, err := b.ingestBody(ctx, st, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	legacy := &msbucket.ObjectManifest{Key: "legacy", Created: time.Now().Unix(), Body: body, ETag: hex.EncodeToString(body.MD5), ContentType: "application/octet-stream"}
	if _, _, err := b.commitVersion(ctx, st, "legacy", legacy, nil, false, nil); err != nil {
		t.Fatal(err)
	}

	bucket, key, source := "bk", "copied", "bk/legacy"
	out, err := b.CopyObject(ctx, s3response.CopyObjectInput{Bucket: &bucket, Key: &key, CopySource: &source})
	if err != nil {
		t.Fatal(err)
	}
	if out.CopyObjectResult.ChecksumCRC64NVME == nil || out.CopyObjectResult.ChecksumType != types.ChecksumTypeFullObject {
		t.Fatalf("copy of a checksum-less source = %+v, want a FULL_OBJECT CRC64NVME", out.CopyObjectResult)
	}
	if want := `"` + legacy.ETag + `"`; *out.CopyObjectResult.ETag != want {
		t.Fatalf("copy ETag = %s, want the source's %s", *out.CopyObjectResult.ETag, want)
	}
}

// A source over S3's single-copy ceiling is refused before any byte is read;
// the manifest is committed directly, since no test can afford the bytes.
func TestCopyObject_SourceOverCopyLimit(t *testing.T) {
	b, mem, _ := newRefTestBackend(t)
	ctx := context.Background()
	st, err := mem.Get(ctx, "bk")
	if err != nil {
		t.Fatal(err)
	}
	huge := &msbucket.ObjectManifest{Key: "huge", Created: time.Now().Unix(), Body: msbucket.Body{Size: maxCopySize + 1}, ETag: "00000000000000000000000000000000"}
	if _, _, err := b.commitVersion(ctx, st, "huge", huge, nil, false, nil); err != nil {
		t.Fatal(err)
	}
	bucket, key, source := "bk", "copied", "bk/huge"
	_, err = b.CopyObject(ctx, s3response.CopyObjectInput{Bucket: &bucket, Key: &key, CopySource: &source})
	if got := apiErrCode(t, err); got != "InvalidRequest" {
		t.Fatalf("copy of a %d-byte source: %s (%v), want InvalidRequest", maxCopySize+1, got, err)
	}
}
