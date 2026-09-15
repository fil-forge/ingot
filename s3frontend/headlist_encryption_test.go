package s3frontend

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/backend"
)

// storedEnvelopeSizes returns the on-disk size of every blob an object
// version stores — the FEE envelopes, each larger than the plaintext span
// it encrypts — and their sum.
func storedEnvelopeSizes(t *testing.T, b *Backend, key string) (sizes []int64, total int64) {
	t.Helper()
	for _, d := range blobDigestsOf(t, b, key, "") {
		fi, err := os.Stat(b.spool.Path(d))
		if err != nil {
			t.Fatalf("stat spooled blob: %v", err)
		}
		sizes = append(sizes, fi.Size())
		total += fi.Size()
	}
	return sizes, total
}

// assertPlaintextSize fails when a reported size is anything other than the
// plaintext length — in particular a stored envelope's size or their sum.
func assertPlaintextSize(t *testing.T, what string, got int64, want int64, envelopes []int64, envelopeTotal int64) {
	t.Helper()
	if got == want {
		return
	}
	for _, e := range envelopes {
		if got == e {
			t.Fatalf("%s = %d: a stored envelope's size, want the plaintext %d", what, got, want)
		}
	}
	if got == envelopeTotal {
		t.Fatalf("%s = %d: the stored envelopes' total, want the plaintext %d", what, got, want)
	}
	t.Fatalf("%s = %d, want the plaintext %d", what, got, want)
}

// assertPartHeaders checks a ?partNumber response's Content-Length,
// Content-Range and PartsCount against the part's plaintext coordinates.
func assertPartHeaders(t *testing.T, what string, length *int64, contentRange *string, partsCount *int32, wantLen int64, wantCR string, wantParts int32, envelopes []int64, envelopeTotal int64) {
	t.Helper()
	if length == nil {
		t.Fatalf("%s: no Content-Length", what)
	}
	assertPlaintextSize(t, what+" Content-Length", *length, wantLen, envelopes, envelopeTotal)
	if contentRange == nil || *contentRange != wantCR {
		t.Fatalf("%s Content-Range = %v, want %q", what, contentRange, wantCR)
	}
	if partsCount == nil || *partsCount != wantParts {
		t.Fatalf("%s PartsCount = %v, want %d", what, partsCount, wantParts)
	}
}

func assertETag(t *testing.T, what string, got *string, want string) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s: no ETag", what)
	}
	if g := strings.Trim(*got, `"`); g != want {
		t.Fatalf("%s ETag = %q, want the plaintext-derived %q", what, g, want)
	}
}

// TestHeadListReportPlaintextSizes: every size and ETag ingot reports comes
// from the manifest's plaintext coordinates, never from the ciphertext it
// stores. A single-PUT object and a multipart object, each split across
// several envelopes, are checked through HEAD, GET (whole, ranged, and
// ?partNumber), ListObjects, ListObjectsV2, ListObjectVersions, and
// GetObjectAttributes. Each assertion also names the failure mode it guards
// against: a stored envelope's size, or the envelopes' total, leaking out.
func TestHeadListReportPlaintextSizes(t *testing.T) {
	ctx := context.Background()
	b, _, _ := newRefTestBackend(t, 64<<10)
	bucket := "bk"

	// Single PUT: 200 KiB + 37 bytes → four envelopes at a 64 KiB ceiling.
	single := testBody((200 << 10) + 37)
	singleKey := "plain/single"
	putObjV(t, b, singleKey, single)
	singleETag := hex.EncodeToString(md5sum(single))

	// Multipart: two parts, the first spanning many envelopes. A composite
	// checksum is declared so GetObjectAttributes emits the per-part list
	// (AWS reports it only for checksummed multipart objects).
	mpKey := "plain/multipart"
	parts := [][]byte{testBody(int(backend.MinPartSize) + 4096), testBody(9 << 10)}
	uploadID := mpCreate(t, b, mpKey, types.ChecksumAlgorithmCrc32c, types.ChecksumTypeComposite)
	var completed []types.CompletedPart
	var mpWhole []byte
	etagCat := md5.New()
	for i, data := range parts {
		pn := int32(i + 1)
		sum := crc32cB64(data)
		out, err := mpUploadPart(t, b, mpKey, uploadID, pn, data, func(in *s3.UploadPartInput) { in.ChecksumCRC32C = &sum })
		if err != nil {
			t.Fatalf("UploadPart %d: %v", pn, err)
		}
		completed = append(completed, types.CompletedPart{PartNumber: &pn, ETag: out.ETag, ChecksumCRC32C: out.ChecksumCRC32C})
		mpWhole = append(mpWhole, data...)
		md := md5.Sum(data)
		etagCat.Write(md[:])
	}
	if _, err := mpComplete(t, b, mpKey, uploadID, completed, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	mpETag := hex.EncodeToString(etagCat.Sum(nil)) + "-2"

	type object struct {
		key       string
		plaintext []byte
		etag      string
	}
	objects := []object{{singleKey, single, singleETag}, {mpKey, mpWhole, mpETag}}

	// The stored envelopes must actually differ from the plaintext, or the
	// assertions below could not tell the two apart.
	envelopes := map[string][]int64{}
	envelopeTotal := map[string]int64{}
	for _, o := range objects {
		sizes, total := storedEnvelopeSizes(t, b, o.key)
		if len(sizes) < 2 {
			t.Fatalf("%s stored %d blobs, want several (small blob ceiling)", o.key, len(sizes))
		}
		if total <= int64(len(o.plaintext)) {
			t.Fatalf("%s stored %d bytes for %d plaintext; envelopes must be larger", o.key, total, len(o.plaintext))
		}
		envelopes[o.key], envelopeTotal[o.key] = sizes, total
	}

	for _, o := range objects {
		want := int64(len(o.plaintext))
		env, envTotal := envelopes[o.key], envelopeTotal[o.key]

		head, err := b.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &o.key})
		if err != nil {
			t.Fatalf("HeadObject %s: %v", o.key, err)
		}
		assertPlaintextSize(t, "HEAD "+o.key+" Content-Length", *head.ContentLength, want, env, envTotal)
		assertETag(t, "HEAD "+o.key, head.ETag, o.etag)

		get, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &o.key})
		if err != nil {
			t.Fatalf("GetObject %s: %v", o.key, err)
		}
		get.Body.Close()
		assertPlaintextSize(t, "GET "+o.key+" Content-Length", *get.ContentLength, want, env, envTotal)
		assertETag(t, "GET "+o.key, get.ETag, o.etag)

		// A ranged GET reports the range's plaintext length and the
		// plaintext total in Content-Range.
		rng := "bytes=10-2009"
		rget, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &o.key, Range: &rng})
		if err != nil {
			t.Fatalf("GetObject %s range: %v", o.key, err)
		}
		rget.Body.Close()
		assertPlaintextSize(t, "ranged GET "+o.key+" Content-Length", *rget.ContentLength, 2000, env, envTotal)
		if wantCR := fmt.Sprintf("bytes 10-2009/%d", want); rget.ContentRange == nil || *rget.ContentRange != wantCR {
			t.Fatalf("ranged GET %s Content-Range = %v, want %q", o.key, rget.ContentRange, wantCR)
		}

		attrs, err := b.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
			Bucket: &bucket, Key: &o.key,
			ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize, types.ObjectAttributesEtag, types.ObjectAttributesObjectParts},
		})
		if err != nil {
			t.Fatalf("GetObjectAttributes %s: %v", o.key, err)
		}
		assertPlaintextSize(t, "GetObjectAttributes "+o.key+" ObjectSize", *attrs.ObjectSize, want, env, envTotal)
		assertETag(t, "GetObjectAttributes "+o.key, attrs.ETag, o.etag)
	}

	// The multipart object's parts: ?partNumber HEAD and GET, and
	// GetObjectAttributes, report each part's plaintext length, and
	// Content-Range its plaintext offset within the plaintext total. HEAD and
	// GET resolve the part on separate paths, so both are checked.
	env, envTotal := envelopes[mpKey], envelopeTotal[mpKey]
	var offset int64
	for i, data := range parts {
		pn := int32(i + 1)
		wantCR := fmt.Sprintf("bytes %d-%d/%d", offset, offset+int64(len(data))-1, len(mpWhole))
		head, err := b.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &mpKey, PartNumber: &pn})
		if err != nil {
			t.Fatalf("HeadObject partNumber=%d: %v", pn, err)
		}
		assertPartHeaders(t, fmt.Sprintf("HEAD partNumber=%d", pn), head.ContentLength, head.ContentRange, head.PartsCount, int64(len(data)), wantCR, int32(len(parts)), env, envTotal)

		get, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &mpKey, PartNumber: &pn})
		if err != nil {
			t.Fatalf("GetObject partNumber=%d: %v", pn, err)
		}
		body, err := io.ReadAll(get.Body)
		get.Body.Close()
		if err != nil {
			t.Fatalf("read GET partNumber=%d: %v", pn, err)
		}
		assertPartHeaders(t, fmt.Sprintf("GET partNumber=%d", pn), get.ContentLength, get.ContentRange, get.PartsCount, int64(len(data)), wantCR, int32(len(parts)), env, envTotal)
		if !bytes.Equal(body, data) {
			t.Fatalf("GET partNumber=%d returned %d bytes that are not part %d", pn, len(body), pn)
		}
		offset += int64(len(data))
	}
	attrs, err := b.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
		Bucket: &bucket, Key: &mpKey, ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectParts},
	})
	if err != nil {
		t.Fatalf("GetObjectAttributes parts: %v", err)
	}
	if attrs.ObjectParts == nil || len(attrs.ObjectParts.Parts) != len(parts) {
		t.Fatalf("GetObjectAttributes ObjectParts = %+v, want %d parts", attrs.ObjectParts, len(parts))
	}
	for i, p := range attrs.ObjectParts.Parts {
		assertPlaintextSize(t, fmt.Sprintf("GetObjectAttributes part %d Size", i+1), *p.Size, int64(len(parts[i])), env, envTotal)
	}

	// Listings: every entry's Size and ETag are the plaintext values.
	wantByKey := map[string]object{}
	for _, o := range objects {
		wantByKey[o.key] = o
	}
	check := func(what, key string, size *int64, etag *string) {
		t.Helper()
		o, ok := wantByKey[key]
		if !ok {
			t.Fatalf("%s listed unexpected key %q", what, key)
		}
		if size == nil {
			t.Fatalf("%s %s: no Size", what, key)
		}
		assertPlaintextSize(t, what+" "+key+" Size", *size, int64(len(o.plaintext)), envelopes[key], envelopeTotal[key])
		assertETag(t, what+" "+key, etag, o.etag)
	}

	v1, err := b.ListObjects(ctx, &s3.ListObjectsInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(v1.Contents) != len(objects) {
		t.Fatalf("ListObjects returned %d keys, want %d", len(v1.Contents), len(objects))
	}
	for _, c := range v1.Contents {
		check("ListObjects", *c.Key, c.Size, c.ETag)
	}

	v2, err := b.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if len(v2.Contents) != len(objects) {
		t.Fatalf("ListObjectsV2 returned %d keys, want %d", len(v2.Contents), len(objects))
	}
	for _, c := range v2.Contents {
		check("ListObjectsV2", *c.Key, c.Size, c.ETag)
	}

	lv, err := b.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: &bucket})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if len(lv.Versions) != len(objects) {
		t.Fatalf("ListObjectVersions returned %d versions, want %d", len(lv.Versions), len(objects))
	}
	for _, v := range lv.Versions {
		check("ListObjectVersions", *v.Key, v.Size, v.ETag)
	}
}
