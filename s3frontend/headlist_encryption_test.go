package s3frontend

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/versitygw/backend"
	"github.com/stretchr/testify/require"
)

// storedEnvelopeSizes returns the on-disk size of every blob an object
// version stores — the FEE envelopes, each larger than the plaintext span
// it encrypts — and their sum.
func storedEnvelopeSizes(t *testing.T, b *Backend, key string) (sizes []int64, total int64) {
	t.Helper()
	for _, d := range blobDigestsOf(t, b, key, "") {
		n := int64(len(storedBytes(t, b, d)))
		sizes = append(sizes, n)
		total += n
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
		require.NotEqual(t, e, got, "%s = %d: a stored envelope's size", what, got)
	}
	require.NotEqual(t, envelopeTotal, got, "%s = %d: the stored envelopes' total", what, got)
	require.FailNow(t, fmt.Sprintf("%s = %d, want the plaintext %d", what, got, want))
}

// assertPartHeaders checks a ?partNumber response's Content-Length,
// Content-Range and PartsCount against the part's plaintext coordinates.
func assertPartHeaders(t *testing.T, what string, length *int64, contentRange *string, partsCount *int32, wantLen int64, wantCR string, wantParts int32, envelopes []int64, envelopeTotal int64) {
	t.Helper()
	require.NotNil(t, length, "%s: no Content-Length", what)
	assertPlaintextSize(t, what+" Content-Length", *length, wantLen, envelopes, envelopeTotal)
	require.NotNil(t, contentRange, "%s Content-Range", what)
	require.Equal(t, wantCR, *contentRange, "%s Content-Range", what)
	require.NotNil(t, partsCount, "%s PartsCount", what)
	require.Equal(t, wantParts, *partsCount, "%s PartsCount", what)
}

func assertETag(t *testing.T, what string, got *string, want string) {
	t.Helper()
	require.NotNil(t, got, "%s: no ETag", what)
	g := strings.Trim(*got, `"`)
	require.Equal(t, want, g, "%s ETag", what)
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
		require.NoError(t, err, "UploadPart %d", pn)
		completed = append(completed, types.CompletedPart{PartNumber: &pn, ETag: out.ETag, ChecksumCRC32C: out.ChecksumCRC32C})
		mpWhole = append(mpWhole, data...)
		md := md5.Sum(data)
		etagCat.Write(md[:])
	}
	_, err := mpComplete(t, b, mpKey, uploadID, completed, nil)
	require.NoError(t, err, "Complete")
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
		require.GreaterOrEqual(t, len(sizes), 2, "%s stored %d blobs, want several (small blob ceiling)", o.key, len(sizes))
		require.Greater(t, total, int64(len(o.plaintext)), "%s stored %d bytes for %d plaintext; envelopes must be larger", o.key, total, len(o.plaintext))
		envelopes[o.key], envelopeTotal[o.key] = sizes, total
	}

	for _, o := range objects {
		want := int64(len(o.plaintext))
		env, envTotal := envelopes[o.key], envelopeTotal[o.key]

		head, err := b.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &o.key})
		require.NoError(t, err, "HeadObject %s", o.key)
		assertPlaintextSize(t, "HEAD "+o.key+" Content-Length", *head.ContentLength, want, env, envTotal)
		assertETag(t, "HEAD "+o.key, head.ETag, o.etag)

		get, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &o.key})
		require.NoError(t, err, "GetObject %s", o.key)
		get.Body.Close()
		assertPlaintextSize(t, "GET "+o.key+" Content-Length", *get.ContentLength, want, env, envTotal)
		assertETag(t, "GET "+o.key, get.ETag, o.etag)

		// A ranged GET reports the range's plaintext length and the
		// plaintext total in Content-Range.
		rng := "bytes=10-2009"
		rget, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &o.key, Range: &rng})
		require.NoError(t, err, "GetObject %s range", o.key)
		rget.Body.Close()
		assertPlaintextSize(t, "ranged GET "+o.key+" Content-Length", *rget.ContentLength, 2000, env, envTotal)
		wantCR := fmt.Sprintf("bytes 10-2009/%d", want)
		require.NotNil(t, rget.ContentRange, "ranged GET %s Content-Range", o.key)
		require.Equal(t, wantCR, *rget.ContentRange, "ranged GET %s Content-Range", o.key)

		attrs, err := b.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
			Bucket: &bucket, Key: &o.key,
			ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize, types.ObjectAttributesEtag, types.ObjectAttributesObjectParts},
		})
		require.NoError(t, err, "GetObjectAttributes %s", o.key)
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
		require.NoError(t, err, "HeadObject partNumber=%d", pn)
		assertPartHeaders(t, fmt.Sprintf("HEAD partNumber=%d", pn), head.ContentLength, head.ContentRange, head.PartsCount, int64(len(data)), wantCR, int32(len(parts)), env, envTotal)

		get, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &mpKey, PartNumber: &pn})
		require.NoError(t, err, "GetObject partNumber=%d", pn)
		body, err := io.ReadAll(get.Body)
		get.Body.Close()
		require.NoError(t, err, "read GET partNumber=%d", pn)
		assertPartHeaders(t, fmt.Sprintf("GET partNumber=%d", pn), get.ContentLength, get.ContentRange, get.PartsCount, int64(len(data)), wantCR, int32(len(parts)), env, envTotal)
		require.Equal(t, data, body, "GET partNumber=%d returned %d bytes that are not part %d", pn, len(body), pn)
		offset += int64(len(data))
	}
	attrs, err := b.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
		Bucket: &bucket, Key: &mpKey, ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectParts},
	})
	require.NoError(t, err, "GetObjectAttributes parts")
	require.NotNil(t, attrs.ObjectParts, "GetObjectAttributes ObjectParts")
	require.Len(t, attrs.ObjectParts.Parts, len(parts), "GetObjectAttributes ObjectParts")
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
		require.True(t, ok, "%s listed unexpected key %q", what, key)
		require.NotNil(t, size, "%s %s: no Size", what, key)
		assertPlaintextSize(t, what+" "+key+" Size", *size, int64(len(o.plaintext)), envelopes[key], envelopeTotal[key])
		assertETag(t, what+" "+key, etag, o.etag)
	}

	v1, err := b.ListObjects(ctx, &s3.ListObjectsInput{Bucket: &bucket})
	require.NoError(t, err, "ListObjects")
	require.Len(t, v1.Contents, len(objects), "ListObjects returned %d keys", len(v1.Contents))
	for _, c := range v1.Contents {
		check("ListObjects", *c.Key, c.Size, c.ETag)
	}

	v2, err := b.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket})
	require.NoError(t, err, "ListObjectsV2")
	require.Len(t, v2.Contents, len(objects), "ListObjectsV2 returned %d keys", len(v2.Contents))
	for _, c := range v2.Contents {
		check("ListObjectsV2", *c.Key, c.Size, c.ETag)
	}

	lv, err := b.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: &bucket})
	require.NoError(t, err, "ListObjectVersions")
	require.Len(t, lv.Versions, len(objects), "ListObjectVersions returned %d versions", len(lv.Versions))
	for _, v := range lv.Versions {
		check("ListObjectVersions", *v.Key, v.Size, v.ETag)
	}
}
