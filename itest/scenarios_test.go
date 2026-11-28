//go:build itest

package itest

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmw "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"
)

// cidOf returns the x-cid header value ingot must return for data: a
// raw-codec CIDv1 over its BLAKE3 hash.
func cidOf(t *testing.T, data []byte) string {
	t.Helper()
	sum := blake3.Sum256(data)
	digest, err := mh.Encode(sum[:], mh.BLAKE3)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, digest).String()
}

// rawHeader reads a response header the SDK has no field for from the
// operation's result metadata.
func rawHeader(t *testing.T, meta middleware.Metadata, name string) string {
	t.Helper()
	resp, ok := awsmw.GetRawResponse(meta).(*smithyhttp.Response)
	require.True(t, ok, "no raw response in result metadata")
	return resp.Header.Get(name)
}

// TestForgeScenarios covers ingot-unique behaviors the upstream versitygw
// suite cannot assert — internal blob-plane properties and session-state
// recovery — against a forge-mode stack whose ingot config lowers
// max_blob_size to 64 KiB (testdata/config-smallblob.yaml) so small objects
// coarse-split into multiple body blobs. One stack is shared by all
// subtests; local blob assertions are delta-based so subtest order doesn't
// matter.
//
// These were ported from the old in-process suite; assertions that merely
// duplicated upstream versitygw coverage (e.g. abort semantics) were dropped
// in the move.
func TestForgeScenarios(t *testing.T) {
	const maxBlob = 64 << 10 // must match testdata/config-smallblob.yaml

	s, endpoint := forgeStack(t, withSmallBlobConfig())
	ctx := t.Context()
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "scenarios")
	cl := sdkClient(forgeS3Conf(endpoint, accessKey, secretKey))

	// AgentDIDDocument: the listener publishes the agent's DID document at the
	// did:web well-known path, ahead of the S3 route table (otherwise the path
	// would be read as bucket ".well-known", key "did.json"). The stack config
	// names the agent did:web:ingot; hilt resolves this document to verify
	// ingot's /s3/* invocations, so every other subtest depends on it too.
	t.Run("AgentDIDDocument", func(t *testing.T) {
		hc := &http.Client{Timeout: 10 * time.Second}
		res, err := hc.Get(endpoint + "/.well-known/did.json")
		require.NoError(t, err, "GET /.well-known/did.json")
		defer res.Body.Close()
		require.Equal(t, http.StatusOK, res.StatusCode, "GET /.well-known/did.json: status %d", res.StatusCode)
		var doc struct {
			ID                 string `json:"id"`
			VerificationMethod []struct {
				ID string `json:"id"`
			} `json:"verificationMethod"`
		}
		require.NoError(t, json.NewDecoder(res.Body).Decode(&doc), "decode DID document")
		require.Equal(t, "did:web:ingot", doc.ID, "DID document id")
		require.Len(t, doc.VerificationMethod, 1, "verificationMethod = %+v, want one method did:web:ingot#key-0", doc.VerificationMethod)
		require.Equal(t, "did:web:ingot#key-0", doc.VerificationMethod[0].ID, "verificationMethod = %+v, want one method did:web:ingot#key-0", doc.VerificationMethod)
	})

	// BlobSplitMultiBlobRoundTrip: a PUT several times larger than
	// max_blob_size is coarsely split into multiple BlobRefs; the
	// whole-object GET, boundary-spanning ranged GETs, and md5 ETag all
	// reconstruct the exact bytes, and the bodies land in the cache by
	// digest (the data-plane inversion) — not journaled into the log.
	t.Run("BlobSplitMultiBlobRoundTrip", func(t *testing.T) {
		const bucket = "blob-split"
		_, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		require.NoError(t, err, "CreateBucket")

		// 3.5 blobs: blob0..blob2 are full (64 KiB), blob3 is a half blob.
		const size = 3*maxBlob + maxBlob/2
		data := patternBytes(size)

		localBefore := localBlobCount(t, ctx, s)
		put, err := cl.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("big"),
			Body:   bytes.NewReader(data),
		})
		require.NoError(t, err, "PutObject")
		require.Equal(t, quotedMD5(data), aws.ToString(put.ETag), "PUT ETag")
		got := localBlobCount(t, ctx, s) - localBefore
		require.Equal(t, 4, got, "PUT added %d cached blobs, want 4 — bodies must be cached by digest, not logged", got)

		require.Equal(t, data, getBody(t, ctx, cl, bucket, "big", ""), "whole GET")

		rangeCases := []struct{ start, end int }{
			{maxBlob - 10, maxBlob + 2000}, // crosses a blob boundary
			{maxBlob + 5, maxBlob + 105},   // wholly inside blob1, non-zero in-blob offset
			{size - 1, size - 1},           // final byte only
			{0, 0},                         // first byte only
		}
		for _, rc := range rangeCases {
			hdr := fmt.Sprintf("bytes=%d-%d", rc.start, rc.end)
			got := getBody(t, ctx, cl, bucket, "big", hdr)
			want := data[rc.start : rc.end+1]
			require.Equal(t, want, got, "range %s mismatch", hdr)
		}

		head, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("big")})
		require.NoError(t, err, "HeadObject")
		require.Equal(t, int64(size), aws.ToInt64(head.ContentLength), "HEAD ContentLength")
		require.Equal(t, quotedMD5(data), aws.ToString(head.ETag), "HEAD ETag")
		// The object's CID (raw-codec CIDv1 over its BLAKE3 hash) rides on
		// HEAD and on every GET, ranged or whole.
		require.Equal(t, cidOf(t, data), rawHeader(t, head.ResultMetadata, "x-cid"), "HEAD x-cid")
		// The Blake3 attribute is accepted by name; the SDK's typed output has
		// no field for the element, so only the request's acceptance is
		// checked here (the element itself is covered by unit tests).
		attrs, err := cl.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
			Bucket: aws.String(bucket), Key: aws.String("big"),
			ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesEtag, "Blake3"},
		})
		require.NoError(t, err, "GetObjectAttributes with Blake3")
		require.Equal(t, strings.Trim(quotedMD5(data), `"`), aws.ToString(attrs.ETag), "GetObjectAttributes ETag")
		get, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("big"), Range: aws.String("bytes=0-9")})
		require.NoError(t, err, "GetObject")
		get.Body.Close()
		require.Equal(t, cidOf(t, data), rawHeader(t, get.ResultMetadata, "x-cid"), "ranged GET x-cid")
	})

	// BlobListPastInline: an object of more blobs than a manifest lists
	// inline (1,024) keeps the rest in an extent block. It reads back whole,
	// in a range across the boundary, and through a copy whose commit writes
	// extents of its own.
	t.Run("BlobListPastInline", func(t *testing.T) {
		const bucket = "blob-extents"
		// The PUT uploads its 1,031 blobs one at a time, past the upstream
		// client's per-request timeout.
		cl := bigObjectClient(t, endpoint, accessKey, secretKey)
		_, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		require.NoError(t, err, "CreateBucket")
		const size = 1030*maxBlob + 100 // 1,031 blobs
		data := patternBytes(size)
		_, err = cl.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("big"),
			Body:   bytes.NewReader(data),
		})
		require.NoError(t, err, "PutObject")
		got := getBody(t, ctx, cl, bucket, "big", "")
		require.Equal(t, data, got, "whole GET mismatch")
		const boundary = 1024 * maxBlob
		for _, rc := range []struct{ start, end int }{
			{boundary - 100, boundary + 100}, // the last inline blob into the extent
			{boundary + 5000, boundary + 9000},
			{size - 1, size - 1},
		} {
			hdr := fmt.Sprintf("bytes=%d-%d", rc.start, rc.end)
			got := getBody(t, ctx, cl, bucket, "big", hdr)
			require.Equal(t, data[rc.start:rc.end+1], got, "range %s mismatch", hdr)
		}
		_, err = cl.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String("copy"),
			CopySource: aws.String(bucket + "/big"),
		})
		require.NoError(t, err, "CopyObject")
		require.Equal(t, data[boundary-100:boundary+101], getBody(t, ctx, cl, bucket, "copy", fmt.Sprintf("bytes=%d-%d", boundary-100, boundary+100)), "copy range across the boundary")
	})

	// ZeroByteObject: a 0-byte object stores no blob, round-trips empty,
	// and carries the well-known empty-content md5 ETag.
	t.Run("ZeroByteObject", func(t *testing.T) {
		const bucket = "zero-byte"
		_, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		require.NoError(t, err, "CreateBucket")

		const emptyMD5 = `"d41d8cd98f00b204e9800998ecf8427e"`
		localBefore := localBlobCount(t, ctx, s)
		put, err := cl.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("empty"),
			Body:   bytes.NewReader(nil),
		})
		require.NoError(t, err, "PutObject")
		require.Equal(t, emptyMD5, aws.ToString(put.ETag), "PUT ETag")
		got := localBlobCount(t, ctx, s) - localBefore
		require.Zero(t, got, "zero-byte PUT added %d cached blobs", got)

		require.Empty(t, getBody(t, ctx, cl, bucket, "empty", ""), "zero-byte GET")
		head, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("empty")})
		require.NoError(t, err, "HeadObject")
		require.Zero(t, aws.ToInt64(head.ContentLength), "HEAD ContentLength")
		require.Equal(t, emptyMD5, aws.ToString(head.ETag), "HEAD ETag")
		require.Equal(t, cidOf(t, nil), rawHeader(t, head.ResultMetadata, "x-cid"), "HEAD x-cid")
	})

	// MultipartPartSpansBlobs: Create → UploadPart×3 → Complete where part 1
	// is larger than max_blob_size, so a single part coarse-splits into
	// multiple internal body blobs. Upstream can assert multipart round-trip
	// but has no notion of ingot's internal split; the boundary-spanning
	// ranged GET proves reassembly across both part and blob boundaries.
	t.Run("MultipartPartSpansBlobs", func(t *testing.T) {
		const bucket, key = "mpbucket", "big/obj"
		_, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		require.NoError(t, err, "CreateBucket")

		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), ContentType: aws.String("text/plain"),
		})
		require.NoError(t, err, "CreateMultipartUpload")
		uploadID := create.UploadId

		// Non-final parts must meet S3's 5 MiB minimum (enforced at Complete);
		// at 64 KiB max_blob_size each spans dozens of internal blobs. The
		// final part is small (exempt from the minimum).
		partData := [][]byte{patternBytes(6 << 20), patternBytes(5 << 20), patternBytes(9 << 10)}
		var completed []types.CompletedPart
		var whole []byte
		for i, data := range partData {
			pn := int32(i + 1)
			up, err := cl.UploadPart(ctx, &s3.UploadPartInput{
				Bucket: aws.String(bucket), Key: aws.String(key), UploadId: uploadID,
				PartNumber: aws.Int32(pn), Body: bytes.NewReader(data),
			})
			require.NoError(t, err, "UploadPart %d", pn)
			completed = append(completed, types.CompletedPart{PartNumber: aws.Int32(pn), ETag: up.ETag})
			whole = append(whole, data...)
		}

		comp, err := cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
		})
		require.NoError(t, err, "CompleteMultipartUpload")
		et := strings.Trim(aws.ToString(comp.ETag), `"`)
		require.True(t, strings.HasSuffix(et, "-3"), "complete ETag = %q, want a multipart -3 suffix", et)

		got := getBody(t, ctx, cl, bucket, key, "")
		require.Equal(t, whole, got, "multipart GET mismatch")
		head, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		require.NoError(t, err, "HeadObject")
		require.Equal(t, int64(len(whole)), aws.ToInt64(head.ContentLength), "HEAD size")
		// The multipart object's CID is the BLAKE3 hash of the whole body,
		// assembled at Complete from the parts' trees.
		require.Equal(t, cidOf(t, whole), rawHeader(t, head.ResultMetadata, "x-cid"), "HEAD x-cid")
		// A ranged GET spanning the part-1→part-2 boundary still reconstructs.
		p1 := len(partData[0])
		hdr := fmt.Sprintf("bytes=%d-%d", p1-10, p1+2000)
		require.Equal(t, whole[p1-10:p1+2001], getBody(t, ctx, cl, bucket, key, hdr), "ranged multipart GET across the part boundary")
	})

	// HeadListPlaintextSizes: every size and ETag ingot reports is the
	// plaintext value from the manifest, never the size of a stored FEE
	// envelope or their sum. The stored envelope sizes are read from
	// upload_intents so the assertions can name that failure mode. Checked through
	// HEAD, GET (whole, ranged, ?partNumber), GetObjectAttributes,
	// ListObjects, ListObjectsV2 and ListObjectVersions, for a single-PUT
	// object and a multipart object, each spanning several envelopes.
	t.Run("HeadListPlaintextSizes", func(t *testing.T) {
		const bucket = "plainsizes"
		_, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		require.NoError(t, err, "CreateBucket")

		type object struct {
			key       string
			plaintext []byte
			etag      string
			envelopes []int64
			envTotal  int64
		}
		var objects []object

		// A stored envelope's size, or the envelopes' total, leaking into a
		// response is the specific failure these assertions name.
		assertSize := func(t *testing.T, what string, got int64, want int64, o object) {
			t.Helper()
			if got == want {
				return
			}
			for _, e := range o.envelopes {
				require.NotEqual(t, e, got, "%s = %d: a stored envelope's size", what, got)
			}
			require.NotEqual(t, o.envTotal, got, "%s = %d: the stored envelopes' total", what, got)
			require.FailNow(t, fmt.Sprintf("%s = %d, want the plaintext %d", what, got, want))
		}
		assertETag := func(t *testing.T, what string, got *string, want string) {
			t.Helper()
			g := strings.Trim(aws.ToString(got), `"`)
			require.Equal(t, want, g, "%s ETag", what)
		}
		// envelopeSizes reports the stored envelope sizes of the blobs a write
		// recorded. They come from upload_intents rather than the cache: a
		// multipart part keeps no local copy.
		envelopeSizes := func(t *testing.T, before, after map[string]int64) ([]int64, int64) {
			t.Helper()
			added := newIntentDigests(before, after)
			require.GreaterOrEqual(t, len(added), 2, "write stored %d envelopes, want several (small blob ceiling)", len(added))
			var sizes []int64
			var total int64
			for _, d := range added {
				sizes = append(sizes, after[d])
				total += after[d]
			}
			return sizes, total
		}

		// Single PUT: 200 KiB + 37 bytes → four envelopes at 64 KiB.
		single := patternBytes((200 << 10) + 37)
		before := uploadIntentSizes(t, ctx, s)
		_, err = cl.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("single"), Body: bytes.NewReader(single)})
		require.NoError(t, err, "PutObject")
		sizes, total := envelopeSizes(t, before, uploadIntentSizes(t, ctx, s))
		sum := md5.Sum(single)
		objects = append(objects, object{"single", single, hex.EncodeToString(sum[:]), sizes, total})

		// Multipart: two parts, the first spanning many envelopes.
		partData := [][]byte{tagged(patternBytes((5<<20)+4096), 0x61), tagged(patternBytes(9<<10), 0x62)}
		before = uploadIntentSizes(t, ctx, s)
		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String("multipart")})
		require.NoError(t, err, "CreateMultipartUpload")
		var completed []types.CompletedPart
		var mpWhole []byte
		etagCat := md5.New()
		for i, data := range partData {
			pn := int32(i + 1)
			up, err := cl.UploadPart(ctx, &s3.UploadPartInput{
				Bucket: aws.String(bucket), Key: aws.String("multipart"), UploadId: create.UploadId,
				PartNumber: aws.Int32(pn), Body: bytes.NewReader(data),
			})
			require.NoError(t, err, "UploadPart %d", pn)
			completed = append(completed, types.CompletedPart{PartNumber: aws.Int32(pn), ETag: up.ETag})
			mpWhole = append(mpWhole, data...)
			md := md5.Sum(data)
			etagCat.Write(md[:])
		}
		_, err = cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String("multipart"), UploadId: create.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
		})
		require.NoError(t, err, "CompleteMultipartUpload")
		sizes, total = envelopeSizes(t, before, uploadIntentSizes(t, ctx, s))
		mp := object{"multipart", mpWhole, hex.EncodeToString(etagCat.Sum(nil)) + "-2", sizes, total}
		objects = append(objects, mp)

		for _, o := range objects {
			require.Greater(t, o.envTotal, int64(len(o.plaintext)), "%s stored %d bytes for %d plaintext; envelopes must be larger", o.key, o.envTotal, len(o.plaintext))
			want := int64(len(o.plaintext))

			head, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(o.key)})
			require.NoError(t, err, "HeadObject %s", o.key)
			assertSize(t, "HEAD "+o.key+" Content-Length", aws.ToInt64(head.ContentLength), want, o)
			assertETag(t, "HEAD "+o.key, head.ETag, o.etag)

			get, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(o.key)})
			require.NoError(t, err, "GetObject %s", o.key)
			get.Body.Close()
			assertSize(t, "GET "+o.key+" Content-Length", aws.ToInt64(get.ContentLength), want, o)
			assertETag(t, "GET "+o.key, get.ETag, o.etag)

			rget, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(o.key), Range: aws.String("bytes=10-2009")})
			require.NoError(t, err, "GetObject %s range", o.key)
			rget.Body.Close()
			assertSize(t, "ranged GET "+o.key+" Content-Length", aws.ToInt64(rget.ContentLength), 2000, o)
			require.Equal(t, fmt.Sprintf("bytes 10-2009/%d", want), aws.ToString(rget.ContentRange), "ranged GET %s Content-Range", o.key)

			attrs, err := cl.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
				Bucket: aws.String(bucket), Key: aws.String(o.key),
				ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize, types.ObjectAttributesEtag},
			})
			require.NoError(t, err, "GetObjectAttributes %s", o.key)
			assertSize(t, "GetObjectAttributes "+o.key+" ObjectSize", aws.ToInt64(attrs.ObjectSize), want, o)
			assertETag(t, "GetObjectAttributes "+o.key, attrs.ETag, o.etag)
		}

		// ?partNumber on the multipart object, via HEAD and GET (separate
		// paths): the part's plaintext length and its plaintext offset within
		// the plaintext total.
		assertPart := func(t *testing.T, what string, length *int64, contentRange *string, partsCount *int32, wantLen int64, wantCR string) {
			t.Helper()
			assertSize(t, what+" Content-Length", aws.ToInt64(length), wantLen, mp)
			require.Equal(t, wantCR, aws.ToString(contentRange), "%s Content-Range", what)
			require.Equal(t, int32(len(partData)), aws.ToInt32(partsCount), "%s PartsCount", what)
		}
		var offset int64
		for i, data := range partData {
			pn := int32(i + 1)
			wantCR := fmt.Sprintf("bytes %d-%d/%d", offset, offset+int64(len(data))-1, len(mpWhole))
			head, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("multipart"), PartNumber: aws.Int32(pn)})
			require.NoError(t, err, "HeadObject partNumber=%d", pn)
			assertPart(t, fmt.Sprintf("HEAD partNumber=%d", pn), head.ContentLength, head.ContentRange, head.PartsCount, int64(len(data)), wantCR)

			get, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("multipart"), PartNumber: aws.Int32(pn)})
			require.NoError(t, err, "GetObject partNumber=%d", pn)
			body, err := io.ReadAll(get.Body)
			get.Body.Close()
			require.NoError(t, err, "read GET partNumber=%d", pn)
			assertPart(t, fmt.Sprintf("GET partNumber=%d", pn), get.ContentLength, get.ContentRange, get.PartsCount, int64(len(data)), wantCR)
			require.Equal(t, data, body, "GET partNumber=%d returned %d bytes that are not part %d", pn, len(body), pn)
			offset += int64(len(data))
		}

		// Listings.
		byKey := map[string]object{}
		for _, o := range objects {
			byKey[o.key] = o
		}
		check := func(what, key string, size *int64, etag *string) {
			t.Helper()
			o, ok := byKey[key]
			require.True(t, ok, "%s listed unexpected key %q", what, key)
			assertSize(t, what+" "+key+" Size", aws.ToInt64(size), int64(len(o.plaintext)), o)
			assertETag(t, what+" "+key, etag, o.etag)
		}
		v1, err := cl.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(bucket)})
		require.NoError(t, err, "ListObjects")
		require.Len(t, v1.Contents, len(objects), "ListObjects returned %d keys", len(v1.Contents))
		for _, c := range v1.Contents {
			check("ListObjects", aws.ToString(c.Key), c.Size, c.ETag)
		}
		v2, err := cl.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		require.NoError(t, err, "ListObjectsV2")
		require.Len(t, v2.Contents, len(objects), "ListObjectsV2 returned %d keys", len(v2.Contents))
		for _, c := range v2.Contents {
			check("ListObjectsV2", aws.ToString(c.Key), c.Size, c.ETag)
		}
		lv, err := cl.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
		require.NoError(t, err, "ListObjectVersions")
		require.Len(t, lv.Versions, len(objects), "ListObjectVersions returned %d versions", len(lv.Versions))
		for _, v := range lv.Versions {
			check("ListObjectVersions", aws.ToString(v.Key), v.Size, v.ETag)
		}
	})

	// MultipartOutOfOrderParts: part numbers, not arrival order, define the
	// object. Parts uploaded 3, 1, 2 list ascending by part number; Complete
	// assembles the body and the md5-of-md5s ETag in part-number order; a
	// range across the part-1→part-2 boundary and ?partNumber=2 both read
	// the re-sequenced blob list. Distinct content per part so a body glued
	// in arrival order cannot pass the comparison.
	t.Run("MultipartOutOfOrderParts", func(t *testing.T) {
		const bucket, key = "mp-order", "obj"
		_, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		require.NoError(t, err, "CreateBucket")
		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		})
		require.NoError(t, err, "CreateMultipartUpload")
		uploadID := create.UploadId

		partData := [][]byte{
			tagged(patternBytes(5<<20), 0x41),
			tagged(patternBytes((5<<20)+4096), 0x42),
			tagged(patternBytes(9<<10), 0x43),
		}
		etags := make([]*string, len(partData))
		for _, pn := range []int32{3, 1, 2} {
			up, err := cl.UploadPart(ctx, &s3.UploadPartInput{
				Bucket: aws.String(bucket), Key: aws.String(key), UploadId: uploadID,
				PartNumber: aws.Int32(pn), Body: bytes.NewReader(partData[pn-1]),
			})
			require.NoError(t, err, "UploadPart %d", pn)
			etags[pn-1] = up.ETag
		}

		lp, err := cl.ListParts(ctx, &s3.ListPartsInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: uploadID})
		require.NoError(t, err, "ListParts")
		require.Len(t, lp.Parts, len(partData), "ListParts returned %d parts", len(lp.Parts))
		for i, p := range lp.Parts {
			want := int32(i + 1)
			require.Equal(t, want, aws.ToInt32(p.PartNumber), "ListParts[%d].PartNumber", i)
			require.Equal(t, int64(len(partData[i])), aws.ToInt64(p.Size), "ListParts part %d size", want)
			require.Equal(t, strings.Trim(aws.ToString(etags[i]), `"`), strings.Trim(aws.ToString(p.ETag), `"`), "ListParts part %d ETag", want)
		}

		var completed []types.CompletedPart
		var whole []byte
		etagCat := md5.New()
		for i, data := range partData {
			completed = append(completed, types.CompletedPart{PartNumber: aws.Int32(int32(i + 1)), ETag: etags[i]})
			whole = append(whole, data...)
			sum := md5.Sum(data)
			etagCat.Write(sum[:])
		}
		wantETag := hex.EncodeToString(etagCat.Sum(nil)) + "-3"

		comp, err := cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
		})
		require.NoError(t, err, "CompleteMultipartUpload")
		require.Equal(t, wantETag, strings.Trim(aws.ToString(comp.ETag), `"`), "complete ETag (md5-of-md5s in part-number order)")

		require.Equal(t, whole, getBody(t, ctx, cl, bucket, key, ""), "GET after out-of-order upload")
		p1 := len(partData[0])
		hdr := fmt.Sprintf("bytes=%d-%d", p1-10, p1+2000)
		require.Equal(t, whole[p1-10:p1+2001], getBody(t, ctx, cl, bucket, key, hdr), "ranged GET across the part boundary")
		part2, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), PartNumber: aws.Int32(2)})
		require.NoError(t, err, "GetObject partNumber=2")
		defer part2.Body.Close()
		got, err := io.ReadAll(part2.Body)
		require.NoError(t, err, "read part 2")
		require.Equal(t, partData[1], got, "GET partNumber=2 returned %d bytes that are not part 2", len(got))
		require.Equal(t, int32(3), aws.ToInt32(part2.PartsCount), "GET partNumber=2 PartsCount")
		// Part 3 arrived first and was hashed at an assumed offset Complete
		// found wrong; the CID must still be the whole body's.
		require.Equal(t, cidOf(t, whole), rawHeader(t, part2.ResultMetadata, "x-cid"), "GET partNumber=2 x-cid")
	})

	// MultipartPartKeepsNoLocalCopy: a part's blobs go only to the provider,
	// so none of them has a local copy after UploadPart,
	// nor after the abort (upstream
	// AbortMultipartUpload_success verifies the registry rows via
	// ListMultipartUploads).
	t.Run("MultipartPartKeepsNoLocalCopy", func(t *testing.T) {
		const bucket, key = "mpabort-local", "obj"
		_, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		require.NoError(t, err, "CreateBucket")
		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		require.NoError(t, err, "CreateMultipartUpload")
		// A 150 KiB part spans three 64 KiB blobs (plaintext split; each is
		// stored as its own envelope).
		part := patternBytes(150 << 10)
		for i := range part {
			part[i] ^= 0xA5
		}
		_, err = cl.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			PartNumber: aws.Int32(1), Body: bytes.NewReader(part),
		})
		require.NoError(t, err, "UploadPart")
		digests := partBlobDigestsHex(t, ctx, s, aws.ToString(create.UploadId), 1)
		require.Len(t, digests, 3, "UploadPart stored %d blobs", len(digests))
		local := localDigests(t, ctx, s, digests)
		require.Empty(t, local, "UploadPart left %d of its blobs on local disk, want 0: a part keeps no local copy", len(local))
		_, err = cl.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
		})
		require.NoError(t, err, "AbortMultipartUpload")
		require.Empty(t, localDigests(t, ctx, s, digests), "abort left part blobs on local disk")
	})

	// MultipartFailedCompleteStaysAbortable: the zombie-session regression. A
	// Complete that fails validation (wrong part ETag) must leave the session
	// open — a retry with the correct ETag succeeds and reads back
	// byte-exact. Upstream tests re-complete-after-success
	// (already_completed) but not recovery from a FAILED complete; the
	// session-state revert out of 'completing' is ingot catalog behavior.
	t.Run("MultipartFailedCompleteStaysAbortable", func(t *testing.T) {
		const bucket, key = "mpretry", "obj"
		_, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		require.NoError(t, err, "CreateBucket")
		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		require.NoError(t, err, "CreateMultipartUpload")
		data := patternBytes(8 << 10)
		up, err := cl.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			PartNumber: aws.Int32(1), Body: bytes.NewReader(data),
		})
		require.NoError(t, err, "UploadPart")

		// Complete with a WRONG part ETag → InvalidPart. The session must not zombie.
		_, err = cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: aws.String("\"deadbeefdeadbeefdeadbeefdeadbeef\"")},
			}},
		})
		require.Error(t, err, "Complete with a wrong part ETag")

		// A retry with the correct ETag now succeeds (session was reverted to open).
		_, err = cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: up.ETag},
			}},
		})
		require.NoError(t, err, "Complete retry after a failed Complete")
		got := getBody(t, ctx, cl, bucket, key, "")
		require.Equal(t, data, got, "retried multipart object mismatch: %d bytes", len(got))
	})

	// CORS: the gateway answers browser CORS from cors_allowed_origins
	// (testdata/config-smallblob.yaml) — a document s3frontend reports as
	// every bucket's CORS configuration, which is what drives versitygw's
	// preflight route and per-route CORS middleware. Asserted on the wire
	// because none of that behavior lives in ingot code.
	t.Run("CORS", func(t *testing.T) {
		const (
			bucket   = "cors"
			key      = "obj"
			origin   = "https://feature-1.dev.example" // matches https://*.dev.example
			maxAge   = "600"
			disallow = "https://evil.example"
		)
		_, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		require.NoError(t, err, "CreateBucket")
		data := []byte("cors body")
		_, err = cl.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(data),
		})
		require.NoError(t, err, "PutObject")

		// Preflight. Browsers send these unsigned, so this also pins that
		// the OPTIONS route sits ahead of SigV4.
		preflightOn := func(t *testing.T, bucket, origin string) *http.Response {
			t.Helper()
			req, err := http.NewRequestWithContext(ctx, http.MethodOptions, fmt.Sprintf("%s/%s/%s", endpoint, bucket, key), nil)
			require.NoError(t, err, "new request")
			req.Header.Set("Origin", origin)
			req.Header.Set("Access-Control-Request-Method", "PUT")
			req.Header.Set("Access-Control-Request-Headers", "authorization, x-amz-content-sha256, x-amz-date")
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err, "preflight")
			t.Cleanup(func() { _ = resp.Body.Close() })
			return resp
		}
		preflight := func(t *testing.T, origin string) *http.Response {
			t.Helper()
			return preflightOn(t, bucket, origin)
		}

		resp := preflight(t, origin)
		// CORS preflight spec allows any "ok status" (200-299).
		assert.True(t, resp.StatusCode >= 200 && resp.StatusCode < 300, "preflight status = %d, want 2xx", resp.StatusCode)
		got := resp.Header.Get("Access-Control-Allow-Origin")
		assert.Equal(t, origin, got, "preflight Allow-Origin: the request origin echoed")
		assert.Contains(t, resp.Header.Get("Access-Control-Allow-Methods"), "PUT", "preflight Allow-Methods")
		assert.Equal(t, maxAge, resp.Header.Get("Access-Control-Max-Age"), "preflight Max-Age")

		assert.Empty(t, preflight(t, disallow).Header.Get("Access-Control-Allow-Origin"), "preflight Allow-Origin for a disallowed origin")

		// A real cross-origin read: a presigned GET is how a browser fetches
		// an object, and the response must expose ETag to JavaScript.
		presigned, err := s3.NewPresignClient(cl).PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		})
		require.NoError(t, err, "PresignGetObject")
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, presigned.URL, nil)
		require.NoError(t, err, "new request")
		req.Header.Set("Origin", origin)
		get, err := http.DefaultClient.Do(req)
		require.NoError(t, err, "presigned GET")
		defer func() { _ = get.Body.Close() }()
		require.Equal(t, http.StatusOK, get.StatusCode, "presigned GET status")
		assert.Equal(t, origin, get.Header.Get("Access-Control-Allow-Origin"), "GET Allow-Origin: the request origin echoed")
		assert.Contains(t, get.Header.Get("Access-Control-Expose-Headers"), "ETag", "GET Expose-Headers")

		// Per-bucket CORS (docs/s3-cors.md): a bucket's own document
		// replaces the gateway-wide one for that bucket alone, and deleting
		// it restores the default.
		t.Run("per bucket", func(t *testing.T) {
			const (
				own     = "https://bucket-own.example" // matches no gateway entry
				sibling = "cors-sibling"
			)
			_, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(sibling)})
			require.NoError(t, err, "CreateBucket %s", sibling)
			_, err = cl.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(sibling), Key: aws.String(key), Body: bytes.NewReader(data),
			})
			require.NoError(t, err, "PutObject %s", sibling)
			allowed := func(t *testing.T, bucket, origin string) bool {
				t.Helper()
				return preflightOn(t, bucket, origin).Header.Get("Access-Control-Allow-Origin") == origin
			}

			_, err = cl.PutBucketCors(ctx, &s3.PutBucketCorsInput{
				Bucket: aws.String(bucket),
				CORSConfiguration: &types.CORSConfiguration{CORSRules: []types.CORSRule{{
					AllowedOrigins: []string{own},
					AllowedMethods: []string{"GET", "PUT"},
					AllowedHeaders: []string{"*"},
					MaxAgeSeconds:  aws.Int32(300),
				}}},
			})
			require.NoError(t, err, "PutBucketCors")
			got, err := cl.GetBucketCors(ctx, &s3.GetBucketCorsInput{Bucket: aws.String(bucket)})
			require.NoError(t, err, "GetBucketCors")
			require.Len(t, got.CORSRules, 1, "GetBucketCors rules = %+v, want the one stored rule for %s", got.CORSRules, own)
			require.Len(t, got.CORSRules[0].AllowedOrigins, 1, "GetBucketCors rules = %+v, want the one stored rule for %s", got.CORSRules, own)
			require.Equal(t, own, got.CORSRules[0].AllowedOrigins[0], "GetBucketCors rules = %+v, want the one stored rule for %s", got.CORSRules, own)
			// The bucket's own document replaces the gateway's: its origin is
			// allowed, the gateway's is not, and the sibling still answers
			// from the gateway's.
			assert.True(t, allowed(t, bucket, own), "preflight from %s on %s: want allowed by the bucket's own document", own, bucket)
			assert.False(t, allowed(t, bucket, origin), "preflight from %s on %s: want refused, the gateway document no longer applies", origin, bucket)
			assert.True(t, allowed(t, sibling, origin), "preflight from %s on %s: want allowed by the gateway document", origin, sibling)
			assert.False(t, allowed(t, sibling, own), "preflight from %s on %s: want refused, the document is %s's alone", own, sibling, bucket)

			// Deleting it restores the default; deleting again is a no-op.
			for range 2 {
				_, err := cl.DeleteBucketCors(ctx, &s3.DeleteBucketCorsInput{Bucket: aws.String(bucket)})
				require.NoError(t, err, "DeleteBucketCors")
			}
			assert.True(t, allowed(t, bucket, origin), "preflight from %s on %s after delete: want allowed by the gateway document again", origin, bucket)
			assert.False(t, allowed(t, bucket, own), "preflight from %s on %s after delete: want refused", own, bucket)
		})
	})
}
