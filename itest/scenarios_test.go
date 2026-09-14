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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestForgeScenarios covers ingot-unique behaviors the upstream versitygw
// suite cannot assert — internal blob-plane properties and session-state
// recovery — against a forge-mode stack whose ingot config lowers
// max_blob_size to 64 KiB (testdata/config-smallblob.yaml) so small objects
// coarse-split into multiple body blobs. One stack is shared by all
// subtests; spool assertions are delta-based so subtest order doesn't
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
		if err != nil {
			t.Fatalf("GET /.well-known/did.json: %v", err)
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("GET /.well-known/did.json: status %d", res.StatusCode)
		}
		var doc struct {
			ID                 string `json:"id"`
			VerificationMethod []struct {
				ID string `json:"id"`
			} `json:"verificationMethod"`
		}
		if err := json.NewDecoder(res.Body).Decode(&doc); err != nil {
			t.Fatalf("decode DID document: %v", err)
		}
		if doc.ID != "did:web:ingot" {
			t.Fatalf("DID document id = %q, want did:web:ingot", doc.ID)
		}
		if len(doc.VerificationMethod) != 1 || doc.VerificationMethod[0].ID != "did:web:ingot#key-0" {
			t.Fatalf("verificationMethod = %+v, want one method did:web:ingot#key-0", doc.VerificationMethod)
		}
	})

	// BlobSplitMultiBlobRoundTrip: a PUT several times larger than
	// max_blob_size is coarsely split into multiple BlobRefs; the
	// whole-object GET, boundary-spanning ranged GETs, and md5 ETag all
	// reconstruct the exact bytes, and the bodies land in the spool by
	// digest (the data-plane inversion) — not journaled into the log.
	t.Run("BlobSplitMultiBlobRoundTrip", func(t *testing.T) {
		const bucket = "blob-split"
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}

		// 3.5 blobs: blob0..blob2 are full (64 KiB), blob3 is a half blob.
		const size = 3*maxBlob + maxBlob/2
		data := patternBytes(size)

		spoolBefore := spoolBlobCount(t, ctx, s)
		put, err := cl.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("big"),
			Body:   bytes.NewReader(data),
		})
		if err != nil {
			t.Fatalf("PutObject: %v", err)
		}
		if want := quotedMD5(data); aws.ToString(put.ETag) != want {
			t.Fatalf("PUT ETag = %s, want %s", aws.ToString(put.ETag), want)
		}
		if got := spoolBlobCount(t, ctx, s) - spoolBefore; got != 4 {
			t.Fatalf("PUT added %d spool blobs, want 4 — bodies must be spooled by digest, not logged", got)
		}

		if got := getBody(t, ctx, cl, bucket, "big", ""); !bytes.Equal(got, data) {
			t.Fatalf("whole GET mismatch: got %d bytes, want %d", len(got), len(data))
		}

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
			if !bytes.Equal(got, want) {
				t.Fatalf("range %s mismatch: got %d bytes, want %d", hdr, len(got), len(want))
			}
		}

		head, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("big")})
		if err != nil {
			t.Fatalf("HeadObject: %v", err)
		}
		if aws.ToInt64(head.ContentLength) != size {
			t.Fatalf("HEAD ContentLength = %d, want %d", aws.ToInt64(head.ContentLength), size)
		}
		if want := quotedMD5(data); aws.ToString(head.ETag) != want {
			t.Fatalf("HEAD ETag = %s, want %s", aws.ToString(head.ETag), want)
		}
	})

	// ZeroByteObject: a 0-byte object stores no blob, round-trips empty,
	// and carries the well-known empty-content md5 ETag.
	t.Run("ZeroByteObject", func(t *testing.T) {
		const bucket = "zero-byte"
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}

		const emptyMD5 = `"d41d8cd98f00b204e9800998ecf8427e"`
		spoolBefore := spoolBlobCount(t, ctx, s)
		put, err := cl.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("empty"),
			Body:   bytes.NewReader(nil),
		})
		if err != nil {
			t.Fatalf("PutObject: %v", err)
		}
		if aws.ToString(put.ETag) != emptyMD5 {
			t.Fatalf("PUT ETag = %s, want %s", aws.ToString(put.ETag), emptyMD5)
		}
		if got := spoolBlobCount(t, ctx, s) - spoolBefore; got != 0 {
			t.Fatalf("zero-byte PUT added %d spool blobs, want 0", got)
		}

		if got := getBody(t, ctx, cl, bucket, "empty", ""); len(got) != 0 {
			t.Fatalf("zero-byte GET returned %d bytes, want 0", len(got))
		}
		head, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("empty")})
		if err != nil {
			t.Fatalf("HeadObject: %v", err)
		}
		if aws.ToInt64(head.ContentLength) != 0 {
			t.Fatalf("HEAD ContentLength = %d, want 0", aws.ToInt64(head.ContentLength))
		}
		if aws.ToString(head.ETag) != emptyMD5 {
			t.Fatalf("HEAD ETag = %s, want %s", aws.ToString(head.ETag), emptyMD5)
		}
	})

	// MultipartPartSpansBlobs: Create → UploadPart×3 → Complete where part 1
	// is larger than max_blob_size, so a single part coarse-splits into
	// multiple internal body blobs. Upstream can assert multipart round-trip
	// but has no notion of ingot's internal split; the boundary-spanning
	// ranged GET proves reassembly across both part and blob boundaries.
	t.Run("MultipartPartSpansBlobs", func(t *testing.T) {
		const bucket, key = "mpbucket", "big/obj"
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}

		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), ContentType: aws.String("text/plain"),
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload: %v", err)
		}
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
			if err != nil {
				t.Fatalf("UploadPart %d: %v", pn, err)
			}
			completed = append(completed, types.CompletedPart{PartNumber: aws.Int32(pn), ETag: up.ETag})
			whole = append(whole, data...)
		}

		comp, err := cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
		})
		if err != nil {
			t.Fatalf("CompleteMultipartUpload: %v", err)
		}
		if et := strings.Trim(aws.ToString(comp.ETag), `"`); !strings.HasSuffix(et, "-3") {
			t.Fatalf("complete ETag = %q, want a multipart -3 suffix", et)
		}

		if got := getBody(t, ctx, cl, bucket, key, ""); !bytes.Equal(got, whole) {
			t.Fatalf("multipart GET mismatch: got %d bytes, want %d", len(got), len(whole))
		}
		head, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			t.Fatalf("HeadObject: %v", err)
		}
		if aws.ToInt64(head.ContentLength) != int64(len(whole)) {
			t.Fatalf("HEAD size = %d, want %d", aws.ToInt64(head.ContentLength), len(whole))
		}
		// A ranged GET spanning the part-1→part-2 boundary still reconstructs.
		p1 := len(partData[0])
		hdr := fmt.Sprintf("bytes=%d-%d", p1-10, p1+2000)
		if got := getBody(t, ctx, cl, bucket, key, hdr); !bytes.Equal(got, whole[p1-10:p1+2001]) {
			t.Fatalf("ranged multipart GET across the part boundary mismatch: got %d bytes", len(got))
		}
	})

	// HeadListPlaintextSizes: every size and ETag ingot reports is the
	// plaintext value from the manifest, never the size of a stored FEE
	// envelope or their sum. The spooled envelopes are measured inside the
	// container so the assertions can name that failure mode. Checked through
	// HEAD, GET (whole, ranged, ?partNumber), GetObjectAttributes,
	// ListObjects, ListObjectsV2 and ListObjectVersions, for a single-PUT
	// object and a multipart object, each spanning several envelopes.
	t.Run("HeadListPlaintextSizes", func(t *testing.T) {
		const bucket = "plainsizes"
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}

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
				if got == e {
					t.Fatalf("%s = %d: a stored envelope's size, want the plaintext %d", what, got, want)
				}
			}
			if got == o.envTotal {
				t.Fatalf("%s = %d: the stored envelopes' total, want the plaintext %d", what, got, want)
			}
			t.Fatalf("%s = %d, want the plaintext %d", what, got, want)
		}
		assertETag := func(t *testing.T, what string, got *string, want string) {
			t.Helper()
			if g := strings.Trim(aws.ToString(got), `"`); g != want {
				t.Fatalf("%s ETag = %q, want the plaintext-derived %q", what, g, want)
			}
		}
		// envelopeSizes measures the spool files a write added.
		envelopeSizes := func(t *testing.T, before, after map[string]bool) ([]int64, int64) {
			t.Helper()
			paths := newSpoolPaths(before, after)
			if len(paths) < 2 {
				t.Fatalf("write spooled %d envelopes, want several (small blob ceiling)", len(paths))
			}
			var sizes []int64
			var total int64
			for _, p := range paths {
				out, errOut, err := s.Exec(ctx, "ingot", "stat", "-c", "%s", p)
				if err != nil {
					t.Fatalf("stat %s: %v (stderr=%s)", p, err, errOut)
				}
				n, err := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
				if err != nil {
					t.Fatalf("parse size %q: %v", out, err)
				}
				sizes = append(sizes, n)
				total += n
			}
			return sizes, total
		}

		// Single PUT: 200 KiB + 37 bytes → four envelopes at 64 KiB.
		single := patternBytes((200 << 10) + 37)
		before := spoolBlobPaths(t, ctx, s)
		if _, err := cl.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("single"), Body: bytes.NewReader(single)}); err != nil {
			t.Fatalf("PutObject: %v", err)
		}
		sizes, total := envelopeSizes(t, before, spoolBlobPaths(t, ctx, s))
		sum := md5.Sum(single)
		objects = append(objects, object{"single", single, hex.EncodeToString(sum[:]), sizes, total})

		// Multipart: two parts, the first spanning many envelopes.
		partData := [][]byte{tagged(patternBytes((5<<20)+4096), 0x61), tagged(patternBytes(9<<10), 0x62)}
		before = spoolBlobPaths(t, ctx, s)
		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String("multipart")})
		if err != nil {
			t.Fatalf("CreateMultipartUpload: %v", err)
		}
		var completed []types.CompletedPart
		var mpWhole []byte
		etagCat := md5.New()
		for i, data := range partData {
			pn := int32(i + 1)
			up, err := cl.UploadPart(ctx, &s3.UploadPartInput{
				Bucket: aws.String(bucket), Key: aws.String("multipart"), UploadId: create.UploadId,
				PartNumber: aws.Int32(pn), Body: bytes.NewReader(data),
			})
			if err != nil {
				t.Fatalf("UploadPart %d: %v", pn, err)
			}
			completed = append(completed, types.CompletedPart{PartNumber: aws.Int32(pn), ETag: up.ETag})
			mpWhole = append(mpWhole, data...)
			md := md5.Sum(data)
			etagCat.Write(md[:])
		}
		if _, err := cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String("multipart"), UploadId: create.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
		}); err != nil {
			t.Fatalf("CompleteMultipartUpload: %v", err)
		}
		sizes, total = envelopeSizes(t, before, spoolBlobPaths(t, ctx, s))
		mp := object{"multipart", mpWhole, hex.EncodeToString(etagCat.Sum(nil)) + "-2", sizes, total}
		objects = append(objects, mp)

		for _, o := range objects {
			if o.envTotal <= int64(len(o.plaintext)) {
				t.Fatalf("%s stored %d bytes for %d plaintext; envelopes must be larger", o.key, o.envTotal, len(o.plaintext))
			}
			want := int64(len(o.plaintext))

			head, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(o.key)})
			if err != nil {
				t.Fatalf("HeadObject %s: %v", o.key, err)
			}
			assertSize(t, "HEAD "+o.key+" Content-Length", aws.ToInt64(head.ContentLength), want, o)
			assertETag(t, "HEAD "+o.key, head.ETag, o.etag)

			get, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(o.key)})
			if err != nil {
				t.Fatalf("GetObject %s: %v", o.key, err)
			}
			get.Body.Close()
			assertSize(t, "GET "+o.key+" Content-Length", aws.ToInt64(get.ContentLength), want, o)
			assertETag(t, "GET "+o.key, get.ETag, o.etag)

			rget, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(o.key), Range: aws.String("bytes=10-2009")})
			if err != nil {
				t.Fatalf("GetObject %s range: %v", o.key, err)
			}
			rget.Body.Close()
			assertSize(t, "ranged GET "+o.key+" Content-Length", aws.ToInt64(rget.ContentLength), 2000, o)
			if wantCR := fmt.Sprintf("bytes 10-2009/%d", want); aws.ToString(rget.ContentRange) != wantCR {
				t.Fatalf("ranged GET %s Content-Range = %q, want %q", o.key, aws.ToString(rget.ContentRange), wantCR)
			}

			attrs, err := cl.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
				Bucket: aws.String(bucket), Key: aws.String(o.key),
				ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize, types.ObjectAttributesEtag},
			})
			if err != nil {
				t.Fatalf("GetObjectAttributes %s: %v", o.key, err)
			}
			assertSize(t, "GetObjectAttributes "+o.key+" ObjectSize", aws.ToInt64(attrs.ObjectSize), want, o)
			assertETag(t, "GetObjectAttributes "+o.key, attrs.ETag, o.etag)
		}

		// ?partNumber on the multipart object, via HEAD and GET (separate
		// paths): the part's plaintext length and its plaintext offset within
		// the plaintext total.
		assertPart := func(t *testing.T, what string, length *int64, contentRange *string, partsCount *int32, wantLen int64, wantCR string) {
			t.Helper()
			assertSize(t, what+" Content-Length", aws.ToInt64(length), wantLen, mp)
			if aws.ToString(contentRange) != wantCR {
				t.Fatalf("%s Content-Range = %q, want %q", what, aws.ToString(contentRange), wantCR)
			}
			if aws.ToInt32(partsCount) != int32(len(partData)) {
				t.Fatalf("%s PartsCount = %d, want %d", what, aws.ToInt32(partsCount), len(partData))
			}
		}
		var offset int64
		for i, data := range partData {
			pn := int32(i + 1)
			wantCR := fmt.Sprintf("bytes %d-%d/%d", offset, offset+int64(len(data))-1, len(mpWhole))
			head, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("multipart"), PartNumber: aws.Int32(pn)})
			if err != nil {
				t.Fatalf("HeadObject partNumber=%d: %v", pn, err)
			}
			assertPart(t, fmt.Sprintf("HEAD partNumber=%d", pn), head.ContentLength, head.ContentRange, head.PartsCount, int64(len(data)), wantCR)

			get, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("multipart"), PartNumber: aws.Int32(pn)})
			if err != nil {
				t.Fatalf("GetObject partNumber=%d: %v", pn, err)
			}
			body, err := io.ReadAll(get.Body)
			get.Body.Close()
			if err != nil {
				t.Fatalf("read GET partNumber=%d: %v", pn, err)
			}
			assertPart(t, fmt.Sprintf("GET partNumber=%d", pn), get.ContentLength, get.ContentRange, get.PartsCount, int64(len(data)), wantCR)
			if !bytes.Equal(body, data) {
				t.Fatalf("GET partNumber=%d returned %d bytes that are not part %d", pn, len(body), pn)
			}
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
			if !ok {
				t.Fatalf("%s listed unexpected key %q", what, key)
			}
			assertSize(t, what+" "+key+" Size", aws.ToInt64(size), int64(len(o.plaintext)), o)
			assertETag(t, what+" "+key, etag, o.etag)
		}
		v1, err := cl.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(bucket)})
		if err != nil {
			t.Fatalf("ListObjects: %v", err)
		}
		if len(v1.Contents) != len(objects) {
			t.Fatalf("ListObjects returned %d keys, want %d", len(v1.Contents), len(objects))
		}
		for _, c := range v1.Contents {
			check("ListObjects", aws.ToString(c.Key), c.Size, c.ETag)
		}
		v2, err := cl.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		if err != nil {
			t.Fatalf("ListObjectsV2: %v", err)
		}
		if len(v2.Contents) != len(objects) {
			t.Fatalf("ListObjectsV2 returned %d keys, want %d", len(v2.Contents), len(objects))
		}
		for _, c := range v2.Contents {
			check("ListObjectsV2", aws.ToString(c.Key), c.Size, c.ETag)
		}
		lv, err := cl.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
		if err != nil {
			t.Fatalf("ListObjectVersions: %v", err)
		}
		if len(lv.Versions) != len(objects) {
			t.Fatalf("ListObjectVersions returned %d versions, want %d", len(lv.Versions), len(objects))
		}
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
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload: %v", err)
		}
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
			if err != nil {
				t.Fatalf("UploadPart %d: %v", pn, err)
			}
			etags[pn-1] = up.ETag
		}

		lp, err := cl.ListParts(ctx, &s3.ListPartsInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: uploadID})
		if err != nil {
			t.Fatalf("ListParts: %v", err)
		}
		if len(lp.Parts) != len(partData) {
			t.Fatalf("ListParts returned %d parts, want %d", len(lp.Parts), len(partData))
		}
		for i, p := range lp.Parts {
			want := int32(i + 1)
			if aws.ToInt32(p.PartNumber) != want {
				t.Fatalf("ListParts[%d].PartNumber = %d, want %d", i, aws.ToInt32(p.PartNumber), want)
			}
			if aws.ToInt64(p.Size) != int64(len(partData[i])) {
				t.Fatalf("ListParts part %d size = %d, want %d", want, aws.ToInt64(p.Size), len(partData[i]))
			}
			if strings.Trim(aws.ToString(p.ETag), `"`) != strings.Trim(aws.ToString(etags[i]), `"`) {
				t.Fatalf("ListParts part %d ETag = %q, want %q", want, aws.ToString(p.ETag), aws.ToString(etags[i]))
			}
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
		if err != nil {
			t.Fatalf("CompleteMultipartUpload: %v", err)
		}
		if got := strings.Trim(aws.ToString(comp.ETag), `"`); got != wantETag {
			t.Fatalf("complete ETag = %q, want %q (md5-of-md5s in part-number order)", got, wantETag)
		}

		if got := getBody(t, ctx, cl, bucket, key, ""); !bytes.Equal(got, whole) {
			t.Fatalf("GET after out-of-order upload mismatch: got %d bytes, want %d", len(got), len(whole))
		}
		p1 := len(partData[0])
		hdr := fmt.Sprintf("bytes=%d-%d", p1-10, p1+2000)
		if got := getBody(t, ctx, cl, bucket, key, hdr); !bytes.Equal(got, whole[p1-10:p1+2001]) {
			t.Fatalf("ranged GET across the part boundary mismatch: got %d bytes", len(got))
		}
		part2, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), PartNumber: aws.Int32(2)})
		if err != nil {
			t.Fatalf("GetObject partNumber=2: %v", err)
		}
		defer part2.Body.Close()
		got, err := io.ReadAll(part2.Body)
		if err != nil {
			t.Fatalf("read part 2: %v", err)
		}
		if !bytes.Equal(got, partData[1]) {
			t.Fatalf("GET partNumber=2 returned %d bytes that are not part 2", len(got))
		}
		if aws.ToInt32(part2.PartsCount) != 3 {
			t.Fatalf("GET partNumber=2 PartsCount = %d, want 3", aws.ToInt32(part2.PartsCount))
		}
	})

	// MultipartAbortCleansSpool: aborting an upload discards its parts — the
	// registry rows go (upstream AbortMultipartUpload_success verifies via
	// ListMultipartUploads) and, ingot-specifically, the parts' spooled blobs
	// are deleted, since under the spool model an abort's cleanup is entirely
	// local (nothing shipped to the network before Complete).
	t.Run("MultipartAbortCleansSpool", func(t *testing.T) {
		const bucket, key = "mpabort-spool", "obj"
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			t.Fatalf("CreateMultipartUpload: %v", err)
		}
		spoolBefore := spoolBlobCount(t, ctx, s)
		// A 150 KiB part spans three 64 KiB blobs (plaintext split; each is
		// stored as its own envelope).
		part := patternBytes(150 << 10)
		for i := range part {
			part[i] ^= 0xA5
		}
		if _, err := cl.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			PartNumber: aws.Int32(1), Body: bytes.NewReader(part),
		}); err != nil {
			t.Fatalf("UploadPart: %v", err)
		}
		if got := spoolBlobCount(t, ctx, s) - spoolBefore; got != 3 {
			t.Fatalf("UploadPart added %d spool blobs, want 3", got)
		}
		if _, err := cl.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
		}); err != nil {
			t.Fatalf("AbortMultipartUpload: %v", err)
		}
		if got := spoolBlobCount(t, ctx, s) - spoolBefore; got != 0 {
			t.Fatalf("abort left %d spooled part blobs behind, want 0", got)
		}
	})

	// MultipartFailedCompleteStaysAbortable: the zombie-session regression. A
	// Complete that fails validation (wrong part ETag) must leave the session
	// open — a retry with the correct ETag succeeds and reads back
	// byte-exact. Upstream tests re-complete-after-success
	// (already_completed) but not recovery from a FAILED complete; the
	// session-state revert out of 'completing' is ingot catalog behavior.
	t.Run("MultipartFailedCompleteStaysAbortable", func(t *testing.T) {
		const bucket, key = "mpretry", "obj"
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			t.Fatalf("CreateMultipartUpload: %v", err)
		}
		data := patternBytes(8 << 10)
		up, err := cl.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			PartNumber: aws.Int32(1), Body: bytes.NewReader(data),
		})
		if err != nil {
			t.Fatalf("UploadPart: %v", err)
		}

		// Complete with a WRONG part ETag → InvalidPart. The session must not zombie.
		if _, err := cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: aws.String("\"deadbeefdeadbeefdeadbeefdeadbeef\"")},
			}},
		}); err == nil {
			t.Fatal("Complete with a wrong part ETag: want an error, got nil")
		}

		// A retry with the correct ETag now succeeds (session was reverted to open).
		if _, err := cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: up.ETag},
			}},
		}); err != nil {
			t.Fatalf("Complete retry after a failed Complete: %v", err)
		}
		if got := getBody(t, ctx, cl, bucket, key, ""); !bytes.Equal(got, data) {
			t.Fatalf("retried multipart object mismatch: %d bytes", len(got))
		}
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
		if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		data := []byte("cors body")
		if _, err := cl.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(data),
		}); err != nil {
			t.Fatalf("PutObject: %v", err)
		}

		// Preflight. Browsers send these unsigned, so this also pins that
		// the OPTIONS route sits ahead of SigV4.
		preflight := func(t *testing.T, origin string) *http.Response {
			t.Helper()
			req, err := http.NewRequestWithContext(ctx, http.MethodOptions, fmt.Sprintf("%s/%s/%s", endpoint, bucket, key), nil)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			req.Header.Set("Origin", origin)
			req.Header.Set("Access-Control-Request-Method", "PUT")
			req.Header.Set("Access-Control-Request-Headers", "authorization, x-amz-content-sha256, x-amz-date")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("preflight: %v", err)
			}
			t.Cleanup(func() { _ = resp.Body.Close() })
			return resp
		}

		resp := preflight(t, origin)
		// CORS preflight spec allows any "ok status" (200-299).
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			t.Errorf("preflight status = %d, want 2xx", resp.StatusCode)
		}
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != origin {
			t.Errorf("preflight Allow-Origin = %q, want the request origin echoed", got)
		}
		if got := resp.Header.Get("Access-Control-Allow-Methods"); !strings.Contains(got, "PUT") {
			t.Errorf("preflight Allow-Methods = %q, want it to include PUT", got)
		}
		if got := resp.Header.Get("Access-Control-Max-Age"); got != maxAge {
			t.Errorf("preflight Max-Age = %q, want %s", got, maxAge)
		}

		if got := preflight(t, disallow).Header.Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("preflight Allow-Origin = %q for a disallowed origin, want unset", got)
		}

		// A real cross-origin read: a presigned GET is how a browser fetches
		// an object, and the response must expose ETag to JavaScript.
		presigned, err := s3.NewPresignClient(cl).PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		})
		if err != nil {
			t.Fatalf("PresignGetObject: %v", err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, presigned.URL, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Origin", origin)
		get, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("presigned GET: %v", err)
		}
		defer func() { _ = get.Body.Close() }()
		if get.StatusCode != http.StatusOK {
			t.Fatalf("presigned GET status = %d, want 200", get.StatusCode)
		}
		if got := get.Header.Get("Access-Control-Allow-Origin"); got != origin {
			t.Errorf("GET Allow-Origin = %q, want the request origin echoed", got)
		}
		if got := get.Header.Get("Access-Control-Expose-Headers"); !strings.Contains(got, "ETag") {
			t.Errorf("GET Expose-Headers = %q, want it to include ETag", got)
		}
	})
}
