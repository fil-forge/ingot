//go:build itest

package itest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestForgeBucketEncryption is the end-to-end bucket-encryption suite, on a
// stack whose ingot allows SSEAlgorithm "none" (testdata/config-allownone.yaml).
// It pins what the unit tests cannot: the API over the wire through hilt's
// authorization, the x-amz-server-side-encryption header on every object
// response, and the stored form on disk and in Postgres — a "none" bucket's
// body is the plaintext itself with no key row, an AES256 one an envelope
// with one. (A GET or HEAD carrying an encryption header is refused; the
// conformance corpus pins that, sse-0031, with a raw request.)
func TestForgeBucketEncryption(t *testing.T) {
	ctx := t.Context()
	s, endpoint := forgeStack(t, withAllowNoneConfig())
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "bucket-encryption")
	cl := sdkClient(forgeS3Conf(endpoint, accessKey, secretKey))

	const bucket = "plain"
	if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	sseConfig := func(algo types.ServerSideEncryption) *types.ServerSideEncryptionConfiguration {
		return &types.ServerSideEncryptionConfiguration{Rules: []types.ServerSideEncryptionRule{{
			ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{SSEAlgorithm: algo},
		}}}
	}
	wantNotFound := func(t *testing.T) {
		t.Helper()
		_, err := cl.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String(bucket)})
		if code, status := apiErrorOf(t, err); code != "ServerSideEncryptionConfigurationNotFoundError" || status != http.StatusNotFound {
			t.Fatalf("GetBucketEncryption = %s/%d, want ServerSideEncryptionConfigurationNotFoundError/404", code, status)
		}
	}
	head := func(t *testing.T, key string) types.ServerSideEncryption {
		t.Helper()
		out, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			t.Fatalf("HeadObject %s: %v", key, err)
		}
		return out.ServerSideEncryption
	}
	get := func(t *testing.T, key string) (types.ServerSideEncryption, []byte) {
		t.Helper()
		out, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			t.Fatalf("GetObject %s: %v", key, err)
		}
		defer out.Body.Close()
		body, err := io.ReadAll(out.Body)
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		return out.ServerSideEncryption, body
	}
	// keyRows counts the blob_encryption_params rows of key's body blobs.
	keyRows := func(t *testing.T, key string) (blobs, rows int) {
		t.Helper()
		digests := objectBlobDigestsHex(t, ctx, s, bucket, key)
		return len(digests), countRowsForDigests(t, ctx, s, "ingot.blob_encryption_params", "digest", digests)
	}
	// localSHA256 is the sha256 of one spooled file inside the ingot container.
	localSHA256 := func(t *testing.T, path string) string {
		t.Helper()
		out, errOut, err := s.Exec(ctx, "ingot", "sha256sum", path)
		if err != nil {
			t.Fatalf("sha256sum %s: %v (stderr=%s)", path, err, errOut)
		}
		return strings.Fields(out)[0]
	}
	putOne := func(t *testing.T, key string, data []byte, sse types.ServerSideEncryption) (types.ServerSideEncryption, string) {
		t.Helper()
		before := localBlobPaths(t, ctx, s)
		out, err := cl.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(data), ServerSideEncryption: sse,
		})
		if err != nil {
			t.Fatalf("PutObject %s: %v", key, err)
		}
		added := newLocalPaths(before, localBlobPaths(t, ctx, s))
		if len(added) != 1 {
			t.Fatalf("PutObject %s spooled %d files, want 1", key, len(added))
		}
		return out.ServerSideEncryption, added[0]
	}

	// Never configured: not found, and encrypting all the same.
	wantNotFound(t)
	data := patternBytes(1 << 20)
	sum := sha256.Sum256(data)
	plainHex := hex.EncodeToString(sum[:])
	if sse, path := putOne(t, "default", data, ""); sse != types.ServerSideEncryptionAes256 || localSHA256(t, path) == plainHex {
		t.Fatalf("unconfigured bucket: PUT reports %q and the stored file %s the plaintext, want AES256 and an envelope", sse, map[bool]string{true: "is", false: "is not"}[localSHA256(t, path) == plainHex])
	}
	if blobs, rows := keyRows(t, "default"); rows != blobs {
		t.Fatalf("unconfigured bucket: %d key rows for %d blobs", rows, blobs)
	}

	// "none": stored as received.
	if _, err := cl.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{Bucket: aws.String(bucket), ServerSideEncryptionConfiguration: sseConfig("none")}); err != nil {
		t.Fatalf("PutBucketEncryption none: %v", err)
	}
	got, err := cl.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("GetBucketEncryption: %v", err)
	}
	if rules := got.ServerSideEncryptionConfiguration.Rules; len(rules) != 1 || rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm != "none" {
		t.Fatalf("GetBucketEncryption = %+v, want one rule naming none", rules)
	}
	t.Run("StoredAsReceived", func(t *testing.T) {
		sse, path := putOne(t, "plain", data, "")
		if sse != "" {
			t.Fatalf("PUT reports %q, want no encryption", sse)
		}
		if got := localSHA256(t, path); got != plainHex {
			t.Fatalf("spooled file sha256 = %s, want the plaintext's %s", got, plainHex)
		}
		if blobs, rows := keyRows(t, "plain"); blobs != 1 || rows != 0 {
			t.Fatalf("%d blobs with %d key rows, want 1 and 0", blobs, rows)
		}
		if sse := head(t, "plain"); sse != "" {
			t.Fatalf("HEAD reports %q, want no encryption", sse)
		}
		sse, body := get(t, "plain")
		if sse != "" || !bytes.Equal(body, data) {
			t.Fatalf("GET reports %q and %d bytes, want no encryption and the %d-byte plaintext", sse, len(body), len(data))
		}
		// The digest of the stored blob is the plaintext's own: the sha256
		// multihash of the bytes.
		if digests := objectBlobDigestsHex(t, ctx, s, bucket, "plain"); digests[0] != "1220"+plainHex {
			t.Fatalf("stored digest %s is not the plaintext's multihash 1220%s", digests[0], plainHex)
		}
	})
	t.Run("ExplicitAES256EncryptsOneObject", func(t *testing.T) {
		sse, path := putOne(t, "asked", data, types.ServerSideEncryptionAes256)
		if sse != types.ServerSideEncryptionAes256 {
			t.Fatalf("PUT with AES256 reports %q, want AES256", sse)
		}
		if localSHA256(t, path) == plainHex {
			t.Fatalf("spooled file is the plaintext; the object was not encrypted")
		}
		if blobs, rows := keyRows(t, "asked"); blobs != 1 || rows != 1 {
			t.Fatalf("%d blobs with %d key rows, want 1 and 1", blobs, rows)
		}
		if sse := head(t, "asked"); sse != types.ServerSideEncryptionAes256 {
			t.Fatalf("HEAD reports %q, want AES256", sse)
		}
		if sse, body := get(t, "asked"); sse != types.ServerSideEncryptionAes256 || !bytes.Equal(body, data) {
			t.Fatalf("GET reports %q, want AES256 and the plaintext back", sse)
		}
	})
	t.Run("MultipartFollowsTheCreate", func(t *testing.T) {
		p1, p2 := patternBytes(5<<20), []byte("tail of the multipart object")
		for _, tc := range []struct {
			key  string
			sse  types.ServerSideEncryption
			rows int
		}{{"mp-plain", "", 0}, {"mp-enc", types.ServerSideEncryptionAes256, 2}} {
			mpu, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(tc.key), ServerSideEncryption: tc.sse})
			if err != nil {
				t.Fatalf("CreateMultipartUpload %s: %v", tc.key, err)
			}
			if mpu.ServerSideEncryption != tc.sse {
				t.Fatalf("%s: create reports %q, want %q", tc.key, mpu.ServerSideEncryption, tc.sse)
			}
			var parts []types.CompletedPart
			for i, p := range [][]byte{p1, p2} {
				n := int32(i + 1)
				up, err := cl.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(tc.key), UploadId: mpu.UploadId, PartNumber: &n, Body: bytes.NewReader(p)})
				if err != nil {
					t.Fatalf("UploadPart %s/%d: %v", tc.key, n, err)
				}
				if up.ServerSideEncryption != tc.sse {
					t.Fatalf("%s: part %d reports %q, want %q", tc.key, n, up.ServerSideEncryption, tc.sse)
				}
				parts = append(parts, types.CompletedPart{ETag: up.ETag, PartNumber: &n})
			}
			done, err := cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(tc.key), UploadId: mpu.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}})
			if err != nil {
				t.Fatalf("CompleteMultipartUpload %s: %v", tc.key, err)
			}
			if done.ServerSideEncryption != tc.sse {
				t.Fatalf("%s: complete reports %q, want %q", tc.key, done.ServerSideEncryption, tc.sse)
			}
			if blobs, rows := keyRows(t, tc.key); blobs != 2 || rows != tc.rows {
				t.Fatalf("%s: %d blobs with %d key rows, want 2 and %d", tc.key, blobs, rows, tc.rows)
			}
			if sse, body := get(t, tc.key); sse != tc.sse || !bytes.Equal(body, append(append([]byte{}, p1...), p2...)) {
				t.Fatalf("%s: GET reports %q (want %q) or the body differs", tc.key, sse, tc.sse)
			}
		}
	})
	t.Run("CopyFollowsTheDestination", func(t *testing.T) {
		// An encrypted source copied without a header lands as received; a
		// plaintext source copied with AES256 lands encrypted.
		for _, tc := range []struct {
			src, dst string
			sse      types.ServerSideEncryption
			rows     int
		}{{"asked", "asked-to-plain", "", 0}, {"plain", "plain-to-enc", types.ServerSideEncryptionAes256, 1}} {
			out, err := cl.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(bucket), Key: aws.String(tc.dst), CopySource: aws.String(bucket + "/" + tc.src), ServerSideEncryption: tc.sse})
			if err != nil {
				t.Fatalf("CopyObject %s → %s: %v", tc.src, tc.dst, err)
			}
			if out.ServerSideEncryption != tc.sse {
				t.Fatalf("%s: copy reports %q, want %q", tc.dst, out.ServerSideEncryption, tc.sse)
			}
			if blobs, rows := keyRows(t, tc.dst); blobs != 1 || rows != tc.rows {
				t.Fatalf("%s: %d blobs with %d key rows, want 1 and %d", tc.dst, blobs, rows, tc.rows)
			}
			if sse, body := get(t, tc.dst); sse != tc.sse || !bytes.Equal(body, data) {
				t.Fatalf("%s: GET reports %q (want %q) or the body differs", tc.dst, sse, tc.sse)
			}
		}
	})
	t.Run("DeleteRestoresEncryption", func(t *testing.T) {
		if _, err := cl.DeleteBucketEncryption(ctx, &s3.DeleteBucketEncryptionInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("DeleteBucketEncryption: %v", err)
		}
		wantNotFound(t)
		if sse, path := putOne(t, "after-delete", data, ""); sse != types.ServerSideEncryptionAes256 || localSHA256(t, path) == plainHex {
			t.Fatalf("after delete: PUT reports %q, want AES256 and an envelope on disk", sse)
		}
		// Configuring AES256 explicitly is accepted and round-trips.
		if _, err := cl.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{Bucket: aws.String(bucket), ServerSideEncryptionConfiguration: sseConfig(types.ServerSideEncryptionAes256)}); err != nil {
			t.Fatalf("PutBucketEncryption AES256: %v", err)
		}
		got, err := cl.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String(bucket)})
		if err != nil {
			t.Fatalf("GetBucketEncryption: %v", err)
		}
		if algo := got.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm; algo != types.ServerSideEncryptionAes256 {
			t.Fatalf("GetBucketEncryption = %q, want AES256", algo)
		}
	})
}
