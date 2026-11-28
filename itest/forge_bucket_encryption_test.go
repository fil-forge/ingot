//go:build itest

package itest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestForgeBucketEncryption is the end-to-end bucket-encryption suite, on a
// stack whose ingot allows SSEAlgorithm "none" (testdata/config-allownone.yaml).
// It pins what the unit tests cannot: the API over the wire through hilt's
// authorization, the x-amz-server-side-encryption header on every object
// response, and the stored form on disk and in Postgres — a "none" bucket's
// body is the plaintext itself with no key row, an AES256 one an envelope
// with one. It also looks at the blob piri holds, found by the sha256 of the
// bytes the client sent: a read back through ingot cannot tell whether the
// stored blob was transformed, because ingot reverses whatever it applied.
// (A GET or HEAD carrying an encryption header is refused; the conformance
// corpus pins that, sse-0031, with a raw request.)
func TestForgeBucketEncryption(t *testing.T) {
	ctx := t.Context()
	s, endpoint := forgeStack(t, withAllowNoneConfig())
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "bucket-encryption")
	cl := sdkClient(forgeS3Conf(endpoint, accessKey, secretKey))

	const bucket = "plain"
	_, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "CreateBucket")
	sseConfig := func(algo types.ServerSideEncryption) *types.ServerSideEncryptionConfiguration {
		return &types.ServerSideEncryptionConfiguration{Rules: []types.ServerSideEncryptionRule{{
			ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{SSEAlgorithm: algo},
		}}}
	}
	wantNotFound := func(t *testing.T) {
		t.Helper()
		_, err := cl.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String(bucket)})
		code, status := apiErrorOf(t, err)
		require.Equal(t, "ServerSideEncryptionConfigurationNotFoundError", code, "GetBucketEncryption = %s/%d", code, status)
		require.Equal(t, http.StatusNotFound, status, "GetBucketEncryption = %s/%d", code, status)
	}
	head := func(t *testing.T, key string) types.ServerSideEncryption {
		t.Helper()
		out, err := cl.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		require.NoError(t, err, "HeadObject %s", key)
		return out.ServerSideEncryption
	}
	get := func(t *testing.T, key string) (types.ServerSideEncryption, []byte) {
		t.Helper()
		out, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		require.NoError(t, err, "GetObject %s", key)
		defer out.Body.Close()
		body, err := io.ReadAll(out.Body)
		require.NoError(t, err, "read %s", key)
		return out.ServerSideEncryption, body
	}
	// keyRows counts the blob_encryption_params rows of key's body blobs.
	keyRows := func(t *testing.T, key string) (blobs, rows int) {
		t.Helper()
		digests := objectBlobDigestsHex(t, ctx, s, bucket, key)
		return len(digests), countRowsForDigests(t, ctx, s, "ingot.blob_encryption_params", "digest", digests)
	}
	// localSHA256 is the sha256 of one cached file inside the ingot container.
	localSHA256 := func(t *testing.T, path string) string {
		t.Helper()
		out, errOut, err := s.Exec(ctx, "ingot", "sha256sum", path)
		require.NoError(t, err, "sha256sum %s: %v (stderr=%s)", path, err, errOut)
		return strings.Fields(out)[0]
	}
	putOne := func(t *testing.T, key string, data []byte, sse types.ServerSideEncryption) (types.ServerSideEncryption, string) {
		t.Helper()
		before := localBlobPaths(t, ctx, s)
		out, err := cl.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(data), ServerSideEncryption: sse,
		})
		require.NoError(t, err, "PutObject %s", key)
		added := newLocalPaths(before, localBlobPaths(t, ctx, s))
		require.Len(t, added, 1, "PutObject %s cached %d files", key, len(added))
		return out.ServerSideEncryption, added[0]
	}

	// Never configured: not found, and encrypting all the same.
	wantNotFound(t)
	data := patternBytes(1 << 20)
	sum := sha256.Sum256(data)
	plainHex := hex.EncodeToString(sum[:])
	sse, path := putOne(t, "default", data, "")
	require.Equal(t, types.ServerSideEncryptionAes256, sse, "unconfigured bucket: PUT")
	require.NotEqual(t, plainHex, localSHA256(t, path), "unconfigured bucket: the stored file is the plaintext")
	blobs, rows := keyRows(t, "default")
	require.Equal(t, blobs, rows, "unconfigured bucket: %d key rows for %d blobs", rows, blobs)
	// The control for the piri-side check below: an encrypted PUT of these
	// bytes leaves piri holding an envelope, so once that envelope is on
	// piri, no blob of the plaintext's digest is. (Checked before the "none"
	// PUT of the same bytes, which creates exactly that blob.)
	waitForPiriBlob(t, ctx, s, hexDigest(t, objectBlobDigestsHex(t, ctx, s, bucket, "default")[0]), 30*time.Second)
	require.False(t, piriHoldsBlob(t, ctx, s, sha256Digest(t, sum)), "unconfigured bucket: piri holds a blob of the plaintext's digest after an encrypted PUT")

	// "none": stored as received.
	_, err = cl.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{Bucket: aws.String(bucket), ServerSideEncryptionConfiguration: sseConfig("none")})
	require.NoError(t, err, "PutBucketEncryption none")
	got, err := cl.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "GetBucketEncryption")
	rules := got.ServerSideEncryptionConfiguration.Rules
	require.Len(t, rules, 1, "GetBucketEncryption = %+v, want one rule naming none", rules)
	require.Equal(t, types.ServerSideEncryption("none"), rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm, "GetBucketEncryption = %+v, want one rule naming none", rules)
	t.Run("StoredAsReceived", func(t *testing.T) {
		sse, path := putOne(t, "plain", data, "")
		require.Empty(t, sse, "PUT")
		require.Equal(t, plainHex, localSHA256(t, path), "cached file sha256 against the plaintext's")
		blobs, rows := keyRows(t, "plain")
		require.Equal(t, 1, blobs, "%d blobs with %d key rows", blobs, rows)
		require.Zero(t, rows, "%d blobs with %d key rows", blobs, rows)
		require.Empty(t, head(t, "plain"), "HEAD reports encryption")
		sse, body := get(t, "plain")
		require.Empty(t, sse, "GET reports %q and %d bytes, want no encryption and the %d-byte plaintext", sse, len(body), len(data))
		require.Equal(t, data, body, "GET reports %q and %d bytes, want no encryption and the %d-byte plaintext", sse, len(body), len(data))
		// The digest of the stored blob is the plaintext's own: the sha256
		// multihash of the bytes.
		require.Equal(t, "1220"+plainHex, objectBlobDigestsHex(t, ctx, s, bucket, "plain")[0], "stored digest against the plaintext's multihash")
		// And piri holds it under that digest: the blob on the network is
		// the uploaded bytes.
		waitForPiriBlob(t, ctx, s, sha256Digest(t, sum), 30*time.Second)
	})
	t.Run("PackSizedBlobOnPiri", func(t *testing.T) {
		// A client-side pack: one PutObject of 128 MiB, identified everywhere
		// by its sha256. Under max_blob_size, so it is one blob.
		const size = 128 << 20
		h := sha256.New()
		_, err := io.Copy(h, newPatternReader(size))
		require.NoError(t, err, "hash pattern")
		var want [32]byte
		copy(want[:], h.Sum(nil))
		big := bigObjectClient(t, endpoint, accessKey, secretKey)
		out, err := big.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String("pack"), Body: newPatternReader(size), ContentLength: aws.Int64(size),
		})
		require.NoError(t, err, "PutObject pack")
		require.Empty(t, out.ServerSideEncryption, "PUT")
		blobs, rows := keyRows(t, "pack")
		require.Equal(t, 1, blobs, "%d blobs with %d key rows", blobs, rows)
		require.Zero(t, rows, "%d blobs with %d key rows", blobs, rows)
		waitForPiriBlob(t, ctx, s, sha256Digest(t, want), 60*time.Second)
	})
	t.Run("ExplicitAES256EncryptsOneObject", func(t *testing.T) {
		sse, path := putOne(t, "asked", data, types.ServerSideEncryptionAes256)
		require.Equal(t, types.ServerSideEncryptionAes256, sse, "PUT with AES256")
		require.NotEqual(t, plainHex, localSHA256(t, path), "cached file is the plaintext; the object was not encrypted")
		blobs, rows := keyRows(t, "asked")
		require.Equal(t, 1, blobs, "%d blobs with %d key rows", blobs, rows)
		require.Equal(t, 1, rows, "%d blobs with %d key rows", blobs, rows)
		require.Equal(t, types.ServerSideEncryptionAes256, head(t, "asked"), "HEAD")
		sse, body := get(t, "asked")
		require.Equal(t, types.ServerSideEncryptionAes256, sse, "GET")
		require.Equal(t, data, body, "GET: the plaintext back")
	})
	t.Run("MultipartFollowsTheCreate", func(t *testing.T) {
		p1, p2 := patternBytes(5<<20), []byte("tail of the multipart object")
		for _, tc := range []struct {
			key  string
			sse  types.ServerSideEncryption
			rows int
		}{{"mp-plain", "", 0}, {"mp-enc", types.ServerSideEncryptionAes256, 2}} {
			mpu, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(tc.key), ServerSideEncryption: tc.sse})
			require.NoError(t, err, "CreateMultipartUpload %s", tc.key)
			require.Equal(t, tc.sse, mpu.ServerSideEncryption, "%s: create", tc.key)
			var parts []types.CompletedPart
			for i, p := range [][]byte{p1, p2} {
				n := int32(i + 1)
				up, err := cl.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(tc.key), UploadId: mpu.UploadId, PartNumber: &n, Body: bytes.NewReader(p)})
				require.NoError(t, err, "UploadPart %s/%d", tc.key, n)
				require.Equal(t, tc.sse, up.ServerSideEncryption, "%s: part %d", tc.key, n)
				parts = append(parts, types.CompletedPart{ETag: up.ETag, PartNumber: &n})
			}
			done, err := cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(tc.key), UploadId: mpu.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}})
			require.NoError(t, err, "CompleteMultipartUpload %s", tc.key)
			require.Equal(t, tc.sse, done.ServerSideEncryption, "%s: complete", tc.key)
			blobs, rows := keyRows(t, tc.key)
			require.Equal(t, 2, blobs, "%s: %d blobs with %d key rows", tc.key, blobs, rows)
			require.Equal(t, tc.rows, rows, "%s: %d blobs with %d key rows", tc.key, blobs, rows)
			sse, body := get(t, tc.key)
			require.Equal(t, tc.sse, sse, "%s: GET reports %q (want %q) or the body differs", tc.key, sse, tc.sse)
			require.Equal(t, append(append([]byte{}, p1...), p2...), body, "%s: GET reports %q (want %q) or the body differs", tc.key, sse, tc.sse)
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
			require.NoError(t, err, "CopyObject %s → %s", tc.src, tc.dst)
			require.Equal(t, tc.sse, out.ServerSideEncryption, "%s: copy", tc.dst)
			blobs, rows := keyRows(t, tc.dst)
			require.Equal(t, 1, blobs, "%s: %d blobs with %d key rows", tc.dst, blobs, rows)
			require.Equal(t, tc.rows, rows, "%s: %d blobs with %d key rows", tc.dst, blobs, rows)
			sse, body := get(t, tc.dst)
			require.Equal(t, tc.sse, sse, "%s: GET reports %q (want %q) or the body differs", tc.dst, sse, tc.sse)
			require.Equal(t, data, body, "%s: GET reports %q (want %q) or the body differs", tc.dst, sse, tc.sse)
		}
	})
	t.Run("DeleteRestoresEncryption", func(t *testing.T) {
		_, err := cl.DeleteBucketEncryption(ctx, &s3.DeleteBucketEncryptionInput{Bucket: aws.String(bucket)})
		require.NoError(t, err, "DeleteBucketEncryption")
		wantNotFound(t)
		sse, path := putOne(t, "after-delete", data, "")
		require.Equal(t, types.ServerSideEncryptionAes256, sse, "after delete: PUT reports %q, want AES256 and an envelope on disk", sse)
		require.NotEqual(t, plainHex, localSHA256(t, path), "after delete: PUT reports %q, want AES256 and an envelope on disk", sse)
		// Configuring AES256 explicitly is accepted and round-trips.
		_, err = cl.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{Bucket: aws.String(bucket), ServerSideEncryptionConfiguration: sseConfig(types.ServerSideEncryptionAes256)})
		require.NoError(t, err, "PutBucketEncryption AES256")
		got, err := cl.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String(bucket)})
		require.NoError(t, err, "GetBucketEncryption")
		algo := got.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm
		require.Equal(t, types.ServerSideEncryptionAes256, algo, "GetBucketEncryption")
	})
	t.Run("ConflictingSubresources", func(t *testing.T) {
		// A query naming two subresources is refused as S3 refuses it, before
		// authentication and without consulting hilt. The SDK never sends one,
		// so the second parameter is added to the request before it is signed.
		withUploads := func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
				return stack.Build.Add(middleware.BuildMiddlewareFunc("AddUploads",
					func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (middleware.BuildOutput, middleware.Metadata, error) {
						req, ok := in.Request.(*smithyhttp.Request)
						if !ok {
							return middleware.BuildOutput{}, middleware.Metadata{}, fmt.Errorf("unexpected request type %T", in.Request)
						}
						req.URL.RawQuery += "&uploads"
						return next.HandleBuild(ctx, in)
					}), middleware.After)
			})
		}
		_, err := cl.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String(bucket)}, withUploads)
		code, status := apiErrorOf(t, err)
		require.Equal(t, "InvalidArgument", code, "GetBucketEncryption with ?uploads = %s/%d", code, status)
		require.Equal(t, http.StatusBadRequest, status, "GetBucketEncryption with ?uploads = %s/%d", code, status)
		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr, "error message = %q, want the conflicting parameters named, sorted", err)
		require.Contains(t, apiErr.ErrorMessage(), "Conflicting query string parameters: encryption, uploads", "error message = %q, want the conflicting parameters named, sorted", err)
	})

	t.Run("AWSCLI", func(t *testing.T) {
		// The commands the PR description shows, run by the AWS CLI itself:
		// the non-standard SSEAlgorithm "none" has to pass the CLI's own
		// parameter validation, round-trip through get-bucket-encryption,
		// and make a put-object store as received, with no SSE header on
		// head-object.
		ep, err := url.Parse(endpoint)
		require.NoError(t, err, "parse ingot endpoint %q", endpoint)
		containerEndpoint := "http://host.docker.internal:" + ep.Port()
		const cliBucket = "plain-cli"
		work := t.TempDir()
		inPath := filepath.Join(work, "in.bin")
		require.NoError(t, os.WriteFile(inPath, data, 0o644), "write input")
		awsAPI := fmt.Sprintf("aws --endpoint-url %q s3api", containerEndpoint)
		noneConfig := `'{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"none"}}]}'`
		script := strings.Join([]string{
			"set -eu",
			"exec > /work/run.log 2>&1",
			"aws configure set default.s3.addressing_style path",
			awsAPI + " create-bucket --bucket " + cliBucket,
			awsAPI + " put-bucket-encryption --bucket " + cliBucket + " --server-side-encryption-configuration " + noneConfig,
			awsAPI + " get-bucket-encryption --bucket " + cliBucket + " > /work/enc.json",
			awsAPI + " put-object --bucket " + cliBucket + " --key obj --body /work/in.bin > /work/put.json",
			awsAPI + " head-object --bucket " + cliBucket + " --key obj > /work/head.json",
			awsAPI + " delete-bucket-encryption --bucket " + cliBucket,
			// Unconfigured again: get-bucket-encryption fails with 404.
			"if " + awsAPI + " get-bucket-encryption --bucket " + cliBucket + " > /work/enc-after.json 2>&1; then echo 'get-bucket-encryption succeeded after delete' >&2; exit 1; fi",
			"grep -q ServerSideEncryptionConfigurationNotFoundError /work/enc-after.json",
		}, "\n")
		cli, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:      awsCLIImage,
				Entrypoint: []string{"/bin/sh"},
				Cmd:        []string{"-c", script},
				Env: map[string]string{
					"AWS_ACCESS_KEY_ID":         accessKey,
					"AWS_SECRET_ACCESS_KEY":     secretKey,
					"AWS_DEFAULT_REGION":        forgeRegion,
					"AWS_EC2_METADATA_DISABLED": "true",
					"AWS_PAGER":                 "",
				},
				Files: []testcontainers.ContainerFile{{HostFilePath: inPath, ContainerFilePath: "/work/in.bin", FileMode: 0o644}},
				HostConfigModifier: func(hc *container.HostConfig) {
					hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
				},
				WaitingFor: wait.ForExit().WithExitTimeout(5 * time.Minute),
			},
			Started: true,
		})
		if cli != nil {
			t.Cleanup(func() { _ = cli.Terminate(context.Background()) })
		}
		require.NoError(t, err, "run aws-cli container")
		state, err := cli.State(ctx)
		require.NoError(t, err, "aws-cli container state")
		logs := string(fileFromContainer(t, ctx, cli, "/work/run.log"))
		require.Zero(t, state.ExitCode, "aws-cli script exited %d:\n%s", state.ExitCode, logs)
		t.Logf("aws-cli output:\n%s", logs)

		var enc struct {
			ServerSideEncryptionConfiguration struct {
				Rules []struct {
					ApplyServerSideEncryptionByDefault struct {
						SSEAlgorithm string `json:"SSEAlgorithm"`
					} `json:"ApplyServerSideEncryptionByDefault"`
				} `json:"Rules"`
			} `json:"ServerSideEncryptionConfiguration"`
		}
		require.NoError(t, json.Unmarshal(fileFromContainer(t, ctx, cli, "/work/enc.json"), &enc), "parse get-bucket-encryption output")
		rules := enc.ServerSideEncryptionConfiguration.Rules
		require.Len(t, rules, 1, "get-bucket-encryption = %+v, want one rule with SSEAlgorithm none", enc)
		require.Equal(t, "none", rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm, "get-bucket-encryption = %+v, want one rule with SSEAlgorithm none", enc)
		var head map[string]any
		require.NoError(t, json.Unmarshal(fileFromContainer(t, ctx, cli, "/work/head.json"), &head), "parse head-object output")
		sse, ok := head["ServerSideEncryption"]
		require.False(t, ok, "head-object reports ServerSideEncryption %v on a none bucket, want none", sse)
		n, _ := head["ContentLength"].(float64)
		require.Equal(t, len(data), int(n), "head-object ContentLength")
	})
}
