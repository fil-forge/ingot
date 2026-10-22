//go:build itest

package itest

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/smelt/pkg/stack"
)

// TestForgeCompleteKeepalive: a CompleteMultipartUpload that runs past the
// keepalive streams its response as S3 does, with x-amz-version-id in the
// early 200's headers, newline padding, then the result. The config's 1ms
// keepalive (testdata/config-mpkeepalive.yaml) passes long before the
// Complete reports its version id, so the headers carry the id only because
// they wait for the report. The idempotent re-Complete reads a few rows and
// may finish within the interval. Both report the version the bucket holds.
func TestForgeCompleteKeepalive(t *testing.T) {
	ctx := t.Context()
	s, endpoint := forgeStack(t, withCompleteKeepaliveConfig())
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "mpkeepalive")
	rec := &completeRecorder{}
	cl := recordingClient(t, endpoint, accessKey, secretKey, rec)

	const bucket, key = "mp-keepalive", "obj"
	if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := cl.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatalf("PutBucketVersioning: %v", err)
	}
	create, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	var parts []types.CompletedPart
	for n := int32(1); n <= 2; n++ {
		part, err := cl.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			PartNumber: aws.Int32(n), Body: bytes.NewReader(tagged(patternBytes(5<<20), byte(0x6a+n))),
		})
		if err != nil {
			t.Fatalf("UploadPart %d: %v", n, err)
		}
		parts = append(parts, types.CompletedPart{PartNumber: aws.Int32(n), ETag: part.ETag})
	}
	complete := &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}

	for _, attempt := range []string{"Complete", "re-Complete"} {
		start := time.Now()
		out, err := cl.CompleteMultipartUpload(ctx, complete)
		if err != nil {
			t.Fatalf("%s: %v", attempt, err)
		}
		t.Logf("%s took %s", attempt, time.Since(start))
		resp := rec.last(t)
		if attempt == "Complete" && !resp.streamed() {
			t.Fatalf("%s response was not streamed: body %q", attempt, resp.body)
		}
		if resp.versionID == "" || resp.versionID != aws.ToString(out.VersionId) {
			t.Fatalf("%s: header x-amz-version-id %q, SDK VersionId %q; want the same non-empty id", attempt, resp.versionID, aws.ToString(out.VersionId))
		}
		versions, err := cl.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
		if err != nil {
			t.Fatalf("ListObjectVersions: %v", err)
		}
		if len(versions.Versions) != 1 || aws.ToString(versions.Versions[0].VersionId) != resp.versionID {
			t.Fatalf("after %s: versions %+v, want the one version %q", attempt, versions.Versions, resp.versionID)
		}
	}
}

// withCompleteKeepaliveConfig mounts testdata/config-mpkeepalive.yaml —
// complete_multipart_keepalive at 1ms, so every Complete streams.
// Dedicated stacks only.
func withCompleteKeepaliveConfig() stack.Option {
	return stack.WithServiceConfig("ingot", "testdata/config-mpkeepalive.yaml")
}

// recordedComplete is a CompleteMultipartUpload response as it came off the
// wire, before the SDK decoded it.
type recordedComplete struct {
	versionID string
	body      []byte
}

// streamed reports whether the body is the XML declaration, newline
// padding, then the document: what a response that committed to 200 before
// its result sends. An unstreamed body has the declaration's newline only.
func (r recordedComplete) streamed() bool {
	decl, rest, ok := bytes.Cut(r.body, []byte("?>\n"))
	if !ok || !bytes.HasPrefix(decl, []byte("<?xml")) {
		return false
	}
	doc := bytes.TrimLeft(rest, "\n")
	return len(doc) < len(rest) && bytes.HasPrefix(doc, []byte("<CompleteMultipartUploadResult"))
}

// completeRecorder is an http.RoundTripper that records the raw response of
// every CompleteMultipartUpload it carries.
type completeRecorder struct {
	mu        sync.Mutex
	responses []*recordedComplete
}

func (c *completeRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil || req.Method != http.MethodPost || !req.URL.Query().Has("uploadId") {
		return resp, err
	}
	r := &recordedComplete{versionID: resp.Header.Get("x-amz-version-id")}
	c.mu.Lock()
	c.responses = append(c.responses, r)
	c.mu.Unlock()
	resp.Body = &recordingBody{ReadCloser: resp.Body, rec: c, into: r}
	return resp, nil
}

// last is the most recent recorded Complete response.
func (c *completeRecorder) last(t *testing.T) recordedComplete {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.responses) == 0 {
		t.Fatal("no CompleteMultipartUpload response recorded")
	}
	return *c.responses[len(c.responses)-1]
}

// recordingBody copies a response body into its record as the SDK reads it.
type recordingBody struct {
	io.ReadCloser
	rec  *completeRecorder
	into *recordedComplete
}

func (b *recordingBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.rec.mu.Lock()
	b.into.body = append(b.into.body, p[:n]...)
	b.rec.mu.Unlock()
	return n, err
}

// recordingClient is sdkClient's addressing and single-attempt retry policy
// over rec, so a streamed error is reported rather than retried away.
func recordingClient(t *testing.T, endpoint, accessKey, secretKey string, rec *completeRecorder) *s3.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(forgeRegion),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		awsconfig.WithHTTPClient(&http.Client{Transport: rec}),
		awsconfig.WithRetryMaxAttempts(1),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}
