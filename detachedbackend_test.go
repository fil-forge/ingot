package ingot

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/valyala/fasthttp"
)

// ctxRecorder is a backend that records the context its GetBucketPolicy and
// Complete were called with.
type ctxRecorder struct {
	backend.BackendUnsupported
	got []context.Context
}

func (r *ctxRecorder) GetBucketPolicy(ctx context.Context, _ string) ([]byte, error) {
	r.got = append(r.got, ctx)
	return nil, nil
}

func (r *ctxRecorder) CompleteMultipartUploadReportingHeaders(ctx context.Context, _ *s3.CompleteMultipartUploadInput, _ func(backend.CompleteMultipartUploadHeaders)) (s3response.CompleteMultipartUploadResult, string, error) {
	r.got = append(r.got, ctx)
	return s3response.CompleteMultipartUploadResult{}, "", nil
}

// TestDetachedBackendDetachesTheRequest: the backend never gets fasthttp's
// request value as its context, and the context it gets answers with the
// request's values.
func TestDetachedBackendDetachesTheRequest(t *testing.T) {
	var rc fasthttp.RequestCtx
	rc.Init(&fasthttp.Request{}, nil, nil)
	rc.SetUserValue("bucket", "bk")
	rec := &ctxRecorder{}
	d := detachedBackend{b: rec}

	if _, err := d.GetBucketPolicy(&rc, "bk"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.CompleteMultipartUploadReportingHeaders(&rc, &s3.CompleteMultipartUploadInput{}, nil); err != nil {
		t.Fatal(err)
	}
	for i, ctx := range rec.got {
		if _, same := ctx.(*fasthttp.RequestCtx); same {
			t.Errorf("call %d got fasthttp's request value as its context", i)
		}
		if got := ctx.Value("bucket"); got != "bk" {
			t.Errorf(`call %d: Value("bucket") = %v, want "bk"`, i, got)
		}
	}
}
