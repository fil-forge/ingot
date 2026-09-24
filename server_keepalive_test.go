package ingot

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fil-forge/libforge/identity"
	"github.com/fil-forge/versitygw/auth"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3response"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/ingot/internal/reqscope"
)

// The CompleteMultipartUpload keepalive tests run buildS3API's real
// middleware chain against a stub backend whose completion outlasts the
// client's read timeout, the way concluding 128 parts outlasts the AWS CLI's
// 60 s.

const (
	testCompleteKeepalive = 100 * time.Millisecond
	// testReadTimeout is the longest silence the client tolerates, as
	// botocore's read timeout does.
	testReadTimeout = 5 * testCompleteKeepalive
	// testSlowCompletion outlasts the read timeout.
	testSlowCompletion = 3 * testReadTimeout
)

func TestCompleteMultipartUploadKeepalive_OutlastsTheReadTimeout(t *testing.T) {
	etag := `"keepalive-etag-2"`
	cases := []struct {
		name      string
		keepalive time.Duration
		want      string
	}{
		{name: "succeeds with the keepalive on", keepalive: testCompleteKeepalive, want: etag},
		// The control: the client gives up on a silent response.
		{name: "times out with the keepalive off", keepalive: -1, want: "<error>"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := startKeepaliveS3API(t, tc.keepalive, func(context.Context) (s3response.CompleteMultipartUploadResult, string, error) {
				time.Sleep(testSlowCompletion)
				return s3response.CompleteMultipartUploadResult{ETag: &etag}, "", nil
			})
			out, err := srv.CompleteMultipartUpload(t.Context(), keepaliveCompleteInput())
			got := "<error>"
			if err == nil {
				got = aws.ToString(out.ETag)
			}

			assert.Equal(t, tc.want, got)
		})
	}
}

// requestScope is what a completion sees of the request-scoped state
// ingot's middleware stores on the request.
type requestScope struct {
	HasSignedRequest bool
	// ServerSpanRecording: the request's server span is still open, so the
	// completion's own spans land inside it.
	ServerSpanRecording bool
}

func TestCompleteMultipartUploadKeepalive_BackendSeesTheRequestScope(t *testing.T) {
	// A recording provider; the global default makes every span a no-op.
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	seen := make(chan requestScope, 1)
	srv := startKeepaliveS3API(t, testCompleteKeepalive, func(ctx context.Context) (s3response.CompleteMultipartUploadResult, string, error) {
		time.Sleep(testSlowCompletion)
		_, hasRequest := reqscope.Request(ctx)
		seen <- requestScope{
			HasSignedRequest:    hasRequest,
			ServerSpanRecording: trace.SpanFromContext(ctx).IsRecording(),
		}
		return s3response.CompleteMultipartUploadResult{ETag: aws.String(`"etag"`)}, "", nil
	})
	_, err := srv.CompleteMultipartUpload(t.Context(), keepaliveCompleteInput())
	require.NoError(t, err)

	assert.Equal(t, requestScope{HasSignedRequest: true, ServerSpanRecording: true}, <-seen)
}

func TestCompleteMultipartUploadKeepalive_SDKRetriesALateFailure(t *testing.T) {
	var calls atomic.Int32
	srv := startKeepaliveS3API(t, testCompleteKeepalive, func(context.Context) (s3response.CompleteMultipartUploadResult, string, error) {
		calls.Add(1)
		time.Sleep(testSlowCompletion)
		return s3response.CompleteMultipartUploadResult{}, "", errors.New("conclude failed")
	})
	_, err := srv.CompleteMultipartUpload(t.Context(), keepaliveCompleteInput())
	require.Error(t, err)

	assert.Greater(t, calls.Load(), int32(1))
}

// startKeepaliveS3API serves buildS3API with the given keepalive interval
// over complete, and returns an SDK client whose connections time out after
// testReadTimeout of silence.
func startKeepaliveS3API(t *testing.T, keepalive time.Duration, complete completeFunc) *s3.Client {
	t.Helper()
	id, err := identity.New("", "did:web:ingot.test")
	require.NoError(t, err)
	account := auth.Account{Access: "keepalive-access", Secret: "keepalive-secret", Role: auth.RoleAdmin}
	cfg := config.ServerConfig{
		Region:                    "us-east-1",
		MaxConnections:            16,
		MaxRequests:               16,
		CompleteKeepaliveInterval: keepalive,
	}
	api, err := buildS3API(t.Context(), keepaliveBackend{complete: complete}, cfg, staticIAM{account: account}, id, zap.NewNop())
	require.NoError(t, err)

	addr := freeAddr(t)
	serveErr := make(chan error, 1)
	go func() { serveErr <- api.ServeMultiPort([]string{addr}) }()
	t.Cleanup(func() {
		require.NoError(t, api.ShutDown())
		if err := <-serveErr; err != nil && !strings.Contains(err.Error(), "closed") {
			t.Fatalf("s3api serve: %v", err)
		}
	})
	waitListening(t, addr)

	return s3.New(s3.Options{
		BaseEndpoint: aws.String("http://" + addr),
		UsePathStyle: true,
		Region:       cfg.Region,
		Credentials:  credentials.NewStaticCredentialsProvider(account.Access, account.Secret, ""),
		HTTPClient: &http.Client{Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				return &idleTimeoutConn{Conn: conn, timeout: testReadTimeout}, nil
			},
		}},
		Retryer: retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxBackoff = time.Millisecond
		}),
	})
}

func keepaliveCompleteInput() *s3.CompleteMultipartUploadInput {
	return &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String("bucket"),
		Key:      aws.String("object"),
		UploadId: aws.String("upload-1"),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: aws.String(`"part-etag"`)}},
		},
	}
}

// idleTimeoutConn fails a Read that waits longer than timeout for data, as
// botocore's socket read timeout does.
type idleTimeoutConn struct {
	net.Conn
	timeout time.Duration
}

func (c *idleTimeoutConn) Read(p []byte) (int, error) {
	if err := c.Conn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.Conn.Read(p)
}

// completeFunc is a backend's CompleteMultipartUpload.
type completeFunc func(context.Context) (s3response.CompleteMultipartUploadResult, string, error)

// keepaliveBackend answers the calls versitygw's auth chain makes the way
// s3frontend.Backend does for an existing bucket without CORS, policy or
// object lock, and completes uploads with complete.
type keepaliveBackend struct {
	backend.BackendUnsupported
	complete completeFunc
}

func (keepaliveBackend) GetBucketAcl(context.Context, *s3.GetBucketAclInput) ([]byte, error) {
	return nil, nil
}

func (keepaliveBackend) GetBucketCors(context.Context, string) ([]byte, error) {
	return nil, s3err.GetAPIError(s3err.ErrNoSuchCORSConfiguration)
}

func (keepaliveBackend) GetBucketPolicy(context.Context, string) ([]byte, error) {
	return nil, s3err.GetAPIError(s3err.ErrNoSuchBucketPolicy)
}

func (keepaliveBackend) GetObjectLockConfiguration(context.Context, string) ([]byte, error) {
	return nil, s3err.GetAPIError(s3err.ErrObjectLockConfigurationNotFound)
}

func (keepaliveBackend) GetBucketVersioning(context.Context, string) (s3response.GetBucketVersioningOutput, error) {
	return s3response.GetBucketVersioningOutput{}, nil
}

func (b keepaliveBackend) CompleteMultipartUpload(ctx context.Context, _ *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	return b.complete(ctx)
}

// staticIAM resolves a single account, standing in for iam.Service so a
// request authenticates without hilt.
type staticIAM struct {
	account auth.Account
}

func (s staticIAM) GetUserAccount(access string) (auth.Account, error) {
	if access != s.account.Access {
		return auth.Account{}, auth.ErrNoSuchUser
	}
	return s.account, nil
}

func (staticIAM) CreateAccount(auth.Account) error { return errors.ErrUnsupported }
func (staticIAM) UpdateUserAccount(string, auth.MutableProps) error {
	return errors.ErrUnsupported
}
func (staticIAM) DeleteUserAccount(string) error            { return errors.ErrUnsupported }
func (staticIAM) ListUserAccounts() ([]auth.Account, error) { return nil, errors.ErrUnsupported }
func (staticIAM) Shutdown() error                           { return nil }
