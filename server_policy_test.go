package ingot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"testing"
	"time"

	awsv4 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/fil-forge/ingot/bucketauthority"
	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/ingot/iam"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/internal/cors"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/s3frontend"
	s3 "github.com/fil-forge/libforge/commands/s3"
	s3bkt "github.com/fil-forge/libforge/commands/s3/bucket"
	"github.com/fil-forge/libforge/identity"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/auth"
	v4 "github.com/fil-forge/versitygw/aws/signer/v4"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap"
)

// fakeAuthority answers PolicyDocument with a canned result or error and
// records what it was given.
type fakeAuthority struct {
	bucketauthority.BucketAuthority
	ok    *s3bkt.PolicyOK
	err   error
	calls int
	req   s3.Request
	body  []byte
}

func (f *fakeAuthority) PolicyDocument(_ context.Context, req s3.Request, body []byte) (*s3bkt.PolicyOK, error) {
	f.calls++
	f.req, f.body = req, body
	return f.ok, f.err
}

// secretIAM resolves every access key to an admin account with a fixed
// secret, standing in for Hilt's authorize round trip so a test can sign
// requests the real versitygw middleware accepts.
type secretIAM struct {
	*iam.Service
	secret string
}

func (s secretIAM) GetUserAccountForRequest(_ fiber.Ctx, access string) (auth.Account, error) {
	return auth.Account{Access: access, Secret: s.secret, Role: auth.RoleAdmin}, nil
}

// TestBuildS3API_BucketPolicy drives the bucket policy operations through
// the real versitygw route table and controllers: a GET answers the stored
// document as JSON with its ETag, a PUT answers 204 with the new ETag, both
// headers coming from the ?policy middleware, which reads the ETag the backend
// computed from the document and the
// backend left in the request's reqscope holder.
func TestBuildS3API_BucketPolicy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	spans := tracetest.NewSpanRecorder()
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)))
	t.Cleanup(func() { otel.SetTracerProvider(prevTP) })

	const origin = "https://app.example"
	corsCfg, err := cors.Build([]string{origin})
	require.NoError(t, err)
	mem := inmem.NewMemStore()
	require.NoError(t, mem.Create(ctx, "photos", did.Undef, registry.CreateState{}))
	f := &fakeAuthority{}
	backend := s3frontend.New(s3frontend.Deps{Registry: mem, CORS: corsCfg, Authority: f})

	id, err := identity.New("", "did:web:ingot.test")
	require.NoError(t, err)
	const secret = "policysecret"
	svc := secretIAM{Service: iam.New(neverAuthorizer{}, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), iam.NewTenantCache()), secret: secret}
	cfg := config.ServerConfig{Region: "us-east-1", MaxConnections: 16, MaxRequests: 16}
	api, err := buildS3API(ctx, backend, cfg, svc, id, zap.NewNop())
	require.NoError(t, err)

	addr := freeAddr(t)
	go func() { _ = api.ServeMultiPort([]string{addr}) }()
	t.Cleanup(func() { require.NoError(t, api.ShutDown()) })
	waitListening(t, addr)

	do := func(t *testing.T, method string, body []byte, sign bool) (*http.Response, string) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, "http://"+addr+"/photos?policy", bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Origin", origin)
		if sign {
			sum := sha256.Sum256(body)
			hash := hex.EncodeToString(sum[:])
			req.Header.Set("X-Amz-Content-Sha256", hash)
			_, err = v4.NewSigner().SignHTTP(ctx,
				awsv4.Credentials{AccessKeyID: "policykey", SecretAccessKey: secret},
				req, hash, "s3", cfg.Region, time.Now(),
				[]string{"host", "x-amz-content-sha256", "x-amz-date"},
				func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
			require.NoError(t, err)
		}
		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
		out, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		return res, string(out)
	}

	const doc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::photos/*"]}]}`
	policy := &s3bkt.PolicyDocument{Version: "2012-10-17", Statements: []s3bkt.Statement{
		{Effect: s3bkt.Allow, Principal: s3bkt.Everyone(), Actions: s3bkt.NewStringSet("s3:GetObject"), Resources: s3bkt.NewStringSet("arn:aws:s3:::photos/*")},
	}}

	t.Run("GET answers the document as JSON with its ETag and the bucket CORS headers", func(t *testing.T) {
		f.ok, f.err = &s3bkt.PolicyOK{Policy: policy}, nil
		res, body := do(t, http.MethodGet, nil, true)
		require.Equal(t, http.StatusOK, res.StatusCode, body)
		require.Equal(t, doc, body)
		require.Equal(t, s3bkt.ETag(*policy), res.Header.Get("ETag"))
		require.Equal(t, "application/json", res.Header.Get("Content-Type"))
		require.Equal(t, origin, res.Header.Get("Access-Control-Allow-Origin"))
		require.Equal(t, http.MethodGet, f.req.Method)
	})

	t.Run("PUT forwards the document and answers 204 with the new ETag", func(t *testing.T) {
		f.ok, f.err = &s3bkt.PolicyOK{}, nil
		res, body := do(t, http.MethodPut, []byte(doc), true)
		require.Equal(t, http.StatusNoContent, res.StatusCode, body)
		require.Equal(t, s3bkt.ETag(*policy), res.Header.Get("ETag"))
		require.Equal(t, doc, string(f.body))
	})

	t.Run("DELETE answers 204", func(t *testing.T) {
		f.ok, f.err = &s3bkt.PolicyOK{}, nil
		res, body := do(t, http.MethodDelete, nil, true)
		require.Equal(t, http.StatusNoContent, res.StatusCode, body)
		require.Empty(t, res.Header.Get("ETag"))
	})

	t.Run("an error is the S3 XML error with the request IDs", func(t *testing.T) {
		f.ok, f.err = nil, bucketauthority.ErrNoPolicy
		res, body := do(t, http.MethodGet, nil, true)
		require.Equal(t, http.StatusNotFound, res.StatusCode, body)
		require.Equal(t, "application/xml", res.Header.Get("Content-Type"))
		require.Contains(t, body, "<Code>NoSuchBucketPolicy</Code>")
		require.Contains(t, body, "<RequestId>"+res.Header.Get("x-amz-request-id")+"</RequestId>")
	})

	t.Run("an anonymous GET never reaches the bucket authority", func(t *testing.T) {
		f.calls = 0
		res, body := do(t, http.MethodGet, nil, false)
		require.Equal(t, http.StatusForbidden, res.StatusCode, body)
		require.Contains(t, body, "<Code>AccessDenied</Code>")
		require.Zero(t, f.calls)
	})

	t.Run("the request has a server span", func(t *testing.T) {
		var names []string
		for _, s := range spans.Ended() {
			names = append(names, s.Name())
		}
		require.Contains(t, names, "s3_GetBucketPolicy")
	})
}
