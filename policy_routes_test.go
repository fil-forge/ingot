package ingot

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	hiltauth "github.com/fil-forge/hilt/pkg/rpc/service/auth"
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
	ucanerrors "github.com/fil-forge/ucantone/errors"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap"
)

// fakeAuthority answers BucketPolicy with a canned result or error and
// records what it was given.
type fakeAuthority struct {
	bucketauthority.BucketAuthority
	ok   *s3bkt.PolicyOK
	err  error
	req  s3.Request
	body []byte
}

func (f *fakeAuthority) BucketPolicy(_ context.Context, req s3.Request, body []byte) (*s3bkt.PolicyOK, error) {
	f.req, f.body = req, body
	return f.ok, f.err
}

// policyApp mounts the policy handler on the bucket path the way buildS3API
// does, ahead of a stand-in for the S3 route table.
func policyApp(authority bucketauthority.BucketAuthority) *fiber.App {
	app := fiber.New()
	h := policyHandler(nil, authority, zap.NewNop())
	app.Get("/:bucket", h)
	app.Put("/:bucket", h)
	app.Delete("/:bucket", h)
	app.Add([]string{http.MethodGet, http.MethodPut, http.MethodDelete}, "/:bucket", func(c fiber.Ctx) error {
		return c.Status(http.StatusTeapot).SendString("S3 route table")
	})
	return app
}

func TestPolicyRoutes(t *testing.T) {
	do := func(t *testing.T, app *fiber.App, method, target string, headers map[string]string, body string) (*http.Response, string) {
		t.Helper()
		req := httptest.NewRequest(method, target, strings.NewReader(body))
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		res, err := app.Test(req)
		require.NoError(t, err)
		defer res.Body.Close()
		out, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		return res, string(out)
	}

	t.Run("PUT ?policy forwards the signed request and body and answers 204 with the ETag", func(t *testing.T) {
		f := &fakeAuthority{ok: &s3bkt.PolicyOK{ETag: `"bafy..."`}}
		res, body := do(t, policyApp(f), http.MethodPut, "/photos?policy", map[string]string{
			"Authorization": "AWS4-HMAC-SHA256 Credential=k/20260928/us/s3/aws4_request, SignedHeaders=host;if-match, Signature=abc",
			"If-Match":      `"old"`,
		}, `{"Statement":[]}`)
		require.Equal(t, http.StatusNoContent, res.StatusCode, body)
		require.Equal(t, `"bafy..."`, res.Header.Get("ETag"))
		require.Equal(t, "PUT", f.req.Method)
		require.Equal(t, "/photos?policy", f.req.URL)
		require.Equal(t, `"old"`, f.req.Headers["If-Match"])
		require.Contains(t, f.req.Headers["Authorization"], "SignedHeaders=host;if-match")
		require.Equal(t, `{"Statement":[]}`, string(f.body))
	})

	t.Run("GET ?policy answers 200 with the document and its ETag", func(t *testing.T) {
		f := &fakeAuthority{ok: &s3bkt.PolicyOK{ETag: `"bafy..."`, Policy: []byte(`{"Statement":[]}`)}}
		res, body := do(t, policyApp(f), http.MethodGet, "/photos?policy", nil, "")
		require.Equal(t, http.StatusOK, res.StatusCode)
		require.Equal(t, `"bafy..."`, res.Header.Get("ETag"))
		require.Contains(t, res.Header.Get("Content-Type"), "application/json")
		require.Equal(t, `{"Statement":[]}`, body)
	})

	t.Run("DELETE ?policy answers 204", func(t *testing.T) {
		f := &fakeAuthority{ok: &s3bkt.PolicyOK{}}
		res, _ := do(t, policyApp(f), http.MethodDelete, "/photos?policy", nil, "")
		require.Equal(t, http.StatusNoContent, res.StatusCode)
		require.Empty(t, res.Header.Get("ETag"))
	})

	t.Run("a bucket request without ?policy falls through to the S3 route table", func(t *testing.T) {
		f := &fakeAuthority{err: errors.New("must not be called")}
		for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
			res, body := do(t, policyApp(f), method, "/photos", nil, "")
			require.Equal(t, http.StatusTeapot, res.StatusCode, method)
			require.Equal(t, "S3 route table", body)
		}
		res, _ := do(t, policyApp(f), http.MethodGet, "/photos?versioning", nil, "")
		require.Equal(t, http.StatusTeapot, res.StatusCode)
	})

	t.Run("each rejection renders its S3 error code and status", func(t *testing.T) {
		for _, tc := range []struct {
			err    error
			code   string
			status int
		}{
			{bucketauthority.ErrNotFound, "NoSuchBucket", 404},
			{bucketauthority.ErrNoPolicy, "NoSuchBucketPolicy", 404},
			{errors.Join(bucketauthority.ErrMalformedPolicy, errors.New("statement 0: unknown principal")), "MalformedPolicy", 400},
			{bucketauthority.ErrPreconditionFailed, "PreconditionFailed", 412},
			{bucketauthority.ErrInvalidPrecondition, "InvalidRequest", 400},
			{bucketauthority.ErrConcurrentChange, "OperationAborted", 409},
			{bucketauthority.ErrUnsupported, "NotImplemented", 501},
			{ucanerrors.New(hiltauth.OperationNotPermittedErrorName, "no"), "AccessDenied", 403},
			{ucanerrors.New(hiltauth.SignatureMismatchErrorName, "no"), "SignatureDoesNotMatch", 403},
			{ucanerrors.New(hiltauth.ForeignBucketErrorName, "no"), "AccessDenied", 403},
			{ucanerrors.New(hiltauth.UnknownAccessKeyErrorName, "no"), "InvalidAccessKeyId", 403},
			{ucanerrors.New(hiltauth.AccessKeyExpiredErrorName, "no"), "InvalidAccessKeyId", 403},
			{errors.New("boom"), "InternalError", 500},
		} {
			f := &fakeAuthority{err: tc.err}
			res, body := do(t, policyApp(f), http.MethodPut, "/photos?policy", nil, "{}")
			require.Equal(t, tc.status, res.StatusCode, tc.code)
			require.Contains(t, res.Header.Get("Content-Type"), "application/xml")
			require.Contains(t, body, "<Code>"+tc.code+"</Code>", tc.code)
		}
	})

	t.Run("no authority answers NotImplemented", func(t *testing.T) {
		res, body := do(t, policyApp(nil), http.MethodGet, "/photos?policy", nil, "")
		require.Equal(t, http.StatusNotImplemented, res.StatusCode)
		require.Contains(t, body, "<Code>NotImplemented</Code>")
	})
}

var _ = did.Undef

// TestBuildS3API_PolicyRoutes drives the policy routes through the real
// versitygw wiring, where they mount ahead of the S3 route table and its
// middleware: the response must still carry the bucket CORS headers, an
// error body the request IDs already in the headers, and the request a
// server span.
func TestBuildS3API_PolicyRoutes(t *testing.T) {
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
	backend := s3frontend.New(s3frontend.Deps{Registry: mem, CORS: corsCfg})

	id, err := identity.New("", "did:web:ingot.test")
	require.NoError(t, err)
	svc := iam.New(neverAuthorizer{}, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), iam.NewTenantCache())
	f := &fakeAuthority{err: bucketauthority.ErrNoPolicy}
	cfg := config.ServerConfig{Region: "us-east-1", MaxConnections: 16, MaxRequests: 16}
	api, err := buildS3API(ctx, backend, cfg, svc, id, f, zap.NewNop())
	require.NoError(t, err)

	addr := freeAddr(t)
	go func() { _ = api.ServeMultiPort([]string{addr}) }()
	t.Cleanup(func() { require.NoError(t, api.ShutDown()) })
	waitListening(t, addr)

	get := func(t *testing.T) (*http.Response, string) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/photos?policy", nil)
		require.NoError(t, err)
		req.Header.Set("Origin", origin)
		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		return res, string(body)
	}

	t.Run("an error body carries the request IDs", func(t *testing.T) {
		res, body := get(t)
		require.Equal(t, http.StatusNotFound, res.StatusCode, body)
		reqID := res.Header.Get("x-amz-request-id")
		require.NotEmpty(t, reqID)
		require.Contains(t, body, "<RequestId>"+reqID+"</RequestId>")
		require.Contains(t, body, "<HostId>"+res.Header.Get("x-amz-id-2")+"</HostId>")
	})

	t.Run("the response carries the bucket CORS headers", func(t *testing.T) {
		f.err, f.ok = nil, &s3bkt.PolicyOK{ETag: `"bafy..."`, Policy: []byte(`{}`)}
		res, body := get(t)
		require.Equal(t, http.StatusOK, res.StatusCode, body)
		require.Equal(t, origin, res.Header.Get("Access-Control-Allow-Origin"))
		require.Contains(t, res.Header.Get("Access-Control-Expose-Headers"), "ETag")
	})

	t.Run("the request has a server span", func(t *testing.T) {
		var names []string
		for _, s := range spans.Ended() {
			names = append(names, s.Name())
		}
		require.Contains(t, names, "S3 GET")
	})
}
