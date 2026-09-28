package ingot

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	hiltauth "github.com/fil-forge/hilt/pkg/rpc/service/auth"
	"github.com/fil-forge/ingot/bucketauthority"
	s3 "github.com/fil-forge/libforge/commands/s3"
	s3bkt "github.com/fil-forge/libforge/commands/s3/bucket"
	"github.com/fil-forge/ucantone/did"
	ucanerrors "github.com/fil-forge/ucantone/errors"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
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
	h := policyHandler(authority, zap.NewNop())
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
		}, `{"statement":[]}`)
		require.Equal(t, http.StatusNoContent, res.StatusCode, body)
		require.Equal(t, `"bafy..."`, res.Header.Get("ETag"))
		require.Equal(t, "PUT", f.req.Method)
		require.Equal(t, "/photos?policy", f.req.URL)
		require.Equal(t, `"old"`, f.req.Headers["If-Match"])
		require.Contains(t, f.req.Headers["Authorization"], "SignedHeaders=host;if-match")
		require.Equal(t, `{"statement":[]}`, string(f.body))
	})

	t.Run("GET ?policy answers 200 with the document and its ETag", func(t *testing.T) {
		f := &fakeAuthority{ok: &s3bkt.PolicyOK{ETag: `"bafy..."`, Policy: []byte(`{"statement":[]}`)}}
		res, body := do(t, policyApp(f), http.MethodGet, "/photos?policy", nil, "")
		require.Equal(t, http.StatusOK, res.StatusCode)
		require.Equal(t, `"bafy..."`, res.Header.Get("ETag"))
		require.Contains(t, res.Header.Get("Content-Type"), "application/json")
		require.Equal(t, `{"statement":[]}`, body)
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
