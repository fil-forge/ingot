package s3frontend_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	hiltauth "github.com/fil-forge/hilt/pkg/rpc/service/auth"
	s3 "github.com/fil-forge/libforge/commands/s3"
	s3bkt "github.com/fil-forge/libforge/commands/s3/bucket"
	"github.com/fil-forge/ucantone/did"
	ucanerrors "github.com/fil-forge/ucantone/errors"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/bucketauthority"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/s3frontend"
)

// policyAuthority answers PolicyDocument with a canned result or error and
// records what it was given.
type policyAuthority struct {
	bucketauthority.BucketAuthority
	ok    *s3bkt.PolicyOK
	err   error
	calls int
	req   s3.Request
	body  []byte
}

func (f *policyAuthority) PolicyDocument(_ context.Context, req s3.Request, body []byte) (*s3bkt.PolicyOK, error) {
	f.calls++
	f.req, f.body = req, body
	return f.ok, f.err
}

const signedAuth = "AWS4-HMAC-SHA256 Credential=k/20260928/us/s3/aws4_request, SignedHeaders=host, Signature=abc"

// policyCtx stashes req on the context the way the gateway middleware does,
// with the policy document holder the ?policy middleware installs;
// documented reads it.
func policyCtx(req s3.Request) (ctx context.Context, documented *bool) {
	documented = new(bool)
	ctx = context.WithValue(context.Background(), reqscope.RequestKey(), req)
	return context.WithValue(ctx, reqscope.PolicyDocumentKey(), documented), documented
}

func policyBackend(t *testing.T, authority bucketauthority.BucketAuthority) *s3frontend.Backend {
	t.Helper()
	mem := inmem.NewMemStore()
	require.NoError(t, mem.Create(t.Context(), "photos", did.Undef, registry.CreateState{}))
	return s3frontend.New(s3frontend.Deps{Registry: mem, Authority: authority})
}

// requireS3Error asserts err is the S3 error code, as versitygw's
// ProcessController reads it: by type assertion, not errors.As, so anything
// else renders as InternalError.
func requireS3Error(t *testing.T, err error, code string) {
	t.Helper()
	s3e, ok := err.(s3err.S3Error)
	require.True(t, ok, "not an s3err.S3Error: %v", err)
	require.Equal(t, code, s3e.BaseError().Code)
}

func TestBucketPolicy(t *testing.T) {
	t.Run("PutBucketPolicy forwards the signed request and document and marks no document answer", func(t *testing.T) {
		f := &policyAuthority{ok: &s3bkt.PolicyOK{}}
		req := s3.Request{Method: http.MethodPut, URL: "/photos?policy", Headers: map[string]string{"Authorization": signedAuth}}
		ctx, documented := policyCtx(req)
		body := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::photos/*"]}]}`
		require.NoError(t, policyBackend(t, f).PutBucketPolicy(ctx, "photos", []byte(body)))
		require.Equal(t, req, f.req)
		require.Equal(t, body, string(f.body))
		require.False(t, *documented)
	})

	t.Run("PutBucketPolicy forwards an If-Match header untouched and succeeds without checking it", func(t *testing.T) {
		f := &policyAuthority{ok: &s3bkt.PolicyOK{}}
		// The tag matches no stored document: the write still succeeds.
		req := s3.Request{Method: http.MethodPut, URL: "/photos?policy", Headers: map[string]string{"Authorization": signedAuth, "If-Match": `"any-tag"`}}
		ctx, _ := policyCtx(req)
		require.NoError(t, policyBackend(t, f).PutBucketPolicy(ctx, "photos", []byte("{}")))
		require.Equal(t, req, f.req)
	})

	t.Run("GetBucketPolicy forwards a signed GET ?policy, returns the document and marks a document answer", func(t *testing.T) {
		for _, req := range []s3.Request{
			{Method: http.MethodGet, URL: "/photos?policy", Headers: map[string]string{"Authorization": signedAuth}},
			{Method: http.MethodGet, URL: "/photos?policy&X-Amz-Signature=abc", Headers: map[string]string{}},
		} {
			policy := &s3bkt.PolicyDocument{Version: "2012-10-17", Statements: []s3bkt.Statement{
				{Effect: s3bkt.Allow, Principal: s3bkt.Only("alice"), Actions: s3bkt.NewStringSet("s3:GetObject"), Resources: s3bkt.NewStringSet("arn:aws:s3:::photos/*")},
			}}
			f := &policyAuthority{ok: &s3bkt.PolicyOK{Policy: policy}}
			ctx, documented := policyCtx(req)
			doc, err := policyBackend(t, f).GetBucketPolicy(ctx, "photos")
			require.NoError(t, err, req.URL)
			require.Equal(t, `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["arn:aws:iam:::user/alice"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::photos/*"]}]}`, string(doc))
			require.Equal(t, req, f.req)
			require.Nil(t, f.body)
			require.True(t, *documented, req.URL)
		}
	})

	t.Run("DeleteBucketPolicy forwards the signed request without a body", func(t *testing.T) {
		f := &policyAuthority{ok: &s3bkt.PolicyOK{}}
		req := s3.Request{Method: http.MethodDelete, URL: "/photos?policy", Headers: map[string]string{"Authorization": signedAuth}}
		ctx, _ := policyCtx(req)
		require.NoError(t, policyBackend(t, f).DeleteBucketPolicy(ctx, "photos"))
		require.Equal(t, req, f.req)
		require.Nil(t, f.body)
	})

	// versitygw's own access checks call GetBucketPolicy: the anonymous
	// public-bucket check on any request, and VerifyAccess on a PUT or DELETE
	// ?policy. Those must neither reach Hilt nor get a policy to evaluate.
	t.Run("GetBucketPolicy outside a signed GET ?policy answers without Hilt", func(t *testing.T) {
		f := &policyAuthority{err: errors.New("must not be called")}
		be := policyBackend(t, f)
		for _, req := range []s3.Request{
			{Method: http.MethodGet, URL: "/photos?policy", Headers: map[string]string{}},
			{Method: http.MethodGet, URL: "/photos?policyStatus", Headers: map[string]string{"Authorization": signedAuth}},
			{Method: http.MethodGet, URL: "/photos/key", Headers: map[string]string{"Authorization": signedAuth}},
			{Method: http.MethodPut, URL: "/photos?policy", Headers: map[string]string{"Authorization": signedAuth}},
		} {
			ctx, _ := policyCtx(req)
			_, err := be.GetBucketPolicy(ctx, "photos")
			requireS3Error(t, err, "NoSuchBucketPolicy")
			_, err = be.GetBucketPolicy(ctx, "nope")
			requireS3Error(t, err, "NoSuchBucket")
		}
		_, err := be.GetBucketPolicy(context.Background(), "photos")
		requireS3Error(t, err, "NoSuchBucketPolicy")
		require.Zero(t, f.calls)
	})

	t.Run("each rejection is the S3 error versitygw renders", func(t *testing.T) {
		for _, tc := range []struct {
			err  error
			code string
		}{
			{bucketauthority.ErrNotFound, "NoSuchBucket"},
			{bucketauthority.ErrNoPolicy, "NoSuchBucketPolicy"},
			{bucketauthority.ErrMalformedPolicy, "MalformedPolicy"},
			{bucketauthority.ErrUnsupported, "NotImplemented"},
			{ucanerrors.New(hiltauth.OperationNotPermittedErrorName, "no"), "AccessDenied"},
			{ucanerrors.New(hiltauth.SignatureMismatchErrorName, "no"), "SignatureDoesNotMatch"},
			{ucanerrors.New(hiltauth.ForeignBucketErrorName, "no"), "AccessDenied"},
			// A revoked, unknown or expired key.
			{ucanerrors.New(hiltauth.UnknownAccessKeyErrorName, "no"), "InvalidAccessKeyId"},
			{ucanerrors.New(hiltauth.AccessKeyExpiredErrorName, "no"), "InvalidAccessKeyId"},
			{errors.New("boom"), "InternalError"},
		} {
			f := &policyAuthority{err: tc.err}
			ctx, _ := policyCtx(s3.Request{Method: http.MethodPut, URL: "/photos?policy", Headers: map[string]string{"Authorization": signedAuth}})
			err := policyBackend(t, f).PutBucketPolicy(ctx, "photos", []byte("{}"))
			requireS3Error(t, err, tc.code)
		}
	})

	t.Run("no authority answers NotImplemented", func(t *testing.T) {
		be := policyBackend(t, nil)
		ctx, _ := policyCtx(s3.Request{Method: http.MethodGet, URL: "/photos?policy", Headers: map[string]string{"Authorization": signedAuth}})
		_, err := be.GetBucketPolicy(ctx, "photos")
		requireS3Error(t, err, "NotImplemented")
		requireS3Error(t, be.PutBucketPolicy(ctx, "photos", []byte("{}")), "NotImplemented")
		requireS3Error(t, be.DeleteBucketPolicy(ctx, "photos"), "NotImplemented")
	})
}
