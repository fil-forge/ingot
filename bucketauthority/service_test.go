package bucketauthority_test

import (
	"net/http"
	"net/url"
	"testing"

	hiltclient "github.com/fil-forge/hilt/pkg/client"
	"github.com/fil-forge/ingot/bucketauthority"
	s3 "github.com/fil-forge/libforge/commands/s3"
	s3bkt "github.com/fil-forge/libforge/commands/s3/bucket"
	"github.com/fil-forge/libforge/testutil"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/binding"
	ucanerrors "github.com/fil-forge/ucantone/errors"
	"github.com/fil-forge/ucantone/server"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/stretchr/testify/require"
)

// TestBucketPolicy forwards the three policy operations to an in-process Hilt
// and maps its named rejections to the authority's sentinels.
func TestBucketPolicy(t *testing.T) {
	hilt := testutil.RandomIssuer(t)
	ingot := testutil.RandomIssuer(t)

	// newService serves /s3/bucket/policy with handle and returns an authority
	// whose client reaches it in process.
	newService := func(t *testing.T, handle func(args *s3bkt.PolicyArguments) (*s3bkt.PolicyOK, error)) *bucketauthority.Service {
		t.Helper()
		srv := server.NewHTTP(hilt)
		srv.Handle(s3bkt.Policy.Command, s3bkt.Policy.Handler(
			func(req *binding.Request[*s3bkt.PolicyArguments], res *binding.Response[*s3bkt.PolicyOK]) error {
				ok, err := handle(req.Task().Arguments())
				if err != nil {
					return res.SetFailure(err)
				}
				return res.SetSuccess(ok)
			}))
		dlg, err := s3bkt.Policy.Delegate(hilt, ingot.DID(), hilt.DID())
		require.NoError(t, err)
		proofs := ucanlib.NewContainerProofStore(container.New(container.WithDelegations(dlg)))
		u, err := url.Parse("http://hilt.test")
		require.NoError(t, err)
		c, err := hiltclient.New(hilt.DID(), *u, ingot, hiltclient.WithBaseProofs(proofs), hiltclient.WithHTTPClient(&http.Client{Transport: srv}))
		require.NoError(t, err)
		return bucketauthority.New(c)
	}

	req := s3.Request{
		Method:  "PUT",
		URL:     "/photos?policy",
		Headers: map[string]string{"If-Match": `"old"`, "X-Amz-Content-Sha256": "abc", "Authorization": "AWS4-HMAC-SHA256 ..."},
	}
	body := []byte(`{"Statement":[]}`)

	t.Run("forwards the request and body unchanged and returns the result", func(t *testing.T) {
		var got *s3bkt.PolicyArguments
		svc := newService(t, func(args *s3bkt.PolicyArguments) (*s3bkt.PolicyOK, error) {
			got = args
			return &s3bkt.PolicyOK{ETag: `"bafy..."`, Policy: []byte(`{"Statement":[]}`)}, nil
		})
		ok, err := svc.BucketPolicy(t.Context(), req, body)
		require.NoError(t, err)
		require.Equal(t, `"bafy..."`, ok.ETag)
		require.Equal(t, []byte(`{"Statement":[]}`), ok.Policy)
		require.Equal(t, req, got.Request)
		require.Equal(t, body, got.Body)
	})

	t.Run("maps Hilt's named rejections", func(t *testing.T) {
		for name, want := range map[string]error{
			"UnknownBucket":       bucketauthority.ErrNotFound,
			"PolicyNotFound":      bucketauthority.ErrNoPolicy,
			"InvalidBucketPolicy": bucketauthority.ErrMalformedPolicy,
			"PreconditionFailed":  bucketauthority.ErrPreconditionFailed,
			"InvalidPrecondition": bucketauthority.ErrInvalidPrecondition,
			"ConcurrentChange":    bucketauthority.ErrConcurrentChange,
		} {
			svc := newService(t, func(*s3bkt.PolicyArguments) (*s3bkt.PolicyOK, error) {
				return nil, ucanerrors.New(name, "refused: "+name)
			})
			_, err := svc.BucketPolicy(t.Context(), req, body)
			require.ErrorIs(t, err, want, name)
		}
	})

	t.Run("passes an authorization rejection through with its name", func(t *testing.T) {
		svc := newService(t, func(*s3bkt.PolicyArguments) (*s3bkt.PolicyOK, error) {
			return nil, ucanerrors.New("OperationNotPermitted", "a principal-bound key")
		})
		_, err := svc.BucketPolicy(t.Context(), req, body)
		var named ucanerrors.Named
		require.ErrorAs(t, err, &named)
		require.Equal(t, "OperationNotPermitted", named.Name())
	})
}
