package client_test

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"

	hiltclient "github.com/fil-forge/ingot/hilt/client"
	s3 "github.com/fil-forge/libforge/commands/s3"
	s3bkt "github.com/fil-forge/libforge/commands/s3/bucket"
	s3req "github.com/fil-forge/libforge/commands/s3/request"
	"github.com/fil-forge/libforge/testutil"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/binding"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/server"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

// newHiltClient builds a Client whose transport is the given in-process server,
// exercising New itself.
func newHiltClient(t *testing.T, hilt ucan.Issuer, srv *server.HTTPServer, issuer ucan.Issuer, proofs ucanlib.ProofStore) *hiltclient.Client {
	t.Helper()
	u, err := url.Parse("http://hilt.test")
	require.NoError(t, err)
	c, err := hiltclient.New(hilt.DID(), *u, issuer, proofs, hiltclient.WithHTTPClient(&http.Client{Transport: srv}))
	require.NoError(t, err)
	return c
}

// rootProofs returns a proof store holding a root delegation from hilt to issuer
// for cmd (subject == issuer == hilt), authorizing issuer to invoke cmd on hilt.
func rootProofs[A, O binding.CBORValue](t *testing.T, cmd binding.Binding[A, O], hilt ucan.Issuer, issuer did.DID) ucanlib.ProofStore {
	t.Helper()
	dlg, err := cmd.Delegate(hilt, issuer, hilt.DID())
	require.NoError(t, err)
	return ucanlib.NewContainerProofStore(container.New(container.WithDelegations(dlg)))
}

// errProofStore is a ProofStore whose ProofChain always fails.
type errProofStore struct{ err error }

func (e errProofStore) ProofChain(ctx context.Context, aud did.DID, cmd ucan.Command, sub did.DID) ([]ucan.Delegation, []cid.Cid, error) {
	return nil, nil, e.err
}

// errRoundTripper is an http.RoundTripper that always fails, forcing Execute to
// return an error.
type errRoundTripper struct{}

func (errRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("transport boom")
}

func TestClientAuthorizeRequest(t *testing.T) {
	hilt := testutil.RandomIssuer(t)
	ingot := testutil.RandomIssuer(t)
	bucketDID := testutil.RandomDID(t)

	// A delegation Hilt attaches to the response for the caller to extract.
	attached, err := s3req.Authorize.Delegate(hilt, ingot.DID(), hilt.DID())
	require.NoError(t, err)

	var gotSub, gotAud did.DID
	srv := server.NewHTTP(hilt)
	srv.Handle(s3req.Authorize.Command, s3req.Authorize.Handler(
		func(req *binding.Request[*s3req.AuthorizeArguments], res *binding.Response[*s3req.AuthorizeOK]) error {
			gotSub, gotAud = req.Invocation().Subject(), req.Invocation().Audience()
			if err := res.SetMetadata(container.New(container.WithDelegations(attached))); err != nil {
				return err
			}
			return res.SetSuccess(&s3req.AuthorizeOK{Bucket: bucketDID})
		}))

	c := newHiltClient(t, hilt, srv, ingot, rootProofs(t, s3req.Authorize, hilt, ingot.DID()))
	ok, ctr, err := c.AuthorizeRequest(t.Context(), s3.Request{Method: "GET", URL: "https://s3.fil.one/bucket/key"})
	require.NoError(t, err)

	require.Equal(t, bucketDID, ok.Bucket)
	require.Equal(t, hilt.DID(), gotSub)
	require.Equal(t, hilt.DID(), gotAud)
	_, found := ctr.Delegation(attached.Link())
	require.True(t, found, "response container should carry the attached delegation")
}

func TestClientCreateBucket(t *testing.T) {
	hilt := testutil.RandomIssuer(t)
	ingot := testutil.RandomIssuer(t)
	bucketDID := testutil.RandomDID(t)

	attached, err := s3bkt.Create.Delegate(hilt, ingot.DID(), hilt.DID())
	require.NoError(t, err)

	srv := server.NewHTTP(hilt)
	srv.Handle(s3bkt.Create.Command, s3bkt.Create.Handler(
		func(req *binding.Request[*s3bkt.CreateArguments], res *binding.Response[*s3req.AuthorizeOK]) error {
			if err := res.SetMetadata(container.New(container.WithDelegations(attached))); err != nil {
				return err
			}
			return res.SetSuccess(&s3req.AuthorizeOK{Bucket: bucketDID})
		}))

	c := newHiltClient(t, hilt, srv, ingot, rootProofs(t, s3bkt.Create, hilt, ingot.DID()))
	ok, ctr, err := c.CreateBucket(t.Context(), s3.Request{Method: "PUT", URL: "https://s3.fil.one/bucket"})
	require.NoError(t, err)

	require.Equal(t, bucketDID, ok.Bucket)
	_, found := ctr.Delegation(attached.Link())
	require.True(t, found)
}

func TestClientBucketInfo(t *testing.T) {
	hilt := testutil.RandomIssuer(t)
	ingot := testutil.RandomIssuer(t)
	bucketDID := testutil.RandomDID(t)
	akDID := testutil.RandomDID(t)

	attached, err := s3bkt.Info.Delegate(hilt, ingot.DID(), hilt.DID())
	require.NoError(t, err)

	var gotArgs *s3bkt.InfoArguments
	srv := server.NewHTTP(hilt)
	srv.Handle(s3bkt.Info.Command, s3bkt.Info.Handler(
		func(req *binding.Request[*s3bkt.InfoArguments], res *binding.Response[*s3bkt.InfoOK]) error {
			gotArgs = req.Task().Arguments()
			if err := res.SetMetadata(container.New(container.WithDelegations(attached))); err != nil {
				return err
			}
			return res.SetSuccess(&s3bkt.InfoOK{ID: bucketDID})
		}))

	c := newHiltClient(t, hilt, srv, ingot, rootProofs(t, s3bkt.Info, hilt, ingot.DID()))
	ok, ctr, err := c.BucketInfo(t.Context(), "mybucket", akDID)
	require.NoError(t, err)

	require.Equal(t, bucketDID, ok.ID)
	require.Equal(t, "mybucket", gotArgs.Name)
	require.Equal(t, akDID, gotArgs.AccessKey)
	_, found := ctr.Delegation(attached.Link())
	require.True(t, found)
}

func TestClientListBuckets(t *testing.T) {
	hilt := testutil.RandomIssuer(t)
	ingot := testutil.RandomIssuer(t)

	srv := server.NewHTTP(hilt)
	srv.Handle(s3bkt.List.Command, s3bkt.List.Handler(
		func(req *binding.Request[*s3bkt.ListArguments], res *binding.Response[*s3bkt.ListOK]) error {
			return res.SetSuccess(&s3bkt.ListOK{Owner: s3bkt.Owner{DisplayName: "Acme"}})
		}))

	c := newHiltClient(t, hilt, srv, ingot, rootProofs(t, s3bkt.List, hilt, ingot.DID()))
	ok, err := c.ListBuckets(t.Context(), s3.Request{Method: "GET", URL: "https://us-west-2.s3.fil.one/"})
	require.NoError(t, err)
	require.Equal(t, "Acme", ok.Owner.DisplayName)
}

func TestClientDeleteBucket(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		hilt := testutil.RandomIssuer(t)
		ingot := testutil.RandomIssuer(t)

		srv := server.NewHTTP(hilt)
		srv.Handle(s3bkt.Delete.Command, s3bkt.Delete.Handler(
			func(req *binding.Request[*s3bkt.DeleteArguments], res *binding.Response[*s3bkt.DeleteOK]) error {
				return res.SetSuccess(&s3bkt.DeleteOK{})
			}))

		c := newHiltClient(t, hilt, srv, ingot, rootProofs(t, s3bkt.Delete, hilt, ingot.DID()))
		require.NoError(t, c.DeleteBucket(t.Context(), s3.Request{Method: "DELETE", URL: "https://s3.fil.one/bucket"}))
	})

	t.Run("failure receipt", func(t *testing.T) {
		hilt := testutil.RandomIssuer(t)
		ingot := testutil.RandomIssuer(t)

		srv := server.NewHTTP(hilt)
		srv.Handle(s3bkt.Delete.Command, s3bkt.Delete.Handler(
			func(req *binding.Request[*s3bkt.DeleteArguments], res *binding.Response[*s3bkt.DeleteOK]) error {
				return res.SetFailure(errors.New("not empty"))
			}))

		c := newHiltClient(t, hilt, srv, ingot, rootProofs(t, s3bkt.Delete, hilt, ingot.DID()))
		require.Error(t, c.DeleteBucket(t.Context(), s3.Request{Method: "DELETE", URL: "https://s3.fil.one/bucket"}))
	})
}

func TestClientErrors(t *testing.T) {
	hilt := testutil.RandomIssuer(t)
	ingot := testutil.RandomIssuer(t)
	req := s3.Request{Method: "GET", URL: "https://s3.fil.one/bucket/key"}

	t.Run("proof chain error", func(t *testing.T) {
		srv := server.NewHTTP(hilt)
		c := newHiltClient(t, hilt, srv, ingot, nil)
		_, _, err := c.AuthorizeRequest(t.Context(), req, hiltclient.WithProofs(errProofStore{err: errors.New("boom")}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "getting proof chain")
	})

	t.Run("execution error", func(t *testing.T) {
		u, err := url.Parse("http://hilt.test")
		require.NoError(t, err)
		c, err := hiltclient.New(hilt.DID(), *u, ingot, rootProofs(t, s3req.Authorize, hilt, ingot.DID()),
			hiltclient.WithHTTPClient(&http.Client{Transport: errRoundTripper{}}))
		require.NoError(t, err)
		_, _, err = c.AuthorizeRequest(t.Context(), req)
		require.Error(t, err)
	})
}
