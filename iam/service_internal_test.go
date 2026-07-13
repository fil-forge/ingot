package iam

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	awsv4 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/fil-forge/versitygw/auth"
	v4 "github.com/fil-forge/versitygw/aws/signer/v4"
	"github.com/fil-forge/versitygw/s3err"

	hiltauth "github.com/fil-forge/hilt/pkg/rpc/service/auth"
	"github.com/fil-forge/hilt/pkg/sigv4"
	"github.com/fil-forge/ingot/registry"
	contentcmds "github.com/fil-forge/libforge/commands/content"
	s3 "github.com/fil-forge/libforge/commands/s3"
	"github.com/fil-forge/ucantone/did"
	ucanerrors "github.com/fil-forge/ucantone/errors"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/stretchr/testify/require"
)

// signedGet builds a real SigV4-signed GET as an s3.Request, signed with the
// fork's vendored signer (the same one versitygw verifies with) so hilt's
// sigv4.VerifyWithKey accepts it.
func signedGet(t *testing.T, host, rawPath, accessKeyID, secret string, when time.Time) s3.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+host+rawPath, nil)
	require.NoError(t, err)
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

	_, err = v4.NewSigner().SignHTTP(context.Background(),
		awsv4.Credentials{AccessKeyID: accessKeyID, SecretAccessKey: secret},
		req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", when,
		[]string{"host", "x-amz-content-sha256", "x-amz-date"},
		func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	require.NoError(t, err)

	headers := map[string]string{"Host": host}
	for k := range req.Header {
		headers[k] = req.Header.Get(k)
	}
	return s3.Request{Method: http.MethodGet, Headers: headers, URL: req.URL.RequestURI()}
}

// fixedResolver returns one bucket→space mapping (or not-found for others).
type fixedResolver struct {
	name  string
	state *registry.State
}

func (r fixedResolver) Get(_ context.Context, name string) (*registry.State, error) {
	if name == r.name {
		return r.state, nil
	}
	return nil, registry.ErrNotFound
}

// retrieveChain mints the RFC chain space→tenant→accessKey→agent for
// /content/retrieve, all subject = space; returns the delegations.
func retrieveChain(t *testing.T, spaceIssuer, accessKey, agent ucan.Issuer) []ucan.Delegation {
	t.Helper()
	space := spaceIssuer.DID()
	tenant, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	root, err := contentcmds.Retrieve.Delegate(spaceIssuer, tenant.DID(), space, delegation.WithNoExpiration())
	require.NoError(t, err)
	mid, err := contentcmds.Retrieve.Delegate(tenant, accessKey.DID(), space, delegation.WithNoExpiration())
	require.NoError(t, err)
	leaf, err := contentcmds.Retrieve.Delegate(accessKey, agent.DID(), space, delegation.WithNoExpiration())
	require.NoError(t, err)
	return []ucan.Delegation{root, mid, leaf}
}

// localService builds a Service with the fast path enabled and the given
// agent + resolver, plus fresh caches.
func localService(agent did.DID, r fixedResolver) *Service {
	return New(nil, NewKeyProofs(), NewVerificationKeyCache(),
		WithLocalAuthorization(agent, r))
}

func TestAuthorizeLocal(t *testing.T) {
	// Access key = the request signer; bucket space = a distinct issuer;
	// agent = this instance. accessKeyID is the did:key identifier.
	accessKey, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	spaceIssuer, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	agent, err := ed25519.GenerateIssuer()
	require.NoError(t, err)

	accessKeyID := accessKey.DID().Identifier()
	const secret = "test-secret-access-key"
	space := spaceIssuer.DID()
	resolver := fixedResolver{name: "bkt", state: &registry.State{Name: "bkt", Space: space}}

	// A real signed GET /bkt/obj, and the key Hilt would derive for it.
	req := signedGet(t, "s3.example", "/bkt/obj", accessKeyID, secret, time.Now())
	sr, err := sigv4.Parse(sigv4.Request{Method: req.Method, Headers: req.Headers, URL: req.URL})
	require.NoError(t, err)
	key, err := sigv4.DeriveKey(sr, secret)
	require.NoError(t, err)

	// authLocal mirrors GetUserAccountForRequest: resolve the requesting
	// key's own store and hand it to the fast path.
	authLocal := func(s *Service) (auth.Account, bool) {
		store := s.proofs.For(accessKey.DID())
		return s.authroizeLocal(context.Background(), req, accessKeyID, store)
	}

	t.Run("all cached: authorized locally", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)

		acct, ok := authLocal(s)
		require.True(t, ok, "should authorize without Hilt")
		require.Equal(t, accessKeyID, acct.Access)
		require.Equal(t, key, acct.SigningKey)
	})

	t.Run("no cached key: fall through", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)
		_, ok := authLocal(s)
		require.False(t, ok)
	})

	t.Run("bad signature: fall through", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: []byte("wrong-key-32-bytes-......xxxxxxx")})
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)
		_, ok := authLocal(s)
		require.False(t, ok)
	})

	t.Run("missing chain: fall through", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		// No delegations cached for this key.
		_, ok := authLocal(s)
		require.False(t, ok)
	})

	t.Run("unknown bucket: fall through", func(t *testing.T) {
		s := localService(agent.DID(), fixedResolver{name: "other"})
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)
		_, ok := authLocal(s)
		require.False(t, ok)
	})

	// Isolation: a full chain deposited under a DIFFERENT access key (even
	// one that reaches the same agent + space) is invisible to this key's
	// store, so the fast path refuses. Per-key stores make riding another
	// key's chain structurally impossible.
	t.Run("another key's chain does not authorize", func(t *testing.T) {
		otherKey, err := ed25519.GenerateIssuer()
		require.NoError(t, err)
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.proofs.Deposit(otherKey.DID(), retrieveChain(t, spaceIssuer, otherKey, agent)...)

		_, ok := authLocal(s)
		require.False(t, ok, "must not see another key's store")
	})
}

// hiltErr reproduces the wrapping depth a real authorize failure arrives with:
// binding.Unpack wraps the decoded ErrorModel, the Hilt client wraps that, and
// GetUserAccountForRequest wraps once more — mapAuthError must see through all
// of it via errors.As.
func hiltErr(name string) error {
	base := ucanerrors.New(name, "rejected by hilt")
	return fmt.Errorf("hilt/iam: authorize request: %w",
		fmt.Errorf("unpacking result: %w",
			fmt.Errorf("executing invocation: %w", base)))
}

func TestMapAuthError(t *testing.T) {
	t.Run("non-named error is not mapped (500-class)", func(t *testing.T) {
		// Transport/internal failures are not ucantone Named errors; the caller
		// wraps them into an InternalError rather than a misleading auth code.
		_, ok := mapAuthError(fmt.Errorf("dial: %w", errors.New("connection refused")))
		require.False(t, ok)
	})

	t.Run("named non-auth error is not mapped", func(t *testing.T) {
		// Being a ucantone Named error does not make it an authorization
		// rejection — an unrecognized name is left 500-class, not forced to a
		// misleading auth code.
		_, ok := mapAuthError(hiltErr("SomeUnrelatedNamedError"))
		require.False(t, ok)
	})

	// These collapse onto auth.ErrNoSuchUser, which versitygw special-cases
	// into InvalidAccessKeyId(access).
	for _, name := range []string{
		hiltauth.UnknownAccessKeyErrorName,
		hiltauth.InvalidAccessKeyIDErrorName,
		hiltauth.AccessKeyExpiredErrorName,
	} {
		t.Run(name+"_to_NoSuchUser", func(t *testing.T) {
			got, ok := mapAuthError(hiltErr(name))
			require.True(t, ok)
			require.ErrorIs(t, got, auth.ErrNoSuchUser)
		})
	}

	// Named rejections mapped to a concrete S3 error: assert both the wire code
	// and HTTP status.
	apiCases := map[string]struct {
		code   string
		status int
	}{
		hiltauth.MalformedSignatureErrorName:    {"AuthorizationHeaderMalformed", 400},
		hiltauth.SignatureMismatchErrorName:     {"SignatureDoesNotMatch", 403},
		hiltauth.SignatureExpiredErrorName:      {"AccessDenied", 403},
		hiltauth.UnsupportedOperationErrorName:  {"NotImplemented", 501},
		hiltauth.UnknownBucketErrorName:         {"NoSuchBucket", 404},
		hiltauth.TenantDisabledErrorName:        {"AccessDenied", 403},
		hiltauth.IssuerForbiddenErrorName:       {"AccessDenied", 403},
		hiltauth.RegionNotServedErrorName:       {"AccessDenied", 403},
		hiltauth.OperationNotPermittedErrorName: {"AccessDenied", 403},
		hiltauth.BucketNotPermittedErrorName:    {"AccessDenied", 403},
	}
	for name, want := range apiCases {
		t.Run(name, func(t *testing.T) {
			got, ok := mapAuthError(hiltErr(name))
			require.True(t, ok)
			var s3e s3err.S3Error
			require.ErrorAs(t, got, &s3e)
			require.Equal(t, want.code, s3e.BaseError().Code)
			require.Equal(t, want.status, s3e.StatusCode())
		})
	}
}
