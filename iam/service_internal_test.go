package iam

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	awsv4 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/fil-forge/versitygw/auth"
	v4 "github.com/fil-forge/versitygw/aws/signer/v4"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/gofiber/fiber/v3"

	hiltclient "github.com/fil-forge/hilt/pkg/client"
	hiltauth "github.com/fil-forge/hilt/pkg/rpc/service/auth"
	"github.com/fil-forge/hilt/pkg/s3perm"
	"github.com/fil-forge/hilt/pkg/sigv4"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/registry"
	contentcmds "github.com/fil-forge/libforge/commands/content"
	s3 "github.com/fil-forge/libforge/commands/s3"
	s3bkt "github.com/fil-forge/libforge/commands/s3/bucket"
	s3req "github.com/fil-forge/libforge/commands/s3/request"
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
	return signedRequest(t, http.MethodGet, host, rawPath, accessKeyID, secret, when)
}

// signedRequest is signedGet for any method, so the operation classifier sees
// the shapes it distinguishes (a DELETE with ?uploadId is an abort, not an
// object delete).
func signedRequest(t *testing.T, method, host, rawPath, accessKeyID, secret string, when time.Time) s3.Request {
	t.Helper()
	req, err := http.NewRequest(method, "http://"+host+rawPath, nil)
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
	return s3.Request{Method: method, Headers: headers, URL: req.URL.RequestURI()}
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
	return chainFor(t, spaceIssuer, accessKey, agent, contentcmds.Retrieve.Command)
}

// chainFor mints the RFC chain space→tenant→accessKey→agent for each command,
// all subject = space, through one tenant. It stands in for the delegations
// Hilt issues when it grants an access key a permission: the whole command set
// that permission maps to (see hilt/pkg/s3perm).
func chainFor(t *testing.T, spaceIssuer, accessKey, agent ucan.Issuer, cmds ...ucan.Command) []ucan.Delegation {
	t.Helper()
	space := spaceIssuer.DID()
	tenant, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	var dlgs []ucan.Delegation
	for _, cmd := range cmds {
		root, err := delegation.Delegate(spaceIssuer, tenant.DID(), space, cmd, delegation.WithNoExpiration())
		require.NoError(t, err)
		mid, err := delegation.Delegate(tenant, accessKey.DID(), space, cmd, delegation.WithNoExpiration())
		require.NoError(t, err)
		leaf, err := delegation.Delegate(accessKey, agent.DID(), space, cmd, delegation.WithNoExpiration())
		require.NoError(t, err)
		dlgs = append(dlgs, root, mid, leaf)
	}
	return dlgs
}

// localService builds a Service with the fast path enabled and the given
// agent + resolver, plus fresh caches.
func localService(agent did.DID, r fixedResolver) *Service {
	return New(&refusingAuthorizer{}, NewKeyProofs(), NewVerificationKeyCache(), NewTenantCache(),
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
	authLocal := func(s *Service) (auth.Account, bool, error) {
		store := s.proofs.For(accessKey.DID())
		return s.authorizeLocal(context.Background(), req, accessKeyID, store)
	}

	// permit caches the effective action set Hilt would have returned for
	// this key on the bucket. Without it the fast path has no answer and
	// defers to Hilt, so every case that expects a decision seeds one.
	permit := func(s *Service, actions ...string) {
		if actions == nil {
			actions = []string{} // an answer of "nothing", not the absence of one
		}
		s.proofs.For(accessKey.DID()).PutPermissions(space, time.Hour, actions)
	}

	t.Run("all cached: authorized locally", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)
		permit(s, "s3:GetObject")

		acct, ok, err := authLocal(s)
		require.NoError(t, err)
		require.True(t, ok, "should authorize without Hilt")
		require.Equal(t, accessKeyID, acct.Access)
		require.Equal(t, key, acct.SigningKey)
	})

	t.Run("no cached key: fall through", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)
		permit(s, "s3:GetObject")
		_, ok, err := authLocal(s)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("bad signature: fall through", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: []byte("wrong-key-32-bytes-......xxxxxxx")})
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)
		permit(s, "s3:GetObject")
		_, ok, err := authLocal(s)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("missing chain: fall through", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		permit(s, "s3:GetObject")
		// No delegations cached for this key.
		_, ok, err := authLocal(s)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("unknown bucket: fall through", func(t *testing.T) {
		s := localService(agent.DID(), fixedResolver{name: "other"})
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)
		permit(s, "s3:GetObject")
		_, ok, err := authLocal(s)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("no cached action set: fall through", func(t *testing.T) {
		// Everything else checks out; without Hilt's answer on what this
		// key may do here, the request goes to Hilt rather than through.
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)

		_, ok, err := authLocal(s)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("empty action set: denied", func(t *testing.T) {
		// A principal whose policy grants nothing on this bucket. The empty
		// set is an answer, so the request is refused, not re-asked.
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)
		permit(s)

		_, ok, err := authLocal(s)
		require.False(t, ok)
		require.Equal(t, s3err.GetAPIError(s3err.ErrAccessDenied), err)
	})

	t.Run("action outside the set: denied", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)
		permit(s, "s3:DeleteObject")

		_, ok, err := authLocal(s)
		require.False(t, ok)
		require.Equal(t, s3err.GetAPIError(s3err.ErrAccessDenied), err)
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
		permit(s, "s3:GetObject")

		_, ok, err := authLocal(s)
		require.NoError(t, err)
		require.False(t, ok, "must not see another key's store")
	})
}

// TestPutOnlyKeyIsDeniedOnReads pins the escalation the action set closes.
// s3perm maps s3:PutObject to a command set that contains every command
// s3:GetObject, s3:ListBucket and s3:AbortMultipartUpload need, so a key
// granted only s3:PutObject satisfies the fast path's chain probe for all
// three. The cached action set is what refuses them.
func TestPutOnlyKeyIsDeniedOnReads(t *testing.T) {
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

	// The delegations Hilt issues for a key granted only s3:PutObject.
	putCommands := s3perm.CommandsFor("s3:PutObject")
	require.NotEmpty(t, putCommands)
	putChains := chainFor(t, spaceIssuer, accessKey, agent, putCommands...)

	// authorize builds a service holding those chains, the derived key for
	// the request, and the put-only action set, then runs the fast path.
	authorize := func(t *testing.T, req s3.Request) (bool, error) {
		t.Helper()
		sr, err := sigv4.Parse(sigv4.Request{Method: req.Method, Headers: req.Headers, URL: req.URL})
		require.NoError(t, err)
		key, err := sigv4.DeriveKey(sr, secret)
		require.NoError(t, err)

		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.proofs.Deposit(accessKey.DID(), putChains...)
		s.proofs.For(accessKey.DID()).PutPermissions(space, time.Hour, []string{"s3:PutObject"})

		_, ok, err := s.authorizeLocal(context.Background(), req, accessKeyID, s.proofs.For(accessKey.DID()))
		return ok, err
	}

	now := time.Now()
	denied := map[string]s3.Request{
		"GetObject":            signedRequest(t, http.MethodGet, "s3.example", "/bkt/obj", accessKeyID, secret, now),
		"ListBucket":           signedRequest(t, http.MethodGet, "s3.example", "/bkt", accessKeyID, secret, now),
		"AbortMultipartUpload": signedRequest(t, http.MethodDelete, "s3.example", "/bkt/obj?uploadId=abc", accessKeyID, secret, now),
	}
	for name, req := range denied {
		t.Run(name+" is denied", func(t *testing.T) {
			// The chains alone would let it through: the operation's
			// commands are a subset of s3:PutObject's.
			op, err := hiltauth.OperationFor(req)
			require.NoError(t, err)
			require.Equal(t, name, op.String())
			require.Subset(t, commandStrings(putCommands), commandStrings(s3perm.CommandsFor(op.Permission())),
				"the defect only exists while the commands are a subset")

			ok, err := authorize(t, req)
			require.False(t, ok)
			require.Equal(t, s3err.GetAPIError(s3err.ErrAccessDenied), err)
		})
	}

	t.Run("PutObject is still authorized", func(t *testing.T) {
		req := signedRequest(t, http.MethodPut, "s3.example", "/bkt/obj", accessKeyID, secret, now)
		ok, err := authorize(t, req)
		require.NoError(t, err)
		require.True(t, ok)
	})
}

// commandStrings renders commands for set comparison in assertions.
func commandStrings(cmds []ucan.Command) []string {
	out := make([]string, 0, len(cmds))
	for _, c := range cmds {
		out = append(out, c.String())
	}
	return out
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

// refusingAuthorizer fails every call: the fast-path tests use it to prove
// Hilt was not consulted.
type refusingAuthorizer struct{ calls int }

func (a *refusingAuthorizer) AuthorizeRequest(context.Context, s3.Request, ...hiltclient.MethodOption) (*s3req.AuthorizeOK, ucan.Container, error) {
	a.calls++
	return nil, nil, errors.New("hilt consulted")
}

func (a *refusingAuthorizer) BucketInfo(context.Context, string, did.DID, ...hiltclient.MethodOption) (*s3bkt.InfoOK, ucan.Container, error) {
	return nil, nil, errors.New("hilt consulted")
}

// httpRequestOf rebuilds the signed request as an *http.Request so it can be
// driven through fiber exactly as the gateway would see it.
func httpRequestOf(t *testing.T, req s3.Request) *http.Request {
	t.Helper()
	hr := httptest.NewRequest(req.Method, "http://"+req.Headers["Host"]+req.URL, nil)
	for k, v := range req.Headers {
		hr.Header.Set(k, v)
	}
	return hr
}

// TestFastPathTenant covers the tenant half of the local fast path: a request
// that verifies locally is stashed with the cached tenant, and one whose
// tenant is not cached falls through to Hilt rather than proceeding
// tenant-less (the write path would refuse it).
func TestFastPathTenant(t *testing.T) {
	accessKey, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	spaceIssuer, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	agent, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	tenant := did.MustParse("did:plc:ewvi7nxzyoun6zhxrhs64oiz")

	accessKeyID := accessKey.DID().Identifier()
	const secret = "test-secret-access-key"
	resolver := fixedResolver{name: "bkt", state: &registry.State{Name: "bkt", Space: spaceIssuer.DID()}}

	req := signedGet(t, "s3.example", "/bkt/obj", accessKeyID, secret, time.Now())
	sr, err := sigv4.Parse(sigv4.Request{Method: req.Method, Headers: req.Headers, URL: req.URL})
	require.NoError(t, err)
	key, err := sigv4.DeriveKey(sr, secret)
	require.NoError(t, err)

	drive := func(t *testing.T, s *Service) (auth.Account, any, error) {
		t.Helper()
		app := fiber.New()
		var acct auth.Account
		var stashed any
		var authErr error
		app.Use(func(c fiber.Ctx) error {
			acct, authErr = s.GetUserAccountForRequest(c, accessKeyID)
			stashed = c.Locals(reqscope.TenantKey())
			return nil
		})
		resp, err := app.Test(httpRequestOf(t, req))
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return acct, stashed, authErr
	}

	// permit caches the effective action set for this key on the bucket, as
	// an authorize response would.
	permit := func(s *Service, actions ...string) {
		s.proofs.For(accessKey.DID()).PutPermissions(spaceIssuer.DID(), time.Hour, actions)
	}

	t.Run("cached tenant is stashed without consulting Hilt", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.tenants.Put(accessKeyID, time.Hour, tenant)
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)
		permit(s, "s3:GetObject")

		acct, stashed, err := drive(t, s)
		require.NoError(t, err)
		require.Equal(t, key, acct.SigningKey)
		require.Equal(t, tenant, stashed)
		require.Zero(t, s.authorizer.(*refusingAuthorizer).calls)
	})

	t.Run("uncached tenant falls through to Hilt", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)
		permit(s, "s3:GetObject")

		_, _, err := drive(t, s)
		require.ErrorContains(t, err, "hilt consulted")
		require.Equal(t, 1, s.authorizer.(*refusingAuthorizer).calls)
	})

	t.Run("action outside the cached set is refused without consulting Hilt", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.tenants.Put(accessKeyID, time.Hour, tenant)
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)
		permit(s, "s3:PutObject")

		_, stashed, err := drive(t, s)
		// s3err.APIError arrives verbatim so versitygw renders AccessDenied.
		var s3e s3err.S3Error
		require.ErrorAs(t, err, &s3e)
		require.Equal(t, "AccessDenied", s3e.BaseError().Code)
		require.Equal(t, 403, s3e.StatusCode())
		require.Zero(t, s.authorizer.(*refusingAuthorizer).calls, "a cached refusal must not re-ask Hilt")
		require.Nil(t, stashed, "a refused request must not carry a tenant onward")
	})
}
