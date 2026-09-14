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

// signedCopy builds a real SigV4-signed CopyObject (PUT /dst/key with an
// x-amz-copy-source naming src/key). signSource controls whether that header is
// among the signed headers; the SDKs always sign it, so the unsigned variant is
// the hand-rolled request the fast path must refuse.
func signedCopy(t *testing.T, host, dstBucket, srcBucket, accessKeyID, secret string, signSource bool) s3.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, "http://"+host+"/"+dstBucket+"/obj", nil)
	require.NoError(t, err)
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	source := "/" + srcBucket + "/obj"
	signed := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	if signSource {
		// The signer covers every x-amz-* header present, so the header is
		// set before signing only when it is meant to be signed.
		req.Header.Set("X-Amz-Copy-Source", source)
		signed = append(signed, "x-amz-copy-source")
	}
	_, err = v4.NewSigner().SignHTTP(context.Background(),
		awsv4.Credentials{AccessKeyID: accessKeyID, SecretAccessKey: secret},
		req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", time.Now(), signed,
		func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	require.NoError(t, err)

	headers := map[string]string{"Host": host}
	for k := range req.Header {
		headers[k] = req.Header.Get(k)
	}
	if !signSource {
		// Added after signing: present on the request, absent from SignedHeaders.
		headers["X-Amz-Copy-Source"] = source
	}
	return s3.Request{Method: http.MethodPut, Headers: headers, URL: req.URL.RequestURI()}
}

// mapResolver returns the bucket→space mapping for several buckets.
type mapResolver map[string]*registry.State

func (r mapResolver) Get(_ context.Context, name string) (*registry.State, error) {
	if st, ok := r[name]; ok {
		return st, nil
	}
	return nil, registry.ErrNotFound
}

// commandChains mints the RFC chain space→tenant→accessKey→agent for each
// command, all subject = space.
func commandChains(t *testing.T, spaceIssuer, accessKey, agent ucan.Issuer, cmds []ucan.Command) []ucan.Delegation {
	t.Helper()
	space := spaceIssuer.DID()
	tenant, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	var out []ucan.Delegation
	for _, cmd := range cmds {
		root, err := delegation.Delegate(spaceIssuer, tenant.DID(), space, cmd, delegation.WithNoExpiration())
		require.NoError(t, err)
		mid, err := delegation.Delegate(tenant, accessKey.DID(), space, cmd, delegation.WithNoExpiration())
		require.NoError(t, err)
		leaf, err := delegation.Delegate(accessKey, agent.DID(), space, cmd, delegation.WithNoExpiration())
		require.NoError(t, err)
		out = append(out, root, mid, leaf)
	}
	return out
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
func localService(agent did.DID, r BucketResolver) *Service {
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

	t.Run("all cached: authorized locally", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.proofs.For(accessKey.DID()).PutPermissions(space, time.Hour, []string{"s3:GetObject"})
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)

		acct, ok, err := authLocal(s)
		require.NoError(t, err)
		require.True(t, ok, "should authorize without Hilt")
		require.Equal(t, accessKeyID, acct.Access)
		require.Equal(t, key, acct.SigningKey)
	})

	t.Run("no cached key: fall through", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.proofs.For(accessKey.DID()).PutPermissions(space, time.Hour, []string{"s3:GetObject"})
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)
		_, ok, err := authLocal(s)
		require.NoError(t, err)
		require.False(t, ok)
	})

	// The permission check is Hilt's own, mirrored: a chain covering the
	// commands is necessary but not sufficient, since several permissions map
	// to the same commands.
	t.Run("no cached action set: fall through", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)
		_, ok, err := authLocal(s)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("action outside the cached set: refused without Hilt", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		// ListBucket maps to the same retrieve command as GetObject.
		s.proofs.For(accessKey.DID()).PutPermissions(space, time.Hour, []string{"s3:ListBucket"})
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)
		_, ok, err := authLocal(s)
		require.False(t, ok)
		require.Equal(t, s3err.GetAPIError(s3err.ErrAccessDenied), err)
	})

	t.Run("empty cached set: refused", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.proofs.For(accessKey.DID()).PutPermissions(space, time.Hour, []string{})
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)
		_, ok, err := authLocal(s)
		require.False(t, ok)
		require.Equal(t, s3err.GetAPIError(s3err.ErrAccessDenied), err)
	})

	t.Run("bad signature: fall through", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: []byte("wrong-key-32-bytes-......xxxxxxx")})
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)
		_, ok, err := authLocal(s)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("missing chain: fall through", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.proofs.For(accessKey.DID()).PutPermissions(space, time.Hour, []string{"s3:GetObject"})
		// No delegations cached for this key.
		_, ok, err := authLocal(s)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("unknown bucket: fall through", func(t *testing.T) {
		s := localService(agent.DID(), fixedResolver{name: "other"})
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.proofs.For(accessKey.DID()).PutPermissions(space, time.Hour, []string{"s3:GetObject"})
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)
		_, ok, err := authLocal(s)
		require.NoError(t, err)
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
		s.proofs.For(accessKey.DID()).PutPermissions(space, time.Hour, []string{"s3:GetObject"})
		s.proofs.Deposit(otherKey.DID(), retrieveChain(t, spaceIssuer, otherKey, agent)...)

		_, ok, err := authLocal(s)
		require.NoError(t, err)
		require.False(t, ok, "must not see another key's store")
	})
}

// TestAuthorizeLocalCopy: a copy is authorized locally only when THIS key's
// store covers the destination's write commands over the destination's space
// AND the source's read over the source's space, and only when the header
// naming the source is signed — the checks Hilt makes, mirrored.
func TestAuthorizeLocalCopy(t *testing.T) {
	accessKey, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	dstIssuer, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	srcIssuer, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	agent, err := ed25519.GenerateIssuer()
	require.NoError(t, err)

	accessKeyID := accessKey.DID().Identifier()
	const secret = "test-secret-access-key"
	resolver := mapResolver{
		"dst": &registry.State{Name: "dst", Space: dstIssuer.DID()},
		"src": &registry.State{Name: "src", Space: srcIssuer.DID()},
	}
	putCmds := s3perm.CommandsFor("s3:PutObject")
	getCmds := s3perm.CommandsFor("s3:GetObject")

	readWrite := []string{"s3:PutObject", "s3:GetObject"}
	writeOnly := []string{"s3:PutObject"}

	// service with the key and permissions cached for req, plus the given
	// chains deposited.
	service := func(t *testing.T, req s3.Request, perms []string, chains ...[]ucan.Delegation) *Service {
		t.Helper()
		sr, err := sigv4.Parse(sigv4.Request{Method: req.Method, Headers: req.Headers, URL: req.URL})
		require.NoError(t, err)
		key, err := sigv4.DeriveKey(sr, secret)
		require.NoError(t, err)
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		for _, space := range []did.DID{dstIssuer.DID(), srcIssuer.DID()} {
			s.proofs.For(accessKey.DID()).PutPermissions(space, time.Hour, perms)
		}
		for _, c := range chains {
			s.proofs.Deposit(accessKey.DID(), c...)
		}
		return s
	}
	authLocal := func(s *Service, req s3.Request) (bool, error) {
		_, ok, err := s.authorizeLocal(context.Background(), req, accessKeyID, s.proofs.For(accessKey.DID()))
		return ok, err
	}
	authorized := func(t *testing.T, s *Service, req s3.Request) {
		t.Helper()
		ok, err := authLocal(s, req)
		require.NoError(t, err)
		require.True(t, ok)
	}
	fallsThrough := func(t *testing.T, s *Service, req s3.Request) {
		t.Helper()
		ok, err := authLocal(s, req)
		require.NoError(t, err)
		require.False(t, ok)
	}
	refused := func(t *testing.T, s *Service, req s3.Request) {
		t.Helper()
		ok, err := authLocal(s, req)
		require.False(t, ok)
		require.Equal(t, s3err.GetAPIError(s3err.ErrAccessDenied), err)
	}

	t.Run("both buckets covered: authorized locally", func(t *testing.T) {
		req := signedCopy(t, "s3.example", "dst", "src", accessKeyID, secret, true)
		s := service(t, req, readWrite,
			commandChains(t, dstIssuer, accessKey, agent, putCmds),
			commandChains(t, srcIssuer, accessKey, agent, getCmds))
		authorized(t, s, req)
	})

	t.Run("destination covered but not the source: fall through", func(t *testing.T) {
		// The destination grant carries /content/retrieve too, but over the
		// destination's space; it must not stand in for the source's.
		req := signedCopy(t, "s3.example", "dst", "src", accessKeyID, secret, true)
		s := service(t, req, readWrite, commandChains(t, dstIssuer, accessKey, agent, putCmds))
		fallsThrough(t, s, req)
	})

	t.Run("source header not signed: fall through", func(t *testing.T) {
		req := signedCopy(t, "s3.example", "dst", "src", accessKeyID, secret, false)
		s := service(t, req, readWrite,
			commandChains(t, dstIssuer, accessKey, agent, putCmds),
			commandChains(t, srcIssuer, accessKey, agent, getCmds))
		fallsThrough(t, s, req)
	})

	t.Run("copy within one bucket needs only that bucket", func(t *testing.T) {
		req := signedCopy(t, "s3.example", "dst", "dst", accessKeyID, secret, true)
		s := service(t, req, readWrite, commandChains(t, dstIssuer, accessKey, agent, putCmds))
		authorized(t, s, req)
	})

	// A PutObject grant's commands include /content/retrieve (a write's
	// cleanup reads), which is all s3:GetObject maps to. Chains alone would
	// therefore let a write-only key read as a copy source; the cached action
	// set is what refuses it, as Hilt's own check does — and refuses from
	// cache, since the set is Hilt's answer for this key and bucket.
	t.Run("write-only key copying within one bucket: refused", func(t *testing.T) {
		req := signedCopy(t, "s3.example", "dst", "dst", accessKeyID, secret, true)
		s := service(t, req, writeOnly, commandChains(t, dstIssuer, accessKey, agent, putCmds))
		refused(t, s, req)
	})

	t.Run("write-only key with chains on both buckets: refused", func(t *testing.T) {
		req := signedCopy(t, "s3.example", "dst", "src", accessKeyID, secret, true)
		s := service(t, req, writeOnly,
			commandChains(t, dstIssuer, accessKey, agent, putCmds),
			commandChains(t, srcIssuer, accessKey, agent, putCmds))
		refused(t, s, req)
	})

	t.Run("source bucket's set unknown: fall through", func(t *testing.T) {
		// The destination's set is cached (the key wrote there) but the
		// source's is not: Hilt has not yet answered for the source, so it
		// decides.
		req := signedCopy(t, "s3.example", "dst", "src", accessKeyID, secret, true)
		s := service(t, req, nil,
			commandChains(t, dstIssuer, accessKey, agent, putCmds),
			commandChains(t, srcIssuer, accessKey, agent, getCmds))
		s.proofs.For(accessKey.DID()).PutPermissions(dstIssuer.DID(), time.Hour, readWrite)
		fallsThrough(t, s, req)
	})
}

// signedRequest builds a real SigV4-signed request with no body as an
// s3.Request (see signedGet).
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

// TestPutOnlyKeyIsDeniedOnReads pins the defect the action set closes: a key
// granted only s3:PutObject holds chains for every command a read, a listing
// or a multipart abort needs (PutObject's command set is a superset of each),
// so a chain probe alone would authorize them. The cached set refuses them
// without consulting Hilt, and still authorizes the write.
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
	now := time.Now()

	// service with the key cached for req, a PutObject-only set on the
	// bucket, and chains for every PutObject command.
	service := func(t *testing.T, req s3.Request) *Service {
		t.Helper()
		sr, err := sigv4.Parse(sigv4.Request{Method: req.Method, Headers: req.Headers, URL: req.URL})
		require.NoError(t, err)
		key, err := sigv4.DeriveKey(sr, secret)
		require.NoError(t, err)
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.proofs.For(accessKey.DID()).PutPermissions(space, time.Hour, []string{"s3:PutObject"})
		s.proofs.Deposit(accessKey.DID(), commandChains(t, spaceIssuer, accessKey, agent, s3perm.CommandsFor("s3:PutObject"))...)
		return s
	}

	denied := map[string]s3.Request{
		"GetObject":            signedRequest(t, http.MethodGet, "s3.example", "/bkt/obj", accessKeyID, secret, now),
		"ListBucket":           signedRequest(t, http.MethodGet, "s3.example", "/bkt", accessKeyID, secret, now),
		"AbortMultipartUpload": signedRequest(t, http.MethodDelete, "s3.example", "/bkt/obj?uploadId=abc", accessKeyID, secret, now),
	}
	for name, req := range denied {
		t.Run(name+" is refused", func(t *testing.T) {
			op, _, err := hiltauth.RequirementsFor(req)
			require.NoError(t, err)
			require.Equal(t, name, op.String())

			s := service(t, req)
			_, ok, err := s.authorizeLocal(context.Background(), req, accessKeyID, s.proofs.For(accessKey.DID()))
			require.False(t, ok)
			require.Equal(t, s3err.GetAPIError(s3err.ErrAccessDenied), err)
			require.Zero(t, s.authorizer.(*refusingAuthorizer).calls)
		})
	}

	t.Run("PutObject is still authorized", func(t *testing.T) {
		req := signedRequest(t, http.MethodPut, "s3.example", "/bkt/obj", accessKeyID, secret, now)
		s := service(t, req)
		_, ok, err := s.authorizeLocal(context.Background(), req, accessKeyID, s.proofs.For(accessKey.DID()))
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("refused end to end without consulting Hilt", func(t *testing.T) {
		req := denied["GetObject"]
		s := service(t, req)
		app := fiber.New()
		var authErr error
		app.Use(func(c fiber.Ctx) error {
			_, authErr = s.GetUserAccountForRequest(c, accessKeyID)
			return nil
		})
		resp, err := app.Test(httpRequestOf(t, req))
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		var s3e s3err.S3Error
		require.ErrorAs(t, authErr, &s3e)
		require.Equal(t, "AccessDenied", s3e.BaseError().Code)
		require.Equal(t, 403, s3e.StatusCode())
		require.Zero(t, s.authorizer.(*refusingAuthorizer).calls, "the cached set is Hilt's answer; do not ask again")
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
		hiltauth.ForeignBucketErrorName:         {"AccessDenied", 403},
		hiltauth.UnsignedCopySourceErrorName:    {"AccessDenied", 403},
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

	t.Run("cached tenant is stashed without consulting Hilt", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.proofs.For(accessKey.DID()).PutPermissions(spaceIssuer.DID(), time.Hour, []string{"s3:GetObject"})
		s.tenants.Put(accessKeyID, time.Hour, tenant)
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)

		acct, stashed, err := drive(t, s)
		require.NoError(t, err)
		require.Equal(t, key, acct.SigningKey)
		require.Equal(t, tenant, stashed)
		require.Zero(t, s.authorizer.(*refusingAuthorizer).calls)
	})

	t.Run("uncached tenant falls through to Hilt", func(t *testing.T) {
		s := localService(agent.DID(), resolver)
		s.keys.Put(accessKeyID, time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key})
		s.proofs.For(accessKey.DID()).PutPermissions(spaceIssuer.DID(), time.Hour, []string{"s3:GetObject"})
		s.proofs.Deposit(accessKey.DID(), retrieveChain(t, spaceIssuer, accessKey, agent)...)

		_, _, err := drive(t, s)
		require.ErrorContains(t, err, "hilt consulted")
		require.Equal(t, 1, s.authorizer.(*refusingAuthorizer).calls)
	})
}
