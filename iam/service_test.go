package iam_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	s3 "github.com/fil-forge/libforge/commands/s3"
	s3bkt "github.com/fil-forge/libforge/commands/s3/bucket"
	s3req "github.com/fil-forge/libforge/commands/s3/request"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/fil-forge/versitygw/auth"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"

	hiltclient "github.com/fil-forge/hilt/pkg/client"
	"github.com/fil-forge/ingot/iam"
	"github.com/fil-forge/ingot/internal/reqscope"
)

// fakeAuthorizer records the request it authorized and returns canned
// results, standing in for the hilt client.
type fakeAuthorizer struct {
	got  s3.Request
	dlgs []ucan.Delegation // delegations in the authorize response container
	res  *s3req.AuthorizeOK
	err  error

	infoBuckets []string          // BucketInfo calls received, by name
	infoDlgs    []ucan.Delegation // delegations in the bucket-info container
	infoErr     error
}

func (f *fakeAuthorizer) AuthorizeRequest(_ context.Context, req s3.Request, _ ...hiltclient.MethodOption) (*s3req.AuthorizeOK, ucan.Container, error) {
	f.got = req
	if f.err != nil {
		return nil, nil, f.err
	}
	return f.res, container.New(container.WithDelegations(f.dlgs...)), nil
}

func (f *fakeAuthorizer) BucketInfo(_ context.Context, name string, _ did.DID, _ ...hiltclient.MethodOption) (*s3bkt.InfoOK, ucan.Container, error) {
	f.infoBuckets = append(f.infoBuckets, name)
	if f.infoErr != nil {
		return nil, nil, f.infoErr
	}
	return &s3bkt.InfoOK{}, container.New(container.WithDelegations(f.infoDlgs...)), nil
}

// newAccessKey generates an access key and returns its accessKeyId (the
// did:key identifier) and DID.
func newAccessKey(t *testing.T) (string, did.DID) {
	t.Helper()
	signer, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	return signer.DID().Identifier(), signer.DID()
}

// testTenant is the tenant every authorizeOK result names.
var testTenant = did.MustParse("did:plc:ewvi7nxzyoun6zhxrhs64oiz")

// authorizeOK builds an AuthorizeOK granting keyDID the given verification
// keys.
func authorizeOK(t *testing.T, keyDID did.DID, keys ...s3.VerificationKey) *s3req.AuthorizeOK {
	t.Helper()
	bucket, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	bucketID := bucket.DID()
	return &s3req.AuthorizeOK{
		Bucket:      &bucketID,
		Tenant:      testTenant,
		Permissions: s3.PermissionSet{Entries: map[did.DID][]string{keyDID: {"s3:GetObject"}}},
		Keys:        s3.KeySet{Entries: map[did.DID][]s3.VerificationKey{keyDID: keys}},
	}
}

// resolveForRequest drives req through a fiber app whose handler resolves the
// account via the service, returning what a real middleware would see.
func resolveForRequest(t *testing.T, svc *iam.Service, access string, req *http.Request) (auth.Account, error) {
	t.Helper()
	app := fiber.New()
	var acct auth.Account
	var authErr error
	app.Use(func(c fiber.Ctx) error {
		acct, authErr = svc.GetUserAccountForRequest(c, access)
		return nil
	})
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return acct, authErr
}

func TestGetUserAccountForRequest(t *testing.T) {
	access, keyDID := newAccessKey(t)
	derivedKey := []byte("derived-signing-key")

	t.Run("returns account with the sigv4 derived key", func(t *testing.T) {
		fake := &fakeAuthorizer{res: authorizeOK(t, keyDID,
			s3.VerificationKey{Kind: s3.KeyKindSigV4a, Data: []byte("ecdsa")},
			s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: derivedKey},
		)}
		svc := iam.New(fake, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), iam.NewTenantCache())

		req := httptest.NewRequest(http.MethodGet, "http://example.com/bucket/key%20name?x-id=GetObject", nil)
		req.Header.Set("X-Amz-Date", "20260707T000000Z")

		acct, err := resolveForRequest(t, svc, access, req)
		require.NoError(t, err)
		require.Equal(t, auth.Account{Access: access, SigningKey: derivedKey, Role: auth.RoleAdmin}, acct)

		// The forwarded request must be what the client signed: raw
		// request-line URL (still percent-encoded) and the signed headers,
		// including Host.
		require.Equal(t, http.MethodGet, fake.got.Method)
		require.Equal(t, "/bucket/key%20name?x-id=GetObject", fake.got.URL)
		require.Equal(t, "example.com", fake.got.Headers["Host"])
		require.Equal(t, "20260707T000000Z", fake.got.Headers["X-Amz-Date"])
	})

	t.Run("stashes the tenant on the request", func(t *testing.T) {
		fake := &fakeAuthorizer{res: authorizeOK(t, keyDID, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: derivedKey})}
		tenants := iam.NewTenantCache()
		svc := iam.New(fake, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), tenants)

		app := fiber.New()
		var stashed any
		app.Use(func(c fiber.Ctx) error {
			_, err := svc.GetUserAccountForRequest(c, access)
			require.NoError(t, err)
			stashed = c.Locals(reqscope.TenantKey())
			return nil
		})
		resp, err := app.Test(httptest.NewRequest(http.MethodGet, "http://example.com/bucket/key", nil))
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		require.Equal(t, testTenant, stashed, "tenant must reach the write path via the request")
		cached, ok := tenants.Get(access)
		require.True(t, ok, "tenant must be cached for the fast path")
		require.Equal(t, testTenant, cached)
	})

	t.Run("no tenant in result", func(t *testing.T) {
		res := authorizeOK(t, keyDID, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: derivedKey})
		res.Tenant = did.Undef
		fake := &fakeAuthorizer{res: res}
		svc := iam.New(fake, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), iam.NewTenantCache())

		_, err := resolveForRequest(t, svc, access,
			httptest.NewRequest(http.MethodGet, "http://example.com/bucket/key", nil))
		require.ErrorContains(t, err, "no tenant")
	})

	t.Run("no sigv4 key in result", func(t *testing.T) {
		fake := &fakeAuthorizer{res: authorizeOK(t, keyDID,
			s3.VerificationKey{Kind: s3.KeyKindSigV4a, Data: []byte("ecdsa")},
		)}
		svc := iam.New(fake, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), iam.NewTenantCache())

		_, err := resolveForRequest(t, svc, access,
			httptest.NewRequest(http.MethodGet, "http://example.com/bucket/key", nil))
		require.ErrorContains(t, err, "no sigv4 signing key")
	})

	t.Run("authorizer error propagates", func(t *testing.T) {
		boom := errors.New("boom")
		svc := iam.New(&fakeAuthorizer{err: boom}, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), iam.NewTenantCache())

		_, err := resolveForRequest(t, svc, access,
			httptest.NewRequest(http.MethodGet, "http://example.com/bucket/key", nil))
		require.ErrorIs(t, err, boom)
	})

	t.Run("malformed access key id short-circuits", func(t *testing.T) {
		fake := &fakeAuthorizer{}
		svc := iam.New(fake, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), iam.NewTenantCache())

		_, err := resolveForRequest(t, svc, "not-a-did-key-identifier",
			httptest.NewRequest(http.MethodGet, "http://example.com/bucket/key", nil))
		require.ErrorIs(t, err, auth.ErrNoSuchUser)
		require.Empty(t, fake.got.Method) // Hilt was never called
	})
}

// TestProofChainCapture covers the delegation plumbing: authorize-response
// delegations land in the cache, incomplete chains trigger exactly one
// /s3/bucket/info fetch, and info failures never fail authentication.
func TestProofChainCapture(t *testing.T) {
	access, keyDID := newAccessKey(t)
	sigv4 := s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: []byte("dk")}
	ctx := context.Background()

	t.Run("complete chain in authorize response: no bucket info call", func(t *testing.T) {
		root, mid, leaf, agent := mintRetrieveChain(t)
		cache := iam.NewKeyProofs()
		fake := &fakeAuthorizer{res: authorizeOK(t, keyDID, sigv4), dlgs: []ucan.Delegation{root, mid, leaf}}
		svc := iam.New(fake, cache, iam.NewVerificationKeyCache(), iam.NewTenantCache())

		_, err := resolveForRequest(t, svc, access,
			httptest.NewRequest(http.MethodGet, "http://example.com/bkt/key", nil))
		require.NoError(t, err)
		require.Empty(t, fake.infoBuckets, "complete chain must not trigger bucket info")

		chain, _, err := cache.For(keyDID).ProofChain(ctx, agent.DID(), leaf.Command(), leaf.Subject())
		require.NoError(t, err)
		require.Len(t, chain, 3)
	})

	t.Run("leaf-only response: bucket info completes the chain", func(t *testing.T) {
		root, mid, leaf, agent := mintRetrieveChain(t)
		cache := iam.NewKeyProofs()
		fake := &fakeAuthorizer{
			res:      authorizeOK(t, keyDID, sigv4),
			dlgs:     []ucan.Delegation{leaf},      // authorize: re-delegation only
			infoDlgs: []ucan.Delegation{root, mid}, // bucket info: the rest
		}
		svc := iam.New(fake, cache, iam.NewVerificationKeyCache(), iam.NewTenantCache())

		_, err := resolveForRequest(t, svc, access,
			httptest.NewRequest(http.MethodGet, "http://example.com/bkt/key", nil))
		require.NoError(t, err)
		require.Equal(t, []string{"bkt"}, fake.infoBuckets, "exactly one info fetch, path-style bucket name")

		chain, _, err := cache.For(keyDID).ProofChain(ctx, agent.DID(), leaf.Command(), leaf.Subject())
		require.NoError(t, err)
		require.Len(t, chain, 3, "chain must resolve after the info fetch")
	})

	t.Run("bucket info failure degrades, auth still succeeds", func(t *testing.T) {
		_, _, leaf, _ := mintRetrieveChain(t)
		fake := &fakeAuthorizer{
			res:     authorizeOK(t, keyDID, sigv4),
			dlgs:    []ucan.Delegation{leaf},
			infoErr: errors.New("hilt down"),
		}
		svc := iam.New(fake, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), iam.NewTenantCache())

		acct, err := resolveForRequest(t, svc, access,
			httptest.NewRequest(http.MethodGet, "http://example.com/bkt/key", nil))
		require.NoError(t, err, "proof-chain trouble must not fail authentication")
		require.Equal(t, access, acct.Access)
		require.Equal(t, []string{"bkt"}, fake.infoBuckets)
	})
}

// TestBaseIAMServiceParity pins the non-request IAMService surface to
// IAMServiceSingle's behavior: account management belongs to Hilt.
func TestBaseIAMServiceParity(t *testing.T) {
	svc := iam.New(&fakeAuthorizer{}, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), iam.NewTenantCache())

	_, err := svc.GetUserAccount("anything")
	require.Error(t, err)

	require.Error(t, svc.CreateAccount(auth.Account{}))
	require.Error(t, svc.UpdateUserAccount("a", auth.MutableProps{}))
	require.Error(t, svc.DeleteUserAccount("a"))
	_, err = svc.ListUserAccounts()
	require.Error(t, err)
	require.NoError(t, svc.Shutdown())
}
