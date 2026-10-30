package iam_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	awsv4 "github.com/aws/aws-sdk-go-v2/aws"
	s3 "github.com/fil-forge/libforge/commands/s3"
	s3bkt "github.com/fil-forge/libforge/commands/s3/bucket"
	s3req "github.com/fil-forge/libforge/commands/s3/request"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/fil-forge/versitygw/auth"
	v4 "github.com/fil-forge/versitygw/aws/signer/v4"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"

	hiltclient "github.com/fil-forge/hilt/pkg/client"
	hiltauth "github.com/fil-forge/hilt/pkg/rpc/service/auth"
	"github.com/fil-forge/ingot/iam"
	"github.com/fil-forge/ingot/internal/reqscope"
)

// fakeAuthorizer records the request it authorized and returns canned
// results, standing in for the hilt client.
type fakeAuthorizer struct {
	got    s3.Request
	calls  int
	proofs []ucan.Delegation // the proof chain in the authorize response container
	res    *s3req.AuthorizeOK
	err    error

	infoBuckets []string          // BucketInfo calls received, by name
	infoDlgs    []ucan.Delegation // delegations in the bucket-info container
	infoErr     error
}

func (f *fakeAuthorizer) AuthorizeRequest(_ context.Context, req s3.Request, _ ...hiltclient.MethodOption) (*s3req.AuthorizeOK, ucan.Container, error) {
	f.got = req
	f.calls++
	if f.err != nil {
		return nil, nil, f.err
	}
	return f.res, container.New(container.WithDelegations(f.proofs...)), nil
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
// signedCopyRequest builds a SigV4-signed CopyObject of src/key into dst/key
// with the copy-source header covered, as every SDK signs it. The buckets a
// request acts on are read from the signed request after hilt accepts it,
// and a copy source is trusted only when signed.
func signedCopyRequest(t *testing.T, dst, src string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "http://example.com/"+dst+"/key", nil)
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	req.Header.Set("X-Amz-Copy-Source", "/"+src+"/key")
	_, err := v4.NewSigner().SignHTTP(context.Background(),
		awsv4.Credentials{AccessKeyID: "AKIATEST", SecretAccessKey: "secret"},
		req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", time.Now(),
		[]string{"host", "x-amz-content-sha256", "x-amz-copy-source", "x-amz-date"},
		func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	require.NoError(t, err)
	return req
}

func resolveForRequest(t *testing.T, svc *iam.Service, access string, req *http.Request) (auth.Account, error) {
	t.Helper()
	acct, _, err := resolveStreamed(t, svc, access, req, false)
	return acct, err
}

// resolveStreamed runs the request through the gateway's auth step as
// resolveForRequest does, streaming the request body as the production
// server does when streamed is set, and returns the body the handler then
// reads.
func resolveStreamed(t *testing.T, svc *iam.Service, access string, req *http.Request, streamed bool) (auth.Account, []byte, error) {
	t.Helper()
	app := fiber.New(fiber.Config{StreamRequestBody: streamed})
	var acct auth.Account
	var authErr error
	var seen []byte
	app.Use(func(c fiber.Ctx) error {
		acct, authErr = svc.GetUserAccountForRequest(c, access)
		seen = bytes.Clone(c.Body())
		return nil
	})
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return acct, seen, authErr
}

// TestRequestBodyForwarding covers the body hilt decides on, end to end
// through the gateway's auth step with a streamed request: a DeleteObjects
// body reaches the authorizer exactly and is left for the handler, any
// other operation's body is neither forwarded nor consumed, and an
// oversized DeleteObjects body is refused before the authorizer is asked.
func TestRequestBodyForwarding(t *testing.T) {
	access, keyDID := newAccessKey(t)
	key := s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: []byte("dk")}
	const body = `<Delete><Object><Key>k</Key><VersionId>v</VersionId></Object></Delete>`

	t.Run("a DeleteObjects body is forwarded and left for the handler", func(t *testing.T) {
		fake := &fakeAuthorizer{res: authorizeOK(t, keyDID, key)}
		svc := iam.New(fake, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), iam.NewTenantCache())
		req := httptest.NewRequest(http.MethodPost, "http://example.com/bkt?delete", strings.NewReader(body))
		_, seen, err := resolveStreamed(t, svc, access, req, true)
		require.NoError(t, err)
		require.Equal(t, 1, fake.calls)
		require.Equal(t, []byte(body), fake.got.Body, "the authorizer must see the exact body")
		require.Equal(t, []byte(body), seen, "the handler must read the same body")
	})

	t.Run("another operation's body is neither forwarded nor consumed", func(t *testing.T) {
		fake := &fakeAuthorizer{res: authorizeOK(t, keyDID, key)}
		svc := iam.New(fake, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), iam.NewTenantCache())
		req := httptest.NewRequest(http.MethodPut, "http://example.com/bkt/k", strings.NewReader("object bytes"))
		_, seen, err := resolveStreamed(t, svc, access, req, true)
		require.NoError(t, err)
		require.Nil(t, fake.got.Body)
		require.Equal(t, []byte("object bytes"), seen)
	})

	t.Run("an oversized DeleteObjects body is refused before the authorizer is asked", func(t *testing.T) {
		fake := &fakeAuthorizer{res: authorizeOK(t, keyDID, key)}
		svc := iam.New(fake, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), iam.NewTenantCache())
		huge := strings.Repeat("x", hiltauth.MaxBodySize+1)
		req := httptest.NewRequest(http.MethodPost, "http://example.com/bkt?delete", strings.NewReader(huge))
		_, _, err := resolveStreamed(t, svc, access, req, true)
		var s3e s3err.APIError
		require.ErrorAs(t, err, &s3e)
		require.Equal(t, s3err.GetAPIError(s3err.ErrEntityTooLarge).Code, s3e.Code)
		require.Zero(t, fake.calls)
	})
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

// TestKeyStoreIsBorrowable: a request authorized normally carries the access
// key's own store, marked as one background work may borrow.
func TestKeyStoreIsBorrowable(t *testing.T) {
	access, keyDID := newAccessKey(t)
	cache := iam.NewKeyProofs()
	fake := &fakeAuthorizer{res: authorizeOK(t, keyDID, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: []byte("dk")})}
	svc := iam.New(fake, cache, iam.NewVerificationKeyCache(), iam.NewTenantCache())

	app := fiber.New()
	var store ucanlib.ProofStore
	var borrowable bool
	app.Use(func(c fiber.Ctx) error {
		_, err := svc.GetUserAccountForRequest(c, access)
		require.NoError(t, err)
		store, _ = c.Locals(reqscope.ProofStoreKey()).(ucanlib.ProofStore)
		borrowable, _ = c.Locals(reqscope.BorrowableKey()).(bool)
		return nil
	})
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "http://example.com/bkt/key", nil))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	require.True(t, borrowable)
	require.Same(t, cache.For(keyDID), store, "the borrowable store is the key's own")
}

func TestRevokedResponseIsNotCached(t *testing.T) {
	access, keyDID := newAccessKey(t)
	sigv4 := s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: []byte("dk")}
	root, mid, leaf, agent := mintRetrieveChain(t)
	cache := iam.NewKeyProofs()
	keys, tenants := iam.NewVerificationKeyCache(), iam.NewTenantCache()
	fake := &fakeAuthorizer{res: authorizeOK(t, keyDID, sigv4), proofs: []ucan.Delegation{root, mid, leaf}}
	svc := iam.New(fake, cache, keys, tenants)

	// The consumer saw leaf revoked, but Hilt's write did not commit: its
	// next response still carries leaf.
	require.Empty(t, iam.NewRevoker(cache, keys, tenants, nil).Revoke(leaf.Link()))

	app := fiber.New()
	var scoped ucanlib.ProofStore
	var acct auth.Account
	var calls int
	borrowable := true
	app.Use(func(c fiber.Ctx) error {
		var err error
		acct, err = svc.GetUserAccountForRequest(c, access)
		require.NoError(t, err)
		scoped, _ = c.Locals(reqscope.ProofStoreKey()).(ucanlib.ProofStore)
		borrowable = c.Locals(reqscope.BorrowableKey()).(bool)
		calls++
		return nil
	})
	for range 2 {
		resp, err := app.Test(httptest.NewRequest(http.MethodGet, "http://example.com/bkt/key", nil))
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
	}
	require.Equal(t, 2, calls)
	require.Equal(t, 2, fake.calls, "every request reaches Hilt while the response carries a revoked delegation")
	require.Equal(t, auth.Account{Access: access, SigningKey: []byte("dk"), Role: auth.RoleAdmin}, acct, "the request is authorized")

	// Nothing is cached for the key: no signing key, no tenant, no action set,
	// no delegations in its store.
	_, found := keys.Get(access, s3.KeyKindSigV4)
	require.False(t, found)
	_, found = tenants.Get(access)
	require.False(t, found)
	_, known := cache.For(keyDID).Permits(*fake.res.Bucket, "s3:GetObject")
	require.False(t, known)
	held, _, err := cache.For(keyDID).ProofChain(context.Background(), agent.DID(), leaf.Command(), leaf.Subject())
	require.NoError(t, err)
	require.Empty(t, held)

	// The request itself still had a chain for its onward retrieval, in a
	// store nothing may borrow past the request.
	require.False(t, borrowable, "a store holding a revoked delegation is not borrowable")
	require.NotNil(t, scoped)
	chain, _, err := scoped.ProofChain(context.Background(), agent.DID(), leaf.Command(), leaf.Subject())
	require.NoError(t, err)
	require.Len(t, chain, 3)
}

// TestRevokedBucketInfoDelegationIsNotCached: a leaf-only authorize response
// is completed from /s3/bucket/info, and a revoked hop there refuses the
// caching just as one in the authorize response does.
func TestRevokedBucketInfoDelegationIsNotCached(t *testing.T) {
	access, keyDID := newAccessKey(t)
	sigv4 := s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: []byte("dk")}
	root, mid, leaf, agent := mintRetrieveChain(t)
	cache := iam.NewKeyProofs()
	keys, tenants := iam.NewVerificationKeyCache(), iam.NewTenantCache()
	fake := &fakeAuthorizer{
		res:      authorizeOK(t, keyDID, sigv4),
		proofs:   []ucan.Delegation{leaf},
		infoDlgs: []ucan.Delegation{root, mid},
	}
	svc := iam.New(fake, cache, keys, tenants)
	require.Empty(t, iam.NewRevoker(cache, keys, tenants, nil).Revoke(mid.Link()))

	app := fiber.New()
	var scoped ucanlib.ProofStore
	app.Use(func(c fiber.Ctx) error {
		_, err := svc.GetUserAccountForRequest(c, access)
		require.NoError(t, err)
		scoped, _ = c.Locals(reqscope.ProofStoreKey()).(ucanlib.ProofStore)
		return nil
	})
	for range 2 {
		resp, err := app.Test(httptest.NewRequest(http.MethodGet, "http://example.com/bkt/key", nil))
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
	}
	require.Equal(t, 2, fake.calls, "every request reaches Hilt while bucket info carries a revoked delegation")

	_, found := keys.Get(access, s3.KeyKindSigV4)
	require.False(t, found)
	_, found = tenants.Get(access)
	require.False(t, found)
	_, known := cache.For(keyDID).Permits(*fake.res.Bucket, "s3:GetObject")
	require.False(t, known)
	held, _, err := cache.For(keyDID).ProofChain(context.Background(), agent.DID(), leaf.Command(), leaf.Subject())
	require.NoError(t, err)
	require.Empty(t, held)

	require.NotNil(t, scoped)
	chain, _, err := scoped.ProofChain(context.Background(), agent.DID(), leaf.Command(), leaf.Subject())
	require.NoError(t, err)
	require.Len(t, chain, 3)
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
		fake := &fakeAuthorizer{res: authorizeOK(t, keyDID, sigv4), proofs: []ucan.Delegation{root, mid, leaf}}
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
			proofs:   []ucan.Delegation{leaf},      // authorize: re-delegation only
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

	t.Run("cross-bucket copy: bucket info for the destination and the source", func(t *testing.T) {
		root, mid, leaf, agent := mintRetrieveChain(t)
		cache := iam.NewKeyProofs()
		fake := &fakeAuthorizer{
			res:      authorizeOK(t, keyDID, sigv4),
			proofs:   []ucan.Delegation{leaf},
			infoDlgs: []ucan.Delegation{root, mid},
		}
		svc := iam.New(fake, cache, iam.NewVerificationKeyCache(), iam.NewTenantCache())

		req := signedCopyRequest(t, "dst", "src")
		_, err := resolveForRequest(t, svc, access, req)
		require.NoError(t, err)
		require.Equal(t, []string{"dst", "src"}, fake.infoBuckets,
			"a copy's chains span two buckets; both need their bucket→tenant→key remainder")

		chain, _, err := cache.For(keyDID).ProofChain(ctx, agent.DID(), leaf.Command(), leaf.Subject())
		require.NoError(t, err)
		require.Len(t, chain, 3)
	})

	t.Run("the effective action set is cached under the bucket hilt named", func(t *testing.T) {
		_, _, leaf, _ := mintRetrieveChain(t)
		cache := iam.NewKeyProofs()
		fake := &fakeAuthorizer{res: authorizeOK(t, keyDID, sigv4), proofs: []ucan.Delegation{leaf}}
		svc := iam.New(fake, cache, iam.NewVerificationKeyCache(), iam.NewTenantCache())

		_, err := resolveForRequest(t, svc, access,
			httptest.NewRequest(http.MethodGet, "http://example.com/bkt/key", nil))
		require.NoError(t, err)

		allowed, known := cache.For(keyDID).Permits(*fake.res.Bucket, "s3:GetObject")
		require.True(t, known, "the authorize answer must be cached")
		require.True(t, allowed)
		allowed, known = cache.For(keyDID).Permits(*fake.res.Bucket, "s3:PutObject")
		require.True(t, known)
		require.False(t, allowed, "an action the key does not hold is denied from cache")
	})

	t.Run("a copy's action set is cached under the source bucket too", func(t *testing.T) {
		_, _, leaf, _ := mintRetrieveChain(t)
		cache := iam.NewKeyProofs()
		res := authorizeOK(t, keyDID, sigv4)
		src, err := ed25519.GenerateIssuer()
		require.NoError(t, err)
		srcID := src.DID()
		res.SourceBucket = &srcID
		fake := &fakeAuthorizer{res: res, proofs: []ucan.Delegation{leaf}}
		svc := iam.New(fake, cache, iam.NewVerificationKeyCache(), iam.NewTenantCache())

		req := signedCopyRequest(t, "dst", "src")
		_, err = resolveForRequest(t, svc, access, req)
		require.NoError(t, err)

		for _, b := range []did.DID{*res.Bucket, srcID} {
			allowed, known := cache.For(keyDID).Permits(b, "s3:GetObject")
			require.True(t, known, "hilt named %s; its set must be cached", b)
			require.True(t, allowed)
		}
	})

	t.Run("bucket info failure degrades, auth still succeeds", func(t *testing.T) {
		_, _, leaf, _ := mintRetrieveChain(t)
		fake := &fakeAuthorizer{
			res:     authorizeOK(t, keyDID, sigv4),
			proofs:  []ucan.Delegation{leaf},
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

// TestGetUserAccount_UserARN pins the one non-request lookup that answers:
// versitygw's PutBucketPolicy validation asks GetUserAccount whether each
// principal a document names exists. A well-formed user ARN does (Hilt decides
// whether the id is one of the tenant's principals); anything else, a bare
// name included, does not, so versitygw refuses the document as malformed.
func TestGetUserAccount_UserARN(t *testing.T) {
	svc := iam.New(&fakeAuthorizer{}, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), iam.NewTenantCache())

	_, err := svc.GetUserAccount("arn:aws:iam:::user/8f2c")
	require.NoError(t, err)
	for _, name := range []string{"arn:aws:iam:::user/", "8f2c", "arn:aws:iam:::root", ""} {
		_, err := svc.GetUserAccount(name)
		require.Equal(t, s3err.GetAPIError(s3err.ErrAdminUserNotFound), err, "name %q", name)
	}

	policy := func(principal string) []byte {
		return []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":` + principal +
			`,"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::photos/*"]}]}`)
	}
	require.NoError(t, auth.ValidatePolicyDocument(policy(`{"AWS":["arn:aws:iam:::user/8f2c"]}`), "photos", svc))
	require.NoError(t, auth.ValidatePolicyDocument(policy(`"*"`), "photos", svc))
	require.Error(t, auth.ValidatePolicyDocument(policy(`{"AWS":["8f2c"]}`), "photos", svc))
}

// TestGetUserAccountForRequest_PlainAccessKey pins the no-root contract: the
// gateway's root account is disabled, so a plain (non-did:key) access key —
// the shape a root credential would have — resolves through this service and
// is rejected as an unknown user before any hilt round-trip.
func TestGetUserAccountForRequest_PlainAccessKey(t *testing.T) {
	fake := &fakeAuthorizer{}
	svc := iam.New(fake, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), iam.NewTenantCache())
	t.Cleanup(func() { require.NoError(t, svc.Shutdown()) })

	for _, access := range []string{"ingot", ""} {
		req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
		_, err := resolveForRequest(t, svc, access, req)
		require.ErrorIs(t, err, auth.ErrNoSuchUser, "access %q", access)
	}
	require.Empty(t, fake.got.Method, "hilt must not be consulted for a malformed access key")
}
