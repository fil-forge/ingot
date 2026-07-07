package iam_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	s3 "github.com/fil-forge/libforge/commands/s3"
	s3req "github.com/fil-forge/libforge/commands/s3/request"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/fil-forge/versitygw/auth"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"

	hiltclient "github.com/fil-forge/ingot/hilt/client"
	hiltiam "github.com/fil-forge/ingot/hilt/iam"
)

// fakeAuthorizer records the request it authorized and returns a canned
// result, standing in for the hilt client.
type fakeAuthorizer struct {
	got    s3.Request
	result *s3req.AuthorizeOK
	err    error
}

func (f *fakeAuthorizer) AuthorizeRequest(_ context.Context, req s3.Request, _ ...hiltclient.MethodOption) (*s3req.AuthorizeOK, ucan.Container, error) {
	f.got = req
	if f.err != nil {
		return nil, nil, f.err
	}
	return f.result, container.New(), nil
}

// newAccessKey generates an access key and returns its accessKeyId (the
// did:key identifier) and DID.
func newAccessKey(t *testing.T) (string, did.DID) {
	t.Helper()
	signer, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	return signer.DID().Identifier(), signer.DID()
}

// authorizeOK builds an AuthorizeOK granting keyDID the given verification
// keys.
func authorizeOK(t *testing.T, keyDID did.DID, keys ...s3.VerificationKey) *s3req.AuthorizeOK {
	t.Helper()
	bucket, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	return &s3req.AuthorizeOK{
		Bucket:      bucket.DID(),
		Permissions: s3.PermissionSet{Entries: map[did.DID][]string{keyDID: {"s3:GetObject"}}},
		Keys:        s3.KeySet{Entries: map[did.DID][]s3.VerificationKey{keyDID: keys}},
	}
}

// resolveForRequest drives req through a fiber app whose handler resolves the
// account via the service, returning what a real middleware would see.
func resolveForRequest(t *testing.T, svc *hiltiam.Service, access string, req *http.Request) (auth.Account, error) {
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
		fake := &fakeAuthorizer{result: authorizeOK(t, keyDID,
			s3.VerificationKey{Kind: s3.KeyKindSigV4a, Data: []byte("ecdsa")},
			s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: derivedKey},
		)}
		svc := hiltiam.New(fake)

		req := httptest.NewRequest(http.MethodGet, "http://example.com/bucket/key%20name?x-id=GetObject", nil)
		req.Header.Set("X-Amz-Date", "20260707T000000Z")

		acct, err := resolveForRequest(t, svc, access, req)
		require.NoError(t, err)
		require.Equal(t, auth.Account{Access: access, SigningKey: derivedKey, Role: auth.RoleUser}, acct)

		// The forwarded request must be what the client signed: raw
		// request-line URL (still percent-encoded) and the signed headers,
		// including Host.
		require.Equal(t, http.MethodGet, fake.got.Method)
		require.Equal(t, "/bucket/key%20name?x-id=GetObject", fake.got.URL)
		require.Equal(t, "example.com", fake.got.Headers["Host"])
		require.Equal(t, "20260707T000000Z", fake.got.Headers["X-Amz-Date"])
	})

	t.Run("no sigv4 key in result", func(t *testing.T) {
		fake := &fakeAuthorizer{result: authorizeOK(t, keyDID,
			s3.VerificationKey{Kind: s3.KeyKindSigV4a, Data: []byte("ecdsa")},
		)}
		svc := hiltiam.New(fake)

		_, err := resolveForRequest(t, svc, access,
			httptest.NewRequest(http.MethodGet, "http://example.com/bucket/key", nil))
		require.ErrorContains(t, err, "no sigv4 signing key")
	})

	t.Run("authorizer error propagates", func(t *testing.T) {
		boom := errors.New("boom")
		svc := hiltiam.New(&fakeAuthorizer{err: boom})

		_, err := resolveForRequest(t, svc, access,
			httptest.NewRequest(http.MethodGet, "http://example.com/bucket/key", nil))
		require.ErrorIs(t, err, boom)
	})

	t.Run("malformed access key id short-circuits", func(t *testing.T) {
		fake := &fakeAuthorizer{}
		svc := hiltiam.New(fake)

		_, err := resolveForRequest(t, svc, "not-a-did-key-identifier",
			httptest.NewRequest(http.MethodGet, "http://example.com/bucket/key", nil))
		require.ErrorIs(t, err, auth.ErrNoSuchUser)
		require.Empty(t, fake.got.Method) // Hilt was never called
	})
}

// TestBaseIAMServiceParity pins the non-request IAMService surface to
// IAMServiceSingle's behavior: account management belongs to Hilt.
func TestBaseIAMServiceParity(t *testing.T) {
	svc := hiltiam.New(&fakeAuthorizer{})

	_, err := svc.GetUserAccount("anything")
	require.Error(t, err)

	require.Error(t, svc.CreateAccount(auth.Account{}))
	require.Error(t, svc.UpdateUserAccount("a", auth.MutableProps{}))
	require.Error(t, svc.DeleteUserAccount("a"))
	_, err = svc.ListUserAccounts()
	require.Error(t, err)
	require.NoError(t, svc.Shutdown())
}
