package iam

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	hiltclient "github.com/fil-forge/hilt/pkg/client"
	"github.com/fil-forge/hilt/pkg/s3perm"
	"github.com/fil-forge/hilt/pkg/sigv4"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/registry"
	s3 "github.com/fil-forge/libforge/commands/s3"
	s3bkt "github.com/fil-forge/libforge/commands/s3/bucket"
	s3req "github.com/fil-forge/libforge/commands/s3/request"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/command"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
)

type statusAuthorizer struct {
	result *s3req.AuthorizeOK
	blocks []ucan.Delegation
	err    error
	calls  int
}

func (a *statusAuthorizer) AuthorizeRequest(context.Context, s3.Request, ...hiltclient.MethodOption) (*s3req.AuthorizeOK, ucan.Container, error) {
	a.calls++
	if a.err != nil {
		return nil, nil, a.err
	}
	return a.result, container.New(container.WithDelegations(a.blocks...)), nil
}

func (a *statusAuthorizer) BucketInfo(context.Context, string, did.DID, ...hiltclient.MethodOption) (*s3bkt.InfoOK, ucan.Container, error) {
	return nil, nil, errors.New("status response must carry its exact parent chain")
}

type statusFixture struct {
	service                   *Service
	remote                    *statusAuthorizer
	access, secret            string
	key, tenant, space, agent ucan.Issuer
	oldParents                []ucan.Delegation
	root                      ucan.Delegation
	oldBlocks, newBlocks      map[string][]ucan.Delegation
	full, allowed             []string
	verification              s3.VerificationKey
}

func newStatusFixture(t *testing.T) *statusFixture {
	t.Helper()
	issuers := make([]ucan.Issuer, 4)
	for i := range issuers {
		value, err := ed25519.GenerateIssuer()
		require.NoError(t, err)
		issuers[i] = value
	}
	f := &statusFixture{key: issuers[0], tenant: issuers[1], space: issuers[2], agent: issuers[3], secret: "synthetic-status-test-secret"}
	f.access = f.key.DID().Identifier()
	f.allowed = []string{"s3:GetObject", "s3:ListBucket", "s3:DeleteObject", "s3:DeleteObjectVersion", "s3:AbortMultipartUpload"}
	f.full = append(append([]string{}, f.allowed...), "s3:PutObject", "s3:DeleteObjectTagging", "s3:DeleteBucketPolicy", "s3:PutEncryptionConfiguration", "s3:PutBucketCORS", "s3:PutBucketTagging")
	f.oldBlocks, f.oldParents = f.chains(t, "active")
	f.newBlocks, _ = f.chains(t, "write-locked")
	req := f.request(t, "PUT", "/bkt/key")
	sr, err := sigv4.Parse(sigv4.Request{Method: req.Method, URL: req.URL, Headers: req.Headers})
	require.NoError(t, err)
	key, err := sigv4.DeriveKey(sr, f.secret)
	require.NoError(t, err)
	f.verification = s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: key}
	f.remote = &statusAuthorizer{}
	f.service = New(f.remote, NewKeyProofs(), NewVerificationKeyCache(), NewTenantCache(),
		WithLocalAuthorization(f.agent.DID(), fixedResolver{name: "bkt", state: &registry.State{Name: "bkt", Space: f.space.DID()}}))
	f.answer("s3:PutObject", f.full, f.oldBlocks)
	return f
}

func (f *statusFixture) chains(t *testing.T, generation string) (map[string][]ucan.Delegation, []ucan.Delegation) {
	t.Helper()
	out := map[string][]ucan.Delegation{}
	var parents []ucan.Delegation
	for _, cmd := range s3perm.CommandsFor(f.full...) {
		if f.root == nil {
			root, err := delegation.Delegate(f.space, f.tenant.DID(), f.space.DID(), command.Top(), delegation.WithNoExpiration())
			require.NoError(t, err)
			f.root = root
		}
		parent, err := delegation.Delegate(f.tenant, f.key.DID(), f.space.DID(), cmd,
			delegation.WithNoExpiration(), delegation.WithNonce([]byte(generation+cmd.String())))
		require.NoError(t, err)
		leaf, err := delegation.Delegate(f.key, f.agent.DID(), f.space.DID(), cmd, delegation.WithExpiration(ucan.Now()+3600))
		require.NoError(t, err)
		out[cmd.String()] = []ucan.Delegation{f.root, parent, leaf}
		parents = append(parents, parent)
	}
	return out, parents
}

func (f *statusFixture) answer(permission string, actions []string, chains map[string][]ucan.Delegation) {
	space := f.space.DID()
	f.remote.result = &s3req.AuthorizeOK{Bucket: &space, Tenant: f.tenant.DID(),
		Permissions: s3.PermissionSet{Entries: map[did.DID][]string{f.key.DID(): actions}},
		Keys:        s3.KeySet{Entries: map[did.DID][]s3.VerificationKey{f.key.DID(): {f.verification}}}}
	f.remote.blocks = nil
	for _, cmd := range s3perm.CommandsFor(permission) {
		f.remote.blocks = append(f.remote.blocks, chains[cmd.String()]...)
	}
}

func (f *statusFixture) request(t *testing.T, method, path string) s3.Request {
	t.Helper()
	return signedRequest(t, method, "s3.example", path, f.access, f.secret, time.Now())
}

func (f *statusFixture) drive(t *testing.T, req s3.Request) (bool, error) {
	t.Helper()
	app := fiber.New()
	var authErr error
	var borrowable bool
	app.Use(func(c fiber.Ctx) error {
		_, authErr = f.service.GetUserAccountForRequest(c, f.access)
		borrowable, _ = c.Locals(reqscope.BorrowableKey()).(bool)
		return nil
	})
	httpReq := httptest.NewRequest(req.Method, "http://"+req.Headers["Host"]+req.URL, bytes.NewReader(req.Body))
	for name, value := range req.Headers {
		httpReq.Header.Set(name, value)
	}
	response, err := app.Test(httpReq)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	return borrowable, authErr
}

func (f *statusFixture) invalidate(t *testing.T) {
	t.Helper()
	revoker := NewRevoker(f.service.proofs, f.service.keys, f.service.tenants, nil)
	for _, parent := range f.oldParents {
		revoker.Revoke(parent.Link())
	}
	_, found := f.service.keys.Get(f.access, s3.KeyKindSigV4)
	require.False(t, found)
}

func requireStatusDenied(t *testing.T, err error) {
	t.Helper()
	var denied s3err.S3Error
	require.ErrorAs(t, err, &denied)
	require.Equal(t, "AccessDenied", denied.BaseError().Code)
	require.Equal(t, http.StatusForbidden, denied.StatusCode())
}

func TestWriteLockedCleanupWarmCacheCannotRegainMutationActions(t *testing.T) {
	f := newStatusFixture(t)
	_, err := f.drive(t, f.request(t, "PUT", "/bkt/key"))
	require.NoError(t, err)
	require.Equal(t, 1, f.remote.calls)
	f.invalidate(t)

	// Refresh real request-side caches with allowed cleanup. These command
	// sets together cover every PutObject command: command probing alone is
	// insufficient, and the narrowed S3 action set must still deny a PUT.
	f.answer("s3:DeleteObject", f.allowed, f.newBlocks)
	_, err = f.drive(t, f.request(t, "DELETE", "/bkt/key"))
	require.NoError(t, err)
	f.answer("s3:AbortMultipartUpload", f.allowed, f.newBlocks)
	_, err = f.drive(t, f.request(t, "DELETE", "/bkt/key?uploadId=upload"))
	require.NoError(t, err)
	calls := f.remote.calls
	for _, shape := range []struct{ method, path string }{{"GET", "/bkt/key"}, {"GET", "/bkt"}, {"DELETE", "/bkt/key"}} {
		_, err = f.drive(t, f.request(t, shape.method, shape.path))
		require.NoError(t, err)
	}
	require.Equal(t, calls, f.remote.calls, "read/list/delete really hit warm caches")
	for _, cmd := range s3perm.CommandsFor("s3:PutObject") {
		chain, _, err := f.service.proofs.For(f.key.DID()).ProofChainVia(t.Context(), f.agent.DID(), cmd, f.space.DID(), f.key.DID())
		require.NoError(t, err)
		require.NotEmpty(t, chain, cmd.String())
	}

	f.remote.err = hiltErr("TenantWriteLocked")
	for _, shape := range []struct{ method, path string }{
		{"PUT", "/bkt/key"}, {"POST", "/bkt/key?uploads"},
		{"PUT", "/bkt/key?uploadId=upload&partNumber=1"}, {"POST", "/bkt/key?uploadId=upload"},
		{"DELETE", "/bkt/key?tagging"},
	} {
		t.Run(shape.method+shape.path, func(t *testing.T) {
			_, err := f.drive(t, f.request(t, shape.method, shape.path))
			requireStatusDenied(t, err)
		})
	}
	copyReq := signedCopy(t, "s3.example", "bkt", "bkt", f.access, f.secret, true)
	_, err = f.drive(t, copyReq)
	requireStatusDenied(t, err)
	require.Equal(t, calls, f.remote.calls, "forbidden data/security actions are denied by the cached action set")
	for _, path := range []string{"/bkt?policy", "/bkt?encryption", "/bkt?cors", "/bkt?tagging"} {
		_, err = f.drive(t, f.request(t, "DELETE", path))
		requireStatusDenied(t, err)
	}
}

func TestLateActiveResponseCarriesRevokedParentAndCannotRepopulateWarmCache(t *testing.T) {
	f := newStatusFixture(t)
	_, err := f.drive(t, f.request(t, "PUT", "/bkt/key"))
	require.NoError(t, err)
	oldResult, oldBlocks := f.remote.result, append([]ucan.Delegation{}, f.remote.blocks...)
	f.invalidate(t)
	f.answer("s3:DeleteObject", f.allowed, f.newBlocks)
	_, err = f.drive(t, f.request(t, "DELETE", "/bkt/key"))
	require.NoError(t, err)
	// Force an in-flight old active answer to arrive beside fresh cleanup
	// roots. Its old parent CID must refuse the whole cache insertion.
	f.service.keys.Delete(f.access)
	f.remote.result, f.remote.blocks = oldResult, oldBlocks
	borrowable, err := f.drive(t, f.request(t, "GET", "/bkt/key"))
	require.NoError(t, err, "already-authorized in-flight request remains an explicit residual limit")
	require.False(t, borrowable)
	_, found := f.service.keys.Get(f.access, s3.KeyKindSigV4)
	require.False(t, found, "late response cannot cache its old verification key")
	permitted, known := f.service.proofs.For(f.key.DID()).Permits(f.space.DID(), "s3:PutObject")
	require.True(t, known)
	require.False(t, permitted, "fresh narrowed action set must survive the late response")
	f.remote.err = hiltErr("TenantWriteLocked")
	_, err = f.drive(t, f.request(t, "PUT", "/bkt/key"))
	requireStatusDenied(t, err)
}
