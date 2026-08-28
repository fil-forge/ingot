// Package iam adapts Hilt's /s3/request/authorize command to versitygw's IAM
// seam, so that S3 requests signed with Hilt-managed access keys authenticate
// through the embedded gateway.
//
// The [Service] implements two interfaces: versitygw's base auth.IAMService —
// where it mirrors auth.IAMServiceSingle, because accounts are managed by
// Hilt and the gateway's admin APIs are not mounted — and the request-scoped
// middlewares.RequestIAMService, which the auth middlewares consult for every
// non-root access key. Per request, the service forwards the raw S3 API
// request to Hilt; Hilt verifies the SigV4 signature, checks the access key's
// permission for the requested action, and returns a scope-bound derived
// signing key (never the raw secret). The gateway then re-verifies the
// signature locally with that key — which also covers per-chunk streaming
// signatures Hilt itself never sees.
//
// Proof state is held per access key in [KeyProofs]: each key's store holds
// only the delegations Hilt issued for that key, so proof chains can never
// cross keys. The delegations Hilt returns ARE consumed — each authorize
// response carries access-key→ingot re-delegations (≤24h TTL); their
// bucket→tenant→access-key remainder comes from /s3/bucket/info, which the
// service fetches once (into the same per-key store) when a leaf's chain
// doesn't yet reach the root.
//
// Every request stashes its key's store on the request context
// ([reqscope.ProofStoreKey]) so an onward Forge retrieval — which sees only ctx +
// space — authorizes with THIS access key's chain, never another's.
//
// A local fast path (the RFC's "local cache" authorization) avoids the Hilt
// round-trip when everything needed is already cached, mirroring Hilt's own
// verification order: the request parses as HMAC SigV4 and a cached derived
// key verifies its signature (hilt/pkg/sigv4.VerifyWithKey) and time bounds;
// the request's S3 action maps to Forge commands (hilt/pkg/s3perm); and the
// key's own store holds a chain to this instance's agent for every such
// command (which, being the key's own store, necessarily carries the key's
// grant too). Anything less falls through to /s3/request/authorize, whose
// response replenishes the caches — so expiry (next UTC midnight, when SigV4
// scope dates roll over) self-heals.
package iam

import (
	"context"
	"fmt"
	"strings"

	"time"

	hiltauth "github.com/fil-forge/hilt/pkg/rpc/service/auth"
	"github.com/fil-forge/hilt/pkg/s3perm"
	"github.com/fil-forge/hilt/pkg/sigv4"
	"github.com/fil-forge/ingot/internal/fasthttputil"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/registry"
	s3 "github.com/fil-forge/libforge/commands/s3"
	s3bkt "github.com/fil-forge/libforge/commands/s3/bucket"
	s3req "github.com/fil-forge/libforge/commands/s3/request"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	ucanerrors "github.com/fil-forge/ucantone/errors"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/versitygw/auth"
	"github.com/fil-forge/versitygw/s3api/middlewares"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/gofiber/fiber/v3"
	"go.uber.org/zap"

	hiltclient "github.com/fil-forge/hilt/pkg/client"
)

// Authorizer is the slice of [hiltclient.Client] the service uses:
// /s3/request/authorize per request, and /s3/bucket/info to complete proof
// chains the authorize response only partially supplies. Each returned
// container carries the delegation blocks for the respective chains.
type Authorizer interface {
	AuthorizeRequest(ctx context.Context, req s3.Request, opts ...hiltclient.MethodOption) (*s3req.AuthorizeOK, ucan.Container, error)
	BucketInfo(ctx context.Context, name string, accessKey did.DID, opts ...hiltclient.MethodOption) (*s3bkt.InfoOK, ucan.Container, error)
}

// BucketResolver maps a bucket name to its local state (the fast path needs
// the bucket's Forge space as the delegation subject). registry.Registry
// satisfies it; the lookup is local.
type BucketResolver interface {
	Get(ctx context.Context, name string) (*registry.State, error)
}

// Service authorizes S3 requests against Hilt. See the package doc for how it
// plugs into versitygw.
type Service struct {
	authorizer Authorizer
	proofs     *KeyProofs
	keys       *VerificationKeyCache
	tenants    *TenantCache
	logger     *zap.Logger

	// agent + buckets enable the local fast path (see WithLocalAuthorization);
	// both nil leaves every request on the Hilt path.
	agent   did.DID
	buckets BucketResolver
}

// Compile-time interface conformance: the base IAM surface, the
// request-scoped lookup the auth middlewares prefer, and the real client
// satisfying Authorizer.
var (
	_ auth.IAMService               = (*Service)(nil)
	_ middlewares.RequestIAMService = (*Service)(nil)
	_ Authorizer                    = (*hiltclient.Client)(nil)
)

// Option configures a Service.
type Option func(*Service)

// WithLogger sets the service logger (default: no-op).
func WithLogger(logger *zap.Logger) Option {
	return func(s *Service) {
		if logger != nil {
			s.logger = logger
		}
	}
}

// WithLocalAuthorization enables the local fast path: agent is this
// instance's identity (the audience of Hilt's re-delegations) and buckets
// resolves bucket names to their Forge spaces. Without it every request
// takes the Hilt path.
func WithLocalAuthorization(agent did.DID, buckets BucketResolver) Option {
	return func(s *Service) {
		s.agent = agent
		s.buckets = buckets
	}
}

// New creates a Service that authorizes requests via authorizer (typically a
// *hiltclient.Client) and deposits the delegations, verification keys and
// tenant DID Hilt returns into proofs (per-access-key), keys and tenants —
// the caches the retrieval path, the write path and the local fast path read
// from.
func New(authorizer Authorizer, proofs *KeyProofs, keys *VerificationKeyCache, tenants *TenantCache, opts ...Option) *Service {
	s := &Service{authorizer: authorizer, proofs: proofs, keys: keys, tenants: tenants, logger: zap.NewNop()}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// GetUserAccountForRequest resolves the account for an in-flight request by
// invoking /s3/request/authorize on Hilt. On success the returned account
// carries the SigV4 signing key Hilt derived for the request's credential
// scope; versitygw verifies the request signature against it locally.
func (s *Service) GetUserAccountForRequest(ctx fiber.Ctx, accessKeyStr string) (auth.Account, error) {
	// The accessKeyId is the access key's did:key identifier (the DID with
	// the prefix stripped). Reject malformed keys before calling out.
	accessKeyID, err := did.Parse(did.KeyPrefix + accessKeyStr)
	if err != nil {
		s.logger.Debug("hilt/iam: access key is not a did:key identifier",
			zap.String("access", accessKeyStr), zap.Error(err))
		return auth.Account{}, auth.ErrNoSuchUser
	}

	// The *fasthttp.RequestCtx doubles as the invocation's context.Context,
	// so the Hilt round-trip is bounded by the request's lifetime.
	reqCtx := ctx.RequestCtx()
	req := fasthttputil.RequestFromHTTPContext(reqCtx)
	ctx.Locals(reqscope.RequestKey(), req)

	// Scope this key's proof store onto the request so an onward Forge
	// retrieval (blockstore.Forge, which has only ctx + space) uses THIS
	// access key's delegations, never another's. Set for every request
	// regardless of authorization outcome — harmless if the request later
	// fails, and needed whichever path authorizes.
	store := s.proofs.For(accessKeyID)
	ctx.Locals(reqscope.ProofStoreKey(), ucanlib.ProofStore(store))

	// Local fast path: with a cached verification key and cached delegation
	// chains covering the request's Forge commands, Hilt is not consulted.
	// The tenant travels with the request too (the write path encrypts to
	// its wrap key); a verified request whose tenant has fallen out of the
	// cache takes the Hilt path, whose response refills every cache.
	if account, ok := s.authorizeLocal(reqCtx, req, accessKeyStr, store); ok {
		if tenant, found := s.tenants.Get(accessKeyStr); found {
			ctx.Locals(reqscope.TenantKey(), tenant)
			return account, nil
		}
	}

	ok, ctr, err := s.authorizer.AuthorizeRequest(reqCtx, req)
	if err != nil {
		// A recognized Hilt auth rejection maps to the closest S3 error;
		// anything else (transport, service, internal, or an unrecognized
		// named error) is 500-class and wrapped generically.
		if mapped, matched := mapAuthError(err); matched {
			s.logger.Debug("hilt/iam: authorize rejected",
				zap.String("access", accessKeyStr), zap.Error(err))
			return auth.Account{}, mapped
		}
		return auth.Account{}, fmt.Errorf("hilt/iam: authorize request: %w", err)
	}

	key, found := sigV4Key(ok.Keys, accessKeyID)
	if !found {
		return auth.Account{}, fmt.Errorf("hilt/iam: no sigv4 signing key for %s in authorize result", accessKeyID)
	}
	// The tenant is a required part of the result: the write path encrypts
	// every object to the tenant's wrap key and will not write without it.
	if !ok.Tenant.Defined() {
		return auth.Account{}, fmt.Errorf("hilt/iam: no tenant for %s in authorize result", accessKeyID)
	}

	// Deposit the returned delegations (access-key→ingot re-delegations,
	// ≤24h TTL) into THIS key's store and complete their chains if needed —
	// best-effort: the request IS authorized; a gap here only affects onward
	// Forge invocations, which will surface it as missing retrieval authority.
	s.cacheProofs(reqCtx, store, ctr, req, accessKeyID)

	// Cache the verification keys until Hilt's own expiry horizon: SigV4
	// derived keys die at the next UTC midnight (credential-scope date
	// rollover), which is also when Hilt expires its re-delegations. Extend
	// by MaxClockSkew: a client with a lagging clock can sign a request with
	// today's scope date at a wall-clock instant just after midnight, and Hilt
	// accepts it within ±MaxClockSkew — keep the date's key cached that much
	// longer so those stragglers still hit the fast path instead of Hilt.
	ttl := untilNextUTCMidnight(time.Now()) + sigv4.MaxClockSkew
	s.keys.Put(accessKeyStr, ttl, ok.Keys.Entries[accessKeyID]...)
	// The tenant is cached to the same horizon so the fast path can stash it,
	// and stashed on this request for the write path.
	s.tenants.Put(accessKeyStr, ttl, ok.Tenant)
	ctx.Locals(reqscope.TenantKey(), ok.Tenant)

	// Bucket is nil for bucket-level operations (CreateBucket, ListBuckets),
	// which authorize without addressing an existing bucket.
	bucketStr := ""
	if ok.Bucket != nil {
		bucketStr = ok.Bucket.String()
	}
	s.logger.Debug("hilt/iam: request authorized",
		zap.String("access", accessKeyStr),
		zap.String("bucket", bucketStr),
		zap.Stringer("tenant", ok.Tenant),
	)

	return auth.Account{
		Access:     accessKeyStr,
		SigningKey: key,
		// Admin so the gateway's role/ACL layers defer entirely: authorization
		// is per-request via Hilt (or the local fast path), which enforces the
		// key's permissions and bucket scope. The gateway doesn't model
		// ownership for hilt-managed keys (buckets default to root-owned) and
		// its admin APIs are not mounted.
		Role: auth.RoleAdmin,
	}, nil
}

// mapAuthError translates an /s3/request/authorize failure into the closest S3
// error, reporting ok=false when it recognizes none. Hilt returns its
// authorization rejections as ucantone Named errors (hilt/pkg/rpc/service/auth)
// with stable names; a recognized name maps to an S3 error returned VERBATIM
// (not %w-wrapped) so versitygw's renderer type-asserts it. Everything else is
// left to the caller as 500-class: a non-Named error, but ALSO a Named error
// whose name we don't recognize — being Named does not make an error an
// authorization rejection. The mapping is best-effort: several Hilt reasons
// collapse onto AccessDenied where S3 has no finer-grained public code at the
// authentication layer.
func mapAuthError(err error) (error, bool) {
	var named ucanerrors.Named
	if !ucanerrors.As(err, &named) {
		return nil, false
	}
	switch named.Name() {
	case hiltauth.UnknownAccessKeyErrorName,
		hiltauth.InvalidAccessKeyIDErrorName,
		hiltauth.AccessKeyExpiredErrorName:
		// versitygw special-cases auth.ErrNoSuchUser into
		// InvalidAccessKeyId(access), which embeds the offending key id.
		return auth.ErrNoSuchUser, true
	case hiltauth.MalformedSignatureErrorName:
		return s3err.MalformedAuth.MissingSignature(), true
	case hiltauth.SignatureMismatchErrorName:
		return s3err.GetAPIError(s3err.ErrSignatureDoesNotMatch), true
	case hiltauth.SignatureExpiredErrorName:
		return s3err.GetAPIError(s3err.ErrExpiredPresignRequest), true
	case hiltauth.UnsupportedOperationErrorName:
		return s3err.GetAPIError(s3err.ErrNotImplemented), true
	case hiltauth.UnknownBucketErrorName:
		return s3err.GetAPIError(s3err.ErrNoSuchBucket), true
	case hiltauth.TenantDisabledErrorName,
		hiltauth.IssuerForbiddenErrorName,
		hiltauth.RegionNotServedErrorName,
		hiltauth.OperationNotPermittedErrorName,
		hiltauth.BucketNotPermittedErrorName:
		return s3err.GetAPIError(s3err.ErrAccessDenied), true
	default:
		// Named, but not a Hilt auth rejection we know — not ours to map.
		return nil, false
	}
}

// authorizeLocal is the fast path, mirroring Hilt's own verification
// order over THIS key's proof store. It reports ok=false whenever anything
// needed isn't cached or doesn't check out; the caller then takes the Hilt
// path, whose response replenishes the caches.
func (s *Service) authorizeLocal(ctx context.Context, req s3.Request, access string, store *DelegationCache) (auth.Account, bool) {
	if s.buckets == nil || !s.agent.Defined() {
		return auth.Account{}, false
	}

	// 1. Parse the signature envelope. Only HMAC SigV4 is fast-pathed: the
	// gateway re-verifies with the returned key and only speaks HMAC.
	sr, err := sigv4.Parse(sigv4.Request{Method: req.Method, Headers: req.Headers, URL: req.URL})
	if err != nil || sr.Scheme != sigv4.SchemeV4 {
		return auth.Account{}, false
	}

	// 2. Cached key verifies the signature and the request is in time
	// bounds. A scope-rolled or rotated key simply fails here.
	key, ok := s.keys.Get(access, s3.KeyKindSigV4)
	if !ok {
		return auth.Account{}, false
	}
	if sigv4.VerifyWithKey(sr, key) != nil {
		return auth.Account{}, false
	}
	if sigv4.ValidateTimeBounds(sr, time.Now()) != nil {
		return auth.Account{}, false
	}

	// 3. The S3 action must map to Forge commands. Bucket-level operations
	// (create/delete/list-buckets) map to none — they go to Hilt regardless.
	op, err := hiltauth.OperationFor(req)
	if err != nil {
		return auth.Account{}, false
	}
	cmds := s3perm.CommandsFor(op.Permission())
	if len(cmds) == 0 {
		return auth.Account{}, false
	}

	// 4. The delegation subject is the bucket's space.
	st, err := s.buckets.Get(ctx, bucketFromURL(req.URL))
	if err != nil || !st.Space.Defined() {
		return auth.Account{}, false
	}

	// 5. Every command must be covered by a chain to this instance's agent
	// in THIS key's store. Because the store holds only keyDID's delegations,
	// a resolving agent chain is necessarily space→…→keyDID→agent — it
	// carries both keyDID's own grant (Hilt's permission + bucket scoping)
	// and the onward re-delegation. Cross-key mixing is structurally
	// impossible, so one probe suffices.
	for _, cmd := range cmds {
		if chain, _, err := store.ProofChain(ctx, s.agent, cmd, st.Space); err != nil || len(chain) == 0 {
			return auth.Account{}, false
		}
	}

	s.logger.Debug("hilt/iam: request authorized locally",
		zap.String("access", access),
		zap.String("operation", op.String()),
	)
	return auth.Account{
		Access:     access,
		SigningKey: key,
		// Admin so the gateway's role/ACL layers defer entirely: authorization
		// is per-request via Hilt (or the local fast path), which enforces the
		// key's permissions and bucket scope. The gateway doesn't model
		// ownership for hilt-managed keys (buckets default to root-owned) and
		// its admin APIs are not mounted.
		Role: auth.RoleAdmin,
	}, true
}

// untilNextUTCMidnight is the cache TTL horizon for verification keys,
// matching Hilt's key/delegation expiry: a SigV4 derived key is bound to
// the credential-scope date and stops verifying when it rolls over.
func untilNextUTCMidnight(now time.Time) time.Duration {
	nowUTC := now.UTC()
	midnight := time.Date(nowUTC.Year(), nowUTC.Month(), nowUTC.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
	return midnight.Sub(nowUTC)
}

// cacheProofs deposits the authorize response's delegations into this key's
// proof store and, when it cannot assemble a root-complete chain for one of
// the fresh leaves, fetches the bucket→tenant→access-key remainder from
// /s3/bucket/info (once) and caches that too.
func (s *Service) cacheProofs(ctx context.Context, store *DelegationCache, ctr ucan.Container, req s3.Request, keyDID did.DID) {
	if ctr == nil || len(ctr.Delegations()) == 0 {
		return
	}
	store.Add(ctr.Delegations()...)

	// A leaf is complete when a chain from its audience (this instance)
	// back to its subject's root exists in the cache. Powerline leaves
	// (undefined subject) can't anchor a chain lookup; skip them here —
	// the retrieval path resolves them against a concrete space, and the
	// bucket-info fetch below covers the missing intermediates either way.
	incomplete := false
	for _, leaf := range ctr.Delegations() {
		if !leaf.Subject().Defined() {
			continue
		}
		chain, _, err := store.ProofChain(ctx, leaf.Audience(), leaf.Command(), leaf.Subject())
		if err != nil || len(chain) == 0 {
			incomplete = true
			break
		}
	}
	if !incomplete {
		return
	}

	bucketName := bucketFromURL(req.URL)
	if bucketName == "" {
		s.logger.Warn("hilt/iam: incomplete proof chain and no bucket in request URL",
			zap.String("url", req.URL))
		return
	}
	_, infoCtr, err := s.authorizer.BucketInfo(ctx, bucketName, keyDID)
	if err != nil {
		s.logger.Warn("hilt/iam: bucket info fetch for proof chain failed",
			zap.String("bucket", bucketName), zap.Error(err))
		return
	}
	if infoCtr != nil {
		store.Add(infoCtr.Delegations()...)
	}
}

// bucketFromURL extracts the bucket name from a path-style request URL
// (ingot serves path-style only). Empty for root-level requests like
// ListBuckets.
func bucketFromURL(rawURL string) string {
	path := rawURL
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	path = strings.TrimPrefix(path, "/")
	if i := strings.IndexByte(path, '/'); i >= 0 {
		path = path[:i]
	}
	return path
}

// sigV4Key picks the sigv4 derived signing key for the access key from an
// authorize result. Hilt may also return a sigv4a key; the gateway only
// verifies HMAC sigv4, so any other kind is skipped.
func sigV4Key(keys s3.KeySet, keyDID did.DID) ([]byte, bool) {
	for _, k := range keys.Entries[keyDID] {
		if k.Kind == s3.KeyKindSigV4 && len(k.Data) > 0 {
			return k.Data, true
		}
	}
	return nil, false
}

// The base IAMService methods mirror auth.IAMServiceSingle: Hilt owns account
// management (via its Fil One tenant API), and ingot does not mount the
// gateway's admin APIs.

// CreateAccount is not supported; accounts are managed by Hilt.
func (*Service) CreateAccount(auth.Account) error {
	return s3err.GetAPIError(s3err.ErrAdminMethodNotSupported)
}

// GetUserAccount resolves accounts only in request context (see
// GetUserAccountForRequest); lookups without a request find nothing.
func (*Service) GetUserAccount(string) (auth.Account, error) {
	return auth.Account{}, s3err.GetAPIError(s3err.ErrAdminUserNotFound)
}

// UpdateUserAccount is not supported; accounts are managed by Hilt.
func (*Service) UpdateUserAccount(string, auth.MutableProps) error {
	return s3err.GetAPIError(s3err.ErrAdminMethodNotSupported)
}

// DeleteUserAccount is not supported; accounts are managed by Hilt.
func (*Service) DeleteUserAccount(string) error {
	return s3err.GetAPIError(s3err.ErrAdminMethodNotSupported)
}

// ListUserAccounts is not supported; accounts are managed by Hilt.
func (*Service) ListUserAccounts() ([]auth.Account, error) {
	return []auth.Account{}, s3err.GetAPIError(s3err.ErrAdminMethodNotSupported)
}

// Shutdown is a no-op; the service holds no resources of its own.
func (*Service) Shutdown() error {
	return nil
}
