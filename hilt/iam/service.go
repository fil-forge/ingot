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
// The service is deliberately uncached: /s3/request/authorize is a per-request
// authorization decision (it checks the S3 action, not just the signature), so
// serving a later request from a cache would skip Hilt's permission check.
// Caching plus local action→permission enforcement is a planned follow-up, as
// is consuming the delegations Hilt re-delegates in the response (dropped
// here; they become useful with per-bucket spaces).
package iam

import (
	"context"
	"fmt"

	s3 "github.com/fil-forge/libforge/commands/s3"
	s3req "github.com/fil-forge/libforge/commands/s3/request"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/versitygw/auth"
	"github.com/fil-forge/versitygw/s3api/middlewares"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/gofiber/fiber/v3"
	"go.uber.org/zap"

	hiltclient "github.com/fil-forge/ingot/hilt/client"
)

// Authorizer is the slice of [hiltclient.Client] the service uses: the
// /s3/request/authorize invocation. The returned container carries the
// delegation blocks Hilt re-delegated to the invocation issuer.
type Authorizer interface {
	AuthorizeRequest(ctx context.Context, req s3.Request, opts ...hiltclient.MethodOption) (*s3req.AuthorizeOK, ucan.Container, error)
}

// Service authorizes S3 requests against Hilt. See the package doc for how it
// plugs into versitygw.
type Service struct {
	authorizer Authorizer
	logger     *zap.Logger
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

// New creates a Service that authorizes requests via authorizer (typically a
// *hiltclient.Client).
func New(authorizer Authorizer, opts ...Option) *Service {
	s := &Service{authorizer: authorizer, logger: zap.NewNop()}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// GetUserAccountForRequest resolves the account for an in-flight request by
// invoking /s3/request/authorize on Hilt. On success the returned account
// carries the SigV4 signing key Hilt derived for the request's credential
// scope; versitygw verifies the request signature against it locally.
func (s *Service) GetUserAccountForRequest(ctx fiber.Ctx, access string) (auth.Account, error) {
	// The accessKeyId is the access key's did:key identifier (the DID with
	// the prefix stripped). Reject malformed keys before calling out.
	keyDID, err := did.Parse(did.KeyPrefix + access)
	if err != nil {
		s.logger.Debug("hilt/iam: access key is not a did:key identifier",
			zap.String("access", access), zap.Error(err))
		return auth.Account{}, auth.ErrNoSuchUser
	}

	// The *fasthttp.RequestCtx doubles as the invocation's context.Context,
	// so the Hilt round-trip is bounded by the request's lifetime.
	rc := ctx.RequestCtx()
	ok, _, err := s.authorizer.AuthorizeRequest(rc, hiltclient.RequestFromHTTP(rc))
	if err != nil {
		// TODO(hilt): map Hilt's unknown-access-key failure to
		// auth.ErrNoSuchUser (-> InvalidAccessKeyId) once the receipt
		// failure model is structured enough to distinguish it from
		// transport or service errors.
		return auth.Account{}, fmt.Errorf("hilt/iam: authorize request: %w", err)
	}

	key, found := sigV4Key(ok.Keys, keyDID)
	if !found {
		return auth.Account{}, fmt.Errorf("hilt/iam: no sigv4 signing key for %s in authorize result", keyDID)
	}

	// The response also carries delegations re-delegated to this instance
	// (proof chains for onward Forge invocations). Nothing consumes them
	// yet — they become useful with per-bucket spaces — so drop them.
	s.logger.Debug("hilt/iam: request authorized",
		zap.String("access", access),
		zap.String("bucket", ok.Bucket.String()),
		zap.Int("delegations_dropped", len(ok.Delegations.Entries)),
	)

	return auth.Account{
		Access:     access,
		SigningKey: key,
		Role:       auth.RoleUser,
	}, nil
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
