package client

import (
	"context"
	"fmt"
	"net/url"

	"github.com/fil-forge/ingot/hilt/client/zapucan"
	s3 "github.com/fil-forge/libforge/commands/s3"
	s3bkt "github.com/fil-forge/libforge/commands/s3/bucket"
	s3req "github.com/fil-forge/libforge/commands/s3/request"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/binding"
	"github.com/fil-forge/ucantone/client"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/execution"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"go.uber.org/zap"
)

// Client invokes Hilt's S3 UCAN RPC commands (the caller is typically Ingot).
// Construct it with [New]; each method invokes one /s3/* command. Commands whose
// result carries re-delegated proof chains (AuthorizeRequest, CreateBucket,
// BucketInfo) also return the response container so the caller can extract the
// delegation blocks via [ucan.Container.Delegations].
type Client struct {
	ServiceID did.DID            // Hilt's DID (invocation subject + audience)
	Issuer    ucan.Issuer        // default invocation issuer (e.g. Ingot)
	Proofs    ucanlib.ProofStore // supplies the Hilt→issuer proof chains
	Executor  execution.Executor
	Logger    *zap.Logger
}

// New creates a Client for Hilt's UCAN RPC API at serviceURL, identified by
// serviceID (Hilt's DID). issuer signs invocations and proofs supplies the
// delegation chains from Hilt to the issuer; both are defaults, overridable per
// call with [WithIssuer] / [WithProofs].
func New(serviceID did.DID, serviceURL url.URL, issuer ucan.Issuer, proofs ucanlib.ProofStore, opts ...Option) (*Client, error) {
	cfg := &clientConfig{logger: zap.NewNop()}
	for _, opt := range opts {
		opt(cfg)
	}

	var httpExecutor execution.Executor
	var err error
	if cfg.httpClient != nil {
		httpExecutor, err = client.NewHTTP(&serviceURL, client.WithHTTPClient(cfg.httpClient))
	} else {
		httpExecutor, err = client.NewHTTP(&serviceURL)
	}
	if err != nil {
		return nil, fmt.Errorf("hilt/client: creating HTTP executor: %w", err)
	}

	if issuer == nil {
		return nil, fmt.Errorf("hilt/client: issuer is required")
	}
	if proofs == nil {
		proofs = ucanlib.NewContainerProofStore(container.New())
	}

	return &Client{
		ServiceID: serviceID,
		Issuer:    issuer,
		Proofs:    proofs,
		Executor:  httpExecutor,
		Logger:    cfg.logger,
	}, nil
}

// AuthorizeRequest invokes /s3/request/authorize. The returned container carries
// the delegations Hilt re-delegated to the invocation issuer.
func (c *Client) AuthorizeRequest(ctx context.Context, req s3.Request, opts ...MethodOption) (*s3req.AuthorizeOK, ucan.Container, error) {
	return invoke(ctx, c, s3req.Authorize, &s3req.AuthorizeArguments{Request: req}, opts...)
}

// CreateBucket invokes /s3/bucket/create. The returned container carries the
// delegation chains that now grant the access key access to the new bucket.
func (c *Client) CreateBucket(ctx context.Context, req s3.Request, opts ...MethodOption) (*s3req.AuthorizeOK, ucan.Container, error) {
	return invoke(ctx, c, s3bkt.Create, &s3bkt.CreateArguments{Request: req}, opts...)
}

// BucketInfo invokes /s3/bucket/info for the named bucket and access key. The
// returned container carries the bucket→access-key delegation chains.
func (c *Client) BucketInfo(ctx context.Context, name string, accessKey did.DID, opts ...MethodOption) (*s3bkt.InfoOK, ucan.Container, error) {
	return invoke(ctx, c, s3bkt.Info, &s3bkt.InfoArguments{Name: name, AccessKey: accessKey}, opts...)
}

// DeleteBucket invokes /s3/bucket/delete. It returns no delegations.
func (c *Client) DeleteBucket(ctx context.Context, req s3.Request, opts ...MethodOption) error {
	_, _, err := invoke(ctx, c, s3bkt.Delete, &s3bkt.DeleteArguments{Request: req}, opts...)
	return err
}

// ListBuckets invokes /s3/bucket/list. It returns no delegations.
func (c *Client) ListBuckets(ctx context.Context, req s3.Request, opts ...MethodOption) (*s3bkt.ListOK, error) {
	ok, _, err := invoke(ctx, c, s3bkt.List, &s3bkt.ListArguments{Request: req}, opts...)
	return ok, err
}

// invoke runs one command: it fetches the proof chain from the issuer to the Hilt
// service, signs and sends the invocation (subject = audience = Hilt), and
// unpacks the receipt. It returns the typed result and the response container so
// callers can extract any delegations Hilt attached.
func invoke[A, O binding.CBORValue](ctx context.Context, c *Client, cmd binding.Binding[A, O], args A, opts ...MethodOption) (O, ucan.Container, error) {
	var zero O
	cfg := &methodConfig{issuer: c.Issuer, proofs: c.Proofs}
	for _, opt := range opts {
		opt(cfg)
	}

	proofs, links, err := cfg.proofs.ProofChain(ctx, cfg.issuer.DID(), cmd.Command, c.ServiceID)
	if err != nil {
		return zero, nil, fmt.Errorf("hilt/client: getting proof chain: %w", err)
	}
	inv, err := cmd.Invoke(cfg.issuer, c.ServiceID, args,
		invocation.WithAudience(c.ServiceID),
		invocation.WithProofs(links...),
	)
	if err != nil {
		return zero, nil, fmt.Errorf("hilt/client: invoking %s: %w", cmd.Command, err)
	}
	log := zapucan.WithInvocation(c.Logger, inv)
	log.Debug("executing invocation")
	res, err := c.Executor.Execute(execution.NewRequest(ctx, inv, execution.WithDelegations(proofs...)))
	if err != nil {
		log.Error("failed to execute invocation", zap.Error(err))
		return zero, nil, fmt.Errorf("hilt/client: executing %s invocation: %w", cmd.Command, err)
	}
	ok, err := cmd.Unpack(res.Receipt())
	if err != nil {
		return zero, nil, fmt.Errorf("hilt/client: unpacking %s result: %w", cmd.Command, err)
	}
	return ok, res.Metadata(), nil
}
