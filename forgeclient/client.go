// Package forgeclient is ingot's Forge edge-client: it invokes
// /blob/add, /ucan/conclude, /index/add (and the /access login flow)
// against the upload service (sprue), behaving like a guppy node.
//
// It is a carried, trimmed copy of github.com/fil-forge/guppy/pkg/client
// (ingot cannot import guppy: guppy embeds ingot → import cycle). The
// blob-retrieval path is dropped (ingot reads via blockstore/forge.go),
// the generic Execute is delegated to internal/ucanexec, and the
// go-log/otel logging is replaced with an injected *zap.Logger. Keep in
// sync with upstream; see DESIGN_NOTES.md §B.
package forgeclient

import (
	"context"
	"fmt"
	"net/url"

	"github.com/fil-forge/ingot/internal/ucanexec"
	"github.com/fil-forge/ingot/tokenstore"
	"github.com/fil-forge/libforge/receipt"
	"github.com/fil-forge/ucantone/client"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/execution"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/zap"
)

// Client is a UCAN-over-HTTP client to the upload service (sprue).
type Client struct {
	signer         ucan.Signer
	serviceID      did.DID
	ucanClient     *client.HTTPClient
	ucanOpts       []client.HTTPOption
	receiptsClient *receipt.Client
	tokenStore     tokenstore.Store
	logger         *zap.Logger
}

// New builds a Client. signer is the agent identity (issuer of every
// invocation); serviceID + serviceURL address sprue.
func New(signer ucan.Signer, serviceID did.DID, serviceURL url.URL, options ...Option) (*Client, error) {
	c := Client{
		signer:         signer,
		serviceID:      serviceID,
		receiptsClient: receipt.NewClient(serviceURL.JoinPath("/receipt/")),
		logger:         zap.NewNop(),
	}

	for _, opt := range options {
		if err := opt(&c); err != nil {
			return nil, err
		}
	}

	if c.tokenStore == nil {
		c.tokenStore = tokenstore.NewMemStore()
	}

	ucanClient, err := client.NewHTTP(&serviceURL, c.ucanOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating UCAN client: %w", err)
	}
	c.ucanClient = ucanClient

	return &c, nil
}

// DID returns the DID of the agent.
func (c *Client) DID() did.DID { return c.signer.DID() }

// Issuer returns the issuing signer of the agent.
func (c *Client) Issuer() ucan.Signer { return c.signer }

// ServiceID returns the DID of the upload service this client targets.
func (c *Client) ServiceID() did.DID { return c.serviceID }

// ProofChain builds a delegation proof chain from aud to sub for cmd.
func (c *Client) ProofChain(ctx context.Context, aud did.DID, cmd ucan.Command, sub did.DID) ([]ucan.Delegation, []cid.Cid, error) {
	return c.tokenStore.ProofChain(ctx, aud, cmd, sub)
}

// ProofAttestations returns attestations for proofs needing them.
func (c *Client) ProofAttestations(ctx context.Context, proofs []ucan.Delegation, authority did.DID) ([]ucan.Invocation, error) {
	return c.tokenStore.ProofAttestations(ctx, proofs, authority)
}

// AddProofs adds delegations to the client's token store.
func (c *Client) AddProofs(ctx context.Context, delegations ...ucan.Delegation) error {
	return c.tokenStore.AddDelegations(ctx, delegations...)
}

// AddAttestations adds attestations to the client's token store.
func (c *Client) AddAttestations(ctx context.Context, attestations ...ucan.Invocation) error {
	return c.tokenStore.AddInvocations(ctx, attestations...)
}

// Reset clears all tokens from the token store.
func (c *Client) Reset(ctx context.Context) error {
	return c.tokenStore.Reset(ctx)
}

// Execute forwards to internal/ucanexec.Execute. Kept as a package-local
// generic so the ported client code reads unchanged.
func Execute[T cbg.CBORUnmarshaler](
	ctx context.Context,
	executor execution.Executor,
	inv ucan.Invocation,
	options ...execution.RequestOption,
) (T, ucan.Receipt, ucan.Container, error) {
	return ucanexec.Execute[T](ctx, executor, inv, options...)
}
