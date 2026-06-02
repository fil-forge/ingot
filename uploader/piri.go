package uploader

import (
	"context"
	"fmt"
	"slices"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/client"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/execution"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/fil-forge/ucantone/ucan/promise"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/internal/ucanexec"
)

// AllocateRequest are the parameters for a /blob/allocate invocation.
type AllocateRequest struct {
	Space  did.DID
	Digest []byte
	Size   uint64
	Cause  cid.Cid
}

// AcceptRequest are the parameters for a /blob/accept invocation.
type AcceptRequest struct {
	Space  did.DID
	Digest []byte
	Size   uint64
	Put    cid.Cid // link to the /http/put task that uploaded the blob
}

// BlobClient is the seam ingot uses to allocate and accept a blob against a
// single piri node. The default implementation (newPiriClient) speaks ucantone
// over HTTP; it is a trimmed copy of fil-forge/sprue's piriclient (the
// non-replica paths), carried here so ingot does not depend on sprue.
type BlobClient interface {
	Allocate(ctx context.Context, req *AllocateRequest, proofs ucanlib.ProofStore, options ...invocation.Option) (*blobcmds.AllocateOK, ucan.Invocation, ucan.Receipt, error)
	Accept(ctx context.Context, req *AcceptRequest, proofs ucanlib.ProofStore, options ...invocation.Option) (*blobcmds.AcceptOK, ucan.Invocation, ucan.Receipt, ucan.Container, error)
}

// ClientFactory builds a BlobClient for the selected provider. ForgeConfig
// defaults this to newPiriClient; hosts may override to inject their own.
type ClientFactory func(provider ProviderInfo, signer ucan.Signer, logger *zap.Logger) (BlobClient, error)

type piriClient struct {
	piriDID did.DID
	signer  ucan.Signer
	client  *client.HTTPClient
	logger  *zap.Logger
}

func newPiriClient(provider ProviderInfo, signer ucan.Signer, logger *zap.Logger) (BlobClient, error) {
	endpoint := provider.Endpoint
	hc, err := client.NewHTTP(&endpoint)
	if err != nil {
		return nil, fmt.Errorf("creating HTTP client: %w", err)
	}
	return &piriClient{piriDID: provider.ID, signer: signer, client: hc, logger: logger}, nil
}

func (c *piriClient) Allocate(ctx context.Context, req *AllocateRequest, proofs ucanlib.ProofStore, options ...invocation.Option) (*blobcmds.AllocateOK, ucan.Invocation, ucan.Receipt, error) {
	prfs, prfLinks, err := proofs.ProofChain(ctx, c.signer.DID(), blobcmds.Allocate.Command, req.Space)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("building proof chain: %w", err)
	}
	attestations, err := proofs.ProofAttestations(ctx, prfs, c.signer.DID())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("getting proof attestations: %w", err)
	}

	options = slices.Clone(options)
	options = append(options, invocation.WithAudience(c.piriDID), invocation.WithProofs(prfLinks...))

	inv, err := blobcmds.Allocate.Invoke(
		c.signer,
		req.Space,
		&blobcmds.AllocateArguments{
			Blob:  blobcmds.Blob{Digest: req.Digest, Size: req.Size},
			Cause: req.Cause,
		},
		options...,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating allocate invocation: %w", err)
	}

	allocOK, rcpt, _, err := ucanexec.Execute[*blobcmds.AllocateOK](
		ctx, c.client, inv,
		execution.WithDelegations(prfs...),
		execution.WithInvocations(attestations...),
	)
	if err != nil {
		return nil, nil, nil, err
	}
	return allocOK, inv, rcpt, nil
}

func (c *piriClient) Accept(ctx context.Context, req *AcceptRequest, proofs ucanlib.ProofStore, options ...invocation.Option) (*blobcmds.AcceptOK, ucan.Invocation, ucan.Receipt, ucan.Container, error) {
	prfs, prfLinks, err := proofs.ProofChain(ctx, c.signer.DID(), blobcmds.Accept.Command, req.Space)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("building proof chain: %w", err)
	}
	attestations, err := proofs.ProofAttestations(ctx, prfs, c.signer.DID())
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("getting proof attestations: %w", err)
	}

	options = slices.Clone(options)
	options = append(options, invocation.WithAudience(c.piriDID), invocation.WithProofs(prfLinks...))

	inv, err := blobcmds.Accept.Invoke(
		c.signer,
		req.Space,
		&blobcmds.AcceptArguments{
			Blob: blobcmds.Blob{Digest: req.Digest, Size: req.Size},
			Put:  promise.AwaitOK{Task: req.Put},
		},
		options...,
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("creating accept invocation: %w", err)
	}

	acceptOK, rcpt, meta, err := ucanexec.Execute[*blobcmds.AcceptOK](
		ctx, c.client, inv,
		execution.WithDelegations(prfs...),
		execution.WithInvocations(attestations...),
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return acceptOK, inv, rcpt, meta, nil
}

var _ BlobClient = (*piriClient)(nil)
