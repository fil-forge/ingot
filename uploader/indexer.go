package uploader

import (
	"context"
	"fmt"
	"net/url"

	assertcmds "github.com/fil-forge/libforge/commands/assert"
	contentcmds "github.com/fil-forge/libforge/commands/content"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/client"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/execution"
	"github.com/fil-forge/ucantone/ipld/datamodel"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/internal/ucanexec"
)

// IndexPublisher publishes an /assert/index claim to the indexing-service. The
// default implementation is a trimmed copy of fil-forge/sprue's indexerclient,
// carried here so ingot does not depend on sprue.
type IndexPublisher interface {
	PublishIndexClaim(ctx context.Context, space did.DID, index cid.Cid, proofs ucanlib.ProofStore, options ...invocation.Option) (ucan.Receipt, error)
}

type indexPublisher struct {
	indexerDID did.DID
	signer     ucan.Signer
	client     *client.HTTPClient
	logger     *zap.Logger
}

// NewIndexPublisher builds an IndexPublisher targeting the indexing-service at
// endpoint, signing as signer.
func NewIndexPublisher(endpoint *url.URL, indexerDID did.DID, signer ucan.Signer, logger *zap.Logger) (IndexPublisher, error) {
	hc, err := client.NewHTTP(endpoint)
	if err != nil {
		return nil, fmt.Errorf("creating HTTP client: %w", err)
	}
	return &indexPublisher{indexerDID: indexerDID, signer: signer, client: hc, logger: logger}, nil
}

// PublishIndexClaim sends an /assert/index claim to the indexer. The proofStore
// supplies the /content/retrieve proof chain (space -> signer) authorizing the
// indexer to fetch the index blob; this publisher re-delegates from signer to
// the indexer using that chain.
func (c *indexPublisher) PublishIndexClaim(ctx context.Context, space did.DID, index cid.Cid, proofs ucanlib.ProofStore, options ...invocation.Option) (ucan.Receipt, error) {
	prfs, prfLinks, err := proofs.ProofChain(ctx, c.signer.DID(), contentcmds.Retrieve.Command, space)
	if err != nil {
		return nil, fmt.Errorf("building proof chain: %w", err)
	}
	attestations, err := proofs.ProofAttestations(ctx, prfs, c.signer.DID())
	if err != nil {
		return nil, fmt.Errorf("building attestations: %w", err)
	}
	indexerDelegation, err := contentcmds.Retrieve.Delegate(c.signer, c.indexerDID, space)
	if err != nil {
		return nil, fmt.Errorf("creating indexer delegation: %w", err)
	}

	options = append([]invocation.Option{
		invocation.WithAudience(c.indexerDID),
		invocation.WithMetadata(
			datamodel.Map{"retrievalAuth": append(prfLinks, indexerDelegation.Link())},
		),
	}, options...)

	inv, err := assertcmds.Index.Invoke(
		c.signer,
		c.signer.DID(),
		&assertcmds.IndexArguments{Index: index},
		options...,
	)
	if err != nil {
		return nil, fmt.Errorf("creating invocation: %w", err)
	}

	_, rcpt, _, err := ucanexec.Execute[*assertcmds.IndexOK](
		ctx, c.client, inv,
		execution.WithDelegations(prfs...),
		execution.WithDelegations(indexerDelegation),
		execution.WithInvocations(attestations...),
	)
	if err != nil {
		return nil, fmt.Errorf("executing assert index invocation: %w", err)
	}
	return rcpt, nil
}

var _ IndexPublisher = (*indexPublisher)(nil)
