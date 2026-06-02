package blockstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	indexclient "github.com/fil-forge/indexing-service/pkg/client"
	contentcmds "github.com/fil-forge/libforge/commands/content"
	"github.com/fil-forge/libforge/ucan/retrieval"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/execution"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/fil-forge/ucantone/ucan/invocation"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore/locator"
	"github.com/fil-forge/ingot/internal/ucanexec"
)

// retrievalAuthTTL bounds the lifetime of the self-issued retrieval proofs.
const retrievalAuthTTL = 60 // seconds

// ErrNotFound is returned by GetBlock when the indexing-service has no location
// commitment for the requested CID.
var ErrNotFound = errors.New("blockstore: not found")

// Forge is a read-only block reader that resolves CIDs through the Forge
// indexing-service and fetches the underlying bytes via authorized,
// UCAN-wrapped ranged reads against piri storage nodes.
//
// Used in ingot's "no_cache" mode: every Get goes to the network. The only
// caching is the small metadata cache inside the IndexLocator (digest ->
// location commitment), which resets on process restart.
type Forge struct {
	locator     locator.Locator
	signer      ucan.Signer // service identity (issuer of /content/retrieve invocations)
	spaceSigner ucan.Signer // space root authority (self-issues retrieval delegations)
	spaces      []did.DID
	httpClient  *http.Client
	logger      *zap.Logger
}

var _ BlockReader = (*Forge)(nil)

// ForgeConfig wires a read-only Forge block reader.
type ForgeConfig struct {
	// IndexerEndpoint is the indexing-service URL.
	IndexerEndpoint string
	// IndexerDID is the indexing-service principal.
	IndexerDID string
	// Spaces scopes the locator queries; for ingot this is the single space it owns.
	Spaces []did.DID
	// Signer is the upload-service identity; issuer of /content/retrieve invocations.
	Signer ucan.Signer
	// SpaceSigner is the keypair of the space ingot owns; root authority for the
	// self-issued space -> service /content/retrieve delegations.
	SpaceSigner ucan.Signer
	// HTTPClient is used for indexer queries and piri retrievals. Optional;
	// defaults to http.DefaultClient.
	HTTPClient *http.Client
	// Logger is optional.
	Logger *zap.Logger
}

// NewForge constructs a Forge block reader, building an indexing-service client
// wrapped by the index locator.
func NewForge(cfg ForgeConfig) (*Forge, error) {
	if cfg.IndexerEndpoint == "" {
		return nil, errors.New("forge blockstore: indexer endpoint is required")
	}
	if cfg.IndexerDID == "" {
		return nil, errors.New("forge blockstore: indexer DID is required")
	}
	if len(cfg.Spaces) == 0 {
		return nil, errors.New("forge blockstore: at least one space is required")
	}
	if cfg.Signer == nil {
		return nil, errors.New("forge blockstore: signer is required")
	}
	if cfg.SpaceSigner == nil {
		return nil, errors.New("forge blockstore: space signer is required")
	}

	endpointURL, err := url.Parse(cfg.IndexerEndpoint)
	if err != nil {
		return nil, fmt.Errorf("forge blockstore: parse indexer endpoint: %w", err)
	}
	indexerDID, err := did.Parse(cfg.IndexerDID)
	if err != nil {
		return nil, fmt.Errorf("forge blockstore: parse indexer DID: %w", err)
	}

	httpc := cfg.HTTPClient
	if httpc == nil {
		httpc = http.DefaultClient
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	idxClient, err := indexclient.New(indexerDID, *endpointURL, indexclient.WithHTTPClient(httpc))
	if err != nil {
		return nil, fmt.Errorf("forge blockstore: build indexing-service client: %w", err)
	}

	authFn := newAuthorizeRetrieval(cfg.SpaceSigner, indexerDID)
	loc := locator.NewIndexLocator(idxClient, authFn)

	return &Forge{
		locator:     loc,
		signer:      cfg.Signer,
		spaceSigner: cfg.SpaceSigner,
		spaces:      cfg.Spaces,
		httpClient:  httpc,
		logger:      logger,
	}, nil
}

// GetBlock resolves the CID through the indexer and retrieves the underlying
// bytes from piri via a UCAN-authorized /content/retrieve invocation, scoped to
// the inner block's byte range within the containing CAR shard.
func (f *Forge) GetBlock(ctx context.Context, c cid.Cid) (block.Block, error) {
	locations, err := f.locator.Locate(ctx, f.spaces, c.Hash())
	if err != nil {
		var nf locator.NotFoundError
		if errors.As(err, &nf) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("forge: locate %s: %w", c, err)
	}
	if len(locations) == 0 {
		return nil, ErrNotFound
	}

	loc := locations[0]
	cm := loc.Commitment
	if len(cm.Location) == 0 {
		return nil, fmt.Errorf("forge: empty location URL set for %s", c)
	}
	target := cm.Location[0]

	// space scopes the retrieve capability; fall back to our configured space if
	// the commitment lacks one.
	space := cm.Space
	if space == did.Undef {
		space = f.spaces[0]
	}

	// audience for the retrieve invocation is the storage provider that issued
	// the location commitment.
	provider := cm.Node

	// Self-issued retrieval proof: space -> service. Short-lived, per call.
	retrievalProof, err := contentcmds.Retrieve.Delegate(
		f.spaceSigner,
		f.signer.DID(),
		space,
		delegation.WithExpiration(ucan.Now()+retrievalAuthTTL),
	)
	if err != nil {
		return nil, fmt.Errorf("forge: build retrieval proof: %w", err)
	}

	inv, err := contentcmds.Retrieve.Invoke(
		f.signer,
		space,
		&contentcmds.RetrieveArguments{
			Blob:  contentcmds.Blob{Digest: cm.Content},
			Range: contentcmds.Range{Start: uint64(loc.Range.Start), End: uint64(loc.Range.End)},
		},
		invocation.WithAudience(provider),
		invocation.WithProofs(retrievalProof.Link()),
	)
	if err != nil {
		return nil, fmt.Errorf("forge: build retrieve invocation: %w", err)
	}

	rclient, err := retrieval.NewClient(target.URL(), retrieval.WithHTTPClient(f.httpClient))
	if err != nil {
		return nil, fmt.Errorf("forge: build retrieval client: %w", err)
	}

	_, _, meta, err := ucanexec.Execute[*contentcmds.RetrieveOK](
		ctx, rclient, inv,
		execution.WithDelegations(retrievalProof),
	)
	if err != nil {
		return nil, fmt.Errorf("forge: retrieve %s: %w", c, err)
	}

	hcRes, ok := meta.(*retrieval.HTTPHeaderResponseContainer)
	if !ok {
		return nil, fmt.Errorf("forge: unexpected retrieval metadata type %T", meta)
	}
	defer hcRes.Body.Close()

	body, err := io.ReadAll(hcRes.Body)
	if err != nil {
		return nil, fmt.Errorf("forge: read retrieve body for %s: %w", c, err)
	}
	// Range is inclusive, so the expected length is End - Start + 1.
	wantLen := loc.Range.End - loc.Range.Start + 1
	if int64(len(body)) != wantLen {
		return nil, fmt.Errorf("forge: %s short read: got %d bytes, want %d", c, len(body), wantLen)
	}

	return block.NewBlockWithCid(body, c)
}

// newAuthorizeRetrieval returns the AuthorizeRetrievalFunc the IndexLocator
// calls before each indexer query. The space signer (root authority) directly
// authorizes the indexer to retrieve any blob in the space — the proof chain is
// one hop (space -> indexer) because ingot's "user" is itself.
func newAuthorizeRetrieval(spaceSigner ucan.Signer, indexerDID did.DID) locator.AuthorizeRetrievalFunc {
	return func(ctx context.Context, spaces []did.DID) ([]ucan.Delegation, error) {
		dlgs := make([]ucan.Delegation, 0, len(spaces))
		for _, space := range spaces {
			dlg, err := contentcmds.Retrieve.Delegate(
				spaceSigner,
				indexerDID,
				space,
				delegation.WithExpiration(ucan.Now()+retrievalAuthTTL),
			)
			if err != nil {
				return nil, err
			}
			dlgs = append(dlgs, dlg)
		}
		return dlgs, nil
	}
}
