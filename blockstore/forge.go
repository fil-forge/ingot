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
	mh "github.com/multiformats/go-multihash"
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
	signer      ucan.Issuer // service identity (issuer of /content/retrieve invocations)
	spaceSigner ucan.Issuer // space root authority (self-issues retrieval delegations)
	spaces      []did.DID
	httpClient  *http.Client
	logger      *zap.Logger
}

var (
	_ BlockReader = (*Forge)(nil)
	_ BlobReader  = (*Forge)(nil)
)

// ForgeConfig wires a read-only Forge block reader.
type ForgeConfig struct {
	// Locator, when set, resolves blob locations instead of the indexing-service
	// — the appliance read tier (e.g. registry.LocalLocator over blob_locations).
	// When nil, an indexing-service-backed locator is built from
	// IndexerEndpoint/IndexerDID. Either way the retrieval path is identical.
	Locator locator.Locator
	// IndexerEndpoint is the indexing-service URL. Required only when Locator is nil.
	IndexerEndpoint string
	// IndexerDID is the indexing-service principal. Required only when Locator is nil.
	IndexerDID string
	// Spaces scopes the locator queries; for ingot this is the single space it owns.
	Spaces []did.DID
	// Signer is the upload-service identity; issuer of /content/retrieve invocations.
	Signer ucan.Issuer
	// SpaceSigner is the keypair of the space ingot owns; root authority for the
	// self-issued space -> service /content/retrieve delegations.
	SpaceSigner ucan.Issuer
	// HTTPClient is used for indexer queries and piri retrievals. Optional;
	// defaults to http.DefaultClient.
	HTTPClient *http.Client
	// Logger is optional.
	Logger *zap.Logger
}

// NewForge constructs a Forge block reader. It uses cfg.Locator when set (the
// appliance read tier); otherwise it builds an indexing-service-backed locator
// from IndexerEndpoint/IndexerDID.
func NewForge(cfg ForgeConfig) (*Forge, error) {
	if len(cfg.Spaces) == 0 {
		return nil, errors.New("forge blockstore: at least one space is required")
	}
	if cfg.Signer == nil {
		return nil, errors.New("forge blockstore: signer is required")
	}
	if cfg.SpaceSigner == nil {
		return nil, errors.New("forge blockstore: space signer is required")
	}

	httpc := cfg.HTTPClient
	if httpc == nil {
		httpc = http.DefaultClient
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	loc := cfg.Locator
	if loc == nil {
		// No locator injected: resolve through the indexing-service.
		if cfg.IndexerEndpoint == "" {
			return nil, errors.New("forge blockstore: indexer endpoint is required (no locator injected)")
		}
		if cfg.IndexerDID == "" {
			return nil, errors.New("forge blockstore: indexer DID is required (no locator injected)")
		}
		endpointURL, err := url.Parse(cfg.IndexerEndpoint)
		if err != nil {
			return nil, fmt.Errorf("forge blockstore: parse indexer endpoint: %w", err)
		}
		indexerDID, err := did.Parse(cfg.IndexerDID)
		if err != nil {
			return nil, fmt.Errorf("forge blockstore: parse indexer DID: %w", err)
		}
		idxClient, err := indexclient.New(indexerDID, *endpointURL, indexclient.WithHTTPClient(httpc))
		if err != nil {
			return nil, fmt.Errorf("forge blockstore: build indexing-service client: %w", err)
		}
		loc = locator.NewIndexLocator(idxClient, newAuthorizeRetrieval(cfg.SpaceSigner, indexerDID))
	}

	return &Forge{
		locator:     loc,
		signer:      cfg.Signer,
		spaceSigner: cfg.SpaceSigner,
		spaces:      cfg.Spaces,
		httpClient:  httpc,
		logger:      logger,
	}, nil
}

// retrieve resolves c through the locator and issues a UCAN-authorized
// /content/retrieve to the piri node that holds it, returning the response body
// stream and the expected byte length (the location Range is inclusive, so
// End-Start+1). The caller owns the returned reader and must Close it. Shared by
// GetBlock (which buffers small catalog blocks) and OpenBlob (which streams large
// body blobs straight through).
func (f *Forge) retrieve(ctx context.Context, c cid.Cid) (io.ReadCloser, int64, error) {
	locations, err := f.locator.Locate(ctx, f.spaces, c.Hash())
	if err != nil {
		var nf locator.NotFoundError
		if errors.As(err, &nf) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, fmt.Errorf("forge: locate %s: %w", c, err)
	}
	if len(locations) == 0 {
		return nil, 0, ErrNotFound
	}

	loc := locations[0]
	cm := loc.Commitment
	if len(cm.Location) == 0 {
		return nil, 0, fmt.Errorf("forge: empty location URL set for %s", c)
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
		return nil, 0, fmt.Errorf("forge: build retrieval proof: %w", err)
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
		return nil, 0, fmt.Errorf("forge: build retrieve invocation: %w", err)
	}

	rclient, err := retrieval.NewClient(target.URL(), retrieval.WithHTTPClient(f.httpClient))
	if err != nil {
		return nil, 0, fmt.Errorf("forge: build retrieval client: %w", err)
	}

	_, _, meta, err := ucanexec.Execute[*contentcmds.RetrieveOK](
		ctx, rclient, inv,
		execution.WithDelegations(retrievalProof),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("forge: retrieve %s: %w", c, err)
	}

	hcRes, ok := meta.(*retrieval.HTTPHeaderResponseContainer)
	if !ok {
		return nil, 0, fmt.Errorf("forge: unexpected retrieval metadata type %T", meta)
	}
	// Range is inclusive, so the expected length is End - Start + 1.
	return hcRes.Body, loc.Range.End - loc.Range.Start + 1, nil
}

// GetBlock resolves the CID through the locator and retrieves the bytes from piri
// via a UCAN-authorized /content/retrieve. It buffers the whole block, so it is
// for small catalog blocks; object-body blobs use the streaming OpenBlob.
func (f *Forge) GetBlock(ctx context.Context, c cid.Cid) (block.Block, error) {
	rc, wantLen, err := f.retrieve(ctx, c)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	body, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("forge: read retrieve body for %s: %w", c, err)
	}
	if int64(len(body)) != wantLen {
		return nil, fmt.Errorf("forge: %s short read: got %d bytes, want %d", c, len(body), wantLen)
	}
	return block.NewBlockWithCid(body, c)
}

// OpenBlob streams an object-body blob from piri by digest, without buffering it
// in memory — the network counterpart of Spool.OpenBlob. Bytes are served
// straight off the /content/retrieve response; the caller owns the reader and
// must Close it.
func (f *Forge) OpenBlob(ctx context.Context, digest mh.Multihash) (io.ReadCloser, error) {
	rc, _, err := f.retrieve(ctx, cid.NewCidV1(cid.Raw, digest))
	return rc, err
}

// newAuthorizeRetrieval returns the AuthorizeRetrievalFunc the IndexLocator
// calls before each indexer query. The space signer (root authority) directly
// authorizes the indexer to retrieve any blob in the space — the proof chain is
// one hop (space -> indexer) because ingot's "user" is itself.
func newAuthorizeRetrieval(spaceSigner ucan.Issuer, indexerDID did.DID) locator.AuthorizeRetrievalFunc {
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
