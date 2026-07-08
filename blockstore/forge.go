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
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/internal/ucanexec"
)

// retrievalAuthTTL bounds the lifetime of per-space retrieval re-delegations
// issued to the indexer (see newAuthorizeRetrieval).
const retrievalAuthTTL = 60 // seconds

// ErrNotFound is returned by GetBlock when the indexing-service has no location
// commitment for the requested CID.
var ErrNotFound = errors.New("blockstore: not found")

// Forge is a read-only block reader that resolves CIDs through a locator
// (the local blob_locations table, or the Forge indexing-service) and
// fetches the underlying bytes via authorized, UCAN-wrapped ranged reads
// against piri storage nodes.
//
// Reads are per-space: every method takes the owning bucket's space DID,
// which selects the blob location and scopes the /content/retrieve
// invocation. Retrieval authority (a space→…→agent proof chain) comes from
// the request-scoped proof store the auth layer stashes on the context
// ([reqscope.ProofStore]) — the store of the access key that made the
// request, so a read can only use that key's own delegations. A request
// with no scoped store (e.g. the root account, which holds no Hilt
// delegations) cannot retrieve.
type Forge struct {
	locator    locator.Locator
	signer     ucan.Issuer // service identity (issuer of /content/retrieve invocations)
	httpClient *http.Client
	logger     *zap.Logger
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
	// Signer is the upload-service identity; issuer of /content/retrieve invocations.
	Signer ucan.Issuer
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
	if cfg.Signer == nil {
		return nil, errors.New("forge blockstore: signer is required")
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
		loc = locator.NewIndexLocator(idxClient, newAuthorizeRetrieval(cfg.Signer, indexerDID))
	}

	return &Forge{
		locator:    loc,
		signer:     cfg.Signer,
		httpClient: httpc,
		logger:     logger,
	}, nil
}

// retrieve resolves c within space through the locator and issues a
// UCAN-authorized /content/retrieve to the piri node that holds it, returning
// the response body stream and the expected byte length (the location Range
// is inclusive, so End-Start+1). The caller owns the returned reader and must
// Close it. Shared by GetBlock (which buffers small catalog blocks) and
// OpenBlob (which streams large body blobs straight through).
func (f *Forge) retrieve(ctx context.Context, space did.DID, c cid.Cid) (io.ReadCloser, int64, error) {
	locations, err := f.locator.Locate(ctx, []did.DID{space}, c.Hash())
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

	// The commitment's space scopes the retrieve capability; fall back to
	// the caller's space if the commitment lacks one (the local locator
	// always stamps it).
	if cm.Space != did.Undef {
		space = cm.Space
	}

	// audience for the retrieve invocation is the storage provider that issued
	// the location commitment.
	provider := cm.Node

	// Retrieval authority is the requesting access key's space→…→agent
	// chain, from the proof store the auth layer scoped onto this request's
	// context. No store (e.g. a root-account read) or an empty chain is an
	// auth gap, not absent data — surface it explicitly.
	store, ok := reqscope.ProofStore(ctx)
	if !ok {
		return nil, 0, fmt.Errorf("forge: no request-scoped retrieval authority for space %s", space)
	}
	proofs, links, err := store.ProofChain(ctx, f.signer.DID(), contentcmds.Retrieve.Command, space)
	if err != nil {
		return nil, 0, fmt.Errorf("forge: retrieval proof chain for %s: %w", space, err)
	}
	if len(proofs) == 0 {
		return nil, 0, fmt.Errorf("forge: no retrieval authority for space %s (no cached delegation chain)", space)
	}

	inv, err := contentcmds.Retrieve.Invoke(
		f.signer,
		space,
		&contentcmds.RetrieveArguments{
			Blob:  contentcmds.Blob{Digest: cm.Content},
			Range: contentcmds.Range{Start: uint64(loc.Range.Start), End: uint64(loc.Range.End)},
		},
		invocation.WithAudience(provider),
		invocation.WithProofs(links...),
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
		execution.WithDelegations(proofs...),
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
func (f *Forge) GetBlock(ctx context.Context, space did.DID, c cid.Cid) (block.Block, error) {
	rc, wantLen, err := f.retrieve(ctx, space, c)
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
func (f *Forge) OpenBlob(ctx context.Context, space did.DID, digest mh.Multihash) (io.ReadCloser, error) {
	rc, _, err := f.retrieve(ctx, space, cid.NewCidV1(cid.Raw, digest))
	return rc, err
}

// newAuthorizeRetrieval returns the AuthorizeRetrievalFunc the IndexLocator
// calls before each indexer query. Per space, the agent re-delegates
// /content/retrieve to the indexer (short-lived) backed by its own cached
// space→…→agent chain from the proof store — the same authority the retrieve
// path uses. Spaces with no cached chain are skipped: the indexer simply
// won't serve those spaces' locations until an authorize supplies one.
func newAuthorizeRetrieval(signer ucan.Issuer, indexerDID did.DID) locator.AuthorizeRetrievalFunc {
	return func(ctx context.Context, spaces []did.DID) ([]ucan.Delegation, error) {
		store, ok := reqscope.ProofStore(ctx)
		if !ok {
			return nil, nil
		}
		var dlgs []ucan.Delegation
		for _, space := range spaces {
			chain, _, err := store.ProofChain(ctx, signer.DID(), contentcmds.Retrieve.Command, space)
			if err != nil {
				return nil, err
			}
			if len(chain) == 0 {
				continue
			}
			// The re-delegation's backing chain travels alongside it (the
			// same shape forgeclient uses): proofs attach at execution
			// time, not on the delegation itself.
			dlg, err := contentcmds.Retrieve.Delegate(
				signer,
				indexerDID,
				space,
				delegation.WithExpiration(ucan.Now()+retrievalAuthTTL),
			)
			if err != nil {
				return nil, err
			}
			dlgs = append(dlgs, dlg)
			dlgs = append(dlgs, chain...)
		}
		return dlgs, nil
	}
}
