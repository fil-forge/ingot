package uploader

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	nethttp "net/http"
	"os"

	"github.com/fil-forge/libforge/blobindex"
	blobcmds "github.com/fil-forge/libforge/commands/blob"
	contentcmds "github.com/fil-forge/libforge/commands/content"
	httpcmds "github.com/fil-forge/libforge/commands/http"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ipld/datamodel"
	ed25519signer "github.com/fil-forge/ucantone/principal/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/fil-forge/ucantone/ucan/promise"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
)

// Uploader is the seam between the log flusher and durable Forge storage.
type Uploader interface {
	// SubmitCAR ships one sealed CAR file (one log segment) to Forge. The
	// implementation streams the file body straight into the HTTP PUT, never
	// materializing it as a []block.Block or re-encoding it as a CAR.
	SubmitCAR(ctx context.Context, roots []cid.Cid, src CARSource) error
}

// CARSource describes a sealed CAR file ready to ship. All fields refer to data
// that already exists on disk or was precomputed at seal time.
type CARSource struct {
	// Path is the absolute path to the sealed CAR file. SubmitCAR streams from
	// this path into the HTTP PUT body.
	Path string
	// Size is the file's byte length, set as the request's Content-Length.
	Size int64
	// SHA256 is the SHA-256 multihash of the CAR's bytes, reused both as the
	// blob digest in allocate/accept and as the CAR digest the index is keyed by.
	SHA256 multihash.Multihash
	// Positions maps each block's CID to its offset/length inside the CAR file.
	Positions map[cid.Cid]blockstore.BlockLoc
}

// Forge is an Uploader that ships CARs to Forge. It plays the orchestrator role
// the forge prototype gave it: select a provider, allocate + HTTP PUT +
// accept the data CAR via that piri, build & ship a sharded-dag-index blob, and
// publish an /assert/index claim against the indexing-service.
//
// ingot owns the *space* (root UCAN authority); it self-issues the
// space -> service delegations (for /blob/allocate, /blob/accept,
// /content/retrieve) that authorize the service signer's invocations to piri.
//
// NOTE: this is a faithful translation of the forge prototype's flow to the
// fil-forge ucantone/libforge APIs. The fil-forge upload pipeline additionally
// expects the /http/put task to carry a receipt (via /ucan/conclude) before
// /blob/accept resolves; wiring that against a live piri is follow-up
// integration work (this path is compiled but not exercised by the in-memory
// test suite).
type Forge struct {
	selector    ProviderSelector
	newClient   ClientFactory
	indexer     IndexPublisher
	signer      ucan.Signer // service identity (issuer of invocations to piri)
	spaceSigner ucan.Signer // space root authority
	proofs      ucanlib.ProofStore
	httpClient  *nethttp.Client
	logger      *zap.Logger
}

// ForgeConfig wires the collaborators of a Forge uploader.
//
// Signer is the upload-service identity used to invoke against piri and as the
// audience of the self-issued space delegations. SpaceSigner is the keypair of
// the space ingot owns; it acts as the root authority for those delegations.
type ForgeConfig struct {
	Selector       ProviderSelector
	IndexPublisher IndexPublisher
	Signer         ucan.Signer
	SpaceSigner    ucan.Signer
	// ClientFactory builds the per-provider BlobClient. Optional; defaults to
	// the carried ucantone piri client.
	ClientFactory ClientFactory
	HTTPClient    *nethttp.Client // optional; defaults to nethttp.DefaultClient
	Logger        *zap.Logger
}

// NewForge validates the config and returns a Forge uploader.
func NewForge(cfg ForgeConfig) (*Forge, error) {
	if cfg.Selector == nil {
		return nil, errors.New("uploader: provider selector is required")
	}
	if cfg.IndexPublisher == nil {
		return nil, errors.New("uploader: index publisher is required")
	}
	if cfg.Signer == nil {
		return nil, errors.New("uploader: signer is required")
	}
	if cfg.SpaceSigner == nil {
		return nil, errors.New("uploader: space signer is required")
	}
	factory := cfg.ClientFactory
	if factory == nil {
		factory = newPiriClient
	}
	httpc := cfg.HTTPClient
	if httpc == nil {
		httpc = nethttp.DefaultClient
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	proofs, err := selfIssuedProofs(cfg.SpaceSigner, cfg.Signer.DID())
	if err != nil {
		return nil, fmt.Errorf("uploader: self-issue proofs: %w", err)
	}

	return &Forge{
		selector:    cfg.Selector,
		newClient:   factory,
		indexer:     cfg.IndexPublisher,
		signer:      cfg.Signer,
		spaceSigner: cfg.SpaceSigner,
		proofs:      proofs,
		httpClient:  httpc,
		logger:      logger,
	}, nil
}

// selfIssuedProofs builds the proof store ingot presents to piri and the
// indexer: space -> service delegations for the capabilities ingot invokes.
func selfIssuedProofs(spaceSigner ucan.Signer, serviceDID did.DID) (ucanlib.ProofStore, error) {
	space := spaceSigner.DID()
	allocDlg, err := blobcmds.Allocate.Delegate(spaceSigner, serviceDID, space, delegation.WithNoExpiration())
	if err != nil {
		return nil, err
	}
	acceptDlg, err := blobcmds.Accept.Delegate(spaceSigner, serviceDID, space, delegation.WithNoExpiration())
	if err != nil {
		return nil, err
	}
	retrieveDlg, err := contentcmds.Retrieve.Delegate(spaceSigner, serviceDID, space, delegation.WithNoExpiration())
	if err != nil {
		return nil, err
	}
	ct := container.New(container.WithDelegations(allocDlg, acceptDlg, retrieveDlg))
	return ucanlib.NewContainerProofStore(ct), nil
}

// SpaceDID returns the DID of the space ingot owns.
func (u *Forge) SpaceDID() did.DID { return u.spaceSigner.DID() }

func (u *Forge) SubmitCAR(ctx context.Context, roots []cid.Cid, src CARSource) error {
	if len(roots) == 0 {
		return errors.New("uploader: at least one root required")
	}
	if src.Size <= 0 || len(src.Positions) == 0 {
		return nil
	}

	// 1. PUT the data CAR by streaming from disk.
	putCAR := func(url string, headers map[string]string) error {
		return httpPutFile(ctx, u.httpClient, url, headers, src.Path, src.Size)
	}
	if err := u.uploadBlob(ctx, src.SHA256, uint64(src.Size), putCAR); err != nil {
		return fmt.Errorf("uploader: ship car: %w", err)
	}

	// 2. Build a sharded-dag-index keyed off the CAR's multihash from the
	//    precomputed positions, and archive it to an in-memory CAR.
	view := blobindex.NewShardedDagIndex(1)
	for c, loc := range src.Positions {
		view.SetSlice(src.SHA256, c.Hash(), blobindex.Range{
			Start: int64(loc.Offset),
			End:   int64(loc.Offset + loc.Length - 1),
		})
	}
	var indexBuf bytes.Buffer
	if err := view.Archive(&indexBuf); err != nil {
		return fmt.Errorf("uploader: archive index: %w", err)
	}
	indexBytes := indexBuf.Bytes()
	indexDigest, err := multihash.Sum(indexBytes, multihash.SHA2_256, -1)
	if err != nil {
		return fmt.Errorf("uploader: hash index: %w", err)
	}

	// 3. PUT the index blob (small: one entry per inner CID).
	putIndex := func(url string, headers map[string]string) error {
		return httpPut(ctx, u.httpClient, url, headers, indexBytes)
	}
	if err := u.uploadBlob(ctx, indexDigest, uint64(len(indexBytes)), putIndex); err != nil {
		return fmt.Errorf("uploader: ship index: %w", err)
	}

	// 4. Publish the /assert/index claim. The indexer fetches our index blob
	//    from piri (authorized by the space -> service /content/retrieve
	//    delegation in our proof store, which the publisher re-delegates to the
	//    indexer).
	indexCID := cid.NewCidV1(uint64(multicodec.Car), indexDigest)
	if _, err := u.indexer.PublishIndexClaim(ctx, u.SpaceDID(), indexCID, u.proofs); err != nil {
		return fmt.Errorf("uploader: publish index claim: %w", err)
	}
	return nil
}

// uploadBlob runs the allocate -> PUT -> accept dance for one blob. putBody is
// invoked at most once, after a successful Allocate, with the URL and headers
// piri returned. If Allocate reports the blob is already present (Address ==
// nil) putBody is skipped and accept proceeds.
func (u *Forge) uploadBlob(
	ctx context.Context,
	digest multihash.Multihash,
	size uint64,
	putBody func(url string, headers map[string]string) error,
) error {
	blob := blobcmds.Blob{Digest: digest, Size: size}

	// Synthesize a self-issued /blob/add invocation as the cause; its task link
	// feeds piri's audit chain. Never sent over the wire.
	causeInv, err := blobcmds.Add.Invoke(
		u.signer, u.SpaceDID(), &blobcmds.AddArguments{Blob: blob},
		invocation.WithAudience(u.signer.DID()),
	)
	if err != nil {
		return fmt.Errorf("synthesize cause: %w", err)
	}
	cause := causeInv.Task().Link()

	provider, err := u.selector.SelectStorageProvider(ctx, blob)
	if err != nil {
		return fmt.Errorf("select provider: %w", err)
	}
	log := u.logger.With(
		zap.Stringer("provider", provider.ID),
		zap.String("endpoint", provider.Endpoint.String()),
	)

	client, err := u.newClient(provider, u.signer, log)
	if err != nil {
		return fmt.Errorf("piri client: %w", err)
	}

	allocOK, allocInv, _, err := client.Allocate(ctx, &AllocateRequest{
		Space:  u.SpaceDID(),
		Digest: digest,
		Size:   blob.Size,
		Cause:  cause,
	}, u.proofs)
	if err != nil {
		return fmt.Errorf("allocate: %w", err)
	}

	// PUT bytes if piri allocated a fresh slot. If Address is nil piri already
	// has the blob; skip the upload.
	if allocOK.Address != nil {
		if err := putBody(allocOK.Address.URL.URL().String(), allocOK.Address.Headers); err != nil {
			return fmt.Errorf("http put: %w", err)
		}
	}

	// Synthesize the /http/put invocation so accept has a stable Put task link
	// to chain off (mirrors genPut in sprue's blob_add handler).
	putInv, err := synthesizePut(blob, allocInv)
	if err != nil {
		return fmt.Errorf("synthesize put: %w", err)
	}

	if _, _, _, _, err := client.Accept(ctx, &AcceptRequest{
		Space:  u.SpaceDID(),
		Digest: digest,
		Size:   blob.Size,
		Put:    putInv.Task().Link(),
	}, u.proofs, invocation.WithNoNonce()); err != nil {
		return fmt.Errorf("accept: %w", err)
	}
	return nil
}

// synthesizePut mirrors genPut in sprue's blob_add handler: derive a principal
// from the blob's digest and issue an /http/put invocation whose Destination
// promise awaits the allocate task. The derived principal's keys are embedded
// in metadata so a holder of the blob can sign the put receipt. The invocation
// is never executed; we only need its task link for AcceptRequest.Put.
func synthesizePut(blob blobcmds.Blob, allocInv ucan.Invocation) (ucan.Invocation, error) {
	provider, err := deriveDIDFromDigest(blob.Digest)
	if err != nil {
		return nil, err
	}
	return httpcmds.Put.Invoke(
		provider,
		provider.DID(),
		&httpcmds.PutArguments{
			Body:        blob,
			Destination: promise.AwaitOK{Task: allocInv.Task().Link()},
		},
		invocation.WithAudience(provider.DID()),
		invocation.WithMetadata(datamodel.Map{
			"keys": datamodel.Map{
				"id": provider.DID().String(),
				"keys": datamodel.Map{
					provider.DID().String(): provider.Bytes(),
				},
			},
		}),
	)
}

// deriveDIDFromDigest mirrors deriveDID in sprue's blob_add handler. The
// derived principal is deterministic per digest.
func deriveDIDFromDigest(digest multihash.Multihash) (ed25519signer.Signer, error) {
	if len(digest) < ed25519.SeedSize {
		return nil, fmt.Errorf("digest too short for ed25519 seed: %d < %d", len(digest), ed25519.SeedSize)
	}
	seed := digest[len(digest)-ed25519.SeedSize:]
	return ed25519signer.FromRaw(seed)
}

func httpPut(ctx context.Context, client *nethttp.Client, urlStr string, headers map[string]string, body []byte) error {
	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPut, urlStr, bytes.NewReader(body))
	if err != nil {
		return err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http put status %s", resp.Status)
	}
	return nil
}

// httpPutFile streams a file body to the given URL. Setting req.ContentLength
// explicitly keeps net/http from defaulting to chunked transfer encoding on a
// non-Reader body — piri's PUT endpoint requires Content-Length.
func httpPutFile(ctx context.Context, client *nethttp.Client, urlStr string, headers map[string]string, path string, size int64) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open car %s: %w", path, err)
	}
	defer f.Close()

	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPut, urlStr, f)
	if err != nil {
		return err
	}
	req.ContentLength = size
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http put status %s", resp.Status)
	}
	return nil
}

var _ Uploader = (*Forge)(nil)
