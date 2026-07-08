package uploader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	nethttp "net/http"
	"os"
	"time"

	"github.com/fil-forge/libforge/blobindex"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	gocache "github.com/patrickmn/go-cache"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/forgeclient"
	"github.com/fil-forge/ingot/internal/reqscope"
)

// Uploader is the seam between the per-plane log flushers and durable
// Forge storage. The data plane and the catalog plane each ship through
// their own pipeline, so the contract is "ship ONE plane's CAR of one
// sealed segment". The implementation streams the file body straight
// into the HTTP PUT, never materializing it as a []block.Block.
type Uploader interface {
	SubmitShard(ctx context.Context, plane blockstore.Plane, space did.DID, shard CARShard) error
}

// CARShard describes one plane's sealed CAR file ready to ship. All
// fields refer to data that already exists on disk or was precomputed
// at seal time.
type CARShard struct {
	// Path is the absolute path to the sealed CAR file. SubmitShard
	// streams from this path into the HTTP PUT body.
	Path string
	// Size is the file's byte length, set as the request's Content-Length.
	Size int64
	// SHA256 is the SHA-256 multihash of the CAR's bytes, reused both as
	// the blob digest in /blob/add and as the CAR digest the index is
	// keyed by.
	SHA256 multihash.Multihash
	// Positions maps each block's CID to its offset/length inside the CAR.
	Positions map[cid.Cid]blockstore.BlockLoc
}

// Forge ships shards to the Forge network as a guppy-style edge client:
// for each shard it invokes /blob/add against the upload service
// (sprue), PUTs the CAR locally, concludes the put receipt, awaits
// accept, then publishes the shard's sharded-dag-index via /index/add.
// Sprue routes the allocate to the home provider's piri and witnesses
// the control plane.
type Forge struct {
	client    *forgeclient.Client
	putClient *nethttp.Client
	logger    *zap.Logger

	// shipProofs caches, per space, the most recent request-scoped proof
	// store seen on an in-request write to that space. The catalog segment
	// ship (SubmitShard) runs asynchronously in the flush goroutine with no
	// request context, so it reuses this to authorize its onward
	// /blob/add + /index/add. See UploadBlob (capture) and SubmitShard (use).
	shipProofs *gocache.Cache
}

// shipProofsTTL bounds how long a captured ship store lives. Comfortably
// longer than the seal→ship latency (seconds) and flush-retry window, so a
// segment sealed just after a write still finds its space's store.
const shipProofsTTL = time.Hour

// ForgeConfig wires a Forge uploader.
//
// Client is the configured edge-client to sprue (it carries the agent
// signer, sprue's DID/URL, and the login-derived token store).
type ForgeConfig struct {
	Client    *forgeclient.Client
	PutClient *nethttp.Client // optional; defaults to nethttp.DefaultClient
	Logger    *zap.Logger
}

// NewForge validates the config and returns a Forge uploader.
func NewForge(cfg ForgeConfig) (*Forge, error) {
	if cfg.Client == nil {
		return nil, errors.New("uploader: forge client is required")
	}
	pc := cfg.PutClient
	if pc == nil {
		pc = nethttp.DefaultClient
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Forge{
		client:     cfg.Client,
		putClient:  pc,
		logger:     logger,
		shipProofs: gocache.New(shipProofsTTL, shipProofsTTL),
	}, nil
}

// captureShipProofs records store as the ship authority for space (called on
// in-request writes). Nil store is ignored.
func (u *Forge) captureShipProofs(space did.DID, store ucanlib.ProofStore) {
	if store == nil {
		return
	}
	u.shipProofs.Set(space.String(), store, gocache.DefaultExpiration)
}

// shipProofStore resolves the proof store authorizing a ship to space: the
// request-scoped store if this runs in-request, else the most recent
// captured store for the space. ok=false means neither exists.
func (u *Forge) shipProofStore(ctx context.Context, space did.DID) (ucanlib.ProofStore, bool) {
	if store, ok := reqscope.ProofStore(ctx); ok {
		return store, true
	}
	if v, ok := u.shipProofs.Get(space.String()); ok {
		return v.(ucanlib.ProofStore), true
	}
	return nil, false
}

func (u *Forge) SubmitShard(ctx context.Context, plane blockstore.Plane, space did.DID, shard CARShard) error {
	if shard.Size <= 0 || len(shard.Positions) == 0 {
		return nil
	}

	// The ship runs async (flush goroutine, no request), so its authority is
	// the store captured from a recent in-request write to this space. No
	// store → fail so the flush retries; a later write repopulates it.
	store, ok := u.shipProofStore(ctx, space)
	if !ok {
		return fmt.Errorf("uploader: no ship authority for space %s (no captured proof store)", space)
	}

	// 1. blob/add the shard CAR via sprue, streaming from disk.
	f, err := os.Open(shard.Path)
	if err != nil {
		return fmt.Errorf("uploader: open %s car %s: %w", plane, shard.Path, err)
	}
	defer f.Close()
	if _, err := u.client.BlobAdd(ctx, space, f,
		forgeclient.WithPrecomputedDigest(shard.SHA256, uint64(shard.Size)),
		forgeclient.WithPutClient(u.putClient),
		forgeclient.WithProofStore(store),
	); err != nil {
		return fmt.Errorf("uploader: ship %s car: %w", plane, err)
	}

	// 2. Build a 1-shard sharded-dag-index keyed off the CAR multihash.
	view := blobindex.NewShardedDagIndex(1)
	for c, loc := range shard.Positions {
		view.SetSlice(shard.SHA256, c.Hash(), blobindex.Range{
			Start: int64(loc.Offset),
			End:   int64(loc.Offset + loc.Length - 1),
		})
	}
	var indexBuf bytes.Buffer
	if err := view.Archive(&indexBuf); err != nil {
		return fmt.Errorf("uploader: archive %s index: %w", plane, err)
	}
	indexBytes := indexBuf.Bytes()
	indexDigest, err := multihash.Sum(indexBytes, multihash.SHA2_256, -1)
	if err != nil {
		return fmt.Errorf("uploader: hash %s index: %w", plane, err)
	}

	// 3. blob/add the index blob via sprue (small: one entry per CID).
	if _, err := u.client.BlobAdd(ctx, space, bytes.NewReader(indexBytes),
		forgeclient.WithPrecomputedDigest(indexDigest, uint64(len(indexBytes))),
		forgeclient.WithPutClient(u.putClient),
	); err != nil {
		return fmt.Errorf("uploader: ship %s index: %w", plane, err)
	}

	// 4. index/add the index CID via sprue (which re-publishes to the
	//    indexing service on ingot's behalf).
	indexCID := cid.NewCidV1(uint64(multicodec.Car), indexDigest)
	if err := u.client.IndexAdd(ctx, space, indexCID); err != nil {
		return fmt.Errorf("uploader: publish %s index: %w", plane, err)
	}
	return nil
}

var _ Uploader = (*Forge)(nil)
