package uploader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	nethttp "net/http"
	"os"

	"github.com/fil-forge/libforge/blobindex"
	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/forgeclient"
)

// Uploader is the seam between the per-plane log flushers and durable
// Forge storage. The data plane and the catalog plane each ship through
// their own pipeline, so the contract is "ship ONE plane's CAR of one
// sealed segment". The implementation streams the file body straight
// into the HTTP PUT, never materializing it as a []block.Block.
type Uploader interface {
	SubmitShard(ctx context.Context, plane blockstore.Plane, shard CARShard) error
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
	space     did.DID
	putClient *nethttp.Client
	logger    *zap.Logger
}

// ForgeConfig wires a Forge uploader.
//
// Client is the configured edge-client to sprue (it carries the agent
// signer, sprue's DID/URL, and the login-derived token store). Space is
// the DID of the space ingot holds /blob/add and /index/add delegations
// over.
type ForgeConfig struct {
	Client    *forgeclient.Client
	Space     did.DID
	PutClient *nethttp.Client // optional; defaults to nethttp.DefaultClient
	Logger    *zap.Logger
}

// NewForge validates the config and returns a Forge uploader.
func NewForge(cfg ForgeConfig) (*Forge, error) {
	if cfg.Client == nil {
		return nil, errors.New("uploader: forge client is required")
	}
	if !cfg.Space.Defined() {
		return nil, errors.New("uploader: space is required")
	}
	pc := cfg.PutClient
	if pc == nil {
		pc = nethttp.DefaultClient
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Forge{client: cfg.Client, space: cfg.Space, putClient: pc, logger: logger}, nil
}

// SpaceDID returns the DID of the space ingot ships to.
func (u *Forge) SpaceDID() did.DID { return u.space }

func (u *Forge) SubmitShard(ctx context.Context, plane blockstore.Plane, shard CARShard) error {
	if shard.Size <= 0 || len(shard.Positions) == 0 {
		return nil
	}

	// 1. blob/add the shard CAR via sprue, streaming from disk.
	f, err := os.Open(shard.Path)
	if err != nil {
		return fmt.Errorf("uploader: open %s car %s: %w", plane, shard.Path, err)
	}
	defer f.Close()
	if _, err := u.client.BlobAdd(ctx, f, u.space,
		forgeclient.WithPrecomputedDigest(shard.SHA256, uint64(shard.Size)),
		forgeclient.WithPutClient(u.putClient),
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
	if _, err := u.client.BlobAdd(ctx, bytes.NewReader(indexBytes), u.space,
		forgeclient.WithPrecomputedDigest(indexDigest, uint64(len(indexBytes))),
		forgeclient.WithPutClient(u.putClient),
	); err != nil {
		return fmt.Errorf("uploader: ship %s index: %w", plane, err)
	}

	// 4. index/add the index CID via sprue (which re-publishes to the
	//    indexing service on ingot's behalf).
	indexCID := cid.NewCidV1(uint64(multicodec.Car), indexDigest)
	if err := u.client.IndexAdd(ctx, indexCID, u.space); err != nil {
		return fmt.Errorf("uploader: publish %s index: %w", plane, err)
	}
	return nil
}

var _ Uploader = (*Forge)(nil)
