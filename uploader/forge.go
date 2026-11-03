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
	blobcmds "github.com/fil-forge/libforge/commands/blob"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	gocache "github.com/patrickmn/go-cache"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/forgeclient"
	"github.com/fil-forge/ingot/internal/reqscope"
)

// Uploader is the seam between the catalog log's flusher and durable
// Forge storage: it ships one sealed segment's CAR. The implementation
// streams the file body straight into the HTTP PUT, never materializing
// it as a []block.Block.
//
// SubmitShard returns the shipped CAR's published location (provider DID +
// retrieval URL from the accept-time commitment) so the flush path can record
// it — plus each inner block's byte range — in the local location/inclusion
// tables before the segment is marked shipped. A zero-value location (no
// Provider/URL) means the implementation shipped nothing durable (e.g. the
// in-memory no-op uploader).
type Uploader interface {
	// SubmitShard ships one sealed CAR to Forge. Alongside the CAR's
	// location it returns the digest of the shard's sharded-dag-index blob:
	// the ship registers TWO blobs in the space (the CAR and its index), and
	// deleting the bucket later must release both. Nil when nothing shipped.
	SubmitShard(ctx context.Context, plane blockstore.Plane, space did.DID, shard CARShard) (BlobLocation, multihash.Multihash, error)
	// CaptureShipAuthority records the request's proof store on ctx, if it
	// carries one that background work may borrow and that holds a /blob/add
	// chain for space, as the authority SubmitShard ships space's segments
	// with. SubmitShard runs outside any request, so every write that appends
	// catalog blocks hands it its request, not only a write that uploads a
	// body.
	CaptureShipAuthority(ctx context.Context, space did.DID)
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

	// shipProofs caches, per space, the most recent borrowable proof store
	// seen on an in-request body upload to that space (UploadBlob,
	// StartBlob). Work that runs outside any request falls back to it: the
	// release sweeper's /blob/remove, the session-expiry aborts,
	// registration renewals, and the catalog segment ship (SubmitShard) when
	// commitProofs can't serve it. An upload's grant covers all of their
	// commands.
	shipProofs *gocache.Cache

	// commitProofs caches, per space, the proof store of the most recent
	// write that committed catalog blocks to it (CaptureShipAuthority), if
	// that store was borrowable and held a /blob/add chain for the space. Hilt
	// grants each catalog-changing operation the ship's commands, but not
	// necessarily the release, abort and registration commands shipProofs
	// serves (a tagging-only key's grant has none of them), so SubmitShard
	// alone reads it, ahead of shipProofs.
	commitProofs *gocache.Cache
}

// shipProofsTTL bounds how long a captured ship store lives. Comfortably
// longer than the seal→ship latency (seconds) and flush-retry window, so a
// segment sealed just after a write still finds its space's store.
const shipProofsTTL = time.Hour

// noCapturedAuthority says why shipProofStore found nothing outside a
// request, for the errors of the calls that need it.
const noCapturedAuthority = "no body upload has left a borrowable store within the last hour"

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
		client:       cfg.Client,
		putClient:    pc,
		logger:       logger,
		shipProofs:   gocache.New(shipProofsTTL, shipProofsTTL),
		commitProofs: gocache.New(shipProofsTTL, shipProofsTTL),
	}, nil
}

// captureShipProofs records the proof store of ctx's request as the ship
// authority for space (called on in-request writes), if background work may
// borrow it (reqscope.BorrowableProofStore): the access key's own store,
// whose chains skip any delegation revoked since it was captured. A write
// authorized by a store only it may use, one holding a delegation known
// revoked, leaves the space's earlier capture in place.
func (u *Forge) captureShipProofs(ctx context.Context, space did.DID) {
	store, ok := reqscope.BorrowableProofStore(ctx)
	if !ok {
		return
	}
	u.shipProofs.Set(space.String(), store, gocache.DefaultExpiration)
}

// CaptureShipAuthority records the request's proof store as space's ship
// authority (Uploader), in commitProofs, if background work may borrow it
// (reqscope.BorrowableProofStore), as captureShipProofs does. A ctx without
// one, such as a background commit's, leaves the capture as it was, and so
// does a store that can't ship, so a key with no /blob/add chain never
// displaces one that has it.
func (u *Forge) CaptureShipAuthority(ctx context.Context, space did.DID) {
	if store, ok := reqscope.BorrowableProofStore(ctx); ok && u.canShip(ctx, store, space) {
		u.commitProofs.Set(space.String(), store, gocache.DefaultExpiration)
	}
}

// canShip reports whether store holds a chain authorizing the agent to
// /blob/add to space: the question the ship's own invocation asks.
func (u *Forge) canShip(ctx context.Context, store ucanlib.ProofStore, space did.DID) bool {
	proofs, _, err := store.ProofChain(ctx, u.client.Issuer().DID(), blobcmds.Add.Command, space)
	return err == nil && len(proofs) > 0
}

// catalogShipStore resolves the proof store a catalog ship to space signs
// with: the request-scoped store if this runs in-request, else the store of
// the space's most recent commit if it still holds a /blob/add chain for the
// space, else shipProofStore's. A captured chain can lapse before the ship
// (an expired or revoked delegation) while the upload's still ships.
func (u *Forge) catalogShipStore(ctx context.Context, space did.DID) (ucanlib.ProofStore, bool) {
	if store, ok := reqscope.ProofStore(ctx); ok {
		return store, true
	}
	if v, ok := u.commitProofs.Get(space.String()); ok {
		if store := v.(ucanlib.ProofStore); u.canShip(ctx, store, space) {
			return store, true
		}
	}
	return u.shipProofStore(ctx, space)
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

// authorityFor resolves the proof store authorizing cmd on space, as
// shipProofStore does, and the chain it holds for cmd. A captured store can
// outlive its chains: they expire, or a revocation names one of their hops.
// An invocation sent without a chain goes out proofless, and the service
// refuses it with an error that says nothing about why, so this fails first,
// naming the command and the space.
func (u *Forge) authorityFor(ctx context.Context, space did.DID, cmd ucan.Command) (ucanlib.ProofStore, []ucan.Delegation, error) {
	store, ok := u.shipProofStore(ctx, space)
	if !ok {
		return nil, nil, fmt.Errorf("uploader: no proof store for space %s (no request scope; %s)", space, noCapturedAuthority)
	}
	proofs, err := u.chainFor(ctx, store, space, cmd)
	if err != nil {
		return nil, nil, err
	}
	return store, proofs, nil
}

// chainFor returns the chain store holds authorizing the agent to cmd on
// space, and fails when it holds none.
func (u *Forge) chainFor(ctx context.Context, store ucanlib.ProofStore, space did.DID, cmd ucan.Command) ([]ucan.Delegation, error) {
	proofs, _, err := store.ProofChain(ctx, u.client.Issuer().DID(), cmd, space)
	if err != nil {
		return nil, fmt.Errorf("uploader: building %s proof chain: %w", cmd, err)
	}
	if len(proofs) == 0 {
		return nil, fmt.Errorf("uploader: no %s authority for space %s (the proof store holds no unexpired, unrevoked chain for it)", cmd, space)
	}
	return proofs, nil
}

func (u *Forge) SubmitShard(ctx context.Context, plane blockstore.Plane, space did.DID, shard CARShard) (BlobLocation, multihash.Multihash, error) {
	if shard.Size <= 0 || len(shard.Positions) == 0 {
		return BlobLocation{}, nil, nil
	}

	// The ship runs async (flush goroutine, no request), so its authority is
	// a store captured from a recent in-request write to this space. None
	// that can ship → fail so the flush retries; a later write repopulates
	// it.
	store, ok := u.catalogShipStore(ctx, space)
	if !ok {
		return BlobLocation{}, nil, fmt.Errorf("uploader: no ship authority for space %s (no captured proof store holds a /blob/add chain)", space)
	}
	// The request's own store and shipProofStore's capture aren't probed
	// above, and a captured store can outlive its chains (see authorityFor):
	// whichever was chosen, fail here, naming the space, rather than ship
	// proofless.
	if _, err := u.chainFor(ctx, store, space, blobcmds.Add.Command); err != nil {
		return BlobLocation{}, nil, err
	}

	// The index blob's bytes and digest are deterministic from the shard, so
	// build them up front: the digest is returned even when the publication
	// below fails partway (its blob/add may have registered before the
	// failure), keeping the caller's record of space registrations complete.
	indexBytes, indexDigest, err := shardIndexBlob(shard)
	if err != nil {
		return BlobLocation{}, nil, fmt.Errorf("uploader: build %s shard index: %w", plane, err)
	}

	// 1. blob/add the shard CAR via sprue, streaming from disk.
	f, err := os.Open(shard.Path)
	if err != nil {
		return BlobLocation{}, nil, fmt.Errorf("uploader: open %s car %s: %w", plane, shard.Path, err)
	}
	defer f.Close()
	added, err := u.client.BlobAdd(ctx, space, f,
		forgeclient.WithPrecomputedDigest(shard.SHA256, uint64(shard.Size)),
		forgeclient.WithPutClient(u.putClient),
		forgeclient.WithProofStore(store),
	)
	if err != nil {
		return BlobLocation{}, nil, fmt.Errorf("uploader: ship %s car: %w", plane, err)
	}
	carLoc, err := locationFromAdded(added)
	if err != nil {
		return BlobLocation{}, nil, err
	}

	// 2-4. Publish the shard's sharded-dag-index to the indexing service.
	// Best-effort: the CAR is already durable on piri and the flush path
	// records block locations locally (blob_locations + shard_inclusions),
	// so ingot's own reads never depend on the network index. Failing the
	// ship here would wedge retention behind indexer availability instead —
	// log loudly and move on. (TODO: queue failed publications for retry so
	// the network index converges.)
	if err := u.publishShardIndex(ctx, space, indexBytes, indexDigest, store); err != nil {
		u.logger.Warn("uploader: index publication failed; segment ships anyway (local inclusions cover reads, network index will lag)",
			zap.Stringer("plane", plane),
			zap.Stringer("space", space),
			zap.Error(err),
		)
	}
	return carLoc, indexDigest, nil
}

// shardIndexBlob builds the 1-shard sharded-dag-index blob for shard (inner
// block digest → byte range, keyed off the CAR multihash) and returns its
// archived bytes + digest.
func shardIndexBlob(shard CARShard) ([]byte, multihash.Multihash, error) {
	view := blobindex.NewShardedDagIndex(1)
	for c, loc := range shard.Positions {
		view.SetSlice(shard.SHA256, c.Hash(), blobindex.Range{
			Start: int64(loc.Offset),
			End:   int64(loc.Offset + loc.Length - 1),
		})
	}
	var indexBuf bytes.Buffer
	if err := view.Archive(&indexBuf); err != nil {
		return nil, nil, fmt.Errorf("archive index: %w", err)
	}
	indexBytes := indexBuf.Bytes()
	indexDigest, err := multihash.Sum(indexBytes, multihash.SHA2_256, -1)
	if err != nil {
		return nil, nil, fmt.Errorf("hash index: %w", err)
	}
	return indexBytes, indexDigest, nil
}

// publishShardIndex blob/adds the shard's sharded-dag-index blob and
// /index/adds its CID via sprue (which re-publishes to the indexing
// service). All invocations carry the ship authority — without it they are
// issued proofless and sprue rejects them.
func (u *Forge) publishShardIndex(ctx context.Context, space did.DID, indexBytes []byte, indexDigest multihash.Multihash, store ucanlib.ProofStore) error {
	if _, err := u.client.BlobAdd(ctx, space, bytes.NewReader(indexBytes),
		forgeclient.WithPrecomputedDigest(indexDigest, uint64(len(indexBytes))),
		forgeclient.WithPutClient(u.putClient),
		forgeclient.WithProofStore(store),
	); err != nil {
		return fmt.Errorf("ship index blob: %w", err)
	}

	indexCID := cid.NewCidV1(uint64(multicodec.Car), indexDigest)
	if err := u.client.IndexAdd(ctx, space, indexCID,
		forgeclient.WithProofStore(store),
	); err != nil {
		return fmt.Errorf("publish index: %w", err)
	}
	return nil
}

var _ Uploader = (*Forge)(nil)
