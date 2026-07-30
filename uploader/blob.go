package uploader

import (
	"bytes"
	"context"
	"fmt"
	"os"

	assertcmds "github.com/fil-forge/libforge/commands/assert"
	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/libforge/digestutil"
	"github.com/fil-forge/ucantone/did"
	ucanerrors "github.com/fil-forge/ucantone/errors"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/forgeclient"
	"github.com/fil-forge/ingot/internal/reqscope"
)

// BlobLocation is where an accepted blob can be retrieved from, as resolved at
// accept time. It is recorded in the local blob-location table and consumed by
// the read path's Locator (the appliance topology resolves reads from this
// table in place of the indexing-service).
type BlobLocation struct {
	Provider string // provider/node DID that issued the location commitment
	URL      string // retrieval URL for the blob
	Size     int64  // whole-blob byte length
}

// UploadOption configures BodyUploader.UploadBlob.
type UploadOption func(*uploadConfig)

type uploadConfig struct {
	conclude bool
}

func newUploadConfig(options ...UploadOption) *uploadConfig {
	cfg := &uploadConfig{conclude: true}
	for _, opt := range options {
		opt(cfg)
	}
	return cfg
}

// WithConclude controls whether UploadBlob triggers accept before returning.
// WithConclude(false) leaves the blob parked — durable on the provider,
// accept deferred (multipart's UploadPart): the returned UploadedBlob has a
// nil Location and carries the state ConcludeBlob needs. Moot on dedup —
// when the provider already held accepted bytes for the content, accept
// already ran and the upload completes regardless.
func WithConclude(conclude bool) UploadOption {
	return func(cfg *uploadConfig) { cfg.conclude = conclude }
}

// UploadedBlob is the result of UploadBlob. Location == nil ⇔ the blob is
// parked (uploaded with WithConclude(false), no dedup hit): persist the task
// links + PutInvocation (the blob_parks row) and finish with ConcludeBlob at
// Complete, or abandon with AbortBlob (AddTask is the Cause). A concluding
// upload — the default — always returns a non-nil Location or errors.
type UploadedBlob struct {
	Digest multihash.Multihash
	Size   int64
	// Location of the accepted blob; nil while parked.
	Location *BlobLocation
	// AddTask is the /blob/add task CID (the abort Cause); AcceptTask is the
	// /blob/accept task CID the conclude polls.
	AddTask    cid.Cid
	AcceptTask cid.Cid
	// PutInvocation is the issued /http/put invocation, populated only while
	// parked. Its metadata embeds derived signer keys — sensitive; delete it
	// once concluded or rejected.
	PutInvocation []byte
}

// BodyUploader makes one object-body blob durable on Forge by digest: allocate
// → PUT (skipped on dedup) → accept, returning its published location.
// WithConclude(false) stops before accept (see UploadedBlob). It is the
// data-plane counterpart to Uploader (which ships catalog CAR segments). Unlike
// the old data-plane pipeline, this is synchronous: a blob is durable — and,
// when concluding, accepted — on Piri before the write path commits the
// manifest that references it (docs/architecture.md §5, §7.1).
type BodyUploader interface {
	UploadBlob(ctx context.Context, space did.DID, digest multihash.Multihash, size int64, localPath string, opts ...UploadOption) (UploadedBlob, error)
}

// UploadBlob uploads one spooled blob to Forge. For a single-shot PutObject the
// allocate→PUT→accept happens in one call (forgeclient.BlobAdd already drives
// the whole flow and returns the location commitment); multipart's deferred
// accept passes WithConclude(false) and finishes with ConcludeBlob at
// Complete (or AbortBlob at Abort).
func (u *Forge) UploadBlob(ctx context.Context, space did.DID, digest multihash.Multihash, size int64, localPath string, opts ...UploadOption) (UploadedBlob, error) {
	cfg := newUploadConfig(opts...)
	f, err := os.Open(localPath)
	if err != nil {
		return UploadedBlob{}, fmt.Errorf("uploader: open spooled blob %s: %w", localPath, err)
	}
	defer f.Close()

	// This runs on the request ctx, so the requesting key's proof store must
	// be present: it authorizes the /blob/add and is captured as the ship
	// authority for space, so the async catalog ship (SubmitShard, no request
	// ctx) can reuse it. In forge mode the IAM layer populates reqscope for
	// every write, so absence is a wiring bug, not a fallback case — proceeding
	// without it issues a proofless /blob/add that sprue rejects with an opaque
	// "not issued by subject and has no proofs" 500 deep in the flow. Fail here
	// instead, naming the space, so the missing store is immediately
	// attributable.
	store, ok := reqscope.ProofStore(ctx)
	if !ok {
		return UploadedBlob{}, fmt.Errorf("uploader: no request-scoped proof store for space %s (IAM layer did not attach one)", space)
	}
	u.captureShipProofs(space, store)

	added, err := u.client.BlobAdd(ctx, space, f,
		forgeclient.WithPrecomputedDigest(digest, uint64(size)),
		forgeclient.WithPutClient(u.putClient),
		forgeclient.WithProofStore(store),
		forgeclient.WithConclude(cfg.conclude),
	)
	if err != nil {
		return UploadedBlob{}, fmt.Errorf("uploader: upload blob: %w", err)
	}
	return uploadedFromAdded(added)
}

// uploadedFromAdded maps a forgeclient result into the uploader's shape: an
// accepted blob's location commitment parses into a BlobLocation; a parked
// (unconcluded) blob carries its pending-accept state through.
func uploadedFromAdded(added forgeclient.AddedBlob) (UploadedBlob, error) {
	ub := UploadedBlob{
		Digest:     added.Digest,
		Size:       int64(added.Size),
		AddTask:    added.AddTask,
		AcceptTask: added.AcceptTask,
	}
	if added.Location == nil {
		ub.PutInvocation = added.PutInvocation
		return ub, nil
	}
	loc, err := locationFromAdded(added)
	if err != nil {
		return UploadedBlob{}, err
	}
	ub.Location = &loc
	return ub, nil
}

// locationFromAdded parses the /assert/location commitment piri issued at
// accept out of a BlobAdd result: the provider DID + retrieval URL a later
// read needs to resolve this blob from the local blob-location table (same
// shape the index locator extracts from indexer results).
func locationFromAdded(added forgeclient.AddedBlob) (BlobLocation, error) {
	loc := BlobLocation{Size: int64(added.Size)}
	if inv := added.Location; inv != nil && inv.Command() == assertcmds.Location.Command {
		var args assertcmds.LocationArguments
		if err := args.UnmarshalCBOR(bytes.NewReader(inv.ArgumentsBytes())); err != nil {
			return BlobLocation{}, fmt.Errorf("uploader: decode location commitment: %w", err)
		}
		loc.Provider = inv.Issuer().String()
		if len(args.Location) > 0 {
			loc.URL = args.Location[0].URL().String()
		}
	}
	return loc, nil
}

var _ BodyUploader = (*Forge)(nil)

// DeferredBodyUploader extends BodyUploader for multipart's deferred accept:
// UploadBlob with WithConclude(false) makes the bytes durable (parked) at
// UploadPart; ConcludeBlob triggers accept at Complete; AbortBlob abandons a
// parked blob at Abort.
type DeferredBodyUploader interface {
	BodyUploader
	ConcludeBlob(ctx context.Context, space did.DID, parked UploadedBlob) (BlobLocation, error)
	AbortBlob(ctx context.Context, space did.DID, digest multihash.Multihash, cause cid.Cid) error
}

// ConcludeBlob finishes a parked upload: it concludes the deferred /http/put
// receipt (triggering /blob/accept on the provider) and returns the published
// location. parked is the UploadedBlob a WithConclude(false) upload returned
// (rehydrated from its blob_parks row). The conclude carries no space proof
// (accept is owned by sprue), so no proof store is required. Safe to retry.
func (u *Forge) ConcludeBlob(ctx context.Context, space did.DID, parked UploadedBlob) (BlobLocation, error) {
	added, err := u.client.BlobConclude(ctx, space, forgeclient.AddedBlob{
		Digest:        parked.Digest,
		Size:          uint64(parked.Size),
		AddTask:       parked.AddTask,
		AcceptTask:    parked.AcceptTask,
		PutInvocation: parked.PutInvocation,
	})
	if err != nil {
		return BlobLocation{}, fmt.Errorf("uploader: conclude blob: %w", err)
	}
	return locationFromAdded(added)
}

// AbortBlob abandons a parked blob via /blob/abort on the upload
// service: sprue recovers the provider from the cause receipt chain and the
// node releases the allocation + parked bytes. cause is the parked blob's
// AddTask. The proof store is request-scoped when present (an S3 Abort) and
// otherwise the store captured at park time (the session-expiry sweeper).
// Errors are logged here (callers treat abort cleanup as best-effort and may
// discard them).
func (u *Forge) AbortBlob(ctx context.Context, space did.DID, digest multihash.Multihash, cause cid.Cid) error {
	u.logger.Info("blob abort",
		zap.Stringer("space", space),
		zap.String("digest", digestutil.Format(digest)),
	)
	store, ok := u.shipProofStore(ctx, space)
	if !ok {
		return fmt.Errorf("uploader: no proof store for space %s (no request scope and no captured write authority)", space)
	}
	if err := u.client.BlobAbort(ctx, space, digest, cause, forgeclient.WithProofStore(store)); err != nil {
		// A BlobAccepted refusal is final, not a fault: the space accepted
		// this content (e.g. a concurrent session completed with the same
		// content-addressed part), so the blob now belongs to the reference
		// index and is released via /blob/remove when its last claim drops.
		var named ucanerrors.Named
		if ucanerrors.As(err, &named) && named.Name() == blobcmds.BlobAcceptedErrorName {
			u.logger.Info("blob abort refused: blob accepted by the space; reference accounting owns it",
				zap.Stringer("space", space),
				zap.String("digest", digestutil.Format(digest)),
			)
		} else {
			u.logger.Error("blob abort failed",
				zap.Stringer("space", space),
				zap.String("digest", digestutil.Format(digest)),
				zap.Error(err),
			)
		}
		return fmt.Errorf("uploader: aborting blob: %w", err)
	}
	return nil
}

var _ DeferredBodyUploader = (*Forge)(nil)

// BlobRemover releases a space's claim on an accepted blob. Because dedup is
// global, Piri deletes the bytes and retires the piece only when no space
// claims the digest at all (docs/architecture.md §6).
type BlobRemover interface {
	RemoveBlob(ctx context.Context, space did.DID, digest multihash.Multihash) error
}

// RemoveBlob releases the space's claim on digest via /blob/remove on the
// upload service: sprue deregisters the blob and forwards a /blob/release to
// the storage nodes holding it; piri deletes the bytes only once no space
// claims the digest (and, for aggregated pieces, once the PDP root retires
// on-chain). The proof store is request-scoped when present (a DeleteObject)
// and otherwise the store captured from a recent write to the space.
// Idempotent.
func (u *Forge) RemoveBlob(ctx context.Context, space did.DID, digest multihash.Multihash) error {
	u.logger.Info("blob remove",
		zap.Stringer("space", space),
		zap.String("digest", digestutil.Format(digest)),
	)
	store, ok := u.shipProofStore(ctx, space)
	if !ok {
		return fmt.Errorf("uploader: no proof store for space %s (no request scope and no captured write authority)", space)
	}
	if err := u.client.BlobRemove(ctx, space, digest, forgeclient.WithProofStore(store)); err != nil {
		// Callers treat removal as best-effort and may discard the error, so
		// log it here — a silent failure leaks bytes on the network with no
		// trace.
		u.logger.Error("blob remove failed",
			zap.Stringer("space", space),
			zap.String("digest", digestutil.Format(digest)),
			zap.Error(err),
		)
		return fmt.Errorf("uploader: removing blob: %w", err)
	}
	return nil
}

var _ BlobRemover = (*Forge)(nil)
