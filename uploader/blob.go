package uploader

import (
	"bytes"
	"context"
	"fmt"
	"os"

	assertcmds "github.com/fil-forge/libforge/commands/assert"
	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/libforge/digestutil"
	ucanerrors "github.com/fil-forge/ucantone/errors"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/forgeclient"
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

// BodyUploader makes one object-body blob durable on Forge by digest: allocate
// → PUT (skipped on dedup) → accept, returning its published location. It is the
// data-plane counterpart to Uploader (which ships catalog CAR segments). Unlike
// the old data-plane pipeline, this is synchronous: a blob is durable and
// accepted on Piri before the write path commits the manifest that references
// it (docs/architecture.md §5, §7.1).
//
// The space is owned by the implementation (Forge is constructed with one), so
// callers need not thread it.
type BodyUploader interface {
	UploadBlob(ctx context.Context, digest multihash.Multihash, size int64, localPath string) (BlobLocation, error)
}

// UploadBlob uploads one spooled blob to Forge. For a single-shot PutObject the
// allocate→PUT→accept happens in one call (forgeclient.BlobAdd already drives
// the whole flow and returns the location commitment); multipart's deferred
// accept uses the decomposed UploadBlobParked/ConcludeBlob pair instead.
func (u *Forge) UploadBlob(ctx context.Context, digest multihash.Multihash, size int64, localPath string) (BlobLocation, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return BlobLocation{}, fmt.Errorf("uploader: open spooled blob %s: %w", localPath, err)
	}
	defer f.Close()

	added, err := u.client.BlobAdd(ctx, f, u.space,
		forgeclient.WithPrecomputedDigest(digest, uint64(size)),
		forgeclient.WithPutClient(u.putClient),
	)
	if err != nil {
		return BlobLocation{}, fmt.Errorf("uploader: upload blob: %w", err)
	}
	return locationOf(added)
}

// locationOf extracts the local blob-location record from a completed add's
// /assert/location commitment.
func locationOf(added forgeclient.AddedBlob) (BlobLocation, error) {
	loc := BlobLocation{Size: int64(added.Size)}
	// added.Location is the /assert/location commitment piri issued at accept;
	// parse its arguments for the provider DID + retrieval URL so a later read
	// can resolve this blob from the local blob-location table (same shape the
	// index locator extracts from indexer results).
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

// ParkedBlobState is the persistable state of a blob that is durable on the
// provider but not yet accepted (the /http/put conclude is deferred). It is
// stored in the blob_parks table between UploadPart and Complete/Abort.
type ParkedBlobState = forgeclient.ParkedBlob

// DeferredBodyUploader is the decomposed BodyUploader for multipart's
// deferred accept: UploadBlobParked makes the bytes durable (parked) at
// UploadPart; ConcludeBlob triggers accept at Complete; AbortBlob
// abandons a parked blob at Abort. Exactly one of UploadBlobParked's returns
// is non-nil — an already-accepted (deduped) blob completes immediately.
type DeferredBodyUploader interface {
	UploadBlobParked(ctx context.Context, digest multihash.Multihash, size int64, localPath string) (*ParkedBlobState, *BlobLocation, error)
	ConcludeBlob(ctx context.Context, parked ParkedBlobState) (BlobLocation, error)
	AbortBlob(ctx context.Context, digest multihash.Multihash, cause cid.Cid) error
}

// UploadBlobParked makes one spooled blob durable on the provider WITHOUT
// accepting it: /blob/add + PUT, conclude deferred. Returns the persistable
// park state, or the completed location when the provider already held
// accepted bytes for this content (dedup).
func (u *Forge) UploadBlobParked(ctx context.Context, digest multihash.Multihash, size int64, localPath string) (*ParkedBlobState, *BlobLocation, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return nil, nil, fmt.Errorf("uploader: open spooled blob %s: %w", localPath, err)
	}
	defer f.Close()

	parked, added, err := u.client.BlobAddParked(ctx, f, u.space,
		forgeclient.WithPrecomputedDigest(digest, uint64(size)),
		forgeclient.WithPutClient(u.putClient),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("uploader: park blob: %w", err)
	}
	if added != nil {
		loc, err := locationOf(*added)
		if err != nil {
			return nil, nil, err
		}
		return nil, &loc, nil
	}
	return parked, nil, nil
}

// ConcludeBlob finishes a parked upload: it concludes the deferred /http/put
// receipt (triggering /blob/accept on the provider) and returns the published
// location. Safe to retry.
func (u *Forge) ConcludeBlob(ctx context.Context, parked ParkedBlobState) (BlobLocation, error) {
	added, err := u.client.BlobConclude(ctx, parked, u.space)
	if err != nil {
		return BlobLocation{}, fmt.Errorf("uploader: conclude blob: %w", err)
	}
	return locationOf(added)
}

// AbortBlob abandons a parked blob via /blob/abort on the upload
// service: sprue recovers the provider from the cause receipt chain and the
// node releases the allocation + parked bytes. cause is the parked blob's
// AddTask. Errors are logged here (callers treat abort cleanup as
// best-effort and may discard them).
func (u *Forge) AbortBlob(ctx context.Context, digest multihash.Multihash, cause cid.Cid) error {
	u.logger.Info("blob abort",
		zap.Stringer("space", u.space),
		zap.String("digest", digestutil.Format(digest)),
	)
	if err := u.client.BlobAbort(ctx, digest, u.space, cause); err != nil {
		// A BlobAccepted refusal is final, not a fault: the space accepted
		// this content (e.g. a concurrent session completed with the same
		// content-addressed part), so the blob now belongs to the reference
		// index and is released via /blob/remove when its last claim drops.
		var named ucanerrors.Named
		if ucanerrors.As(err, &named) && named.Name() == blobcmds.BlobAcceptedErrorName {
			u.logger.Info("blob abort refused: blob accepted by the space; reference accounting owns it",
				zap.Stringer("space", u.space),
				zap.String("digest", digestutil.Format(digest)),
			)
		} else {
			u.logger.Error("blob abort failed",
				zap.Stringer("space", u.space),
				zap.String("digest", digestutil.Format(digest)),
				zap.Error(err),
			)
		}
		return fmt.Errorf("uploader: aborting blob: %w", err)
	}
	return nil
}

var _ DeferredBodyUploader = (*Forge)(nil)

// BlobRemover releases this space's claim on an accepted blob. Because dedup is
// global, Piri deletes the bytes and retires the piece only when no space
// claims the digest at all (docs/architecture.md §6). The space is owned by the
// implementation (like UploadBlob).
type BlobRemover interface {
	RemoveBlob(ctx context.Context, digest multihash.Multihash) error
}

// RemoveBlob releases the space's claim on digest via /blob/remove on the
// upload service: sprue deregisters the blob and forwards the removal to the
// storage nodes holding it; piri deletes the bytes only once no space claims
// the digest (and, for aggregated pieces, once the PDP root retires
// on-chain). Idempotent.
func (u *Forge) RemoveBlob(ctx context.Context, digest multihash.Multihash) error {
	u.logger.Info("blob remove",
		zap.Stringer("space", u.space),
		zap.String("digest", digestutil.Format(digest)),
	)
	if err := u.client.BlobRemove(ctx, digest, u.space); err != nil {
		// Callers treat removal as best-effort and may discard the error, so
		// log it here — a silent failure leaks bytes on the network with no
		// trace.
		u.logger.Error("blob remove failed",
			zap.Stringer("space", u.space),
			zap.String("digest", digestutil.Format(digest)),
			zap.Error(err),
		)
		return fmt.Errorf("uploader: removing blob: %w", err)
	}
	return nil
}

var _ BlobRemover = (*Forge)(nil)
