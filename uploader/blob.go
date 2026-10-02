package uploader

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	assertcmds "github.com/fil-forge/libforge/commands/assert"
	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/libforge/digestutil"
	"github.com/fil-forge/ucantone/did"
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

// UploadedBlob is a blob uploaded to its provider whose accept is still to
// run: persist the task links + PutInvocation (the blob_parks row) and finish
// with ConcludeBlobs, or abandon with AbortBlob (AddTask is the Cause).
type UploadedBlob struct {
	Digest multihash.Multihash
	Size   int64
	// AddTask is the /blob/add task CID (the abort Cause); AcceptTask is the
	// /blob/accept task CID the conclude polls.
	AddTask    cid.Cid
	AcceptTask cid.Cid
	// PutInvocation is the issued /http/put invocation. Its metadata embeds derived signer keys — sensitive; delete it
	// once concluded or rejected.
	PutInvocation []byte
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

// DeferredBodyUploader finishes or abandons a parked blob — one whose bytes
// are on its provider but whose accept has not run: ConcludeBlobs triggers
// accept (at the end of a PUT, or at multipart Complete); AbortBlob abandons
// a parked blob (at multipart Abort, or a failed send).
type DeferredBodyUploader interface {
	// ConcludeBlobs concludes parked uploads, returning their locations in
	// the order given. A multipart complete has one parked blob per part, and
	// concluding them together is what keeps its cost flat in the part count.
	//
	// On error the slice still has one entry per blob. A non-nil entry is a
	// blob the upload service accepted before the failure, which the caller
	// must record so it is neither concluded again nor aborted as parked; a
	// nil entry is a blob still parked.
	ConcludeBlobs(ctx context.Context, space did.DID, parked []UploadedBlob) ([]*BlobLocation, error)
	// AbortBlob releases a parked blob on its provider. A blob the space has
	// already accepted cannot be aborted: the error wraps [ErrBlobAccepted]
	// and the caller releases it as an accepted blob instead.
	AbortBlob(ctx context.Context, space did.DID, add cid.Cid) error
}

// ConcludeBlobs finishes many parked uploads in as few exchanges with the
// upload service as the batch cap allows: it delivers the blobs' deferred
// /http/put receipts together, triggering their /blob/accept invocations, and
// returns the published locations in the order given.
func (u *Forge) ConcludeBlobs(ctx context.Context, space did.DID, parked []UploadedBlob) ([]*BlobLocation, error) {
	if len(parked) == 0 {
		return nil, nil
	}
	req := make([]forgeclient.AddedBlob, len(parked))
	for i, p := range parked {
		req[i] = forgeclient.AddedBlob{
			Digest:        p.Digest,
			Size:          uint64(p.Size),
			AddTask:       p.AddTask,
			AcceptTask:    p.AcceptTask,
			PutInvocation: p.PutInvocation,
		}
	}
	added, err := u.client.BlobConcludeBatch(ctx, space, req)
	// Every blob comes back, located or still parked, whether or not the
	// batch as a whole succeeded. What was located is converted before the
	// error is looked at, so the caller can record it.
	locations := make([]*BlobLocation, len(parked))
	for i, a := range added {
		if a.Location == nil {
			continue
		}
		loc, lerr := locationFromAdded(a)
		if lerr != nil {
			err = errors.Join(err, lerr)
			continue
		}
		locations[i] = &loc
	}
	if err != nil {
		return locations, fmt.Errorf("uploader: conclude blobs: %w", err)
	}
	return locations, nil
}

// ErrBlobAccepted reports an abort the provider refused because the space
// has accepted the blob. The blob is not parked any more, whatever the local
// tables say; it belongs to reference accounting and is released with
// RemoveBlob, never aborted.
var ErrBlobAccepted = errors.New("uploader: blob accepted by the space; release it with remove")

// AbortBlob abandons a parked blob via /blob/abort on the upload
// service: sprue recovers the provider from the receipt chain of the add and
// the node releases the allocation + parked bytes. add is the parked blob's
// AddTask. The proof store is request-scoped when present (an S3 Abort) and
// otherwise the store captured at park time (the session-expiry sweeper).
// A refusal because the space has accepted the blob is returned wrapping
// [ErrBlobAccepted], so the caller can release the blob instead. Errors are
// logged here (callers treat abort cleanup as best-effort and may discard
// them).
func (u *Forge) AbortBlob(ctx context.Context, space did.DID, add cid.Cid) error {
	u.logger.Info("blob abort",
		zap.Stringer("space", space),
		zap.Stringer("add", add),
	)
	store, ok := u.shipProofStore(ctx, space)
	if !ok {
		return fmt.Errorf("uploader: no proof store for space %s (no request scope and no captured write authority)", space)
	}
	if err := u.client.BlobAbort(ctx, space, add, forgeclient.WithProofStore(store)); err != nil {
		// A BlobAccepted refusal is final, not a fault: the space accepted
		// this content (e.g. a concurrent session completed with the same
		// content-addressed part), so the blob now belongs to the reference
		// index and is released via /blob/remove when its last claim drops.
		var named ucanerrors.Named
		if ucanerrors.As(err, &named) && named.Name() == blobcmds.BlobAcceptedErrorName {
			u.logger.Info("blob abort refused: blob accepted by the space; reference accounting owns it",
				zap.Stringer("space", space),
				zap.Stringer("add", add),
			)
			return fmt.Errorf("uploader: aborting blob: %w", ErrBlobAccepted)
		}
		u.logger.Error("blob abort failed",
			zap.Stringer("space", space),
			zap.Stringer("add", add),
			zap.Error(err),
		)
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
