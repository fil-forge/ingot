package uploader

import (
	"bytes"
	"context"
	"fmt"
	"os"

	assertcmds "github.com/fil-forge/libforge/commands/assert"
	"github.com/fil-forge/libforge/digestutil"
	"github.com/fil-forge/ucantone/did"
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
	UploadBlob(ctx context.Context, space did.DID, digest multihash.Multihash, size int64, localPath string) (BlobLocation, error)
}

// UploadBlob uploads one spooled blob to Forge. For a single-shot PutObject the
// allocate→PUT→accept happens in one call (forgeclient.BlobAdd already drives
// the whole flow and returns the location commitment); multipart's deferred
// accept will need a decomposed path (a later phase).
func (u *Forge) UploadBlob(ctx context.Context, space did.DID, digest multihash.Multihash, size int64, localPath string) (BlobLocation, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return BlobLocation{}, fmt.Errorf("uploader: open spooled blob %s: %w", localPath, err)
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
		return BlobLocation{}, fmt.Errorf("uploader: no request-scoped proof store for space %s (IAM layer did not attach one)", space)
	}
	u.captureShipProofs(space, store)

	added, err := u.client.BlobAdd(ctx, space, f,
		forgeclient.WithPrecomputedDigest(digest, uint64(size)),
		forgeclient.WithPutClient(u.putClient),
		forgeclient.WithProofStore(store),
	)
	if err != nil {
		return BlobLocation{}, fmt.Errorf("uploader: upload blob: %w", err)
	}

	return locationFromAdded(added)
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

// BlobRemover releases this space's claim on an accepted blob. Because dedup is
// global, Piri deletes the bytes and retires the piece only when no space
// claims the digest at all (docs/architecture.md §6). The space is owned by the
// implementation (like UploadBlob).
type BlobRemover interface {
	RemoveBlob(ctx context.Context, space did.DID, digest multihash.Multihash) error
}

// RemoveBlob releases the space's claim on digest.
//
// TODO(phase 7 / smelt): wire forgeclient.BlobRemove against the upload service
// — the libforge blob.Remove binding exists, but the Piri/Sprue handler is
// to-build (docs/architecture.md §9). Until it lands this is a logged no-op, so
// the reference-index bookkeeping (blob_refs count → 0 → RemoveBlob) is
// exercised end-to-end without a working network primitive; bytes accumulate on
// Piri until the handler exists.
func (u *Forge) RemoveBlob(_ context.Context, space did.DID, digest multihash.Multihash) error {
	u.logger.Info("blob remove (no-op; Piri handler to-build)",
		zap.Stringer("space", space),
		zap.String("digest", digestutil.Format(digest)),
	)
	return nil
}

var _ BlobRemover = (*Forge)(nil)
