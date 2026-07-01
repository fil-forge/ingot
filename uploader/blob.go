package uploader

import (
	"bytes"
	"context"
	"fmt"
	"os"

	assertcmds "github.com/fil-forge/libforge/commands/assert"
	"github.com/multiformats/go-multihash"

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
// accept will need a decomposed path (a later phase).
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
