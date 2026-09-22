package uploader

import (
	"context"
	"fmt"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/forgeclient"
)

// UploadRegistrar keeps the upload service's content-entry list for a space in
// step with the bucket catalog, one entry per committed object version. The
// entry is what sprue counts to report the space's object count; the bytes are
// accounted separately, per blob, by BodyUploader and BlobRemover.
//
// A version's root is its manifest CID, so the count follows S3: a new version
// registers, a retired one retracts, and a bucket that retains noncurrent
// versions keeps counting them. Delete markers are versions too and count the
// same way, which is what AWS reports.
type UploadRegistrar interface {
	RegisterUpload(ctx context.Context, space did.DID, root cid.Cid) error
	RetractUpload(ctx context.Context, space did.DID, root cid.Cid) error
}

// RegisterUpload records root as a content entry in space via /upload/add on
// the upload service. The proof store is request-scoped when present (the write
// that committed the version) and otherwise the store captured from a recent
// write to the space. Idempotent: sprue upserts by root and counts only the
// first add.
func (u *Forge) RegisterUpload(ctx context.Context, space did.DID, root cid.Cid) error {
	store, ok := u.shipProofStore(ctx, space)
	if !ok {
		return fmt.Errorf("uploader: no proof store for space %s (no request scope and no captured write authority)", space)
	}
	if err := u.client.UploadAdd(ctx, space, root, forgeclient.WithProofStore(store)); err != nil {
		// Callers treat registration as best-effort and may discard the error,
		// so log it here — a silent failure under-reports the space's object
		// count with no trace.
		u.logger.Error("upload add failed",
			zap.Stringer("space", space),
			zap.Stringer("root", root),
			zap.Error(err),
		)
		return fmt.Errorf("uploader: registering upload: %w", err)
	}
	return nil
}

// RetractUpload drops root's content entry from space via /upload/remove on the
// upload service. It does not touch the blobs the root covered — the reference
// index releases those per digest, since a blob can be claimed by more than one
// version. Idempotent.
func (u *Forge) RetractUpload(ctx context.Context, space did.DID, root cid.Cid) error {
	store, ok := u.shipProofStore(ctx, space)
	if !ok {
		return fmt.Errorf("uploader: no proof store for space %s (no request scope and no captured write authority)", space)
	}
	if err := u.client.UploadRemove(ctx, space, root, forgeclient.WithProofStore(store)); err != nil {
		u.logger.Error("upload remove failed",
			zap.Stringer("space", space),
			zap.Stringer("root", root),
			zap.Error(err),
		)
		return fmt.Errorf("uploader: retracting upload: %w", err)
	}
	return nil
}

var _ UploadRegistrar = (*Forge)(nil)
