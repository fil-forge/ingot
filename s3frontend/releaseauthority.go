package s3frontend

import (
	"context"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/ucantone/did"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/registry"
)

// releaseAuthority captures the space's authority to remove and abort blobs
// from the request, for the releases a write queues, which run after the
// request has gone. A command the request holds no chain for is left empty,
// and the release then borrows authority the space captured since.
func (b *Backend) releaseAuthority(ctx context.Context, space did.DID) registry.ReleaseTask {
	var args registry.ReleaseTask
	var err error
	if args.RemoveProofs, err = b.proofs.Capture(ctx, space, blobcmds.Remove.Command); err != nil {
		b.logger.Debug("no /blob/remove authority to queue with a release", zap.Stringer("space", space), zap.Error(err))
	}
	if args.AbortProofs, err = b.proofs.Capture(ctx, space, blobcmds.Abort.Command); err != nil {
		b.logger.Debug("no /blob/abort authority to queue with a release", zap.Stringer("space", space), zap.Error(err))
	}
	return args
}
