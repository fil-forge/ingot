package s3frontend

import (
	"context"
	"errors"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/queue/spaceshardqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/uploader"
)

// addBlob takes a write's pending reference to a blob through
// spaceshardqueue.Queue.Add, answering S3's SlowDown, which the client
// retries, when a release of the same blob keeps the add waiting.
func (b *Backend) addBlob(ctx context.Context, space did.DID, bucket string, digest multihash.Multihash, size int64, mode string, sb *uploader.StreamedBlob, release registry.ReleaseTask) (taskrunner.TaskID, error) {
	ref, err := b.blobs.Add(ctx, space, bucket, digest, size, mode, sb, release)
	if errors.Is(err, spaceshardqueue.ErrBusy) {
		return ref, s3err.GetAPIError(s3err.ErrSlowDown)
	}
	return ref, err
}
