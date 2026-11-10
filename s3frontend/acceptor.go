package s3frontend

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/fil-forge/ucantone/did"

	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
)

// A multipart upload's blobs are accepted on Forge while the client is still
// uploading, so Complete has only the last few parts left to accept. The part
// rows are the queue: UploadPart records a part 'parked', its blobs parked
// before the row, and queues the upload's queue-multipart-upload.accept in the
// same transaction. That accept, which runs on the upload's subject in turn
// with its other changes, queues a queue-space-shard.accept for each blob of
// the upload's parked parts, which concludes the blob and marks accepted the
// parts it completes. The blob's accept runs under the blob's own subject,
// the one its releases and a write's adds run under, so it never overlaps
// them: a release of the blob runs before the conclude, and the accept finds
// the park gone, or after it, and removes the accepted blob.

// acceptListPage is how many parked parts an upload's accept reads at a time.
var acceptListPage = 1000

// acceptRetry retries a failed accept a few times. A later part's accept, or
// Complete, queues whatever is left again.
var acceptRetry = taskrunner.RetryPolicy{First: 5 * time.Second, Max: time.Minute, Multiplier: 2, Attempts: 5}

// registerAcceptTasks registers queue-multipart-upload.accept with r. Call it
// before r starts.
func (b *Backend) registerAcceptTasks(r *taskrunner.Runner) error {
	return r.Handle(registry.MultipartQueue, registry.MultipartAcceptAction, b.runAccept, taskrunner.WithRetry(acceptRetry))
}

// runAccept is queue-multipart-upload.accept: it queues, with its commit, a
// queue-space-shard.accept for each blob of the upload's parked parts, and
// marks a part with no blobs accepted. A session that is gone or completed
// has nothing to accept: an abort released its parts, and a Complete accepted
// them all. Its result is the number of blobs it queued.
func (b *Backend) runAccept(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	uploadID := t.Subject
	sess, err := b.multipart.GetSession(ctx, uploadID)
	if errors.Is(err, registry.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get session: %w", err)
	}
	if sess.State != registry.SessionOpen {
		return nil, nil
	}

	var parked []registry.MultipartPart
	after := 0
	for {
		parts, err := b.multipart.ListParkedParts(ctx, uploadID, after, acceptListPage)
		if err != nil {
			return nil, fmt.Errorf("list parked parts: %w", err)
		}
		if len(parts) == 0 {
			break
		}
		parked = append(parked, parts...)
		after = parts[len(parts)-1].PartNumber
	}
	for _, p := range parked {
		if len(p.BlobDigests) == 0 {
			st := registry.AcceptedEmptyPart(p)
			if err := ex.Write(ctx, st.SQL, st.Args...); err != nil {
				return nil, err
			}
		}
	}
	specs := blobAccepts(sess.Space, parked)
	if _, err := ex.SubmitAll(ctx, specs); err != nil {
		return nil, err
	}
	return []byte(strconv.Itoa(len(specs))), nil
}

// blobAccepts returns the queue-space-shard.accept of each blob of parts in
// space, each once: parts can share a blob.
func blobAccepts(space did.DID, parts []registry.MultipartPart) []taskrunner.Spec {
	seen := map[string]bool{}
	var specs []taskrunner.Spec
	for _, p := range parts {
		for _, d := range p.BlobDigests {
			if !seen[string(d)] {
				seen[string(d)] = true
				specs = append(specs, registry.BlobAcceptSpec(space, d))
			}
		}
	}
	return specs
}
