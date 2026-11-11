package s3frontend

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/attribute"

	"github.com/fil-forge/ingot/internal/tracing"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/uploader"
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

// acceptConcludeTimeout bounds one blob's conclude. The Forge client sets no
// timeout of its own, so without it a conclude whose answer never came would
// hold the blob's subject for good. A conclude that runs out fails the task,
// and the retry concludes it again, which the upload service treats as a
// repeat. Package-level so tests can shrink it.
var acceptConcludeTimeout = 2 * time.Minute

// acceptRetry retries a failed accept a few times. A later part's accept, or
// Complete, queues whatever is left again.
var acceptRetry = taskrunner.RetryPolicy{First: 5 * time.Second, Max: time.Minute, Multiplier: 2, Attempts: 5}

// registerAcceptTasks registers queue-multipart-upload.accept and
// queue-space-shard.accept with r. Call it before r starts.
func (b *Backend) registerAcceptTasks(r *taskrunner.Runner) error {
	if err := r.Handle(registry.MultipartQueue, registry.MultipartAcceptAction, b.runAccept, taskrunner.WithRetry(acceptRetry)); err != nil {
		return err
	}
	return r.Handle(registry.ReleaseQueue, registry.BlobAcceptAction, b.runBlobAccept, taskrunner.WithRetry(acceptRetry))
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

// runBlobAccept is queue-space-shard.accept: it concludes the blob its
// subject names, if it is still parked, and marks accepted, with its commit,
// the parts of the space's open sessions it completes. A blob already located
// needs nothing concluded (a dedup); a parked one concludes its deferred
// /http/put receipt, firing /blob/accept. A blob with neither has been
// released, and its parts are left parked. So is a parked blob nothing in the
// space references any more, as a dropped part's is: its release aborts the
// allocation instead.
func (b *Backend) runBlobAccept(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) (_ []byte, err error) {
	space, digest, err := registry.ParseReleaseSubject(t.Subject)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}
	ctx, span := tracing.Start(ctx, "blobs.conclude", attribute.Int("ingot.blobs.total", 1))
	defer func() { tracing.End(span, err) }()

	var writes []registry.Statement
	if loc, err := b.locations.GetLocation(ctx, space, digest); err == nil && loc != nil {
		writes = registry.LocatedBlob(space, digest)
	} else if err != nil && !errors.Is(err, registry.ErrNotFound) {
		return nil, fmt.Errorf("lookup location: %w", err)
	} else {
		park, err := b.parks.GetPark(ctx, space, digest)
		if errors.Is(err, registry.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("lookup park: %w", err)
		}
		if n, err := b.claims.CountReferences(ctx, space, digest, uuid.Nil); err != nil {
			return nil, fmt.Errorf("count references: %w", err)
		} else if n == 0 {
			return nil, nil
		}
		blob, err := parkedBlob(digest, park)
		if err != nil {
			return nil, err
		}
		cctx, cancel := context.WithTimeout(ctx, acceptConcludeTimeout)
		locations, err := b.deferred.ConcludeBlobs(cctx, space, []uploader.UploadedBlob{blob})
		cancel()
		b.multipartMetrics.pass(ctx, err)
		if err != nil {
			return nil, fmt.Errorf("conclude blob: %w", err)
		}
		if len(locations) == 0 || locations[0] == nil {
			return nil, fmt.Errorf("conclude blob %x: no location returned", digest)
		}
		span.SetAttributes(attribute.Int("ingot.blobs.concluded", 1))
		writes = registry.ConcludedBlob(space, digest, blob.AddTask.Bytes(), registry.BlobLocation{
			Provider: locations[0].Provider,
			URL:      locations[0].URL,
			Size:     locations[0].Size,
		})
	}
	writes = append(writes, registry.AcceptedParts(space, digest)...)
	for _, st := range writes {
		if err := ex.Write(ctx, st.SQL, st.Args...); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// parkedBlob is the blob a park left on its provider, ready to conclude.
func parkedBlob(digest mh.Multihash, park *registry.BlobPark) (uploader.UploadedBlob, error) {
	addTask, err := cid.Cast(park.AddTask)
	if err != nil {
		return uploader.UploadedBlob{}, fmt.Errorf("decode park add task: %w", err)
	}
	acceptTask, err := cid.Cast(park.AcceptTask)
	if err != nil {
		return uploader.UploadedBlob{}, fmt.Errorf("decode park accept task: %w", err)
	}
	return uploader.UploadedBlob{
		Digest:        digest,
		Size:          park.Size,
		AddTask:       addTask,
		AcceptTask:    acceptTask,
		PutInvocation: park.PutInvocation,
	}, nil
}
