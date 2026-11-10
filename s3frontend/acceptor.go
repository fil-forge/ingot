package s3frontend

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/attribute"

	"github.com/fil-forge/ingot/forgeclient"
	"github.com/fil-forge/ingot/internal/tracing"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/uploader"
)

// A multipart upload's blobs are accepted on Forge while the client is still
// uploading, so Complete has only the last few parts left to accept. The part
// rows are the queue: UploadPart records a part 'parked', its blobs parked
// before the row, and queues the upload's queue-multipart-upload-shard.accept
// in the same transaction. The accept concludes the upload's parked parts in
// batches and marks them 'accepted'. It runs on the upload's own subject, apart
// from the upload's other work, and stays consistent with it through the part
// rows: a part is marked accepted only while its row is unchanged. A release
// of a blob a dropped part named can race the conclude of that blob; the
// upload service decides which wins, since a released allocation refuses the
// accept and an accepted blob refuses the abort, which the release then
// removes instead.

// acceptBatchBlobs caps the blobs one batch concludes: one /ucan/conclude's
// worth. A part is never split across batches, so a batch takes at least one
// part however many blobs it has.
var acceptBatchBlobs = forgeclient.MaxConcludeReceipts

// acceptBatchTimeout bounds one batch's conclude. The Forge client sets no
// timeout of its own, so without it a conclude whose answer never came would
// hold the upload's accept for good. A batch that runs out fails the task;
// accepts the upload service ran before the timeout are concluded again on
// the retry, which it treats as a repeat. Package-level so tests can shrink
// it.
var acceptBatchTimeout = 2 * time.Minute

// acceptRetry retries a failed background accept a few times. A later part's
// accept, or the one Complete runs, concludes whatever is left.
var acceptRetry = taskrunner.RetryPolicy{First: 5 * time.Second, Max: time.Minute, Multiplier: 2, Attempts: 5}

// acceptFlush is the queue's sync no-op: running it on an upload's subject
// waits for the upload's running accept to finish, and leaves a pending one
// standing.
const acceptFlush = "flush"

// RegisterAcceptTasks registers the queue-multipart-upload-shard handlers
// with r. Call it before r starts.
func (b *Backend) RegisterAcceptTasks(r *taskrunner.Runner) error {
	b.accepts = r
	if err := r.Handle(registry.AcceptQueue, registry.AcceptAction, b.runAccept, taskrunner.WithRetry(acceptRetry)); err != nil {
		return err
	}
	return r.Handle(registry.AcceptQueue, acceptFlush, func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) {
		return nil, nil
	})
}

// awaitRunningAccept waits for uploadID's running accept, if any, to finish.
// queue-multipart-upload.abort calls it before it queues any release. The
// upload's queued accepts, and the flushes of earlier aborts that gave up, are
// cancelled first: the flush would otherwise wait for a worker to run each,
// and no accept is queued meanwhile, since only the upload's parts and its
// Complete queue one, on the subject the abort holds. The accept running may
// have concluded a blob the upload service is still accepting, and the
// service registers the blob before it answers: a release run sooner could
// remove the blob before that registration lands, and leave the blob
// registered and stored with nothing left to remove it. The wait is bounded
// by acceptWaitBudget; past it the abort fails, for its caller to retry.
func (b *Backend) awaitRunningAccept(ctx context.Context, uploadID string) error {
	for _, action := range []string{registry.AcceptAction, acceptFlush} {
		queued, err := b.accepts.Pending(ctx, registry.AcceptQueue, []string{uploadID}, action)
		if err != nil {
			return fmt.Errorf("find the upload's queued %s: %w", action, err)
		}
		// Cancel leaves one that has started, which the flush waits for.
		if err := b.accepts.Cancel(ctx, registry.AcceptQueue, queued[uploadID]...); err != nil {
			return fmt.Errorf("cancel the upload's queued %s: %w", action, err)
		}
	}
	expires := time.Now().Add(acceptWaitBudget)
	_, err := b.accepts.Run(ctx, taskrunner.Spec{Queue: registry.AcceptQueue, Subject: uploadID, Action: acceptFlush, Expires: &expires})
	if errors.Is(err, taskrunner.ErrExpired) {
		return fmt.Errorf("the upload's running accept did not finish in time; retry")
	}
	return err
}

// runAccept is queue-multipart-upload-shard.accept: it concludes the upload's
// parked parts, or those of them the task names, in part order and in
// batches, including parts parked while it runs that sort after the batch it
// is on, and records them accepted with its commit. A session that is gone
// or completed has nothing to accept: an abort released its parts, and a
// Complete accepted them all. A failed batch fails the task, which records
// nothing. Its result is the number of parts it marked accepted.
func (b *Backend) runAccept(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	uploadID := t.Subject
	args, err := registry.AcceptTaskFrom(t)
	if err != nil {
		return nil, taskrunner.Permanent(err)
	}
	var only map[int]bool
	if len(args.Parts) > 0 {
		only = make(map[int]bool, len(args.Parts))
		for _, n := range args.Parts {
			only[int(n)] = true
		}
	}
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

	// Each digest once across the task: parts can share a blob, and the
	// first batch's writes are not committed when a later one runs.
	done := map[string]bool{}
	var accepted []int
	after := 0
	for {
		queued, err := b.multipart.ListParkedParts(ctx, uploadID, after, acceptBatchBlobs)
		if err != nil {
			return nil, fmt.Errorf("list parked parts: %w", err)
		}
		if len(queued) == 0 {
			break
		}
		var batch []registry.MultipartPart
		blobs := 0
		for _, p := range queued {
			if len(batch) > 0 && blobs+len(p.BlobDigests) > acceptBatchBlobs {
				break
			}
			after = p.PartNumber
			if only != nil && !only[p.PartNumber] {
				continue
			}
			batch = append(batch, p)
			blobs += len(p.BlobDigests)
		}
		if len(batch) == 0 {
			continue
		}
		marked, err := b.acceptBatch(ctx, ex, sess.Space, batch, done)
		b.multipartMetrics.pass(ctx, err)
		if err != nil {
			return nil, err
		}
		accepted = append(accepted, marked...)
	}
	return []byte(strconv.Itoa(len(accepted))), nil
}

// acceptBatch concludes the parked blobs of batch, adds the writes that
// record them and mark the parts accepted, and returns the numbers of the
// parts it marks. A blob already located needs nothing concluded (a dedup); a parked
// one concludes its deferred /http/put receipt, firing /blob/accept. A blob
// with neither has been released, which only a part replaced or dropped
// since the listing allows, and its part is left. done holds the digests
// earlier batches recorded, and gains this batch's.
func (b *Backend) acceptBatch(ctx context.Context, ex *taskrunner.Exec, space did.DID, batch []registry.MultipartPart, done map[string]bool) (_ []int, err error) {
	total := 0
	for _, p := range batch {
		total += len(p.BlobDigests)
	}
	ctx, span := tracing.Start(ctx, "blobs.conclude",
		attribute.Int("ingot.parts", len(batch)),
		attribute.Int("ingot.blobs.total", total),
	)
	defer func() { tracing.End(span, err) }()

	var writes []registry.Statement
	var toConclude []uploader.UploadedBlob
	gone := map[string]bool{}
	for _, p := range batch {
		for _, digest := range p.BlobDigests {
			k := string(digest)
			if done[k] || gone[k] {
				continue
			}
			if loc, err := b.locations.GetLocation(ctx, space, digest); err == nil && loc != nil {
				done[k] = true
				writes = append(writes, registry.LocatedBlob(space, digest)...)
				continue
			} else if err != nil && !errors.Is(err, registry.ErrNotFound) {
				return nil, fmt.Errorf("lookup location: %w", err)
			}
			park, err := b.parks.GetPark(ctx, space, digest)
			if errors.Is(err, registry.ErrNotFound) {
				gone[k] = true
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("lookup park: %w", err)
			}
			blob, err := parkedBlob(digest, park)
			if err != nil {
				return nil, err
			}
			done[k] = true
			toConclude = append(toConclude, blob)
		}
	}
	span.SetAttributes(
		attribute.Int("ingot.blobs.concluded", len(toConclude)),
		attribute.Int("ingot.blobs.unparked", len(gone)),
	)

	if len(toConclude) > 0 {
		cctx, cancel := context.WithTimeout(ctx, acceptBatchTimeout)
		// Every parked blob goes to the upload service in one exchange (the
		// client splits only past its batch cap), so thousands of parts cost
		// a couple of round trips rather than one per part.
		locations, err := b.deferred.ConcludeBlobs(cctx, space, toConclude)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("conclude blobs: %w", err)
		}
		for i, blob := range toConclude {
			if i >= len(locations) || locations[i] == nil {
				return nil, fmt.Errorf("conclude blobs: blob %x returned no location", blob.Digest)
			}
			loc := locations[i]
			writes = append(writes, registry.ConcludedBlob(space, blob.Digest, blob.AddTask.Bytes(), registry.BlobLocation{
				Provider: loc.Provider,
				URL:      loc.URL,
				Size:     loc.Size,
			})...)
		}
	}

	// The writes mark a part accepted only once its blobs are located, so a
	// part whose park was taken while it concluded stays parked.
	var marked []int
	for _, p := range batch {
		if !anyIn(p.BlobDigests, gone) {
			writes = append(writes, registry.AcceptedPart(space, p))
			marked = append(marked, p.PartNumber)
		}
	}
	for _, st := range writes {
		if err := ex.Write(ctx, st.SQL, st.Args...); err != nil {
			return nil, err
		}
	}
	return marked, nil
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

// anyIn reports whether any of digests is in set.
func anyIn(digests []mh.Multihash, set map[string]bool) bool {
	for _, d := range digests {
		if set[string(d)] {
			return true
		}
	}
	return false
}
