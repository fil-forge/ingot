package multipartqueue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/fil-forge/ucantone/did"
	mh "github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/queue/spaceshardqueue"
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

// errAcceptWaitExpired is a wait for a blob's accept that ran out of the
// accept wait.
var errAcceptWaitExpired = errors.New("multipartqueue: the parts' accepts did not finish in time")

// runAccept is queue-multipart-upload.accept: it queues, with its commit, a
// queue-space-shard.accept for each blob of the upload's parked parts, and
// marks a part with no blobs accepted. A session that is gone or completed
// has nothing to accept: an abort released its parts, and a Complete accepted
// them all. Its result is the number of blobs it queued.
func (q *Queue) runAccept(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	uploadID := t.Subject
	sess, err := q.multipart.GetSession(ctx, uploadID)
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
		parts, err := q.multipart.ListParkedParts(ctx, uploadID, after, acceptListPage)
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
				specs = append(specs, spaceshardqueue.AcceptSpec(space, d))
			}
		}
	}
	return specs
}

// acceptRequested makes sure every part Complete names has its blobs
// accepted on Forge. The upload's accept has been queueing its parts' blobs
// for acceptance while the client uploaded them; Complete queues the accept
// of each blob of the parts it names that are still parked, after the
// response's headers have gone out, and then waits for each of those blobs'
// accepts, brought forward from any retry's backoff. It queues them and waits
// outside its own commit: the accepts' writes commit before Complete reads
// the parts again, and before the object's tree is assembled, which reads
// accepted blobs back through their locations.
// Parts the upload holds and Complete omits are left out: they are orphans of
// this completion. Then every named part must be accepted. One with a blob
// released since the request validated it is no longer the part the client
// named, and Complete fails with ErrInvalidPart; the client uploads it again.
// One whose accept failed for good fails the request, which clients retry.
// Accepts that do not finish within the accept wait, one waiting to retry
// among them, fail with ErrInProgress rather than hold the request past the
// client's read timeout.
func (q *Queue) acceptRequested(ctx context.Context, uploadID string, space did.DID, requested []registry.MultipartPart) error {
	expires := time.Now().Add(q.limits.AcceptWait)
	err := q.acceptAndWait(ctx, space, requested, expires)
	if errors.Is(err, taskrunner.ErrExpired) || errors.Is(err, errAcceptWaitExpired) ||
		(errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil) {
		q.completeDeferred.Add(ctx, 1)
		q.logger.Info("multipart completion deferred: the parts' accepts did not finish in time",
			zap.String("upload_id", uploadID))
		return ErrInProgress
	} else if err != nil {
		return fmt.Errorf("multipartqueue: accept parts: %w", err)
	}
	current, err := q.multipart.ListParts(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("multipartqueue: list parts: %w", err)
	}
	byNum := make(map[int]registry.MultipartPart, len(current))
	for _, p := range current {
		byNum[p.PartNumber] = p
	}
	for _, rp := range requested {
		cur, ok := byNum[rp.PartNumber]
		if !ok || !sameDigests(cur.BlobDigests, rp.BlobDigests) {
			return ErrInvalidPart
		}
		if cur.State == registry.PartAccepted || len(cur.BlobDigests) == 0 {
			continue
		}
		parked, err := q.anyParked(ctx, space, cur.BlobDigests)
		if err != nil {
			return err
		}
		if parked {
			return fmt.Errorf("multipartqueue: part %d is not accepted yet; retry", cur.PartNumber)
		}
		return ErrInvalidPart
	}
	return nil
}

// acceptAndWait queues the accept of each blob of the parts Complete names
// that are still parked, committed at once, and waits for them, every step
// bounded by expires.
func (q *Queue) acceptAndWait(ctx context.Context, space did.DID, requested []registry.MultipartPart, expires time.Time) error {
	ctx, cancel := context.WithDeadline(ctx, expires)
	defer cancel()
	var parked []registry.MultipartPart
	for _, p := range requested {
		if p.State == registry.PartParked {
			parked = append(parked, p)
		}
	}
	if _, err := q.runner.SubmitAll(ctx, blobAccepts(space, parked)); err != nil {
		return err
	}
	return q.awaitBlobAccepts(ctx, space, requested, expires)
}

// awaitBlobAccepts waits, until expires, for the pending accepts of the
// blobs of parts in space to be over, making each due now first. The
// space's accepts are brought forward by their subjects in one statement,
// which returns them and stops their being replaced, so each keeps its ID
// whatever another upload submits meanwhile. Other uploads' accepts in the
// space are due now already, so bringing them forward too changes nothing
// for them. An accept that fails and waits to retry is waited for through
// its retries: Complete succeeds if one of them does within the wait. A blob
// with two accepts, one queued here beside an earlier one waiting to retry,
// is waited for until either succeeds.
func (q *Queue) awaitBlobAccepts(ctx context.Context, space did.DID, parts []registry.MultipartPart, expires time.Time) error {
	due, err := q.blobs.ExpediteAccepts(ctx, space)
	if err != nil {
		return fmt.Errorf("bring the blobs' accepts forward: %w", err)
	}
	seen := map[string]bool{}
	var waiting [][]taskrunner.TaskID
	for _, p := range parts {
		for _, d := range p.BlobDigests {
			subject := spaceshardqueue.Subject(space, d)
			if !seen[subject] {
				seen[subject] = true
				if len(due[subject]) > 0 {
					waiting = append(waiting, due[subject])
				}
			}
		}
	}

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		first error
		slots = make(chan struct{}, acceptWaits)
	)
	for _, ids := range waiting {
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-slots; wg.Done() }()
			err := q.blobs.AwaitAccepts(ctx, ids)
			if errors.Is(err, context.DeadlineExceeded) && !time.Now().Before(expires) {
				err = errAcceptWaitExpired
			}
			if err != nil {
				mu.Lock()
				if first == nil {
					first = err
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return first
}

// anyParked reports whether any of digests is still parked in space, its
// accept yet to succeed.
func (q *Queue) anyParked(ctx context.Context, space did.DID, digests []mh.Multihash) (bool, error) {
	for _, d := range digests {
		_, err := q.parks.GetPark(ctx, space, d)
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, registry.ErrNotFound) {
			return false, fmt.Errorf("multipartqueue: lookup park: %w", err)
		}
	}
	return false, nil
}

// sameDigests reports whether a and b list the same digests in order.
func sameDigests(a, b []mh.Multihash) bool {
	return slices.EqualFunc(a, b, func(x, y mh.Multihash) bool { return bytes.Equal(x, y) })
}
