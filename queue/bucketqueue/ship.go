package bucketqueue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/uploader"
)

// A bucket's catalog ships from its log as a queue-bucket.ship on the
// bucket's subject, so it never runs beside a commit: a commit journals its
// blocks under the segment it appended to, and the ship of that segment
// drops them, without a lock between the two. The log seals on its own
// trigger and reports each seal (QueueShip, which New hands the log as its
// SealReporter callback), and a ship ships every sealed
// segment still unshipped, oldest first. The ship's authority is the one the
// bucket's latest write captured (bucketop.ShipAuthority): a seal from the
// log's age trigger has no request of its own.

// shipRetry retries a ship with no attempt limit: dropping one that failed
// for any reason but its authority would leave an idle bucket's catalog only
// on local disk.
var shipRetry = taskrunner.RetryPolicy{First: time.Second, Max: 5 * time.Minute, Multiplier: 2}

// BucketShipper is the slice of the catalog log a ship needs.
type BucketShipper interface {
	ShipBucket(ctx context.Context, bucket string) error
}

// SealReporter is the slice of the catalog log that reports its seals: the
// queue hands it QueueShip when it is built. It is matched by a runtime type
// assertion (the log is a blockstore.Log), which fails silently on a
// signature drift; the compile-time assertion in the root package pins
// *logstore.Manager to this shape so a drift is a build error instead.
type SealReporter interface {
	OnSealed(fn func(bucket string))
}

// registerShip registers queue-bucket.ship.
func (q *Queue) registerShip(r *taskrunner.Runner) error {
	return r.Handle(registry.BucketQueue, registry.BucketShipAction, q.runShip, taskrunner.WithRetry(shipRetry))
}

// QueueShip queues a ship of bucket's sealed catalog segments, due now, as
// a replacement, so a bucket has at most one pending. The log calls it when
// a segment seals. A ship that cannot be queued is owed, and queued again
// with backoff until it is (retryOwedShips): an idle bucket has no next seal
// to queue one.
func (q *Queue) QueueShip(bucket string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := q.queueShip(ctx, bucket); err != nil {
		q.logger.Warn("could not queue a catalog ship; it is retried",
			zap.String("bucket", bucket), zap.Error(err))
		q.oweShip(bucket)
	}
}

// awaitShipAuthority records that bucket's ship found no authority to send
// with. Retrying cannot help, but each commit that appends to the bucket's
// log captures fresh authority and queues the ship again with its own writes
// (awaitingShip), until a ship succeeds. A seal can be a long way off, so the
// ship does not wait for one.
func (q *Queue) awaitShipAuthority(bucket string) {
	q.ships.mu.Lock()
	defer q.ships.mu.Unlock()
	if q.ships.awaiting == nil {
		q.ships.awaiting = map[string]bool{}
	}
	q.ships.awaiting[bucket] = true
}

// awaitingShip reports whether bucket's ship awaits authority.
func (q *Queue) awaitingShip(bucket string) bool {
	q.ships.mu.Lock()
	defer q.ships.mu.Unlock()
	return q.ships.awaiting[bucket]
}

// queueAwaitedShip queues st's bucket's ship with a commit that appended to
// its log, when the ship awaits authority: the append captured fresh ship
// authority.
func (q *Queue) queueAwaitedShip(ctx context.Context, ex *taskrunner.Exec, st *registry.State) error {
	if !q.awaitingShip(st.Name) {
		return nil
	}
	raw, err := encodeBucketTask(&BucketTask{Name: st.Name})
	if err != nil {
		return err
	}
	spec := q.bucketSpec(st.Space, registry.BucketShipAction, raw)
	now := registry.DueNow()
	spec.NotBefore, spec.Replace = &now, true
	_, err = ex.Submit(ctx, spec)
	return err
}

// The default backoff between attempts to queue an owed ship.
const (
	defaultShipRetryFirst = time.Second
	defaultShipRetryMax   = time.Minute
)

// ships is the state of the buckets' ships that the queue keeps itself.
type ships struct {
	// mu guards the fields below.
	mu sync.Mutex
	// owed are the buckets whose ship could not be queued, which
	// retryOwedShips queues once it can.
	owed map[string]bool
	// awaiting are the buckets whose ship found no authority to send with,
	// which the bucket's next commit queues again (awaitShipAuthority).
	awaiting map[string]bool
	// retrying is set while retryOwedShips runs.
	retrying bool
	// stop, closed by StopShips, ends retryOwedShips.
	stop chan struct{}
	// stopped is set once StopShips has run.
	stopped bool
	// retryFirst and retryMax bound the backoff between its attempts.
	retryFirst, retryMax time.Duration
}

// oweShip records that bucket's ship could not be queued, and starts the
// retries unless they run already or the backend is draining.
func (q *Queue) oweShip(bucket string) {
	q.ships.mu.Lock()
	defer q.ships.mu.Unlock()
	if q.ships.owed == nil {
		q.ships.owed = map[string]bool{}
	}
	q.ships.owed[bucket] = true
	if q.ships.retrying || q.ships.stopped {
		return
	}
	if q.ships.stop == nil {
		q.ships.stop = make(chan struct{})
	}
	q.ships.retrying = true
	go q.retryOwedShips(q.ships.stop)
}

// retryOwedShips queues the owed ships, with backoff while any fails, until
// none is owed or stop closes. A bucket deleted meanwhile is owed nothing
// (queueShip). After a restart, the log reports its unshipped segments again.
func (q *Queue) retryOwedShips(stop <-chan struct{}) {
	delay := q.ships.retryFirst
	for {
		select {
		case <-stop:
			return
		case <-time.After(delay):
		}
		q.ships.mu.Lock()
		owed := make([]string, 0, len(q.ships.owed))
		for bucket := range q.ships.owed {
			owed = append(owed, bucket)
		}
		q.ships.mu.Unlock()
		failed := false
		for _, bucket := range owed {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := q.queueShip(ctx, bucket)
			cancel()
			if err != nil {
				failed = true
				continue
			}
			q.ships.mu.Lock()
			delete(q.ships.owed, bucket)
			q.ships.mu.Unlock()
		}
		q.ships.mu.Lock()
		if len(q.ships.owed) == 0 {
			q.ships.retrying = false
			q.ships.mu.Unlock()
			return
		}
		q.ships.mu.Unlock()
		if failed {
			delay = min(2*delay, q.ships.retryMax)
		}
	}
}

// StopShips ends the retries of owed ships, for a drain.
func (q *Queue) StopShips() {
	q.ships.mu.Lock()
	defer q.ships.mu.Unlock()
	if q.ships.stopped {
		return
	}
	q.ships.stopped = true
	if q.ships.stop != nil {
		close(q.ships.stop)
	}
}

func (q *Queue) queueShip(ctx context.Context, bucket string) error {
	st, err := q.reg.Get(ctx, bucket)
	if errors.Is(err, registry.ErrNotFound) {
		// Deleted: its log goes with it.
		return nil
	}
	if err != nil {
		return err
	}
	args, err := encodeBucketTask(&BucketTask{Name: bucket})
	if err != nil {
		return err
	}
	spec := q.bucketSpec(st.Space, registry.BucketShipAction, args)
	now := registry.DueNow()
	spec.NotBefore, spec.Replace = &now, true
	_, err = q.runner.Submit(ctx, spec)
	return err
}

// runShip is queue-bucket.ship. No authority to ship with
// (uploader.ErrNoShipAuthority) fails it for good: no retry can find any
// until the bucket's next write captures fresh authority, and that write's
// commit queues the ship again (awaitShipAuthority). Any other failure is
// retried.
func (q *Queue) runShip(ctx context.Context, _ *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	var args BucketTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode ship: %w", err))
	}
	shipper, ok := q.log.(BucketShipper)
	if !ok {
		return nil, nil
	}
	// A ship queued for a bucket since deleted, or for an earlier bucket of
	// the name, has nothing to ship: opening the log would recreate a store
	// for the name, or ship another space's catalog.
	st, err := q.reg.Get(ctx, args.Name)
	if errors.Is(err, registry.ErrNotFound) {
		q.ships.mu.Lock()
		delete(q.ships.awaiting, args.Name)
		q.ships.mu.Unlock()
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if st.Space.String() != t.Subject {
		return nil, nil
	}
	if err := shipper.ShipBucket(ctx, args.Name); err != nil {
		err = fmt.Errorf("ship catalog of %q: %w", args.Name, err)
		if errors.Is(err, uploader.ErrNoShipAuthority) {
			q.awaitShipAuthority(args.Name)
			return nil, taskrunner.Permanent(err)
		}
		return nil, err
	}
	q.ships.mu.Lock()
	delete(q.ships.awaiting, args.Name)
	q.ships.mu.Unlock()
	return nil, nil
}
