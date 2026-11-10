package s3frontend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
// trigger and reports each seal (QueueShip), and a ship ships every sealed
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

// registerShip registers queue-bucket.ship.
func (b *Backend) registerShip(r *taskrunner.Runner) error {
	return r.Handle(registry.BucketQueue, registry.BucketShipAction, b.runShip, taskrunner.WithRetry(shipRetry))
}

// QueueShip queues a ship of bucket's sealed catalog segments, due now, as
// a replacement, so a bucket has at most one pending. The log calls it when
// a segment seals. A ship that cannot be queued is owed, and queued again
// with backoff until it is (retryOwedShips): an idle bucket has no next seal
// to queue one.
func (b *Backend) QueueShip(bucket string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.queueShip(ctx, bucket); err != nil {
		b.logger.Warn("could not queue a catalog ship; it is retried",
			zap.String("bucket", bucket), zap.Error(err))
		b.oweShip(bucket)
	}
}

// awaitShipAuthority records that bucket's ship found no authority to send
// with. Retrying cannot help, but each commit that appends to the bucket's
// log captures fresh authority and queues the ship again with its own writes
// (awaitingShip), until a ship succeeds. A seal can be a long way off, so the
// ship does not wait for one.
func (b *Backend) awaitShipAuthority(bucket string) {
	b.shipMu.Lock()
	defer b.shipMu.Unlock()
	if b.shipAwaiting == nil {
		b.shipAwaiting = map[string]bool{}
	}
	b.shipAwaiting[bucket] = true
}

// awaitingShip reports whether bucket's ship awaits authority.
func (b *Backend) awaitingShip(bucket string) bool {
	b.shipMu.Lock()
	defer b.shipMu.Unlock()
	return b.shipAwaiting[bucket]
}

// shipNoticeFirst and shipNoticeMax bound the backoff between attempts to
// queue an owed ship. Package-level so tests can shrink them.
var (
	shipNoticeFirst = time.Second
	shipNoticeMax   = time.Minute
)

// oweShip records that bucket's ship could not be queued, and starts the
// retries unless they run already or the backend is draining.
func (b *Backend) oweShip(bucket string) {
	b.shipMu.Lock()
	defer b.shipMu.Unlock()
	if b.shipOwed == nil {
		b.shipOwed = map[string]bool{}
	}
	b.shipOwed[bucket] = true
	if b.shipRetrying || b.shipStopped {
		return
	}
	if b.shipStop == nil {
		b.shipStop = make(chan struct{})
	}
	b.shipRetrying = true
	go b.retryOwedShips(b.shipStop)
}

// retryOwedShips queues the owed ships, with backoff while any fails, until
// none is owed or stop closes. A bucket deleted meanwhile is owed nothing
// (queueShip). After a restart, the log reports its unshipped segments again.
func (b *Backend) retryOwedShips(stop <-chan struct{}) {
	delay := shipNoticeFirst
	for {
		select {
		case <-stop:
			return
		case <-time.After(delay):
		}
		b.shipMu.Lock()
		owed := make([]string, 0, len(b.shipOwed))
		for bucket := range b.shipOwed {
			owed = append(owed, bucket)
		}
		b.shipMu.Unlock()
		failed := false
		for _, bucket := range owed {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := b.queueShip(ctx, bucket)
			cancel()
			if err != nil {
				failed = true
				continue
			}
			b.shipMu.Lock()
			delete(b.shipOwed, bucket)
			b.shipMu.Unlock()
		}
		b.shipMu.Lock()
		if len(b.shipOwed) == 0 {
			b.shipRetrying = false
			b.shipMu.Unlock()
			return
		}
		b.shipMu.Unlock()
		if failed {
			delay = min(2*delay, shipNoticeMax)
		}
	}
}

// stopShipRetries ends the retries of owed ships.
func (b *Backend) stopShipRetries() {
	b.shipMu.Lock()
	defer b.shipMu.Unlock()
	if b.shipStopped {
		return
	}
	b.shipStopped = true
	if b.shipStop != nil {
		close(b.shipStop)
	}
}

func (b *Backend) queueShip(ctx context.Context, bucket string) error {
	st, err := b.reg.Get(ctx, bucket)
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
	spec := b.bucketSpec(st.Space, registry.BucketShipAction, args)
	now := registry.DueNow()
	spec.NotBefore, spec.Replace = &now, true
	_, err = b.buckets.Submit(ctx, spec)
	return err
}

// runShip is queue-bucket.ship. No authority to ship with
// (uploader.ErrNoShipAuthority) fails it for good: no retry can find any
// until the bucket's next write captures fresh authority, and that write's
// commit queues the ship again (awaitShipAuthority). Any other failure is
// retried.
func (b *Backend) runShip(ctx context.Context, _ *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	var args BucketTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode ship: %w", err))
	}
	shipper, ok := b.log.(BucketShipper)
	if !ok {
		return nil, nil
	}
	// A ship queued for a bucket since deleted, or for an earlier bucket of
	// the name, has nothing to ship: opening the log would recreate a store
	// for the name, or ship another space's catalog.
	st, err := b.reg.Get(ctx, args.Name)
	if errors.Is(err, registry.ErrNotFound) {
		b.shipMu.Lock()
		delete(b.shipAwaiting, args.Name)
		b.shipMu.Unlock()
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
			b.awaitShipAuthority(args.Name)
			return nil, taskrunner.Permanent(err)
		}
		return nil, err
	}
	b.shipMu.Lock()
	delete(b.shipAwaiting, args.Name)
	b.shipMu.Unlock()
	return nil, nil
}
