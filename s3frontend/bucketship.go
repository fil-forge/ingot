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

// registerShip registers queue-bucket.ship, and queues the ships the log
// reported before the handler existed.
func (b *Backend) registerShip(r *taskrunner.Runner) error {
	if err := r.Handle(registry.BucketQueue, registry.BucketShipAction, b.runShip, taskrunner.WithRetry(shipRetry)); err != nil {
		return err
	}
	b.shipMu.Lock()
	early := b.earlyShips
	b.earlyShips, b.shipReady = nil, true
	b.shipMu.Unlock()
	for _, bucket := range early {
		b.QueueShip(bucket)
	}
	return nil
}

// QueueShip queues a ship of bucket's sealed catalog segments, due now, as
// a replacement, so a bucket has at most one pending. The log calls it when
// a segment seals. A failure to queue is logged; the bucket's next seal
// queues one again.
func (b *Backend) QueueShip(bucket string) {
	b.shipMu.Lock()
	if !b.shipReady {
		b.earlyShips = append(b.earlyShips, bucket)
		b.shipMu.Unlock()
		return
	}
	b.shipMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.queueShip(ctx, bucket); err != nil {
		b.logger.Warn("could not queue a catalog ship; the bucket's next seal queues one",
			zap.String("bucket", bucket), zap.Error(err))
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
// seal queues a ship of every segment left unshipped. Any other failure is
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
	if err := shipper.ShipBucket(ctx, args.Name); err != nil {
		err = fmt.Errorf("ship catalog of %q: %w", args.Name, err)
		if errors.Is(err, uploader.ErrNoShipAuthority) {
			return nil, taskrunner.Permanent(err)
		}
		return nil, err
	}
	return nil, nil
}
