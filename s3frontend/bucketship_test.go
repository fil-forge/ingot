package s3frontend

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

// shipFails is a catalog log whose ships fail with err.
type shipFails struct {
	blockstore.Log
	err   error
	ships atomic.Int32
}

func (s *shipFails) ShipBucket(context.Context, string) error {
	s.ships.Add(1)
	return s.err
}

// TestShipWithoutAuthorityFailsForGood: a ship with no authority to send
// with fails at once, since only the bucket's next write can bring some, and
// that write's seal queues a ship of its own. Any other failure is retried.
func TestShipWithoutAuthorityFailsForGood(t *testing.T) {
	for name, tc := range map[string]struct {
		err   error
		state string
	}{
		"no authority": {fmt.Errorf("ship segment 3: %w", uploader.ErrNoShipAuthority), "failed"},
		"unreachable":  {errors.New("upload service unreachable"), "pending"},
	} {
		t.Run(name, func(t *testing.T) {
			log := &shipFails{err: tc.err}
			b, mem := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) {
				log.Log = d.Log
				d.Log = log
			})
			ctx := context.Background()
			if err := b.queueShip(ctx, "bk"); err != nil {
				t.Fatalf("queue ship: %v", err)
			}
			deadline := time.Now().Add(10 * time.Second)
			for log.ships.Load() == 0 || shipState(t, mem) == "pending" && tc.state == "failed" {
				if time.Now().After(deadline) {
					t.Fatalf("ship ran %d times, state %q; want it %s", log.ships.Load(), shipState(t, mem), tc.state)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if got := shipState(t, mem); got != tc.state {
				t.Fatalf("ship state = %q, want %q", got, tc.state)
			}
		})
	}
}

// shipState is the state of bk's only queue-bucket.ship task.
func shipState(t *testing.T, mem *testStore) string {
	t.Helper()
	tasks, err := mem.queued(context.Background(), registry.BucketQueue)
	if err != nil {
		t.Fatalf("queued: %v", err)
	}
	state := ""
	for _, task := range tasks {
		if task.Action == registry.BucketShipAction {
			if state != "" {
				t.Fatal("more than one ship queued")
			}
			state = task.State
		}
	}
	return state
}

// flakyGet is a registry whose first fails lookups fail, as a database
// failing over does.
type flakyGet struct {
	registry.Registry
	fails atomic.Int32
}

func (f *flakyGet) Get(ctx context.Context, name string) (*registry.State, error) {
	if f.fails.Add(-1) >= 0 {
		return nil, errors.New("connection reset")
	}
	return f.Registry.Get(ctx, name)
}

// A ship notice that cannot be queued is retried until it is: an idle bucket
// has no next seal to queue one.
func TestAShipThatCannotBeQueuedIsRetried(t *testing.T) {
	saved := shipNoticeFirst
	shipNoticeFirst = 20 * time.Millisecond
	t.Cleanup(func() { shipNoticeFirst = saved })
	reg := &flakyGet{}
	b, mem := newDeferredBackend(t, &parkingUploader{}, func(d *Deps) {
		reg.Registry = d.Registry
		d.Registry = reg
	})
	reg.fails.Store(2)
	b.QueueShip("bk")

	require.Eventually(t, func() bool {
		tasks, err := mem.queued(context.Background(), registry.BucketQueue)
		if err != nil {
			return false
		}
		for _, task := range tasks {
			if task.Action == registry.BucketShipAction {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "the owed ship was never queued")
}
