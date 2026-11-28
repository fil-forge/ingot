package ingot

import (
	"context"
	"errors"

	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/ingot/queue/bucketqueue"
	"path/filepath"
	"testing"

	"github.com/fil-forge/libforge/identity"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/s3frontend"
)

// TestTaskRunnerHasTheBackendsQueues: the daemon's runner configures every
// queue the backend registers handlers on, so startup does not fail at
// registration, and is named after the agent's DID for placement.
func TestTaskRunnerHasTheBackendsQueues(t *testing.T) {
	id, err := identity.New("", "did:web:ingot.test")
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	r, err := provideTaskRunner(config.Config{TaskWorkers: config.TaskWorkerConfig{Bucket: 2}}, nil, id, zap.NewNop())
	if err != nil {
		t.Fatalf("provideTaskRunner: %v", err)
	}
	cache, err := blockstore.NewBlobCache(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("NewBlobCache: %v", err)
	}
	b, err := s3frontend.New(s3frontend.Deps{Tasks: r, Capturer: testfakes.NopUploader{}, Intents: unusedIntents{t: t}, Cache: cache})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Its gauges are on the global meter provider, which a later test may
	// install a reader behind.
	t.Cleanup(func() { _ = b.CloseMetrics() })
	if got := r.Node(); got != "did:web:ingot.test" {
		t.Fatalf("runner node = %q, want the agent's DID", got)
	}
}

func TestTaskQueuesGiveTheBucketQueueItsWorkerCount(t *testing.T) {
	for _, q := range taskQueues(config.Config{TaskWorkers: config.TaskWorkerConfig{Bucket: 7}}) {
		if q.Name == bucketqueue.Name {
			if q.Workers != 7 {
				t.Fatalf("bucket queue workers = %d, want 7", q.Workers)
			}
			return
		}
	}
	t.Fatal("no bucket queue")
}

func TestTaskQueuesGiveTheBucketQueueTwoWorkersWhenTheConfigLeavesItUnset(t *testing.T) {
	for _, q := range taskQueues(config.Config{}) {
		if q.Name == bucketqueue.Name {
			if q.Workers != 2 {
				t.Fatalf("bucket queue workers = %d, want 2", q.Workers)
			}
			return
		}
	}
	t.Fatal("no bucket queue")
}

// unusedIntents is an intent store for a backend that runs no task, so
// nothing asks it anything. The one question an eviction asks fails the test
// with a reason; any other would panic on the embedded nil store.
type unusedIntents struct {
	registry.IntentStore
	t *testing.T
}

func (u unusedIntents) HasIntent(context.Context, multihash.Multihash) (bool, error) {
	u.t.Errorf("the intent store was asked about a blob, but this backend runs no task")
	return false, errors.New("unused intent store")
}
