package ingot

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/ingot/internal/testfakes"
	"github.com/fil-forge/ingot/queue/bucketqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/s3frontend"
	"github.com/fil-forge/libforge/identity"
)

// TestTaskRunnerHasTheBackendsQueues: the daemon's runner configures every
// queue the backend registers handlers on, so startup does not fail at
// registration, and is named after the agent's DID for placement.
func TestTaskRunnerHasTheBackendsQueues(t *testing.T) {
	id, err := identity.New("", "did:web:ingot.test")
	require.NoError(t, err, "identity")
	r, err := provideTaskRunner(config.Config{TaskWorkers: config.TaskWorkerConfig{Bucket: 2}}, nil, id, zap.NewNop())
	require.NoError(t, err, "provideTaskRunner")
	cache, err := blockstore.NewBlobCache(filepath.Join(t.TempDir(), "cache"))
	require.NoError(t, err, "NewBlobCache")
	b, err := s3frontend.New(s3frontend.Deps{Tasks: r, Capturer: testfakes.NopUploader{}, Intents: unusedIntents{t: t}, Cache: cache})
	require.NoError(t, err, "New")
	// Its gauges are on the global meter provider, which a later test may
	// install a reader behind.
	t.Cleanup(func() { _ = b.CloseMetrics() })
	got := r.Node()
	require.Equal(t, "did:web:ingot.test", got, "runner node")
}

func TestTaskQueuesGiveTheBucketQueueItsWorkerCount(t *testing.T) {
	for _, q := range taskQueues(config.Config{TaskWorkers: config.TaskWorkerConfig{Bucket: 7}}) {
		if q.Name == bucketqueue.Name {
			require.Equal(t, 7, q.Workers, "bucket queue workers")
			return
		}
	}
	require.FailNow(t, "no bucket queue")
}

func TestTaskQueuesGiveTheBucketQueueTwoWorkersWhenTheConfigLeavesItUnset(t *testing.T) {
	for _, q := range taskQueues(config.Config{}) {
		if q.Name == bucketqueue.Name {
			require.Equal(t, 2, q.Workers, "bucket queue workers")
			return
		}
	}
	require.FailNow(t, "no bucket queue")
}

// unusedIntents is an intent store for a backend that runs no task, so
// nothing asks it anything. The one question an eviction asks fails the test
// with a reason; any other would panic on the embedded nil store.
type unusedIntents struct {
	registry.IntentStore
	t *testing.T
}

func (u unusedIntents) HasIntent(context.Context, multihash.Multihash) (bool, error) {
	assert.Fail(u.t, "the intent store was asked about a blob, but this backend runs no task")
	return false, errors.New("unused intent store")
}
