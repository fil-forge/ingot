package s3frontend

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/queue/shardqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/taskrunner/taskrunnertest"
)

// shardFixture runs a backend holding only a cache, whose queue-shard
// handlers run over a fresh schema.
type shardFixture struct {
	b   *Backend
	env taskrunnertest.Env
}

func newShardFixture(t *testing.T, ttl time.Duration) *shardFixture {
	t.Helper()
	env := taskrunnertest.NewEnv(t)
	r, err := taskrunner.New(env.DB, taskrunner.Options{
		Schema: env.Schema,
		// New registers every queue's handlers; only queue-shard runs any.
		Queues: []taskrunner.QueueOptions{
			{Name: registry.MultipartQueue},
			{Name: registry.BucketQueue},
			{Name: registry.UploadQueue},
			{Name: registry.ReleaseQueue},
			{Name: shardqueue.Name, Workers: 2},
		},
		Node:         "did:web:ingot.test",
		LeaseTTL:     time.Second,
		PollInterval: 20 * time.Millisecond,
		Logger:       zaptest.NewLogger(t),
	})
	require.NoError(t, err)
	dir := t.TempDir()
	cache, err := blockstore.NewBlobCache(filepath.Join(dir, "cache"))
	require.NoError(t, err)
	b, err := New(Deps{Tasks: r, Cache: cache, Intents: newTestStore(t), CacheTTL: ttl, Logger: zaptest.NewLogger(t)})
	require.NoError(t, err)
	require.NoError(t, r.Migrate(context.Background()))
	require.NoError(t, r.Start(context.Background()))
	t.Cleanup(func() { _ = r.Stop(context.Background()) })
	return &shardFixture{b: b, env: env}
}

// hold writes a blob's local copy and hands it to the cache, as a PUT does
// once the provider has accepted it.
func (f *shardFixture) hold(t *testing.T, body string) multihash.Multihash {
	t.Helper()
	local, d := writeLocalCopy(t, f.b, body)
	f.b.shards.Cache(t.Context(), d, local)
	return d
}

// writeLocalCopy writes body to a temporary file in b's cache, as a stream
// does, and returns it with the digest of body.
func writeLocalCopy(t *testing.T, b *Backend, body string) (*blockstore.TempBlob, multihash.Multihash) {
	t.Helper()
	local, err := b.cache.CreateTemp()
	require.NoError(t, err)
	_, err = local.Write([]byte(body))
	require.NoError(t, err)
	return local, mustSum([]byte(body))
}

func (f *shardFixture) cached(d multihash.Multihash) bool {
	_, err := os.Stat(f.b.cache.Path(d))
	return err == nil
}

// tasks counts the digest's queue-shard tasks of action in any of states, in
// one query, so a task that finishes meanwhile is counted once either way.
func (f *shardFixture) tasks(t *testing.T, d multihash.Multihash, action string, states ...string) int {
	t.Helper()
	queued, err := taskrunnertest.Tasks(t.Context(), f.env.DB, f.env.Schema, shardqueue.Name)
	require.NoError(t, err)
	n := 0
	for _, q := range queued {
		if q.Subject == hex.EncodeToString(d) && q.Action == action && slices.Contains(states, q.State) {
			n++
		}
	}
	return n
}

// The budget pass evicts through queue-shard.evict, run as a sync task, so its
// removal takes its turn with the digest's other tasks; the eviction pending
// at the TTL is left to find the file gone.
func TestBudgetPassEvictsThroughTheQueue(t *testing.T) {
	f := newShardFixture(t, time.Hour)
	var digests []multihash.Multihash
	for _, c := range "abc" {
		d := f.hold(t, strings.Repeat(string(c), 1000))
		require.Eventually(t, func() bool { return f.cached(d) }, 5*time.Second, 10*time.Millisecond)
		digests = append(digests, d)
	}
	// The pass evicts by modification time, oldest first. Each file is
	// cached when the queue runs its task, in no set order, so the test
	// dates them a minute apart, in upload order.
	now := time.Now()
	for i, d := range digests {
		at := now.Add(time.Duration(i-len(digests)) * time.Minute)
		require.NoError(t, os.Chtimes(f.b.cache.Path(d), at, at))
	}
	budgetToEvict(f.b, 1, 1000)

	stats, err := f.b.SweepLocalBlobs(t.Context())
	require.NoError(t, err)

	require.Equal(t, LocalBlobSweepStats{BudgetFiles: 1, BudgetBytes: 1000}, stats)
	require.Equal(t, []bool{false, true, true}, []bool{f.cached(digests[0]), f.cached(digests[1]), f.cached(digests[2])})
	require.Equal(t, 1, f.tasks(t, digests[0], "evict", "succeeded"), "the sync eviction")
	require.Equal(t, 1, f.tasks(t, digests[0], "evict", "pending"), "the TTL's eviction, which will find the file gone")
}
