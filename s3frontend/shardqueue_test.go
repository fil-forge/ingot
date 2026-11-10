package s3frontend

import (
	"context"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/taskrunner/taskrunnertest"
)

// shardFixture runs the queue-shard handlers over a fresh schema, with a
// backend holding only a cache.
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
			{Name: ShardQueue, Workers: 2},
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
	f.b.cacheWritten(t.Context(), d, local)
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

// read reads the blob from the cache, as a GET served from local disk does.
func (f *shardFixture) read(t *testing.T, d multihash.Multihash) {
	t.Helper()
	rc, err := f.b.cache.OpenBlob(t.Context(), did.Undef, d)
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, rc)
	require.NoError(t, rc.Close())
}

// tasks counts the digest's queue-shard tasks of action in any of states, in
// one query, so a task that finishes meanwhile is counted once either way.
func (f *shardFixture) tasks(t *testing.T, d multihash.Multihash, action string, states ...string) int {
	t.Helper()
	queued, err := taskrunnertest.Tasks(t.Context(), f.env.DB, f.env.Schema, ShardQueue)
	require.NoError(t, err)
	n := 0
	for _, q := range queued {
		if q.Subject == hex.EncodeToString(d) && q.Action == action && slices.Contains(states, q.State) {
			n++
		}
	}
	return n
}

// A written blob's copy is in the cache when the write is done, and leaves it
// once its time to live runs out.
func TestWrittenBlobIsCachedUntilItsTTL(t *testing.T) {
	f := newShardFixture(t, 300*time.Millisecond)
	d := f.hold(t, "a")

	require.True(t, f.cached(d), "in the cache")
	require.Zero(t, f.b.cache.InFlight(), "no copy left in progress")
	require.Eventually(t, func() bool { return !f.cached(d) }, 5*time.Second, 10*time.Millisecond, "evicted at its TTL")
	require.Eventually(t, func() bool { return f.tasks(t, d, shardEvict, "succeeded") == 1 }, 5*time.Second, 10*time.Millisecond)

	// Both tasks were placed on this instance, whose disk holds the file.
	queued, err := taskrunnertest.Tasks(t.Context(), f.env.DB, f.env.Schema, ShardQueue)
	require.NoError(t, err)
	for _, q := range queued {
		require.True(t, q.Node != nil && *q.Node == "did:web:ingot.test", "task %s.%s placed on %v", q.Subject, q.Action, q.Node)
	}
}

// A written blob is cached, and its eviction queued, even when the request
// that wrote it has ended by the time the eviction is queued.
func TestWrittenBlobIsCachedAfterItsRequestEnds(t *testing.T) {
	f := newShardFixture(t, time.Hour)
	local, d := writeLocalCopy(t, f.b, "a")
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	f.b.cacheWritten(ended, d, local)

	require.True(t, f.cached(d), "in the cache")
	require.Eventually(t, func() bool { return f.tasks(t, d, shardEvict, "pending") == 1 }, 5*time.Second, 10*time.Millisecond, "its eviction queued")
}

// A blob written again just as its earlier copy's eviction falls due keeps
// the new copy, with a fresh eviction: the adoption and the new eviction run
// in one task on the digest's subject, so the earlier eviction runs before
// both or is replaced by the new one.
func TestRewriteAtExpiryKeepsTheNewCopy(t *testing.T) {
	f := newShardFixture(t, time.Hour)
	d := f.hold(t, "a")
	require.Equal(t, 1, f.tasks(t, d, shardEvict, "pending"))
	_, err := f.env.DB.Exec(t.Context(), `UPDATE `+f.env.Table("queue_shard_tasks")+`
		SET not_before = now() WHERE subject = $1 AND action = $2 AND state = 'pending'`, hex.EncodeToString(d), shardEvict)
	require.NoError(t, err)

	f.hold(t, "a")
	time.Sleep(200 * time.Millisecond) // past several polls for due work
	require.True(t, f.cached(d), "the new copy outlived the earlier eviction")
	require.Equal(t, 1, f.tasks(t, d, shardEvict, "pending"), "one eviction, a TTL out")
}

// A written copy that cannot be adopted is discarded and leaves nothing in
// flight.
func TestAWrittenCopyNotAdoptedIsDiscarded(t *testing.T) {
	f := newShardFixture(t, time.Hour)
	local, d := writeLocalCopy(t, f.b, "a")
	require.NoError(t, os.RemoveAll(filepath.Dir(f.b.cache.Path(d))))
	f.b.cacheWritten(t.Context(), d, local)
	require.True(t, local.Ended(), "the copy was discarded")
	require.Zero(t, f.b.cache.InFlight(), "no copy left in progress")
}

// A read with less than half the time to live left renews it: the pending
// eviction is replaced by a later one, so the blob outlives its first TTL.
func TestReadRenewsTheTTL(t *testing.T) {
	const ttl = time.Second
	f := newShardFixture(t, ttl)
	d := f.hold(t, "a")
	require.Eventually(t, func() bool { return f.cached(d) }, 5*time.Second, 10*time.Millisecond)
	start := time.Now()

	time.Sleep(ttl * 6 / 10)
	f.read(t, d)
	require.Eventually(t, func() bool { return f.tasks(t, d, shardCache, "succeeded") == 2 }, 5*time.Second, 10*time.Millisecond, "the read renewed")

	time.Sleep(time.Until(start.Add(ttl * 13 / 10)))
	require.True(t, f.cached(d), "still cached past its first TTL")
	require.Equal(t, 1, f.tasks(t, d, shardEvict, "pending"), "one eviction pending, the later")
	require.Eventually(t, func() bool { return !f.cached(d) }, 5*time.Second, 10*time.Millisecond, "evicted at the renewed TTL")
}

// A read with more than half the time to live left writes no task.
func TestReadEarlyInTheTTLWritesNothing(t *testing.T) {
	f := newShardFixture(t, time.Hour)
	d := f.hold(t, "a")
	require.Eventually(t, func() bool { return f.cached(d) }, 5*time.Second, 10*time.Millisecond)

	f.read(t, d)
	f.read(t, d)

	require.Equal(t, 1, f.tasks(t, d, shardCache, "succeeded", "pending"), "only the write's")
}

// Concurrent reads of a blob due for renewal queue one renewal between them.
func TestConcurrentReadsRenewOnce(t *testing.T) {
	f := newShardFixture(t, time.Hour)
	d := f.hold(t, "a")
	require.Eventually(t, func() bool { return f.cached(d) }, 5*time.Second, 10*time.Millisecond)
	// As after a restart: no record of the blob's last renewal.
	f.b.renewals.forget(string(d))

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.read(t, d)
		}()
	}
	wg.Wait()

	require.Equal(t, 2, f.tasks(t, d, shardCache, "succeeded", "pending"), "the write's and one renewal")
}

// A blob nothing caches schedules no eviction: a write that kept no copy
// queues nothing, and a renewal for a copy already gone finds none.
func TestNothingCachedSchedulesNoEviction(t *testing.T) {
	f := newShardFixture(t, time.Hour)
	d := mustSum([]byte("a"))

	f.b.cacheWritten(t.Context(), d, nil)
	require.NoError(t, f.b.submitShardCache(t.Context(), d))

	require.Eventually(t, func() bool { return f.tasks(t, d, shardCache, "succeeded") == 1 }, 5*time.Second, 10*time.Millisecond)
	require.Zero(t, f.tasks(t, d, shardEvict, "pending"))
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
	require.Equal(t, 1, f.tasks(t, digests[0], shardEvict, "succeeded"), "the sync eviction")
	require.Equal(t, 1, f.tasks(t, digests[0], shardEvict, "pending"), "the TTL's eviction, which will find the file gone")
}
