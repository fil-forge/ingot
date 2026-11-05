package s3frontend

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/taskrunner/taskrunnertest"
)

// shardFixture runs the queue-shard handlers over a fresh schema, with a
// backend holding only a spool and a cache.
type shardFixture struct {
	b   *Backend
	env taskrunnertest.Env
}

func newShardFixture(t *testing.T, ttl time.Duration) *shardFixture {
	t.Helper()
	env := taskrunnertest.NewEnv(t)
	r, err := taskrunner.New(env.DB, taskrunner.Options{
		Schema:       env.Schema,
		Queues:       []taskrunner.QueueOptions{{Name: ShardQueue, Workers: 2}},
		Node:         "did:web:ingot.test",
		LeaseTTL:     time.Second,
		PollInterval: 20 * time.Millisecond,
		Logger:       zaptest.NewLogger(t),
	})
	require.NoError(t, err)
	dir := t.TempDir()
	spool, err := blockstore.NewSpool(filepath.Join(dir, "spool"))
	require.NoError(t, err)
	cache, err := blockstore.NewBlobCache(filepath.Join(dir, "cache"))
	require.NoError(t, err)
	b := New(Deps{Spool: spool, Cache: cache, Intents: inmem.NewMemStore(), CacheTTL: ttl, Logger: zaptest.NewLogger(t)})
	require.NoError(t, b.RegisterShardTasks(r))
	require.NoError(t, r.Migrate(context.Background()))
	require.NoError(t, r.Start(context.Background()))
	t.Cleanup(func() { _ = r.Stop(context.Background()) })
	return &shardFixture{b: b, env: env}
}

// hold spools a blob and hands it to the cache as a held blob, as a PUT does
// once the provider has accepted it.
func (f *shardFixture) hold(t *testing.T, body string) multihash.Multihash {
	t.Helper()
	d, _, err := f.b.spool.WriteBlob(t.Context(), bytes.NewReader([]byte(body)))
	require.NoError(t, err)
	f.b.cacheHeld(t.Context(), testutil.RandomDID(t), d)
	return d
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

// tasks counts the digest's queue-shard tasks of action in state.
func (f *shardFixture) tasks(t *testing.T, d multihash.Multihash, action, state string) int {
	t.Helper()
	var n int
	require.NoError(t, f.env.DB.QueryRow(t.Context(),
		`SELECT count(*) FROM `+f.env.Table("queue_shard_tasks")+` WHERE subject = $1 AND action = $2 AND state = $3`,
		hex.EncodeToString(d), action, state).Scan(&n))
	return n
}

// A held blob moves into the cache, and leaves it once its time to live runs
// out.
func TestHeldBlobIsCachedUntilItsTTL_Live(t *testing.T) {
	f := newShardFixture(t, 300*time.Millisecond)
	d := f.hold(t, "a")

	require.Eventually(t, func() bool { return f.cached(d) }, 5*time.Second, 10*time.Millisecond, "moved into the cache")
	require.False(t, fileExists(f.b.spool.Path(d)), "no spool copy left")
	require.Eventually(t, func() bool { return !f.cached(d) }, 5*time.Second, 10*time.Millisecond, "evicted at its TTL")
	require.Eventually(t, func() bool { return f.tasks(t, d, shardEvict, "succeeded") == 1 }, 5*time.Second, 10*time.Millisecond)

	// Both tasks were placed on this instance, whose disk holds the file.
	var unplaced int
	require.NoError(t, f.env.DB.QueryRow(t.Context(),
		`SELECT count(*) FROM `+f.env.Table("queue_shard_tasks")+` WHERE node IS DISTINCT FROM $1`,
		"did:web:ingot.test").Scan(&unplaced))
	require.Zero(t, unplaced)
}

// A held blob moves into the cache even when the request that held it has
// ended by the time the move is queued.
func TestHeldBlobIsCachedAfterItsRequestEnds_Live(t *testing.T) {
	f := newShardFixture(t, time.Hour)
	d, _, err := f.b.spool.WriteBlob(t.Context(), bytes.NewReader([]byte("a")))
	require.NoError(t, err)
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	f.b.cacheHeld(ended, testutil.RandomDID(t), d)

	require.Eventually(t, func() bool { return f.cached(d) }, 5*time.Second, 10*time.Millisecond, "moved into the cache")
	require.Eventually(t, func() bool { return f.tasks(t, d, shardEvict, "pending") == 1 }, 5*time.Second, 10*time.Millisecond, "its eviction queued")
}

// A read with less than half the time to live left renews it: the pending
// eviction is replaced by a later one, so the blob outlives its first TTL.
func TestReadRenewsTheTTL_Live(t *testing.T) {
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
func TestReadEarlyInTheTTLWritesNothing_Live(t *testing.T) {
	f := newShardFixture(t, time.Hour)
	d := f.hold(t, "a")
	require.Eventually(t, func() bool { return f.cached(d) }, 5*time.Second, 10*time.Millisecond)

	f.read(t, d)
	f.read(t, d)

	require.Equal(t, 1, f.tasks(t, d, shardCache, "succeeded")+f.tasks(t, d, shardCache, "pending"), "only the move")
}

// Concurrent reads of a blob due for renewal queue one renewal between them.
func TestConcurrentReadsRenewOnce_Live(t *testing.T) {
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

	require.Equal(t, 2, f.tasks(t, d, shardCache, "succeeded")+f.tasks(t, d, shardCache, "pending"), "the move and one renewal")
}

// A blob nothing caches schedules no eviction: a renewal for a copy already
// gone, or a move that found no spool copy.
func TestNothingCachedSchedulesNoEviction_Live(t *testing.T) {
	f := newShardFixture(t, time.Hour)
	d, _, err := f.b.spool.WriteBlob(t.Context(), bytes.NewReader([]byte("a")))
	require.NoError(t, err)
	_, err = f.b.spool.Remove(d)
	require.NoError(t, err)

	f.b.cacheHeld(t.Context(), testutil.RandomDID(t), d)
	require.NoError(t, f.b.submitShardCache(t.Context(), d, false))

	require.Eventually(t, func() bool { return f.tasks(t, d, shardCache, "succeeded") == 2 }, 5*time.Second, 10*time.Millisecond)
	require.Zero(t, f.tasks(t, d, shardEvict, "pending"))
}

// The budget pass evicts through queue-shard.evict, run as a sync task, so its
// removal takes its turn with the digest's other tasks; the eviction pending
// at the TTL is left to find the file gone.
func TestBudgetPassEvictsThroughTheQueue_Live(t *testing.T) {
	f := newShardFixture(t, time.Hour)
	var digests []multihash.Multihash
	for _, c := range "abc" {
		d := f.hold(t, strings.Repeat(string(c), 1000))
		require.Eventually(t, func() bool { return f.cached(d) }, 5*time.Second, 10*time.Millisecond)
		digests = append(digests, d)
		// Distinct modification times, so the oldest is evicted first.
		time.Sleep(2 * time.Millisecond)
	}
	budgetToEvict(f.b, 1, 1000)

	stats, err := f.b.SweepLocalBlobs(t.Context())
	require.NoError(t, err)

	require.Equal(t, LocalBlobSweepStats{BudgetFiles: 1, BudgetBytes: 1000}, stats)
	require.Equal(t, []bool{false, true, true}, []bool{f.cached(digests[0]), f.cached(digests[1]), f.cached(digests[2])})
	require.Equal(t, 1, f.tasks(t, digests[0], shardEvict, "succeeded"), "the sync eviction")
	require.Equal(t, 1, f.tasks(t, digests[0], shardEvict, "pending"), "the TTL's eviction, which will find the file gone")
}
