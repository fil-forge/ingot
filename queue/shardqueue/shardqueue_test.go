package shardqueue

import (
	"context"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/taskrunner/taskrunnertest"
)

// fixture runs the queue over a fresh schema and a cache of its own.
type fixture struct {
	q     *Queue
	cache *blockstore.BlobCache
	env   taskrunnertest.Env
}

func newFixture(t *testing.T, ttl time.Duration) *fixture {
	t.Helper()
	env := taskrunnertest.NewEnv(t)
	r, err := taskrunner.New(env.DB, taskrunner.Options{
		Schema:       env.Schema,
		Queues:       []taskrunner.QueueOptions{{Name: Name, Workers: 2}},
		Node:         "did:web:ingot.test",
		LeaseTTL:     time.Second,
		PollInterval: 20 * time.Millisecond,
		Logger:       zaptest.NewLogger(t),
	})
	require.NoError(t, err)
	cache, err := blockstore.NewBlobCache(filepath.Join(t.TempDir(), "cache"))
	require.NoError(t, err)
	q, err := New(r, Deps{Cache: cache, TTL: ttl, Logger: zaptest.NewLogger(t)})
	require.NoError(t, err)
	require.NoError(t, r.Migrate(context.Background()))
	require.NoError(t, r.Start(context.Background()))
	t.Cleanup(func() { _ = r.Stop(context.Background()) })
	return &fixture{q: q, cache: cache, env: env}
}

// hold writes a blob's local copy and caches it, as a PUT does once the
// provider has accepted it.
func (f *fixture) hold(t *testing.T, body string) multihash.Multihash {
	t.Helper()
	local, d := f.writeLocalCopy(t, body)
	f.q.Cache(t.Context(), d, local)
	return d
}

// writeLocalCopy writes body to a temporary file in the cache, as a stream
// does, and returns it with the digest of body.
func (f *fixture) writeLocalCopy(t *testing.T, body string) (*blockstore.TempBlob, multihash.Multihash) {
	t.Helper()
	local, err := f.cache.CreateTemp()
	require.NoError(t, err)
	_, err = local.Write([]byte(body))
	require.NoError(t, err)
	d, err := multihash.Sum([]byte(body), multihash.SHA2_256, -1)
	require.NoError(t, err)
	return local, d
}

func (f *fixture) cached(d multihash.Multihash) bool {
	_, err := os.Stat(f.cache.Path(d))
	return err == nil
}

// read reads the blob from the cache, as a GET served from local disk does.
func (f *fixture) read(t *testing.T, d multihash.Multihash) {
	t.Helper()
	rc, err := f.cache.OpenBlob(t.Context(), did.Undef, d)
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, rc)
	require.NoError(t, rc.Close())
}

// tasks counts the digest's queue-shard tasks of action in any of states, in
// one query, so a task that finishes meanwhile is counted once either way.
func (f *fixture) tasks(t *testing.T, d multihash.Multihash, action string, states ...string) int {
	t.Helper()
	queued, err := taskrunnertest.Tasks(t.Context(), f.env.DB, f.env.Schema, Name)
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
	f := newFixture(t, 300*time.Millisecond)
	d := f.hold(t, "a")

	require.True(t, f.cached(d), "in the cache")
	require.Zero(t, f.cache.InFlight(), "no copy left in progress")
	require.Eventually(t, func() bool { return !f.cached(d) }, 5*time.Second, 10*time.Millisecond, "evicted at its TTL")
	require.Eventually(t, func() bool { return f.tasks(t, d, evictAction, "succeeded") == 1 }, 5*time.Second, 10*time.Millisecond)

	// Both tasks were placed on this instance, whose disk holds the file.
	queued, err := taskrunnertest.Tasks(t.Context(), f.env.DB, f.env.Schema, Name)
	require.NoError(t, err)
	for _, q := range queued {
		require.True(t, q.Node != nil && *q.Node == "did:web:ingot.test", "task %s.%s placed on %v", q.Subject, q.Action, q.Node)
	}
}

// A written blob is cached, and its eviction queued, even when the request
// that wrote it has ended by the time the eviction is queued.
func TestWrittenBlobIsCachedAfterItsRequestEnds(t *testing.T) {
	f := newFixture(t, time.Hour)
	local, d := f.writeLocalCopy(t, "a")
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	f.q.Cache(ended, d, local)

	require.True(t, f.cached(d), "in the cache")
	require.Eventually(t, func() bool { return f.tasks(t, d, evictAction, "pending") == 1 }, 5*time.Second, 10*time.Millisecond, "its eviction queued")
}

// A blob written again just as its earlier copy's eviction falls due keeps
// the new copy, with a fresh eviction: the adoption and the new eviction run
// in one task on the digest's subject, so the earlier eviction runs before
// both or is replaced by the new one.
func TestRewriteAtExpiryKeepsTheNewCopy(t *testing.T) {
	f := newFixture(t, time.Hour)
	d := f.hold(t, "a")
	require.Equal(t, 1, f.tasks(t, d, evictAction, "pending"))
	_, err := f.env.DB.Exec(t.Context(), `UPDATE `+f.env.Table("queue_shard_tasks")+`
		SET not_before = now() WHERE subject = $1 AND action = $2 AND state = 'pending'`, hex.EncodeToString(d), evictAction)
	require.NoError(t, err)

	f.hold(t, "a")
	time.Sleep(200 * time.Millisecond) // past several polls for due work
	require.True(t, f.cached(d), "the new copy outlived the earlier eviction")
	require.Equal(t, 1, f.tasks(t, d, evictAction, "pending"), "one eviction, a TTL out")
}

// A written copy that cannot be adopted is discarded and leaves nothing in
// flight.
func TestAWrittenCopyNotAdoptedIsDiscarded(t *testing.T) {
	f := newFixture(t, time.Hour)
	local, d := f.writeLocalCopy(t, "a")
	require.NoError(t, os.RemoveAll(filepath.Dir(f.cache.Path(d))))
	f.q.Cache(t.Context(), d, local)
	require.True(t, local.Ended(), "the copy was discarded")
	require.Zero(t, f.cache.InFlight(), "no copy left in progress")
}

// A write that kept no copy caches nothing.
func TestNoCopyCachesNothing(t *testing.T) {
	f := newFixture(t, time.Hour)
	d, err := multihash.Sum([]byte("a"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	f.q.Cache(t.Context(), d, nil)
	require.Zero(t, f.tasks(t, d, cacheAction, "succeeded", "pending"))
}

// A read with less than half the time to live left renews it: the pending
// eviction is replaced by a later one, so the blob outlives its first TTL.
func TestReadRenewsTheTTL(t *testing.T) {
	const ttl = time.Second
	f := newFixture(t, ttl)
	d := f.hold(t, "a")
	require.Eventually(t, func() bool { return f.cached(d) }, 5*time.Second, 10*time.Millisecond)
	start := time.Now()

	time.Sleep(ttl * 6 / 10)
	f.read(t, d)
	require.Eventually(t, func() bool { return f.tasks(t, d, cacheAction, "succeeded") == 2 }, 5*time.Second, 10*time.Millisecond, "the read renewed")

	time.Sleep(time.Until(start.Add(ttl * 13 / 10)))
	require.True(t, f.cached(d), "still cached past its first TTL")
	require.Equal(t, 1, f.tasks(t, d, evictAction, "pending"), "one eviction pending, the later")
	require.Eventually(t, func() bool { return !f.cached(d) }, 5*time.Second, 10*time.Millisecond, "evicted at the renewed TTL")
}

// A read with more than half the time to live left writes no task.
func TestReadEarlyInTheTTLWritesNothing(t *testing.T) {
	f := newFixture(t, time.Hour)
	d := f.hold(t, "a")
	require.Eventually(t, func() bool { return f.cached(d) }, 5*time.Second, 10*time.Millisecond)

	f.read(t, d)
	f.read(t, d)

	require.Equal(t, 1, f.tasks(t, d, cacheAction, "succeeded", "pending"), "only the write's")
}

// Concurrent reads of a blob due for renewal queue one renewal between them.
func TestConcurrentReadsRenewOnce(t *testing.T) {
	f := newFixture(t, time.Hour)
	d := f.hold(t, "a")
	require.Eventually(t, func() bool { return f.cached(d) }, 5*time.Second, 10*time.Millisecond)
	// As after a restart: no record of the blob's last renewal.
	f.q.renewals.forget(string(d))

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.read(t, d)
		}()
	}
	wg.Wait()

	require.Equal(t, 2, f.tasks(t, d, cacheAction, "succeeded", "pending"), "the write's and one renewal")
}

// A renewal for a copy already gone schedules no eviction.
func TestNothingCachedSchedulesNoEviction(t *testing.T) {
	f := newFixture(t, time.Hour)
	d, err := multihash.Sum([]byte("a"), multihash.SHA2_256, -1)
	require.NoError(t, err)

	require.NoError(t, f.q.submitCache(t.Context(), d))

	require.Eventually(t, func() bool { return f.tasks(t, d, cacheAction, "succeeded") == 1 }, 5*time.Second, 10*time.Millisecond)
	require.Zero(t, f.tasks(t, d, evictAction, "pending"))
}

// An eviction reports the copies it removes, and why, except the budget
// pass's, which counts its own.
func TestEvictionsReportTheirRemovals(t *testing.T) {
	f := newFixture(t, time.Hour)
	var mu sync.Mutex
	var got []Removal
	f.q.removed = func(_ context.Context, why Removal, freed int64) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, why)
	}
	budget, released := f.hold(t, "a"), f.hold(t, "b")

	_, err := f.q.Evict(t.Context(), budget)
	require.NoError(t, err)
	spec, err := f.q.ReleasedSpec(released)
	require.NoError(t, err)
	_, err = f.q.tasks.Run(t.Context(), spec)
	require.NoError(t, err)

	require.False(t, f.cached(budget) || f.cached(released), "both copies removed")
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []Removal{Released}, got)
}
