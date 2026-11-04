package s3frontend

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/taskrunner"
)

// A cached blob's lifetime is kept by queue-shard tasks, serialized on the
// digest, since the cache holds one file per digest whichever spaces wrote it.
// Once the provider holds a blob, a queue-shard.cache task moves its spool copy
// into the cache and schedules a queue-shard.evict for when the copy's time to
// live runs out. A read from the cache with less than half that time left runs
// queue-shard.cache again with nothing to move, which schedules a later
// eviction. Evictions are submitted as replacements, so the later one is kept
// and the earlier dropped. The budget pass bounds the disk besides (see
// SweepLocalBlobs), since a burst of writes can fill it before anything
// expires: it runs queue-shard.evict as a sync task, so its removals take
// their turn with the digest's other tasks.

// ShardQueue is the task queue the cache's copies are kept on. Its subject is
// the blob's digest, in hex. Its tasks are placed on the runner's node, the
// instance whose disk holds the cache.
const ShardQueue = "queue-shard"

// The queue-shard actions.
const (
	shardCache = "cache"
	shardEvict = "evict"
)

// DefaultCacheTTL is how long a cached blob stays cached after it was last
// written or read, when Deps.CacheTTL is zero.
const DefaultCacheTTL = time.Hour

// renewalCapacity caps how many blobs' last renewals the backend remembers.
// The least recently renewed entry is dropped first, and its blob's next read
// renews it again.
const renewalCapacity = 65536

// shardEvictRetry retries a failed eviction a few times. The budget pass
// removes whatever is left over budget.
var shardEvictRetry = taskrunner.RetryPolicy{First: time.Second, Multiplier: 2, Attempts: 3}

// ShardCacheTask is a queue-shard.cache task's arguments.
type ShardCacheTask struct {
	// Take moves the blob's spool copy into the cache first. Without it the
	// task only extends the time a cached copy has left.
	Take bool
	// TTL is how long the copy stays cached from when the task runs, in
	// nanoseconds.
	TTL int64
}

// ShardEvictTask is a queue-shard.evict task's arguments.
type ShardEvictTask struct {
	// Budget marks an eviction the budget pass runs, which counts its own
	// removals. Without it the copy's time to live ran out.
	Budget bool
}

// RegisterShardTasks registers the queue-shard handlers with r, and keeps r
// for submitting them. Call it before r starts.
func (b *Backend) RegisterShardTasks(r *taskrunner.Runner) error {
	b.shards = r
	if err := r.Handle(ShardQueue, shardCache, b.runShardCache); err != nil {
		return err
	}
	if err := r.Handle(ShardQueue, shardEvict, b.runShardEvict, taskrunner.WithRetry(shardEvictRetry)); err != nil {
		return err
	}
	b.cache.OnRead(b.renewCached)
	return nil
}

// shardNode is the placement for queue-shard tasks: this instance, or nowhere
// when the runner names no instance.
func (b *Backend) shardNode() *string {
	if n := b.shards.Node(); n != "" {
		return &n
	}
	return nil
}

// submitShardCache queues a queue-shard.cache task for digest, and records
// the renewal so the reads that follow do not repeat it.
func (b *Backend) submitShardCache(ctx context.Context, digest multihash.Multihash, take bool) error {
	var buf bytes.Buffer
	if err := (&ShardCacheTask{Take: take, TTL: int64(b.cacheTTL)}).MarshalCBOR(&buf); err != nil {
		return fmt.Errorf("encode cache task: %w", err)
	}
	if _, err := b.shards.Submit(ctx, taskrunner.Spec{
		Queue:   ShardQueue,
		Subject: hex.EncodeToString(digest),
		Action:  shardCache,
		Args:    buf.Bytes(),
		Node:    b.shardNode(),
	}); err != nil {
		return err
	}
	b.renewals.touch(string(digest), time.Now())
	return nil
}

// renewCached extends a cached blob's time in the cache when a read finds it
// with less than half its time to live left, by this process's record of its
// last renewal. A blob this process has no record of (since a restart, or
// once the record is dropped) is renewed on its first read. A failure costs
// only an earlier eviction, and is logged.
func (b *Backend) renewCached(ctx context.Context, digest multihash.Multihash) {
	if at, ok := b.renewals.get(string(digest)); ok && time.Since(at) < b.cacheTTL/2 {
		return
	}
	// The read may end before the task is queued; the renewal should not.
	if err := b.submitShardCache(context.WithoutCancel(ctx), digest, false); err != nil {
		b.logger.Warn("could not extend a cached blob's time in the cache",
			zap.String("digest", hex.EncodeToString(digest)), zap.Error(err))
	}
}

// runShardCache is queue-shard.cache: it moves the blob's spool copy into the
// cache when asked to, then, if the cache holds the blob, dates the file now,
// which orders the budget pass, and schedules its eviction for when its time
// to live runs out. A failed move fails the task without retrying: the copy
// stays in the spool, as it does when the move is refused for a write still
// holding it, and reads find it there.
func (b *Backend) runShardCache(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	digest, err := hex.DecodeString(t.Subject)
	if err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("cache task digest: %w", err))
	}
	var args ShardCacheTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode cache task: %w", err))
	}
	if args.Take {
		if _, err := b.cache.Take(b.spool, digest); err != nil {
			return nil, err
		}
	}
	now := time.Now()
	cached, err := b.cache.Touch(digest, now)
	if err != nil || !cached {
		return nil, err
	}
	at := now.Add(time.Duration(args.TTL))
	_, err = ex.Submit(ctx, taskrunner.Spec{
		Queue:     ShardQueue,
		Subject:   t.Subject,
		Action:    shardEvict,
		NotBefore: &at,
		Replace:   true,
		Node:      b.shardNode(),
	})
	return nil, err
}

// evictCached removes a cached copy for the budget pass and returns the bytes
// freed, through a sync queue-shard.evict task. With no task queues it removes
// the copy itself.
func (b *Backend) evictCached(ctx context.Context, digest multihash.Multihash) (int64, error) {
	if b.shards == nil {
		return b.cache.Remove(digest)
	}
	var buf bytes.Buffer
	if err := (&ShardEvictTask{Budget: true}).MarshalCBOR(&buf); err != nil {
		return 0, fmt.Errorf("encode evict task: %w", err)
	}
	res, err := b.shards.Run(ctx, taskrunner.Spec{
		Queue:   ShardQueue,
		Subject: hex.EncodeToString(digest),
		Action:  shardEvict,
		Args:    buf.Bytes(),
		Node:    b.shardNode(),
	})
	if err != nil {
		return 0, err
	}
	freed, n := binary.Varint(res)
	if n <= 0 {
		return 0, fmt.Errorf("evict task result %x", res)
	}
	return freed, nil
}

// runShardEvict is queue-shard.evict: it removes the cached copy, if there is
// one, and returns the bytes freed as a varint. A reader that already opened
// the file keeps reading it.
func (b *Backend) runShardEvict(ctx context.Context, _ *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	digest, err := hex.DecodeString(t.Subject)
	if err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("evict task digest: %w", err))
	}
	var args ShardEvictTask
	if len(t.Args) > 0 {
		if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
			return nil, taskrunner.Permanent(fmt.Errorf("decode evict task: %w", err))
		}
	}
	freed, err := b.cache.Remove(digest)
	if err != nil {
		return nil, err
	}
	if !args.Budget {
		b.localBlobMetrics.removedFile(ctx, removedExpired, freed)
	}
	return binary.AppendVarint(nil, freed), nil
}
