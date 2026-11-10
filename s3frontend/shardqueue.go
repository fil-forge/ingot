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

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/taskrunner"
)

// A cached blob's lifetime is kept by queue-shard tasks, serialized on the
// digest, since the cache holds one file per digest whichever spaces wrote it.
// Once a write has adopted its copy into the cache, a queue-shard.cache task
// schedules a queue-shard.evict for when the copy's time to live runs out. A
// read from the cache with less than half that time left runs
// queue-shard.cache again, which schedules a later eviction. Evictions are submitted as replacements, so the later one is kept
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

// shardSubmitTimeLimit bounds how long a request waits to cache a written
// blob or queue a cached blob's renewal, which goes on after the request
// itself ends, so a stalled database delays a PUT or a cached GET by this
// much at most.
const shardSubmitTimeLimit = 5 * time.Second

// renewalCapacity caps how many blobs' last renewals the backend remembers.
// The least recently renewed entry is dropped first, and its blob's next read
// renews it again.
const renewalCapacity = 65536

// shardEvictRetry retries a failed eviction for about half an hour, which
// rides out a passing fault such as a database failover. One that fails for
// longer, such as a disk that refuses the removal, is left a failed task to
// see and resubmit once it is fixed, rather than retried for good. Until then
// the copy stays, unless the budget pass removes it or a read of the blob
// schedules a new eviction.
var shardEvictRetry = taskrunner.RetryPolicy{First: time.Second, Max: 5 * time.Minute, Multiplier: 2, Attempts: 14}

// shardCacheRetry retries a failed renewal as shardEvictRetry does, since a
// copy left without its eviction stays on disk until the budget pass. The task
// is safe to run again: the later eviction replaces the earlier.
var shardCacheRetry = shardEvictRetry

// ShardCacheTask is a queue-shard.cache task's arguments.
type ShardCacheTask struct {
	// TTL is how long the copy stays cached from when the task runs, in
	// nanoseconds.
	TTL int64
}

// ShardEvictTask is a queue-shard.evict task's arguments.
type ShardEvictTask struct {
	// Budget marks an eviction the budget pass runs, which counts its own
	// removals.
	Budget bool
	// Released marks an eviction a blob's release queues, the blob being gone
	// from its space. Without either, the copy's time to live ran out.
	Released bool
}

// registerShardTasks registers the queue-shard handlers with r, and keeps r
// for submitting them. Call it before r starts.
func (b *Backend) registerShardTasks(r *taskrunner.Runner) error {
	b.shards = r
	if err := r.Handle(ShardQueue, shardCache, b.handleShardCache, taskrunner.WithRetry(shardCacheRetry)); err != nil {
		return err
	}
	if err := r.Handle(ShardQueue, shardEvict, b.runShardEvict, taskrunner.WithRetry(shardEvictRetry)); err != nil {
		return err
	}
	if b.cache != nil {
		b.cache.OnRead(b.renewCached)
	}
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
// the renewal so the reads that follow do not repeat it. The request may end
// before the task is queued; the task should not, but it waits no longer than
// shardSubmitTimeLimit.
func (b *Backend) submitShardCache(ctx context.Context, digest multihash.Multihash) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shardSubmitTimeLimit)
	defer cancel()
	spec, err := b.shardCacheSpec(digest)
	if err != nil {
		return err
	}
	if _, err := b.shards.Submit(ctx, spec); err != nil {
		return err
	}
	b.renewals.touch(string(digest), time.Now())
	return nil
}

// runShardCache adopts local into the cache under digest in a sync
// queue-shard.cache task, which holds the digest's subject while it adopts
// the copy and schedules its eviction.
func (b *Backend) runShardCache(ctx context.Context, digest multihash.Multihash, local *blockstore.TempBlob) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shardSubmitTimeLimit)
	defer cancel()
	spec, err := b.shardCacheSpec(digest)
	if err != nil {
		return err
	}
	if _, err := b.shards.Run(context.WithValue(ctx, adoptKey{}, local), spec); err != nil {
		return err
	}
	b.renewals.touch(string(digest), time.Now())
	return nil
}

// adoptKey is the context key a sync queue-shard.cache task finds the
// temporary copy it adopts under.
type adoptKey struct{}

// shardCacheSpec is digest's queue-shard.cache task.
func (b *Backend) shardCacheSpec(digest multihash.Multihash) (taskrunner.Spec, error) {
	var buf bytes.Buffer
	if err := (&ShardCacheTask{TTL: int64(b.cacheTTL)}).MarshalCBOR(&buf); err != nil {
		return taskrunner.Spec{}, fmt.Errorf("encode cache task: %w", err)
	}
	return taskrunner.Spec{
		Queue:   ShardQueue,
		Subject: hex.EncodeToString(digest),
		Action:  shardCache,
		Args:    buf.Bytes(),
		Node:    b.shardNode(),
	}, nil
}

// renewCached extends a cached blob's time in the cache when a read finds it
// with less than half its time to live left, by this process's record of its
// last renewal. A blob this process has no record of (since a restart, or
// once the record is dropped) is renewed on its first read. Of concurrent
// reads, the one that claims the record renews and the rest go on. A failure
// costs only an earlier eviction: it is logged, and the record dropped so the
// next read tries again.
func (b *Backend) renewCached(ctx context.Context, digest multihash.Multihash) {
	if !b.renewals.claim(string(digest), time.Now(), b.cacheTTL/2) {
		return
	}
	if err := b.submitShardCache(ctx, digest); err != nil {
		b.renewals.forget(string(digest))
		b.logger.Warn("could not extend a cached blob's time in the cache",
			zap.String("digest", hex.EncodeToString(digest)), zap.Error(err))
	}
}

// handleShardCache is queue-shard.cache: run sync by a write, it first adopts
// the write's copy into the cache. Then, if the cache holds the blob, it
// dates the file now, which orders the budget pass, and schedules its
// eviction for when its time to live runs out. A failed step retries the
// task (shardCacheRetry).
func (b *Backend) handleShardCache(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	digest, err := hex.DecodeString(t.Subject)
	if err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("cache task digest: %w", err))
	}
	var args ShardCacheTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode cache task: %w", err))
	}
	if local, _ := ctx.Value(adoptKey{}).(*blockstore.TempBlob); local != nil && !local.Ended() {
		if _, err := b.cache.Adopt(local, digest); err != nil {
			return nil, taskrunner.Permanent(fmt.Errorf("adopt the written copy: %w", err))
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
// freed, through a sync queue-shard.evict task.
func (b *Backend) evictCached(ctx context.Context, digest multihash.Multihash) (int64, error) {
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
	switch {
	case args.Released:
		b.localBlobMetrics.removedFile(ctx, removedReleased, freed)
	case !args.Budget:
		b.localBlobMetrics.removedFile(ctx, removedExpired, freed)
	}
	return binary.AppendVarint(nil, freed), nil
}
