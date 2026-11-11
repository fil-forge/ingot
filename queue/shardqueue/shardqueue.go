// Package shardqueue is queue-shard: the tasks that keep the cache's copies
// of blobs.
//
// A cached blob's lifetime is kept by queue-shard tasks, serialized on the
// digest, since the cache holds one file per digest whichever spaces wrote it.
// Once a write has adopted its copy into the cache, a queue-shard.cache task
// schedules a queue-shard.evict for when the copy's time to live runs out. A
// read from the cache with less than half that time left runs
// queue-shard.cache again, which schedules a later eviction. Evictions are
// submitted as replacements, so the later one is kept and the earlier dropped.
// The budget pass bounds the disk besides, since a burst of writes can fill
// it before anything expires: it runs queue-shard.evict as a sync task
// (Queue.Evict), so its removals take their turn with the digest's other
// tasks.
package shardqueue

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/taskrunner"
)

// Name is the task queue the cache's copies are kept on. Its subject is the
// blob's digest, in hex. Its tasks are placed on the runner's node, the
// instance whose disk holds the cache.
const Name = "queue-shard"

// The queue-shard actions.
const (
	cacheAction = "cache"
	evictAction = "evict"
)

// DefaultTTL is how long a cached blob stays cached after it was last written
// or read, when Deps.TTL is zero.
const DefaultTTL = time.Hour

// submitTimeLimit bounds how long a request waits to cache a written blob or
// queue a cached blob's renewal, which goes on after the request itself ends,
// so a stalled database delays a PUT or a cached GET by this much at most.
const submitTimeLimit = 5 * time.Second

// renewalCapacity caps how many blobs' last renewals the queue remembers. The
// least recently renewed entry is dropped first, and its blob's next read
// renews it again.
const renewalCapacity = 65536

// evictRetry retries a failed eviction for about half an hour, which rides
// out a passing fault such as a database failover. One that fails for longer,
// such as a disk that refuses the removal, is left a failed task to see and
// resubmit once it is fixed, rather than retried for good. Until then the copy
// stays, unless the budget pass removes it or a read of the blob schedules a
// new eviction.
var evictRetry = taskrunner.RetryPolicy{First: time.Second, Max: 5 * time.Minute, Multiplier: 2, Attempts: 14}

// cacheRetry retries a failed renewal as evictRetry does, since a copy left
// without its eviction stays on disk until the budget pass. The task is safe
// to run again: the later eviction replaces the earlier.
var cacheRetry = evictRetry

// CacheTask is a queue-shard.cache task's arguments.
type CacheTask struct {
	// TTL is how long the copy stays cached from when the task runs, in
	// nanoseconds.
	TTL int64
}

// EvictTask is a queue-shard.evict task's arguments.
type EvictTask struct {
	// Budget marks an eviction the budget pass runs, which counts its own
	// removals.
	Budget bool
	// Released marks an eviction a blob's release queues, the blob being gone
	// from its space. Without either, the copy's time to live ran out.
	Released bool
}

// Removal is why an eviction removed a copy, other than the budget pass.
type Removal int

const (
	// Expired is a copy whose time to live ran out.
	Expired Removal = iota
	// Released is a copy of a blob released from its space.
	Released
)

// Deps is what the queue's handlers need.
type Deps struct {
	// Cache holds the copies. Required.
	Cache *blockstore.BlobCache
	// TTL is how long a copy stays cached after it was cached or last renewed
	// (0 → DefaultTTL).
	TTL time.Duration
	// Removed, when set, is told the bytes each eviction other than the
	// budget pass's removed, and why.
	Removed func(ctx context.Context, why Removal, freed int64)
	// Logger is optional; defaults to zap.NewNop().
	Logger *zap.Logger
}

// Queue runs queue-shard.
type Queue struct {
	tasks   *taskrunner.Runner
	cache   *blockstore.BlobCache
	ttl     time.Duration
	removed func(ctx context.Context, why Removal, freed int64)
	logger  *zap.Logger
	// renewals is when this process last renewed each cached blob's time in
	// the cache.
	renewals *recencyMap
}

// New registers the queue-shard handlers with r, and has the cache's reads
// renew their blobs. Call it before r starts.
func New(r *taskrunner.Runner, d Deps) (*Queue, error) {
	if d.Cache == nil {
		return nil, errors.New("shardqueue: no cache")
	}
	q := &Queue{
		tasks:    r,
		cache:    d.Cache,
		ttl:      cmp.Or(d.TTL, DefaultTTL),
		removed:  d.Removed,
		logger:   cmp.Or(d.Logger, zap.NewNop()),
		renewals: newRecencyMap(renewalCapacity),
	}
	if err := r.Handle(Name, cacheAction, q.runCache, taskrunner.WithRetry(cacheRetry)); err != nil {
		return nil, err
	}
	if err := r.Handle(Name, evictAction, q.runEvict, taskrunner.WithRetry(evictRetry)); err != nil {
		return nil, err
	}
	d.Cache.OnRead(q.renew)
	return q, nil
}

// node is the placement for queue-shard tasks: this instance, or nowhere when
// the runner names no instance.
func (q *Queue) node() *string {
	if n := q.tasks.Node(); n != "" {
		return &n
	}
	return nil
}

// Cache adopts a written blob's local copy into the cache once the provider
// holds it, so the reads that follow are served locally, and discards the copy
// if it was not adopted. A write that kept no copy (nil) caches nothing. The
// adoption runs in a sync queue-shard.cache task, which schedules the copy's
// eviction in the same task, so an eviction of an earlier copy cannot run
// between the two and remove this one. The request may end before the task
// has run; the task should not, but it waits no longer than submitTimeLimit.
// A failure costs only the cached copy, or its scheduled eviction, which the
// budget pass then stands in for, and is logged.
func (q *Queue) Cache(ctx context.Context, digest multihash.Multihash, local *blockstore.TempBlob) {
	if local == nil {
		return
	}
	defer local.Discard() // nothing, once adopted
	if err := q.adopt(ctx, digest, local); err != nil {
		q.logger.Warn("could not cache a written blob's local copy",
			zap.String("digest", hex.EncodeToString(digest)), zap.Error(err))
	}
}

// adopt runs Cache's queue-shard.cache task.
func (q *Queue) adopt(ctx context.Context, digest multihash.Multihash, local *blockstore.TempBlob) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), submitTimeLimit)
	defer cancel()
	spec, err := q.cacheSpec(digest)
	if err != nil {
		return err
	}
	if _, err := q.tasks.Run(context.WithValue(ctx, adoptKey{}, local), spec); err != nil {
		return err
	}
	q.renewals.touch(string(digest), time.Now())
	return nil
}

// submitCache queues a queue-shard.cache task for digest, and records the
// renewal so the reads that follow do not repeat it. The request may end
// before the task is queued; the task should not, but it waits no longer than
// submitTimeLimit.
func (q *Queue) submitCache(ctx context.Context, digest multihash.Multihash) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), submitTimeLimit)
	defer cancel()
	spec, err := q.cacheSpec(digest)
	if err != nil {
		return err
	}
	if _, err := q.tasks.Submit(ctx, spec); err != nil {
		return err
	}
	q.renewals.touch(string(digest), time.Now())
	return nil
}

// adoptKey is the context key a sync queue-shard.cache task finds the
// temporary copy it adopts under.
type adoptKey struct{}

// cacheSpec is digest's queue-shard.cache task.
func (q *Queue) cacheSpec(digest multihash.Multihash) (taskrunner.Spec, error) {
	var buf bytes.Buffer
	if err := (&CacheTask{TTL: int64(q.ttl)}).MarshalCBOR(&buf); err != nil {
		return taskrunner.Spec{}, fmt.Errorf("encode cache task: %w", err)
	}
	return taskrunner.Spec{
		Queue:   Name,
		Subject: hex.EncodeToString(digest),
		Action:  cacheAction,
		Args:    buf.Bytes(),
		Node:    q.node(),
	}, nil
}

// renew extends a cached blob's time in the cache when a read finds it with
// less than half its time to live left, by this process's record of its last
// renewal. A blob this process has no record of (since a restart, or once the
// record is dropped) is renewed on its first read. Of concurrent reads, the
// one that claims the record renews and the rest go on. A failure costs only
// an earlier eviction: it is logged, and the record dropped so the next read
// tries again.
func (q *Queue) renew(ctx context.Context, digest multihash.Multihash) {
	if !q.renewals.claim(string(digest), time.Now(), q.ttl/2) {
		return
	}
	if err := q.submitCache(ctx, digest); err != nil {
		q.renewals.forget(string(digest))
		q.logger.Warn("could not extend a cached blob's time in the cache",
			zap.String("digest", hex.EncodeToString(digest)), zap.Error(err))
	}
}

// runCache is queue-shard.cache: run sync by a write, it first adopts the
// write's copy into the cache. Then, if the cache holds the blob, it dates the
// file now, which orders the budget pass, and schedules its eviction for when
// its time to live runs out. A failed step retries the task (cacheRetry).
func (q *Queue) runCache(ctx context.Context, ex *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	digest, err := hex.DecodeString(t.Subject)
	if err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("cache task digest: %w", err))
	}
	var args CacheTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("decode cache task: %w", err))
	}
	if local, _ := ctx.Value(adoptKey{}).(*blockstore.TempBlob); local != nil && !local.Ended() {
		if _, err := q.cache.Adopt(local, digest); err != nil {
			return nil, taskrunner.Permanent(fmt.Errorf("adopt the written copy: %w", err))
		}
	}
	now := time.Now()
	cached, err := q.cache.Touch(digest, now)
	if err != nil || !cached {
		return nil, err
	}
	at := now.Add(time.Duration(args.TTL))
	_, err = ex.Submit(ctx, taskrunner.Spec{
		Queue:     Name,
		Subject:   t.Subject,
		Action:    evictAction,
		NotBefore: &at,
		Replace:   true,
		Node:      q.node(),
	})
	return nil, err
}

// Evict removes a cached copy for the budget pass and returns the bytes freed,
// through a sync queue-shard.evict task.
func (q *Queue) Evict(ctx context.Context, digest multihash.Multihash) (int64, error) {
	spec, err := q.evictSpec(digest, EvictTask{Budget: true})
	if err != nil {
		return 0, err
	}
	res, err := q.tasks.Run(ctx, spec)
	if err != nil {
		return 0, err
	}
	freed, n := binary.Varint(res)
	if n <= 0 {
		return 0, fmt.Errorf("evict task result %x", res)
	}
	return freed, nil
}

// ReleasedSpec is the queue-shard.evict a blob's release spawns for its
// cached copy, the blob being gone from its space.
func (q *Queue) ReleasedSpec(digest multihash.Multihash) (taskrunner.Spec, error) {
	return q.evictSpec(digest, EvictTask{Released: true})
}

// evictSpec is digest's queue-shard.evict task with args.
func (q *Queue) evictSpec(digest multihash.Multihash, args EvictTask) (taskrunner.Spec, error) {
	var buf bytes.Buffer
	if err := args.MarshalCBOR(&buf); err != nil {
		return taskrunner.Spec{}, fmt.Errorf("encode evict task: %w", err)
	}
	return taskrunner.Spec{
		Queue:   Name,
		Subject: hex.EncodeToString(digest),
		Action:  evictAction,
		Args:    buf.Bytes(),
		Node:    q.node(),
	}, nil
}

// runEvict is queue-shard.evict: it removes the cached copy, if there is one,
// and returns the bytes freed as a varint. A reader that already opened the
// file keeps reading it.
func (q *Queue) runEvict(ctx context.Context, _ *taskrunner.Exec, t taskrunner.Task) ([]byte, error) {
	digest, err := hex.DecodeString(t.Subject)
	if err != nil {
		return nil, taskrunner.Permanent(fmt.Errorf("evict task digest: %w", err))
	}
	var args EvictTask
	if len(t.Args) > 0 {
		if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
			return nil, taskrunner.Permanent(fmt.Errorf("decode evict task: %w", err))
		}
	}
	freed, err := q.cache.Remove(digest)
	if err != nil {
		return nil, err
	}
	if q.removed != nil && !args.Budget {
		why := Expired
		if args.Released {
			why = Released
		}
		q.removed(ctx, why, freed)
	}
	return binary.AppendVarint(nil, freed), nil
}
