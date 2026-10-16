package catalogwalk

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/cars"
	"github.com/fil-forge/ingot/internal/tracing"
	"github.com/fil-forge/ingot/registry"
)

// Defaults for Config's zero values.
const (
	// DefaultInterval is the least time between two walks of a bucket.
	DefaultInterval = 6 * time.Hour
	// DefaultRate is the most blocks a walk reads per second.
	DefaultRate = 100
	// defaultMinSubtreeBlocks is the size, in blocks, from which a subtree's
	// figures are remembered.
	defaultMinSubtreeBlocks = 32
)

// subtreeBatch is how many newly remembered subtrees a walk saves at a time.
const subtreeBatch = 256

// retryAfter is how long a bucket whose walk failed waits for another.
const retryAfter = 15 * time.Minute

// errNoAuthority stops a walk whose borrowed authority has lapsed.
var errNoAuthority = errors.New("catalogwalk: no read authority held for the space")

// Config tunes the walker.
type Config struct {
	// Interval is the least time between two walks of a bucket. A bucket
	// whose root hasn't changed since its last walk is not walked again.
	// Zero → DefaultInterval.
	Interval time.Duration
	// Rate is the most blocks a walk reads per second. Zero → DefaultRate.
	Rate int
	// MinSubtreeBlocks is the size, in blocks, from which a subtree's figures
	// are remembered for later walks. Zero → 32.
	MinSubtreeBlocks int64
}

// Deps are the walker's collaborators.
type Deps struct {
	// Reads serves catalog blocks, from local segments or from Forge (the
	// layered read store, so a walk's reads take the request path's tiers).
	Reads blockstore.BlockReader
	// Usage lists the buckets with catalog storage on Forge, their current
	// roots and their last walks.
	Usage registry.CatalogUsageStore
	// Walks persists remembered subtrees and completed walks.
	Walks registry.CatalogWalkStore
	// Authority lends the read authority each walk needs.
	Authority *Authority
	Logger    *zap.Logger
}

// Walker walks one bucket's live catalog at a time. Its methods are not safe
// for concurrent use.
//
// A walk counts a block once per link to it, so a block several others link
// to is counted, and read, once for each. In return, a subtree's figures
// depend only on its CID: a walk remembers those of large subtrees, and a
// later walk reads only the blocks whose subtrees changed since, reusing the
// rest whole.
type Walker struct {
	cfg       Config
	reads     blockstore.BlockReader
	usage     registry.CatalogUsageStore
	walks     registry.CatalogWalkStore
	authority *Authority
	logger    *zap.Logger
	limiter   *rate.Limiter
	now       func() time.Time

	// failedUntil holds back a bucket whose walk failed, by name.
	failedUntil map[string]time.Time
}

// New returns a Walker. Zero-valued Config fields take their defaults.
func New(cfg Config, d Deps) *Walker {
	cfg.Interval = cmp.Or(cfg.Interval, DefaultInterval)
	cfg.Rate = cmp.Or(cfg.Rate, DefaultRate)
	cfg.MinSubtreeBlocks = cmp.Or(cfg.MinSubtreeBlocks, defaultMinSubtreeBlocks)
	logger := d.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Walker{
		cfg:         cfg,
		reads:       d.Reads,
		usage:       d.Usage,
		walks:       d.Walks,
		authority:   d.Authority,
		logger:      logger,
		limiter:     rate.NewLimiter(rate.Limit(cfg.Rate), 1),
		now:         time.Now,
		failedUntil: map[string]time.Time{},
	}
}

// Walked describes a walk Walk ran.
type Walked struct {
	Bucket string
	Space  did.DID
	// Reads is the blocks the walk read; Reused is how many times it took a
	// remembered subtree's figures instead of reading the subtree.
	Reads, Reused int64
	// LiveBytes is the CAR frame bytes reachable from the bucket's root,
	// set once the walk completed.
	LiveBytes int64
	Completed bool
}

// Due returns the buckets due a walk, the one longest without a completed
// walk first, the larger first between two equally overdue. A bucket is due
// when its root differs from the one its last completed walk reached (or it
// has none), that walk finished at least Interval ago, and Authority holds
// read authority for its space (an empty bucket needs none). A bucket whose
// walk failed waits retryAfter; one whose authority lapsed mid-walk is due
// again once authority is held. Due also forgets the authority that can no
// longer be lent.
func (w *Walker) Due(ctx context.Context) ([]registry.CatalogUsage, error) {
	w.authority.Prune()
	usage, err := w.usage.CatalogUsage(ctx)
	if err != nil {
		return nil, err
	}
	now := w.now()
	var due []registry.CatalogUsage
	for _, u := range usage {
		if until, ok := w.failedUntil[u.Bucket]; ok {
			if now.Before(until) {
				continue
			}
			delete(w.failedUntil, u.Bucket)
		}
		if u.Walk != nil {
			if u.Walk.Root.Equals(u.Root) {
				continue
			}
			if now.Sub(time.Unix(u.Walk.FinishedAt, 0)) < w.cfg.Interval {
				continue
			}
		}
		if u.Root.Defined() && !w.authority.Holds(u.Space) {
			continue
		}
		due = append(due, u)
	}
	slices.SortStableFunc(due, func(a, b registry.CatalogUsage) int {
		return cmp.Or(cmp.Compare(lastWalked(a), lastWalked(b)), cmp.Compare(b.Bytes, a.Bytes))
	})
	return due, nil
}

// lastWalked is when u's last completed walk finished, 0 if none has.
func lastWalked(u registry.CatalogUsage) int64 {
	if u.Walk == nil {
		return 0
	}
	return u.Walk.FinishedAt
}

// Walk walks the bucket u names (an entry from Due), computing the bytes
// reachable from its root and recording them with its usage. It rereads the
// bucket's usage first, root and figures in one query, so what it records is
// of the moment the walk starts, however long ago Due listed the bucket. A
// bucket that by then has nothing shipped, or whose root is back at the one
// its last walk reached, is left alone. A walk that fails holds the bucket
// back for retryAfter.
func (w *Walker) Walk(ctx context.Context, u registry.CatalogUsage) (Walked, error) {
	cur, err := w.usage.BucketCatalogUsage(ctx, u.Bucket)
	walked := Walked{Bucket: u.Bucket, Space: u.Space}
	if err == nil {
		if cur == nil || (cur.Walk != nil && cur.Walk.Root.Equals(cur.Root)) {
			return walked, nil
		}
		walked, err = w.walk(ctx, *cur)
	}
	if err != nil && !errors.Is(err, errNoAuthority) && ctx.Err() == nil {
		w.failedUntil[u.Bucket] = w.now().Add(retryAfter)
	}
	return walked, err
}

func (w *Walker) walk(ctx context.Context, u registry.CatalogUsage) (Walked, error) {
	started := w.now().Unix()
	walked := Walked{Bucket: u.Bucket, Space: u.Space}
	remembered, err := w.walks.CatalogSubtrees(ctx, u.Bucket)
	if err != nil {
		return walked, err
	}
	b := &bucketWalk{
		Walker: w,
		bucket: u.Bucket,
		space:  u.Space,
		// A walk's reads stay out of its trace: each network read is an
		// outbound request and a query, and a walk makes thousands.
		readCtx: tracing.Unsampled(ctx),
		known:   make(map[cid.Cid]registry.CatalogSubtree, len(remembered)),
	}
	for _, st := range remembered {
		b.known[st.CID] = st
	}

	var live int64
	var tops []cid.Cid
	if u.Root.Defined() {
		live, _, tops, err = b.subtree(ctx, u.Root)
	}
	if serr := b.save(ctx); err == nil {
		err = serr
	}
	walked.Reads, walked.Reused = b.read, b.reused
	if err != nil {
		return walked, fmt.Errorf("catalogwalk: walk %s: %w", u.Space, err)
	}

	record := registry.CatalogWalk{
		Root:               u.Root,
		StartedAt:          started,
		FinishedAt:         w.now().Unix(),
		LiveBytes:          live,
		ForgeBytes:         u.Bytes,
		DeadOnArrivalBytes: u.DeadOnArrivalBytes,
		UnmeasuredBytes:    u.UnmeasuredBytes,
	}
	if err := w.walks.CompleteCatalogWalk(ctx, u.Bucket, record, b.unreached(tops)); err != nil {
		return walked, err
	}
	walked.LiveBytes, walked.Completed = live, true
	return walked, nil
}

// bucketWalk is one walk's state.
type bucketWalk struct {
	*Walker
	bucket  string
	space   did.DID
	readCtx context.Context // the context reads borrow authority onto

	// known holds the subtrees remembered before the walk and those it has
	// computed since; pending those it computed and has not saved yet.
	known   map[cid.Cid]registry.CatalogSubtree
	pending []registry.CatalogSubtree
	read    int64 // blocks read
	reused  int64 // remembered subtrees taken whole
}

// subtree returns the bytes and blocks reachable from c, counting a block
// once per link to it, and the nearest remembered subtrees at or below c.
func (b *bucketWalk) subtree(ctx context.Context, c cid.Cid) (int64, int64, []cid.Cid, error) {
	if st, ok := b.known[c]; ok {
		b.reused++
		return st.Bytes, st.Blocks, []cid.Cid{c}, nil
	}
	readCtx, ok := b.authority.Borrow(b.readCtx, b.space)
	if !ok {
		return 0, 0, nil, errNoAuthority
	}
	if err := b.limiter.Wait(ctx); err != nil {
		return 0, 0, nil, err
	}
	blk, err := b.reads.GetBlock(readCtx, b.space, c)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("read %s: %w", c, err)
	}
	b.read++

	data := blk.RawData()
	size, blocks := cars.FrameSize(c, uint64(len(data))), int64(1)
	var inside []cid.Cid
	if c.Prefix().Codec == cid.DagCBOR {
		var links []cid.Cid
		if err := cbg.ScanForLinks(bytes.NewReader(data), func(link cid.Cid) {
			if link.Prefix().Codec == cid.DagCBOR {
				links = append(links, link)
			}
		}); err != nil {
			return 0, 0, nil, fmt.Errorf("scan %s: %w", c, err)
		}
		for _, link := range links {
			n, k, below, err := b.subtree(ctx, link)
			if err != nil {
				return 0, 0, nil, err
			}
			size, blocks, inside = size+n, blocks+k, append(inside, below...)
		}
	}

	if blocks < b.cfg.MinSubtreeBlocks {
		return size, blocks, inside, nil
	}
	st := registry.CatalogSubtree{CID: c, Bytes: size, Blocks: blocks, Children: inside}
	b.known[c] = st
	b.pending = append(b.pending, st)
	if len(b.pending) >= subtreeBatch {
		if err := b.save(ctx); err != nil {
			return 0, 0, nil, err
		}
	}
	return size, blocks, []cid.Cid{c}, nil
}

// unreached lists the known subtrees the remembered subtrees in tops don't
// reach through their children: those of roots before this walk's, and those
// computed by walks cut short. Everything the root reaches stays remembered,
// whether this walk read it or reused it whole.
func (b *bucketWalk) unreached(tops []cid.Cid) []cid.Cid {
	reached := make(map[cid.Cid]struct{}, len(b.known))
	pending := slices.Clone(tops)
	for len(pending) > 0 {
		c := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if _, ok := reached[c]; ok {
			continue
		}
		reached[c] = struct{}{}
		pending = append(pending, b.known[c].Children...)
	}
	var out []cid.Cid
	for c := range b.known {
		if _, ok := reached[c]; !ok {
			out = append(out, c)
		}
	}
	return out
}

// save stores the subtrees computed since the last save, so a walk cut
// short still spares the next one their reads. It saves under its own
// short deadline, since it runs even after ctx is done.
func (b *bucketWalk) save(ctx context.Context) error {
	if len(b.pending) == 0 {
		return nil
	}
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if err := b.walks.PutCatalogSubtrees(saveCtx, b.bucket, b.pending); err != nil {
		return err
	}
	b.pending = b.pending[:0]
	return nil
}
