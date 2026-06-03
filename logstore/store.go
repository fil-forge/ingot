package logstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
)

// Compile-time assertion that *Store satisfies blockstore.Log.
var _ blockstore.Log = (*Store)(nil)

// Store is the LSM-style log: one open segment accepting appends, plus
// N sealed segments (some shipped, some pending) that serve reads in
// front of the network blockstore.
//
// Each segment splits into a data CAR and a catalog CAR that ship
// through INDEPENDENT pipelines: the store runs one flush worker per
// plane configured to ship, and a plane configured never to ship is
// retained on local disk indefinitely.
//
// Concurrency:
//   - catMu (RWMutex) guards open + sealed slice + nextSeq.
//   - appMu (Mutex) serializes appenders so the open-segment append fds
//     have a single writer.
type Store struct {
	cfg    Config
	logger *zap.Logger

	catMu   sync.RWMutex
	open    *Segment
	sealed  []*Segment // newest-first; includes shipped-and-retained
	nextSeq uint64

	appMu sync.Mutex

	// flushQ holds one queue per plane that is configured to ship. A
	// non-shipping plane has no entry (and no worker).
	flushQ  map[blockstore.Plane]chan *Segment
	closing chan struct{}
	wg      sync.WaitGroup

	openedAt time.Time

	// sealReq is a coalesced "seal the open segment now" channel.
	sealReq chan struct{}
}

// Open initializes a Store: scans Dir, reconciles with cfg.Meta,
// re-enqueues unshipped segments per plane, force-seals any
// previously-open segment, and starts a fresh open segment.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.defaults()

	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("logstore: mkdir %s: %w", cfg.Dir, err)
	}

	s := &Store{
		cfg:     cfg,
		logger:  cfg.Logger,
		flushQ:  map[blockstore.Plane]chan *Segment{},
		closing: make(chan struct{}),
		sealReq: make(chan struct{}, 1),
	}
	for _, p := range blockstore.Planes {
		if cfg.plane(p).Ship {
			s.flushQ[p] = make(chan *Segment, 64)
		}
	}

	if err := s.recover(ctx); err != nil {
		return nil, err
	}

	// Force-seal a recovered open segment so a fresh open is always
	// brand-new on each process startup.
	if s.open != nil {
		if err := s.open.seal(ctx, cfg.Meta); err != nil {
			return nil, fmt.Errorf("logstore: force-seal recovered open segment: %w", err)
		}
		s.sealed = append([]*Segment{s.open}, s.sealed...)
		s.enqueueFlush(s.open)
		s.open = nil
	}

	// One flush worker per shipping plane, plus the seal ticker.
	for _, p := range blockstore.Planes {
		if cfg.plane(p).Ship {
			s.wg.Add(1)
			go s.flushLoop(p)
		}
	}
	s.wg.Add(1)
	go s.sealTickerLoop()

	return s, nil
}

// AppendBatch persists the data + catalog blocks of one S3 op's batch
// to the open segment, along with an op-root record identifying the
// (bucket, root) it produced. Both CARs + the ops record are fsynced
// before returning. Either block slice may be empty.
func (s *Store) AppendBatch(ctx context.Context, dataBlocks, catalogBlocks []block.Block, opRoot blockstore.OpRoot) error {
	if !opRoot.Root.Defined() {
		return errors.New("logstore: AppendBatch: opRoot.Root must be defined")
	}

	s.appMu.Lock()
	defer s.appMu.Unlock()

	open, err := s.ensureOpenLockedAppMu(ctx)
	if err != nil {
		return err
	}
	if err := open.append(dataBlocks, catalogBlocks, opRoot); err != nil {
		return err
	}

	if open.Size() >= s.cfg.SealBytes {
		s.requestSeal()
	}
	return nil
}

// Get returns the block from the local log if any segment contains it,
// or ErrNotFound otherwise. Searches open first, then sealed
// newest-first.
func (s *Store) Get(ctx context.Context, c cid.Cid) (block.Block, error) {
	s.catMu.RLock()
	open := s.open
	sealed := make([]*Segment, len(s.sealed))
	copy(sealed, s.sealed)
	s.catMu.RUnlock()

	if open != nil {
		if blk, err := open.get(ctx, c); err == nil {
			return blk, nil
		} else if !errors.Is(err, blockstore.ErrNotFound) {
			return nil, err
		}
	}
	for _, seg := range sealed {
		blk, err := seg.get(ctx, c)
		if err == nil {
			return blk, nil
		}
		if !errors.Is(err, blockstore.ErrNotFound) {
			return nil, err
		}
	}
	return nil, blockstore.ErrNotFound
}

// Close seals the open segment, drains the flush queues, and stops
// background goroutines. Safe to call once.
func (s *Store) Close(ctx context.Context) error {
	s.catMu.Lock()
	already := s.closing == nil
	if !already {
		select {
		case <-s.closing:
			already = true
		default:
		}
	}
	if !already {
		close(s.closing)
	}
	s.catMu.Unlock()
	if already {
		return nil
	}

	// Force-seal the open segment so anything still buffered is durable
	// (sealed state persists; recovery re-enqueues it for ship next
	// start).
	s.appMu.Lock()
	s.catMu.Lock()
	open := s.open
	s.open = nil
	s.catMu.Unlock()
	if open != nil {
		if err := open.seal(ctx, s.cfg.Meta); err != nil {
			s.logger.Error("logstore: seal at close", zap.Error(err))
		} else {
			s.catMu.Lock()
			s.sealed = append([]*Segment{open}, s.sealed...)
			s.catMu.Unlock()
			s.enqueueFlush(open)
		}
	}
	s.appMu.Unlock()

	for _, q := range s.flushQ {
		close(q)
	}
	s.wg.Wait()
	return nil
}

// requestSeal coalesces seal triggers.
func (s *Store) requestSeal() {
	select {
	case s.sealReq <- struct{}{}:
	default:
	}
}

// enqueueFlush hands a sealed segment to each shipping plane's worker.
// Non-blocking: a full queue is logged and retried on the next restart.
func (s *Store) enqueueFlush(seg *Segment) {
	for _, p := range blockstore.Planes {
		q, ok := s.flushQ[p]
		if !ok {
			continue
		}
		select {
		case q <- seg:
		default:
			s.logger.Warn("logstore: flush queue full; segment will retry on restart",
				zap.Stringer("plane", p), zap.Uint64("seq", seg.Seq()))
		}
	}
}

// ensureOpenLockedAppMu returns the current open segment, creating a
// fresh one if none exists. Caller must hold appMu.
func (s *Store) ensureOpenLockedAppMu(ctx context.Context) (*Segment, error) {
	s.catMu.RLock()
	open := s.open
	s.catMu.RUnlock()
	if open != nil {
		return open, nil
	}

	seq, err := s.cfg.Meta.NextSegmentSeq(ctx)
	if err != nil {
		return nil, err
	}
	seg, err := createOpenSegment(ctx, s.cfg.Dir, seq, s.cfg.Meta, s.logger)
	if err != nil {
		return nil, err
	}

	s.catMu.Lock()
	if s.open == nil {
		s.open = seg
		s.openedAt = time.Now()
		if seq >= s.nextSeq {
			s.nextSeq = seq + 1
		}
		s.catMu.Unlock()
		return seg, nil
	}
	s.catMu.Unlock()
	if err := seg.retireOpen(); err != nil {
		s.logger.Warn("logstore: retire raced new segment", zap.Error(err))
	}
	if err := s.cfg.Meta.DeleteSegment(ctx, seq); err != nil {
		s.logger.Warn("logstore: delete raced new segment row", zap.Error(err))
	}
	s.catMu.RLock()
	open = s.open
	s.catMu.RUnlock()
	return open, nil
}

// sealOpenIfDue seals the current open segment if one exists and is due
// (or force is set). Enqueues to each shipping plane.
func (s *Store) sealOpenIfDue(ctx context.Context, force bool) error {
	s.appMu.Lock()
	defer s.appMu.Unlock()

	s.catMu.RLock()
	open := s.open
	openedAt := s.openedAt
	s.catMu.RUnlock()
	if open == nil {
		return nil
	}
	if !force {
		if open.Size() < s.cfg.SealBytes && time.Since(openedAt) < s.cfg.SealAge {
			return nil
		}
	}

	if err := open.seal(ctx, s.cfg.Meta); err != nil {
		return err
	}

	s.catMu.Lock()
	if s.open == open {
		s.open = nil
		s.sealed = append([]*Segment{open}, s.sealed...)
	}
	s.catMu.Unlock()

	s.enqueueFlush(open)
	return nil
}

// flushLoop drains one plane's queue, shipping each sealed segment's
// shard of that plane. Exits on close.
func (s *Store) flushLoop(plane blockstore.Plane) {
	defer s.wg.Done()
	q := s.flushQ[plane]
	for {
		select {
		case <-s.closing:
			return
		case seg, ok := <-q:
			if !ok {
				return
			}
			s.flushOne(plane, seg)
		}
	}
}

// flushOne ships one plane's shard of a sealed segment, retrying with
// backoff. On success it stamps the plane's ship state (in memory + in
// Meta — which advances forge_root_cid for the catalog plane) and runs
// the retention sweep.
func (s *Store) flushOne(plane blockstore.Plane, seg *Segment) {
	ctx := context.Background()
	flush := s.cfg.plane(plane).Flush
	const maxAttempts = 5
	backoff := time.Second

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := flush(ctx, seg)
		if err == nil {
			now := time.Now().Unix()
			seg.markShipped(plane, now)
			var opRoots []blockstore.OpRoot
			if plane == blockstore.PlaneCatalog {
				opRoots = seg.OpRoots()
			}
			if merr := s.cfg.Meta.MarkSegmentShipped(ctx, seg.Seq(), plane, now, opRoots); merr != nil {
				s.logger.Error("logstore: mark shipped",
					zap.Stringer("plane", plane), zap.Uint64("seq", seg.Seq()), zap.Error(merr))
			}
			s.runRetention(ctx)
			return
		}
		s.logger.Warn("logstore: ship attempt failed",
			zap.Stringer("plane", plane), zap.Uint64("seq", seg.Seq()),
			zap.Int("attempt", attempt), zap.Error(err))
		select {
		case <-s.closing:
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
	s.logger.Error("logstore: ship exhausted retries; plane remains unshipped",
		zap.Stringer("plane", plane), zap.Uint64("seq", seg.Seq()))
}

// runRetention retires, per shipping plane, the CARs of shipped
// segments beyond that plane's Retain window, then drops any segment
// whose both planes have fully retired off disk. Never-ship planes are
// never retired.
func (s *Store) runRetention(ctx context.Context) {
	type retireReq struct {
		seg   *Segment
		plane blockstore.Plane
	}

	s.catMu.RLock()
	sealed := make([]*Segment, len(s.sealed))
	copy(sealed, s.sealed)
	s.catMu.RUnlock()

	var toRetire []retireReq
	for _, p := range blockstore.Planes {
		pc := s.cfg.plane(p)
		if !pc.Ship {
			continue
		}
		shippedSeen := 0
		for _, seg := range sealed {
			if !seg.IsShipped(p) || seg.IsRetired(p) {
				continue
			}
			shippedSeen++
			if shippedSeen <= pc.Retain {
				continue
			}
			toRetire = append(toRetire, retireReq{seg: seg, plane: p})
		}
	}

	for _, tr := range toRetire {
		if err := tr.seg.retirePlane(tr.plane); err != nil {
			s.logger.Warn("logstore: retire plane",
				zap.Stringer("plane", tr.plane), zap.Uint64("seq", tr.seg.Seq()), zap.Error(err))
		}
	}

	// Drop fully-retired segments from the read tier + DB.
	s.catMu.Lock()
	var keep, remove []*Segment
	for _, seg := range s.sealed {
		if seg.FullyRetired() {
			remove = append(remove, seg)
		} else {
			keep = append(keep, seg)
		}
	}
	s.sealed = keep
	s.catMu.Unlock()

	for _, seg := range remove {
		if err := s.cfg.Meta.DeleteSegment(ctx, seg.Seq()); err != nil {
			s.logger.Warn("logstore: delete segment row",
				zap.Uint64("seq", seg.Seq()), zap.Error(err))
		}
	}
}

// sealTickerLoop wakes periodically and seals the open segment if it is
// due (over SealAge or SealBytes), and also services explicit seal
// requests.
func (s *Store) sealTickerLoop() {
	defer s.wg.Done()
	interval := s.cfg.SealAge / 4
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.closing:
			return
		case <-t.C:
			if err := s.sealOpenIfDue(context.Background(), false); err != nil {
				s.logger.Warn("logstore: tick seal", zap.Error(err))
			}
		case <-s.sealReq:
			if err := s.sealOpenIfDue(context.Background(), false); err != nil {
				s.logger.Warn("logstore: req seal", zap.Error(err))
			}
		}
	}
}
