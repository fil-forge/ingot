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
// blockstore.Log is the consumer-facing contract (AppendBatch /
// Get / Close); *Store is the production LSM implementation that
// backs it.
var _ blockstore.Log = (*Store)(nil)

// Store is the LSM-style log: one open segment accepting appends,
// plus N sealed segments (some flushed, some pending flush) that
// serve reads in front of the network blockstore.
//
// Concurrency:
//   - catMu (RWMutex) guards open + sealed slice + nextSeq. Writers
//     hold Lock briefly during seal/retire/new-open swaps. Readers
//     hold RLock to take a stable snapshot of the segment list, then
//     do file I/O outside the lock.
//   - appMu (Mutex) serializes appenders against each other so the
//     open-segment append fd has a single writer.
type Store struct {
	cfg    Config
	logger *zap.Logger

	catMu   sync.RWMutex
	open    *Segment
	sealed  []*Segment // newest-first; includes flushed-and-retained
	nextSeq uint64

	appMu sync.Mutex

	flushQ  chan *Segment
	closing chan struct{}
	wg      sync.WaitGroup

	openedAt time.Time

	// sealReq is a coalesced "seal the open segment now" channel.
	// AppendBatch sends after exceeding SealBytes; the seal-ticker
	// sends on every tick if the open segment has been open longer
	// than SealAge.
	sealReq chan struct{}
}

// Open initializes a Store: scans Dir, reconciles with cfg.Meta,
// re-enqueues unflushed segments for the flusher, force-seals any
// previously-open segment, and starts a fresh open segment ready to
// accept appends.
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
		flushQ:  make(chan *Segment, 64),
		closing: make(chan struct{}),
		sealReq: make(chan struct{}, 1),
	}

	if err := s.recover(ctx); err != nil {
		return nil, err
	}

	// Force-seal a recovered open segment (if any) so a fresh open is
	// always brand-new on each process startup. This avoids the
	// complications of resuming append into a partially-written file.
	if s.open != nil {
		if err := s.open.seal(ctx, cfg.Meta); err != nil {
			return nil, fmt.Errorf("logstore: force-seal recovered open segment: %w", err)
		}
		s.sealed = append([]*Segment{s.open}, s.sealed...)
		select {
		case s.flushQ <- s.open:
		default:
			s.logger.Warn("logstore: flush queue full at recovery; segment will retry on next tick")
		}
		s.open = nil
	}

	s.wg.Add(2)
	go s.flushLoop()
	go s.sealTickerLoop()

	return s, nil
}

// AppendBatch persists `blocks` to the open segment along with an
// op-root record identifying the (bucket, root) this batch's S3
// op produced. fsyncs CAR + ops sidecar before returning. After
// AppendBatch returns nil, both blocks and op-root are durable; the
// caller may safely advance the bucket's published Root.
//
// An empty blocks slice is legal — an MST mutation can produce a
// new root that points at a node already materialized in a prior
// segment (e.g., trimTop after Delete unwraps to an existing
// subtree). In that case only the OpRoot record is written;
// nothing new lands in the CAR.
func (s *Store) AppendBatch(ctx context.Context, blocks []block.Block, opRoot blockstore.OpRoot) error {
	if !opRoot.Root.Defined() {
		return errors.New("logstore: AppendBatch: opRoot.Root must be defined")
	}

	s.appMu.Lock()
	defer s.appMu.Unlock()

	open, err := s.ensureOpenLockedAppMu(ctx)
	if err != nil {
		return err
	}
	if err := open.append(blocks, opRoot); err != nil {
		return err
	}

	// Trigger seal if size threshold hit. Non-blocking signal — the
	// actual seal happens off this goroutine to keep AppendBatch
	// latency bounded by fsync.
	if open.Size() >= s.cfg.SealBytes {
		s.requestSeal()
	}
	return nil
}

// Get returns the block from the local log if any segment contains
// it, or ErrNotFound otherwise. Searches open first, then sealed
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

// Close seals the open segment, drains the flush queue, and stops
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

	// Force-seal the open segment so anything still buffered makes it
	// into the flush queue.
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
			select {
			case s.flushQ <- open:
			case <-ctx.Done():
			}
		}
	}
	s.appMu.Unlock()

	close(s.flushQ)
	s.wg.Wait()
	return nil
}

// requestSeal coalesces seal triggers — the channel has buffer 1 so
// repeated triggers between two ticks of the seal goroutine are
// folded into one.
func (s *Store) requestSeal() {
	select {
	case s.sealReq <- struct{}{}:
	default:
	}
}

// ensureOpenLockedAppMu returns the current open segment, creating a
// fresh one if none exists. Caller must hold appMu (so concurrent
// AppendBatches don't race on segment creation).
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
	// Lost a race; another caller created an open segment first.
	s.catMu.Unlock()
	if err := seg.retire(); err != nil {
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

// sealOpenIfDue seals the current open segment if one exists. Sends
// to flushQ. Idempotent: returns nil if there's nothing to seal.
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

	select {
	case s.flushQ <- open:
	case <-s.closing:
		return nil
	}
	return nil
}

// flushLoop drains flushQ, calling cfg.Flush for each sealed segment.
// On success, transitions the segment to StateFlushed and runs the
// retention sweep. On failure, requeues with backoff so transient
// errors (network blips) don't permanently stall the pipeline.
//
// Exits when either the closing signal fires or flushQ is closed
// (whichever comes first).
func (s *Store) flushLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.closing:
			return
		case seg, ok := <-s.flushQ:
			if !ok {
				return
			}
			s.flushOne(seg)
		}
	}
}

func (s *Store) flushOne(seg *Segment) {
	ctx := context.Background()
	const maxAttempts = 5
	backoff := time.Second

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := s.cfg.Flush(ctx, seg)
		if err == nil {
			seg.stateMu.Lock()
			seg.state = StateFlushed
			seg.stateMu.Unlock()
			s.runRetention(ctx)
			return
		}
		s.logger.Warn("logstore: flush attempt failed",
			zap.Uint64("seq", seg.Seq()),
			zap.Int("attempt", attempt),
			zap.Error(err))
		select {
		case <-s.closing:
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
	s.logger.Error("logstore: flush exhausted retries; segment remains sealed",
		zap.Uint64("seq", seg.Seq()))
	// Leaving the segment in sealed state; recovery will pick it up
	// at next process restart, or operators can intervene.
}

// runRetention removes flushed segments older than cfg.Retain from
// disk and the catalog.
func (s *Store) runRetention(ctx context.Context) {
	s.catMu.Lock()
	// Walk newest-first, count flushed segments. Once we exceed
	// Retain flushed segments, the rest are retire candidates.
	var (
		flushedSeen int
		keep        []*Segment
		retire      []*Segment
	)
	for _, seg := range s.sealed {
		if seg.State() != StateFlushed {
			keep = append(keep, seg)
			continue
		}
		flushedSeen++
		if flushedSeen <= s.cfg.Retain {
			keep = append(keep, seg)
			continue
		}
		retire = append(retire, seg)
	}
	s.sealed = keep
	s.catMu.Unlock()

	for _, seg := range retire {
		if err := seg.retire(); err != nil {
			s.logger.Warn("logstore: retire", zap.Uint64("seq", seg.Seq()), zap.Error(err))
		}
		if err := s.cfg.Meta.DeleteSegment(ctx, seg.Seq()); err != nil {
			s.logger.Warn("logstore: delete segment row",
				zap.Uint64("seq", seg.Seq()), zap.Error(err))
		}
	}
}

// sealTickerLoop wakes periodically (every SealAge / 4) and seals
// the open segment if it has been open longer than SealAge or its
// size is over SealBytes (the latter is also signaled directly via
// requestSeal but we double-check defensively).
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
