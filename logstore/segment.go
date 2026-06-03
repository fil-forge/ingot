package logstore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/cars"
)

// placeholderRoot is the placeholder CAR header root. Each CAR is
// multi-rooted by intent; the per-op roots live in the .ops sidecar
// (and the catalog .idx), not the CAR header.
var placeholderRoot = cid.NewCidV1(cid.Raw, []byte{0x00, 0x00})

// segPlane is one plane's on-disk state within a segment: its CAR file,
// its .idx sidecar, the in-memory block index, the dedup gate, the
// running size, the seal-time sha256, and the ship/retire bookkeeping.
// A Segment owns two of these (data + catalog); they append, seal,
// ship, and retire independently.
type segPlane struct {
	kind    blockstore.Plane
	carPath string
	idxPath string

	// fdRW is the append/read fd for an open segment (closed at seal).
	// fdRO is the read-only fd opened at seal time.
	fdRW *os.File
	fdRO *os.File

	// index maps each block's CID to its on-disk byte position in this
	// plane's CAR; seen is the dedup gate kept in sync with index's key
	// set.
	index map[cid.Cid]blockstore.BlockLoc
	seen  *cid.Set

	size int64  // current CAR byte size
	sha  []byte // seal-time sha256 of the CAR

	shippedAt int64 // unix seconds when this plane shipped; 0 = not shipped
	retired   bool  // CAR + idx unlinked off local disk
}

func newSegPlane(dir string, seq uint64, kind blockstore.Plane) segPlane {
	var carName, idxName string
	if kind == blockstore.PlaneData {
		carName, idxName = dataCARName(seq), dataIdxName(seq)
	} else {
		carName, idxName = catCARName(seq), catIdxName(seq)
	}
	return segPlane{
		kind:    kind,
		carPath: filepath.Join(dir, carName),
		idxPath: filepath.Join(dir, idxName),
		index:   map[cid.Cid]blockstore.BlockLoc{},
		seen:    cid.NewSet(),
	}
}

// Segment is one log entry split across two CAR files — the data plane
// (raw object-body chunks) and the catalog plane (dag-cbor MST nodes,
// manifests, indexes) — plus a shared .ops sidecar recording the
// (bucket, root) of each batch. Open segments accept appends; sealed
// segments are read-only and ship/retire per plane.
//
// Concurrency: append is serialized by Store.appMu. stateMu guards
// every mutable field below; readers RLock for lookups, writers Lock
// for append/seal/retire/ship.
type Segment struct {
	seq    uint64
	dir    string
	logger *zap.Logger

	stateMu sync.RWMutex

	state    State
	sealedAt int64

	data segPlane
	cat  segPlane

	// opRoots is the ordered per-batch (bucket, root) record. Roots are
	// MST nodes → a catalog-plane concept; opsFD is the append-only
	// sidecar (open segment only, closed at seal).
	opRoots []blockstore.OpRoot
	opsFD   *os.File
}

// planeRef returns the segPlane for p.
func (s *Segment) planeRef(p blockstore.Plane) *segPlane {
	if p == blockstore.PlaneData {
		return &s.data
	}
	return &s.cat
}

// Seq returns the segment's identifier.
func (s *Segment) Seq() uint64 { return s.seq }

// State reports the current lifecycle state.
func (s *Segment) State() State {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.state
}

// Size reports the combined on-disk byte size of both CARs. Drives the
// shared seal-on-size trigger.
func (s *Segment) Size() int64 {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.data.size + s.cat.size
}

// PlaneSize reports the on-disk byte size of plane p's CAR.
func (s *Segment) PlaneSize(p blockstore.Plane) int64 {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.planeRef(p).size
}

// SHA256 returns the seal-time sha256 of plane p's CAR file. Empty for
// open segments.
func (s *Segment) SHA256(p blockstore.Plane) []byte {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	src := s.planeRef(p).sha
	out := make([]byte, len(src))
	copy(out, src)
	return out
}

// SealedAt returns the seal-time unix-seconds timestamp. Zero for open
// segments.
func (s *Segment) SealedAt() int64 {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.sealedAt
}

// ShippedAt returns the unix-seconds ship timestamp of plane p, or 0.
func (s *Segment) ShippedAt(p blockstore.Plane) int64 {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.planeRef(p).shippedAt
}

// IsShipped reports whether plane p has shipped to Forge.
func (s *Segment) IsShipped(p blockstore.Plane) bool {
	return s.ShippedAt(p) != 0
}

// IsRetired reports whether plane p's CAR has been unlinked.
func (s *Segment) IsRetired(p blockstore.Plane) bool {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.planeRef(p).retired
}

// FullyRetired reports whether both planes have retired off local disk.
func (s *Segment) FullyRetired() bool {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.data.retired && s.cat.retired
}

// OpRoots returns a copy of the per-batch (bucket, root) records.
func (s *Segment) OpRoots() []blockstore.OpRoot {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	out := make([]blockstore.OpRoot, len(s.opRoots))
	copy(out, s.opRoots)
	return out
}

// Positions returns a copy of plane p's cid → on-disk-position table.
// Used by the flush path to build a sharded-dag-index without
// rescanning the file.
func (s *Segment) Positions(p blockstore.Plane) map[cid.Cid]blockstore.BlockLoc {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	src := s.planeRef(p).index
	out := make(map[cid.Cid]blockstore.BlockLoc, len(src))
	for c, loc := range src {
		out[c] = loc
	}
	return out
}

// DataCARPath / CatCARPath return the absolute paths to the two CARs.
func (s *Segment) DataCARPath() string { return s.data.carPath }
func (s *Segment) CatCARPath() string  { return s.cat.carPath }

// PlaneCARPath returns the absolute path to plane p's CAR file.
func (s *Segment) PlaneCARPath(p blockstore.Plane) string { return s.planeRef(p).carPath }

// OpsPath returns the absolute path to the shared ops sidecar.
func (s *Segment) OpsPath() string { return s.opsPath() }

func (s *Segment) opsPath() string { return filepath.Join(s.dir, opsName(s.seq)) }

func dataCARName(seq uint64) string { return fmt.Sprintf("seg-%020d.data.car", seq) }
func catCARName(seq uint64) string  { return fmt.Sprintf("seg-%020d.cat.car", seq) }
func dataIdxName(seq uint64) string { return fmt.Sprintf("seg-%020d.data.idx", seq) }
func catIdxName(seq uint64) string  { return fmt.Sprintf("seg-%020d.cat.idx", seq) }
func opsName(seq uint64) string     { return fmt.Sprintf("seg-%020d.ops", seq) }

// createPlaneCAR creates a fresh CAR file with a placeholder header,
// fsyncs it, and returns the open fd + header length.
func createPlaneCAR(path string) (*os.File, int64, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, 0, err
	}
	hdrLen, err := cars.WriteHeader(f, []cid.Cid{placeholderRoot})
	if err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, 0, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, 0, err
	}
	return f, hdrLen, nil
}

// createOpenSegment creates a brand-new segment in the open state:
// initializes both CAR files with headers, opens the shared ops
// sidecar, and records the row in Meta.
func createOpenSegment(ctx context.Context, dir string, seq uint64, meta Meta, logger *zap.Logger) (*Segment, error) {
	s := &Segment{
		seq:    seq,
		dir:    dir,
		logger: logger,
		state:  StateOpen,
		data:   newSegPlane(dir, seq, blockstore.PlaneData),
		cat:    newSegPlane(dir, seq, blockstore.PlaneCatalog),
	}

	df, dsz, err := createPlaneCAR(s.data.carPath)
	if err != nil {
		return nil, fmt.Errorf("logstore: open data car %d: %w", seq, err)
	}
	s.data.fdRW = df
	s.data.size = dsz

	cf, csz, err := createPlaneCAR(s.cat.carPath)
	if err != nil {
		_ = df.Close()
		_ = os.Remove(s.data.carPath)
		return nil, fmt.Errorf("logstore: open catalog car %d: %w", seq, err)
	}
	s.cat.fdRW = cf
	s.cat.size = csz

	opsFile, err := os.OpenFile(s.opsPath(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		_ = df.Close()
		_ = cf.Close()
		_ = os.Remove(s.data.carPath)
		_ = os.Remove(s.cat.carPath)
		return nil, fmt.Errorf("logstore: open ops %d: %w", seq, err)
	}
	s.opsFD = opsFile

	if err := meta.InsertSegmentOpen(ctx, seq); err != nil {
		_ = df.Close()
		_ = cf.Close()
		_ = opsFile.Close()
		_ = os.Remove(s.data.carPath)
		_ = os.Remove(s.cat.carPath)
		_ = os.Remove(s.opsPath())
		return nil, err
	}

	return s, nil
}

// writeFresh writes the not-yet-seen blocks to this plane's CAR at the
// current size and returns the fresh blocks + their on-disk positions.
// It does NOT mutate seen/index/size — the caller commits those only
// after a successful fsync (so a fsync error doesn't poison dedup
// state). Caller holds the segment write lock.
func (p *segPlane) writeFresh(blocks []block.Block) ([]block.Block, []cars.BlockPosition, error) {
	fresh := make([]block.Block, 0, len(blocks))
	for _, blk := range blocks {
		if p.seen.Has(blk.Cid()) {
			continue
		}
		fresh = append(fresh, blk)
	}
	if len(fresh) == 0 {
		return nil, nil, nil
	}
	positions, err := cars.WriteBlocksAt(p.fdRW, p.size, fresh)
	if err != nil {
		return nil, nil, err
	}
	return fresh, positions, nil
}

// commitFresh updates the dedup set, position table, and running size
// after a successful fsync.
func (p *segPlane) commitFresh(fresh []block.Block, positions []cars.BlockPosition) {
	for i, blk := range fresh {
		p.seen.Add(blk.Cid())
		p.index[blk.Cid()] = blockstore.BlockLoc{Offset: positions[i].Offset, Length: positions[i].Length}
	}
	if n := len(positions); n > 0 {
		end := int64(positions[n-1].Offset) + int64(positions[n-1].Length)
		if end > p.size {
			p.size = end
		}
	}
}

// append writes the data + catalog blocks of one batch to their CARs,
// records the op-root, fsyncs both CARs and the ops sidecar together,
// then commits the in-memory index. Caller must hold Store.appMu.
//
// Cross-plane durability: AppendBatch returns success only after the
// data CAR, the catalog CAR, and the ops record are all fsynced — so
// the bucket Root may advance once and have both planes durable. Either
// block slice may be empty (an MST-only mutation writes no data blocks;
// a trimTop-to-existing-subtree writes neither).
func (s *Segment) append(dataBlocks, catBlocks []block.Block, opRoot blockstore.OpRoot) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if s.state != StateOpen || s.data.fdRW == nil || s.cat.fdRW == nil {
		return errors.New("logstore: segment not open for append")
	}

	dataFresh, dataPos, err := s.data.writeFresh(dataBlocks)
	if err != nil {
		return fmt.Errorf("logstore: append data seg %d: %w", s.seq, err)
	}
	catFresh, catPos, err := s.cat.writeFresh(catBlocks)
	if err != nil {
		return fmt.Errorf("logstore: append catalog seg %d: %w", s.seq, err)
	}

	// Append the op-root record unconditionally — even an all-duplicate
	// batch still represents a real bucket-Root advance the flusher must
	// replay.
	opsRec, err := encodeOpRecord(opRoot)
	if err != nil {
		return fmt.Errorf("logstore: encode oprec seg %d: %w", s.seq, err)
	}
	if _, err := s.opsFD.Write(opsRec); err != nil {
		return fmt.Errorf("logstore: write ops seg %d: %w", s.seq, err)
	}

	if err := syncAll(s.data.fdRW, s.cat.fdRW, s.opsFD); err != nil {
		return fmt.Errorf("logstore: fsync seg %d: %w", s.seq, err)
	}

	s.data.commitFresh(dataFresh, dataPos)
	s.cat.commitFresh(catFresh, catPos)
	s.opRoots = append(s.opRoots, opRoot)
	return nil
}

// seal closes both open CAR fds + the ops fd, hashes both CARs, writes
// both .idx sidecars, and records the sealed state in Meta. After this
// returns the segment is StateSealed and each plane is ready to ship.
func (s *Segment) seal(ctx context.Context, meta Meta) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if s.state != StateOpen {
		// Idempotent: already sealed.
		return nil
	}

	if err := syncAll(s.data.fdRW, s.cat.fdRW, s.opsFD); err != nil {
		return fmt.Errorf("logstore: pre-seal fsync %d: %w", s.seq, err)
	}
	for _, p := range []*segPlane{&s.data, &s.cat} {
		if err := p.fdRW.Close(); err != nil {
			return fmt.Errorf("logstore: close %s car %d: %w", p.kind, s.seq, err)
		}
		p.fdRW = nil
	}
	if err := s.opsFD.Close(); err != nil {
		return fmt.Errorf("logstore: close ops %d: %w", s.seq, err)
	}
	s.opsFD = nil

	for _, p := range []*segPlane{&s.data, &s.cat} {
		sum, err := hashFile(p.carPath)
		if err != nil {
			return fmt.Errorf("logstore: hash %s %d: %w", p.kind, s.seq, err)
		}
		p.sha = sum
	}
	s.sealedAt = time.Now().Unix()
	s.state = StateSealed

	// Write idx sidecars (op-roots live in the catalog idx — roots are
	// catalog nodes).
	if err := writePlaneIdx(&s.data, s.seq, s.sealedAt, nil); err != nil {
		return fmt.Errorf("logstore: write data idx %d: %w", s.seq, err)
	}
	if err := writePlaneIdx(&s.cat, s.seq, s.sealedAt, s.opRoots); err != nil {
		return fmt.Errorf("logstore: write catalog idx %d: %w", s.seq, err)
	}

	if err := meta.MarkSegmentSealed(ctx, s.seq, s.sealedAt,
		s.data.size, s.data.sha, s.cat.size, s.cat.sha, s.opRoots); err != nil {
		return fmt.Errorf("logstore: mark sealed %d: %w", s.seq, err)
	}

	for _, p := range []*segPlane{&s.data, &s.cat} {
		ro, err := os.Open(p.carPath)
		if err != nil {
			return fmt.Errorf("logstore: open ro %s car %d: %w", p.kind, s.seq, err)
		}
		p.fdRO = ro
	}
	return nil
}

// markShipped stamps plane p's ship timestamp. Called by the flusher
// after the plane's CAR has shipped (or was trivially empty).
func (s *Segment) markShipped(p blockstore.Plane, shippedAt int64) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.planeRef(p).shippedAt = shippedAt
}

// retirePlane closes plane p's read/write fds and unlinks its CAR + idx
// sidecar. When both planes are retired it also drops the shared ops
// sidecar. Safe to call after the plane has shipped (or, for a
// never-ship plane, never called).
func (s *Segment) retirePlane(p blockstore.Plane) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	pf := s.planeRef(p)
	if pf.retired {
		return nil
	}
	if pf.fdRO != nil {
		_ = pf.fdRO.Close()
		pf.fdRO = nil
	}
	if pf.fdRW != nil {
		_ = pf.fdRW.Close()
		pf.fdRW = nil
	}
	for _, name := range []string{pf.carPath, pf.idxPath} {
		if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("logstore: unlink %s: %w", name, err)
		}
	}
	pf.retired = true
	// Drop the in-memory index/dedup set: the blocks are no longer local,
	// so reads must fall through to the network tier.
	pf.index = nil
	pf.seen = nil

	if s.data.retired && s.cat.retired {
		if s.opsFD != nil {
			_ = s.opsFD.Close()
			s.opsFD = nil
		}
		if err := os.Remove(s.opsPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("logstore: unlink %s: %w", s.opsPath(), err)
		}
	}
	return nil
}

// retireOpen closes an open (never-sealed) segment's fds and unlinks
// its files. Used to clean up a segment that lost the open-segment
// creation race.
func (s *Segment) retireOpen() error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	for _, p := range []*segPlane{&s.data, &s.cat} {
		if p.fdRW != nil {
			_ = p.fdRW.Close()
			p.fdRW = nil
		}
		if p.fdRO != nil {
			_ = p.fdRO.Close()
			p.fdRO = nil
		}
	}
	if s.opsFD != nil {
		_ = s.opsFD.Close()
		s.opsFD = nil
	}
	for _, name := range []string{s.data.carPath, s.data.idxPath, s.cat.carPath, s.cat.idxPath, s.opsPath()} {
		if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("logstore: unlink %s: %w", name, err)
		}
	}
	return nil
}

// get returns the block at the given CID from whichever plane holds it,
// or blockstore.ErrNotFound (including when the holding plane has been
// retired off local disk, so the layered reader falls through to the
// network tier). The read lock is held through ReadAt so a concurrent
// retire/seal cannot close the fd mid-read.
func (s *Segment) get(_ context.Context, c cid.Cid) (block.Block, error) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	for _, p := range []*segPlane{&s.data, &s.cat} {
		if p.retired {
			continue
		}
		loc, ok := p.index[c]
		if !ok {
			continue
		}
		fd := p.fdRO
		if fd == nil {
			fd = p.fdRW
		}
		if fd == nil {
			return nil, fmt.Errorf("logstore: segment %d has no read fd", s.seq)
		}
		buf := make([]byte, loc.Length)
		if _, err := fd.ReadAt(buf, int64(loc.Offset)); err != nil {
			return nil, fmt.Errorf("logstore: read seg %d offset %d: %w", s.seq, loc.Offset, err)
		}
		return block.NewBlockWithCid(buf, c)
	}
	return nil, blockstore.ErrNotFound
}

// syncAll fsyncs the given files in parallel and joins any errors.
func syncAll(files ...*os.File) error {
	var wg sync.WaitGroup
	errs := make([]error, len(files))
	for i, f := range files {
		wg.Add(1)
		go func(i int, f *os.File) {
			defer wg.Done()
			errs[i] = f.Sync()
		}(i, f)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// === idx sidecar (one per plane) ===

type idxBlockJSON struct {
	CID    string `json:"cid"`
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

type idxOpRootJSON struct {
	Bucket string `json:"bucket"`
	Root   string `json:"root"`
}

type idxFileJSON struct {
	Seq      uint64          `json:"seq"`
	Plane    string          `json:"plane"`
	SizeByte int64           `json:"size_bytes"`
	SHA256   string          `json:"sha256_hex"`
	SealedAt int64           `json:"sealed_at"`
	Blocks   []idxBlockJSON  `json:"blocks"`
	OpRoots  []idxOpRootJSON `json:"op_roots,omitempty"`
}

// writePlaneIdx persists plane p's idx sidecar. opRoots is non-nil only
// for the catalog plane. Caller holds the segment write lock and has
// populated p.sha + s.sealedAt.
func writePlaneIdx(p *segPlane, seq uint64, sealedAt int64, opRoots []blockstore.OpRoot) error {
	blocks := make([]idxBlockJSON, 0, len(p.index))
	for c, loc := range p.index {
		blocks = append(blocks, idxBlockJSON{CID: c.String(), Offset: loc.Offset, Length: loc.Length})
	}
	ops := make([]idxOpRootJSON, len(opRoots))
	for i, opr := range opRoots {
		ops[i] = idxOpRootJSON{Bucket: opr.Bucket, Root: opr.Root.String()}
	}
	body := idxFileJSON{
		Seq:      seq,
		Plane:    p.kind.String(),
		SizeByte: p.size,
		SHA256:   fmt.Sprintf("%x", p.sha),
		SealedAt: sealedAt,
		Blocks:   blocks,
		OpRoots:  ops,
	}
	data, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		return err
	}
	tmp := p.idxPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p.idxPath)
}

// loadSealedPlaneFromIdx hydrates one plane in the sealed state from its
// .idx sidecar and opens its read fd. Returns the plane's op-roots
// (catalog plane only).
func loadSealedPlaneFromIdx(dir string, seq uint64, kind blockstore.Plane) (*segPlane, []blockstore.OpRoot, error) {
	p := newSegPlane(dir, seq, kind)
	data, err := os.ReadFile(p.idxPath)
	if err != nil {
		return nil, nil, fmt.Errorf("logstore: read %s idx %d: %w", kind, seq, err)
	}
	var raw idxFileJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, nil, fmt.Errorf("logstore: parse %s idx %d: %w", kind, seq, err)
	}
	if raw.Seq != seq {
		return nil, nil, fmt.Errorf("logstore: %s idx seq %d != filename %d", kind, raw.Seq, seq)
	}
	for _, b := range raw.Blocks {
		c, err := cid.Decode(b.CID)
		if err != nil {
			return nil, nil, fmt.Errorf("logstore: %s idx bad cid %q: %w", kind, b.CID, err)
		}
		p.index[c] = blockstore.BlockLoc{Offset: b.Offset, Length: b.Length}
		p.seen.Add(c)
	}
	sha, err := hexDecode(raw.SHA256)
	if err != nil {
		return nil, nil, fmt.Errorf("logstore: %s idx bad sha %q: %w", kind, raw.SHA256, err)
	}
	p.sha = sha
	p.size = raw.SizeByte

	ops := make([]blockstore.OpRoot, len(raw.OpRoots))
	for i, o := range raw.OpRoots {
		c, err := cid.Decode(o.Root)
		if err != nil {
			return nil, nil, fmt.Errorf("logstore: %s idx bad root %q: %w", kind, o.Root, err)
		}
		ops[i] = blockstore.OpRoot{Bucket: o.Bucket, Root: c}
	}

	ro, err := os.Open(p.carPath)
	if err != nil {
		return nil, nil, fmt.Errorf("logstore: open sealed %s car %d: %w", kind, seq, err)
	}
	p.fdRO = ro
	return &p, ops, nil
}

// loadOrRetiredPlane loads a plane from its .idx sidecar, or — when the
// sidecar is absent because the plane already shipped and retired off
// disk while the other plane stayed local — returns a retired
// placeholder (empty index, no fd) so reads of that plane fall through
// to the network.
func loadOrRetiredPlane(dir string, seq uint64, kind blockstore.Plane) (*segPlane, []blockstore.OpRoot, error) {
	p := newSegPlane(dir, seq, kind)
	if _, err := os.Stat(p.idxPath); errors.Is(err, os.ErrNotExist) {
		p.retired = true
		p.index = nil
		p.seen = nil
		return &p, nil, nil
	}
	return loadSealedPlaneFromIdx(dir, seq, kind)
}

// loadSealedFromIdx hydrates a sealed Segment from its two .idx
// sidecars. sealedAt and the per-plane ship timestamps come from the DB
// row (0 when unknown, e.g. rehydration without a row). A plane whose
// sidecar is gone is treated as already-retired.
func loadSealedFromIdx(dir string, seq uint64, sealedAt, dataShippedAt, catShippedAt int64, logger *zap.Logger) (*Segment, error) {
	dataP, _, err := loadOrRetiredPlane(dir, seq, blockstore.PlaneData)
	if err != nil {
		return nil, err
	}
	catP, opRoots, err := loadOrRetiredPlane(dir, seq, blockstore.PlaneCatalog)
	if err != nil {
		if dataP.fdRO != nil {
			_ = dataP.fdRO.Close()
		}
		return nil, err
	}
	dataP.shippedAt = dataShippedAt
	catP.shippedAt = catShippedAt
	return &Segment{
		seq:      seq,
		dir:      dir,
		logger:   logger,
		state:    StateSealed,
		sealedAt: sealedAt,
		data:     *dataP,
		cat:      *catP,
		opRoots:  opRoots,
	}, nil
}

// rebuildOpenPlane reconstructs one plane of a torn/sidecar-less open
// segment by scanning its CAR (truncating any torn last frame) and
// reopening the fd at EOF.
func rebuildOpenPlane(dir string, seq uint64, kind blockstore.Plane, logger *zap.Logger) (*segPlane, error) {
	p := newSegPlane(dir, seq, kind)
	scan, err := cars.ScanFile(p.carPath)
	if err != nil && !errors.Is(err, cars.ErrTorn) {
		return nil, fmt.Errorf("logstore: scan recovered %s car %d: %w", kind, seq, err)
	}
	if errors.Is(err, cars.ErrTorn) {
		if terr := os.Truncate(p.carPath, scan.LastGoodEnd); terr != nil {
			return nil, fmt.Errorf("logstore: truncate torn %s car %d: %w", kind, seq, terr)
		}
		logger.Warn("logstore: truncated torn trailing frame",
			zap.Stringer("plane", kind),
			zap.Uint64("seq", seq),
			zap.Int64("truncated_at", scan.LastGoodEnd))
	}
	p.size = scan.LastGoodEnd
	for _, f := range scan.Frames {
		c := f.Block.Cid()
		p.index[c] = blockstore.BlockLoc{Offset: f.Offset, Length: f.Length}
		p.seen.Add(c)
	}
	fd, err := os.OpenFile(p.carPath, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("logstore: reopen %s car %d: %w", kind, seq, err)
	}
	if _, err := fd.Seek(p.size, io.SeekStart); err != nil {
		_ = fd.Close()
		return nil, fmt.Errorf("logstore: seek %s car %d: %w", kind, seq, err)
	}
	p.fdRW = fd
	return &p, nil
}

// rebuildOpenFromDisk reconstructs an in-memory open Segment from a
// segment that was open at crash time (both CARs scanned + truncated
// independently, ops replayed). The returned segment is in StateOpen
// with fds repositioned at EOF; the caller is expected to immediately
// seal() it — recovery never resumes appending to a recovered open
// segment.
func rebuildOpenFromDisk(dir string, seq uint64, logger *zap.Logger) (*Segment, error) {
	dataP, err := rebuildOpenPlane(dir, seq, blockstore.PlaneData, logger)
	if err != nil {
		return nil, err
	}
	catP, err := rebuildOpenPlane(dir, seq, blockstore.PlaneCatalog, logger)
	if err != nil {
		if dataP.fdRW != nil {
			_ = dataP.fdRW.Close()
		}
		return nil, err
	}

	opsPath := filepath.Join(dir, opsName(seq))
	ops, err := readAllOps(opsPath)
	if err != nil {
		return nil, fmt.Errorf("logstore: read ops %d: %w", seq, err)
	}
	opsFD, err := os.OpenFile(opsPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("logstore: reopen ops %d: %w", seq, err)
	}
	if _, err := opsFD.Seek(0, io.SeekEnd); err != nil {
		_ = opsFD.Close()
		return nil, fmt.Errorf("logstore: seek ops %d: %w", seq, err)
	}

	return &Segment{
		seq:     seq,
		dir:     dir,
		logger:  logger,
		state:   StateOpen,
		data:    *dataP,
		cat:     *catP,
		opRoots: ops,
		opsFD:   opsFD,
	}, nil
}

// === ops sidecar codec ===
//
// Each record is a 4-byte big-endian length prefix followed by a
// minimal CBOR-encoded payload: a 2-element array
// [bucket: text, root: cid bytes].

const opRecMaxSize = 1 << 20 // 1 MiB ceiling per record (defensive)

func encodeOpRecord(opr blockstore.OpRoot) ([]byte, error) {
	if !opr.Root.Defined() {
		return nil, errors.New("logstore: opRoot.Root must be defined")
	}
	if len(opr.Bucket) > 1<<16 {
		return nil, errors.New("logstore: bucket name too long")
	}
	bucketBytes := []byte(opr.Bucket)
	rootBytes := opr.Root.Bytes()

	// Manual CBOR: array(2) + text(bucket) + bytes(root).
	body := make([]byte, 0, 16+len(bucketBytes)+len(rootBytes))
	body = appendCborHead(body, 4 /*MajArray*/, 2)
	body = appendCborHead(body, 3 /*MajTextString*/, uint64(len(bucketBytes)))
	body = append(body, bucketBytes...)
	body = appendCborHead(body, 2 /*MajByteString*/, uint64(len(rootBytes)))
	body = append(body, rootBytes...)

	buf := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(body)))
	copy(buf[4:], body)
	return buf, nil
}

func readAllOps(path string) ([]blockstore.OpRoot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []blockstore.OpRoot
	for off := 0; off < len(data); {
		if len(data)-off < 4 {
			break // torn trailing prefix — drop
		}
		length := int(binary.BigEndian.Uint32(data[off : off+4]))
		if length <= 0 || length > opRecMaxSize || off+4+length > len(data) {
			break // torn trailing record — drop
		}
		body := data[off+4 : off+4+length]
		opr, err := decodeOpRecord(body)
		if err != nil {
			return nil, fmt.Errorf("logstore: ops record at %d: %w", off, err)
		}
		out = append(out, opr)
		off += 4 + length
	}
	return out, nil
}

func decodeOpRecord(body []byte) (blockstore.OpRoot, error) {
	r := newCborReader(body)
	maj, count, err := r.readHead()
	if err != nil {
		return blockstore.OpRoot{}, err
	}
	if maj != 4 || count != 2 {
		return blockstore.OpRoot{}, fmt.Errorf("expected array(2), got %d/%d", maj, count)
	}
	bm, blen, err := r.readHead()
	if err != nil {
		return blockstore.OpRoot{}, err
	}
	if bm != 3 {
		return blockstore.OpRoot{}, fmt.Errorf("expected text bucket, got maj %d", bm)
	}
	bucket, err := r.readBytes(int(blen))
	if err != nil {
		return blockstore.OpRoot{}, err
	}
	rm, rlen, err := r.readHead()
	if err != nil {
		return blockstore.OpRoot{}, err
	}
	if rm != 2 {
		return blockstore.OpRoot{}, fmt.Errorf("expected bytes root, got maj %d", rm)
	}
	rootBytes, err := r.readBytes(int(rlen))
	if err != nil {
		return blockstore.OpRoot{}, err
	}
	c, err := cid.Cast(rootBytes)
	if err != nil {
		return blockstore.OpRoot{}, err
	}
	return blockstore.OpRoot{Bucket: string(bucket), Root: c}, nil
}

// hashFile returns the sha256 of the file at path.
func hashFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func hexDecode(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("odd length")
	}
	out := make([]byte, len(s)/2)
	for i := 0; i < len(out); i++ {
		hi, ok1 := unhex(s[2*i])
		lo, ok2 := unhex(s[2*i+1])
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("bad hex char")
		}
		out[i] = hi<<4 | lo
	}
	return out, nil
}

func unhex(b byte) (byte, bool) {
	switch {
	case b >= '0' && b <= '9':
		return b - '0', true
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10, true
	case b >= 'A' && b <= 'F':
		return b - 'A' + 10, true
	}
	return 0, false
}

// === minimal CBOR head encoding/decoding ===

func appendCborHead(buf []byte, maj uint8, val uint64) []byte {
	switch {
	case val < 24:
		return append(buf, byte(maj<<5)|byte(val))
	case val < 1<<8:
		return append(buf, byte(maj<<5)|24, byte(val))
	case val < 1<<16:
		return append(buf, byte(maj<<5)|25, byte(val>>8), byte(val))
	case val < 1<<32:
		return append(buf, byte(maj<<5)|26,
			byte(val>>24), byte(val>>16), byte(val>>8), byte(val))
	default:
		return append(buf, byte(maj<<5)|27,
			byte(val>>56), byte(val>>48), byte(val>>40), byte(val>>32),
			byte(val>>24), byte(val>>16), byte(val>>8), byte(val))
	}
}

type cborReader struct {
	buf []byte
	pos int
}

func newCborReader(b []byte) *cborReader { return &cborReader{buf: b} }

func (r *cborReader) readHead() (uint8, uint64, error) {
	if r.pos >= len(r.buf) {
		return 0, 0, io.EOF
	}
	first := r.buf[r.pos]
	r.pos++
	maj := first >> 5
	low := first & 0x1f
	switch {
	case low < 24:
		return maj, uint64(low), nil
	case low == 24:
		if r.pos+1 > len(r.buf) {
			return 0, 0, io.ErrUnexpectedEOF
		}
		v := uint64(r.buf[r.pos])
		r.pos++
		return maj, v, nil
	case low == 25:
		if r.pos+2 > len(r.buf) {
			return 0, 0, io.ErrUnexpectedEOF
		}
		v := uint64(r.buf[r.pos])<<8 | uint64(r.buf[r.pos+1])
		r.pos += 2
		return maj, v, nil
	case low == 26:
		if r.pos+4 > len(r.buf) {
			return 0, 0, io.ErrUnexpectedEOF
		}
		v := uint64(r.buf[r.pos])<<24 | uint64(r.buf[r.pos+1])<<16 |
			uint64(r.buf[r.pos+2])<<8 | uint64(r.buf[r.pos+3])
		r.pos += 4
		return maj, v, nil
	case low == 27:
		if r.pos+8 > len(r.buf) {
			return 0, 0, io.ErrUnexpectedEOF
		}
		v := uint64(r.buf[r.pos])<<56 | uint64(r.buf[r.pos+1])<<48 |
			uint64(r.buf[r.pos+2])<<40 | uint64(r.buf[r.pos+3])<<32 |
			uint64(r.buf[r.pos+4])<<24 | uint64(r.buf[r.pos+5])<<16 |
			uint64(r.buf[r.pos+6])<<8 | uint64(r.buf[r.pos+7])
		r.pos += 8
		return maj, v, nil
	default:
		return 0, 0, fmt.Errorf("invalid cbor head 0x%x", first)
	}
}

func (r *cborReader) readBytes(n int) ([]byte, error) {
	if r.pos+n > len(r.buf) {
		return nil, io.ErrUnexpectedEOF
	}
	b := r.buf[r.pos : r.pos+n]
	r.pos += n
	return b, nil
}
