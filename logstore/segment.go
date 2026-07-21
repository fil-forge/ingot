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

// placeholderRoot is the placeholder CAR header root. The per-op MST
// roots live in the .ops sidecar (catalog plane) and the .idx, not the
// CAR header.
var placeholderRoot = cid.NewCidV1(cid.Raw, []byte{0x00, 0x00})

// Segment is one plane's log entry: a single CAR file plus a .idx
// sidecar. A catalog-plane segment also owns an append-only .ops sidecar
// recording each batch's (bucket, root); data-plane segments have no
// op-roots. Open segments accept appends; sealed segments are read-only
// and ship/retire independently.
//
// Concurrency: append is serialized by PlaneLog.appMu. stateMu guards
// every mutable field below; readers RLock for lookups, writers Lock for
// append/seal/retire/ship.
type Segment struct {
	seq    uint64
	plane  blockstore.Plane
	dir    string // the plane's subdirectory
	logger *zap.Logger

	stateMu sync.RWMutex

	state    State
	sealedAt int64

	carPath string
	idxPath string

	// fdRW is the append/read fd for an open segment (closed at seal).
	// fdRO is the read-only fd opened at seal time.
	fdRW *os.File
	fdRO *os.File

	// index maps each block's CID to its on-disk byte position in this
	// CAR; seen is the dedup gate kept in sync with index's key set.
	index map[cid.Cid]blockstore.BlockLoc
	seen  *cid.Set

	size int64  // current CAR byte size
	sha  []byte // seal-time sha256 of the CAR

	shippedAt int64 // unix seconds when shipped; 0 = not shipped
	retired   bool  // CAR + idx unlinked off local disk

	// opRoots is the ordered per-batch (bucket, root) record; non-nil
	// only for the catalog plane. opsFD is its append-only sidecar (open
	// segment only, closed at seal).
	opRoots []blockstore.OpRoot
	opsFD   *os.File
}

// hasOps reports whether this segment maintains an .ops sidecar (catalog
// plane only — op-roots are MST roots).
func (s *Segment) hasOps() bool { return s.plane == blockstore.PlaneCatalog }

// Seq returns the segment's identifier.
func (s *Segment) Seq() uint64 { return s.seq }

// Plane returns the segment's plane.
func (s *Segment) Plane() blockstore.Plane { return s.plane }

// State reports the current lifecycle state.
func (s *Segment) State() State {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.state
}

// Size reports the on-disk byte size of the CAR. Drives this plane's
// seal-on-size trigger.
func (s *Segment) Size() int64 {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.size
}

// SHA256 returns the seal-time sha256 of the CAR. Empty for open segments.
func (s *Segment) SHA256() []byte {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	out := make([]byte, len(s.sha))
	copy(out, s.sha)
	return out
}

// SealedAt returns the seal-time unix-seconds timestamp. Zero for open
// segments.
func (s *Segment) SealedAt() int64 {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.sealedAt
}

// ShippedAt returns the unix-seconds ship timestamp, or 0.
func (s *Segment) ShippedAt() int64 {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.shippedAt
}

// IsShipped reports whether the segment has shipped to Forge.
func (s *Segment) IsShipped() bool { return s.ShippedAt() != 0 }

// IsRetired reports whether the CAR has been unlinked off local disk.
func (s *Segment) IsRetired() bool {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.retired
}

// OpRoots returns a copy of the per-batch (bucket, root) records (catalog
// plane only; empty for data).
func (s *Segment) OpRoots() []blockstore.OpRoot {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	out := make([]blockstore.OpRoot, len(s.opRoots))
	copy(out, s.opRoots)
	return out
}

// Positions returns a copy of the cid → on-disk-position table. Used by
// the flush path to build a sharded-dag-index without rescanning the file.
func (s *Segment) Positions() map[cid.Cid]blockstore.BlockLoc {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	out := make(map[cid.Cid]blockstore.BlockLoc, len(s.index))
	for c, loc := range s.index {
		out[c] = loc
	}
	return out
}

// CARPath returns the absolute path to the CAR file.
func (s *Segment) CARPath() string { return s.carPath }

// OpsPath returns the absolute path to the .ops sidecar (catalog plane).
func (s *Segment) OpsPath() string { return s.opsPath() }

func (s *Segment) opsPath() string { return filepath.Join(s.dir, opsName(s.seq)) }

func carName(seq uint64) string { return fmt.Sprintf("seg-%020d.car", seq) }
func idxName(seq uint64) string { return fmt.Sprintf("seg-%020d.idx", seq) }
func opsName(seq uint64) string { return fmt.Sprintf("seg-%020d.ops", seq) }

// newSegment builds an in-memory Segment shell (no files opened).
func newSegment(dir string, seq uint64, plane blockstore.Plane, logger *zap.Logger) *Segment {
	return &Segment{
		seq:     seq,
		plane:   plane,
		dir:     dir,
		logger:  logger,
		carPath: filepath.Join(dir, carName(seq)),
		idxPath: filepath.Join(dir, idxName(seq)),
		index:   map[cid.Cid]blockstore.BlockLoc{},
		seen:    cid.NewSet(),
	}
}

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

// createOpenSegment creates a brand-new single-plane segment in the open
// state: initializes the CAR file with a header, opens the .ops sidecar
// (catalog plane only), and records the row (stamped with its bucket) in
// Meta.
func createOpenSegment(ctx context.Context, dir string, seq uint64, plane blockstore.Plane, bucket string, meta Meta, logger *zap.Logger) (*Segment, error) {
	s := newSegment(dir, seq, plane, logger)
	s.state = StateOpen

	f, sz, err := createPlaneCAR(s.carPath)
	if err != nil {
		return nil, fmt.Errorf("logstore: open %s car %d: %w", plane, seq, err)
	}
	s.fdRW = f
	s.size = sz

	if s.hasOps() {
		opsFile, oerr := os.OpenFile(s.opsPath(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
		if oerr != nil {
			_ = f.Close()
			_ = os.Remove(s.carPath)
			return nil, fmt.Errorf("logstore: open ops %d: %w", seq, oerr)
		}
		s.opsFD = opsFile
	}

	if err := meta.InsertSegmentOpen(ctx, plane, seq, bucket); err != nil {
		_ = f.Close()
		if s.opsFD != nil {
			_ = s.opsFD.Close()
		}
		_ = os.Remove(s.carPath)
		if s.hasOps() {
			_ = os.Remove(s.opsPath())
		}
		return nil, err
	}

	return s, nil
}

// writeFresh writes the not-yet-seen blocks to the CAR at the current
// size and returns the fresh blocks + their on-disk positions. It does
// NOT mutate seen/index/size — the caller commits those only after a
// successful fsync. Caller holds the segment write lock.
func (s *Segment) writeFresh(blocks []block.Block) ([]block.Block, []cars.BlockPosition, error) {
	fresh := make([]block.Block, 0, len(blocks))
	for _, blk := range blocks {
		if s.seen.Has(blk.Cid()) {
			continue
		}
		fresh = append(fresh, blk)
	}
	if len(fresh) == 0 {
		return nil, nil, nil
	}
	positions, err := cars.WriteBlocksAt(s.fdRW, s.size, fresh)
	if err != nil {
		return nil, nil, err
	}
	return fresh, positions, nil
}

// commitFresh updates the dedup set, position table, and running size
// after a successful fsync.
func (s *Segment) commitFresh(fresh []block.Block, positions []cars.BlockPosition) {
	for i, blk := range fresh {
		s.seen.Add(blk.Cid())
		s.index[blk.Cid()] = blockstore.BlockLoc{Offset: positions[i].Offset, Length: positions[i].Length}
	}
	if n := len(positions); n > 0 {
		end := int64(positions[n-1].Offset) + int64(positions[n-1].Length)
		if end > s.size {
			s.size = end
		}
	}
}

// append writes one batch's blocks to the CAR, records any op-roots
// (catalog plane), fsyncs the CAR (+ ops), then commits the in-memory
// index. Caller must hold PlaneLog.appMu. opRoots is non-empty only for
// the catalog plane; an empty batch with no op-roots is still valid.
func (s *Segment) append(blocks []block.Block, opRoots []blockstore.OpRoot) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if s.state != StateOpen || s.fdRW == nil {
		return errors.New("logstore: segment not open for append")
	}

	fresh, positions, err := s.writeFresh(blocks)
	if err != nil {
		return fmt.Errorf("logstore: append %s seg %d: %w", s.plane, s.seq, err)
	}

	if s.hasOps() && len(opRoots) > 0 {
		var rec []byte
		for _, opr := range opRoots {
			enc, eerr := encodeOpRecord(opr)
			if eerr != nil {
				return fmt.Errorf("logstore: encode oprec seg %d: %w", s.seq, eerr)
			}
			rec = append(rec, enc...)
		}
		if _, werr := s.opsFD.Write(rec); werr != nil {
			return fmt.Errorf("logstore: write ops seg %d: %w", s.seq, werr)
		}
	}

	syncFiles := []*os.File{s.fdRW}
	if s.hasOps() {
		syncFiles = append(syncFiles, s.opsFD)
	}
	if err := syncAll(syncFiles...); err != nil {
		return fmt.Errorf("logstore: fsync seg %d: %w", s.seq, err)
	}

	s.commitFresh(fresh, positions)
	if s.hasOps() {
		s.opRoots = append(s.opRoots, opRoots...)
	}
	return nil
}

// seal closes the open CAR fd (+ ops fd), hashes the CAR, writes the .idx
// sidecar, and records the sealed state in Meta. After this returns the
// segment is StateSealed and ready to ship.
func (s *Segment) seal(ctx context.Context, meta Meta) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if s.state != StateOpen {
		// Idempotent: already sealed.
		return nil
	}

	syncFiles := []*os.File{s.fdRW}
	if s.hasOps() {
		syncFiles = append(syncFiles, s.opsFD)
	}
	if err := syncAll(syncFiles...); err != nil {
		return fmt.Errorf("logstore: pre-seal fsync %d: %w", s.seq, err)
	}
	if err := s.fdRW.Close(); err != nil {
		return fmt.Errorf("logstore: close %s car %d: %w", s.plane, s.seq, err)
	}
	s.fdRW = nil
	if s.opsFD != nil {
		if err := s.opsFD.Close(); err != nil {
			return fmt.Errorf("logstore: close ops %d: %w", s.seq, err)
		}
		s.opsFD = nil
	}

	sum, err := hashFile(s.carPath)
	if err != nil {
		return fmt.Errorf("logstore: hash %s %d: %w", s.plane, s.seq, err)
	}
	s.sha = sum
	s.sealedAt = time.Now().Unix()
	s.state = StateSealed

	if err := s.writeIdx(); err != nil {
		return fmt.Errorf("logstore: write idx %d: %w", s.seq, err)
	}

	if err := meta.MarkSegmentSealed(ctx, s.plane, s.seq, s.sealedAt, s.size, s.sha, s.opRoots); err != nil {
		return fmt.Errorf("logstore: mark sealed %d: %w", s.seq, err)
	}

	ro, err := os.Open(s.carPath)
	if err != nil {
		return fmt.Errorf("logstore: open ro %s car %d: %w", s.plane, s.seq, err)
	}
	s.fdRO = ro
	return nil
}

// markShipped stamps the ship timestamp. Called by the flusher after the
// CAR has shipped (or was trivially empty).
func (s *Segment) markShipped(shippedAt int64) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.shippedAt = shippedAt
}

// retire closes the segment's fds and unlinks its CAR + idx (+ ops for
// the catalog plane). Safe to call after the segment has shipped (or, for
// a never-ship plane, never called).
func (s *Segment) retire() error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.retired {
		return nil
	}
	if s.fdRO != nil {
		_ = s.fdRO.Close()
		s.fdRO = nil
	}
	if s.fdRW != nil {
		_ = s.fdRW.Close()
		s.fdRW = nil
	}
	if s.opsFD != nil {
		_ = s.opsFD.Close()
		s.opsFD = nil
	}
	for _, name := range s.files() {
		if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("logstore: unlink %s: %w", name, err)
		}
	}
	s.retired = true
	// Drop the in-memory index/dedup set: the blocks are no longer local,
	// so reads must fall through to the network tier.
	s.index = nil
	s.seen = nil
	return nil
}

// retireOpen closes an open (never-sealed) segment's fds and unlinks its
// files. Used to clean up a segment that lost the open-segment creation
// race.
func (s *Segment) retireOpen() error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.fdRW != nil {
		_ = s.fdRW.Close()
		s.fdRW = nil
	}
	if s.fdRO != nil {
		_ = s.fdRO.Close()
		s.fdRO = nil
	}
	if s.opsFD != nil {
		_ = s.opsFD.Close()
		s.opsFD = nil
	}
	for _, name := range s.files() {
		if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("logstore: unlink %s: %w", name, err)
		}
	}
	return nil
}

// files returns every on-disk path this segment owns.
func (s *Segment) files() []string {
	names := []string{s.carPath, s.idxPath}
	if s.hasOps() {
		names = append(names, s.opsPath())
	}
	return names
}

// get returns the block at the given CID, or blockstore.ErrNotFound
// (including when the segment has been retired off local disk, so the
// layered reader falls through to the network tier). The read lock is
// held through ReadAt so a concurrent retire/seal cannot close the fd
// mid-read.
func (s *Segment) get(_ context.Context, c cid.Cid) (block.Block, error) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	if s.retired {
		return nil, blockstore.ErrNotFound
	}
	loc, ok := s.index[c]
	if !ok {
		return nil, blockstore.ErrNotFound
	}
	fd := s.fdRO
	if fd == nil {
		fd = s.fdRW
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

// === idx sidecar (one per segment) ===

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

// writeIdx persists the segment's .idx sidecar. Caller holds the segment
// write lock and has populated sha + sealedAt.
func (s *Segment) writeIdx() error {
	blocks := make([]idxBlockJSON, 0, len(s.index))
	for c, loc := range s.index {
		blocks = append(blocks, idxBlockJSON{CID: c.String(), Offset: loc.Offset, Length: loc.Length})
	}
	ops := make([]idxOpRootJSON, len(s.opRoots))
	for i, opr := range s.opRoots {
		ops[i] = idxOpRootJSON{Bucket: opr.Bucket, Root: opr.Root.String()}
	}
	body := idxFileJSON{
		Seq:      s.seq,
		Plane:    s.plane.String(),
		SizeByte: s.size,
		SHA256:   fmt.Sprintf("%x", s.sha),
		SealedAt: s.sealedAt,
		Blocks:   blocks,
		OpRoots:  ops,
	}
	data, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.idxPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.idxPath)
}

// loadSealedFromIdx hydrates a sealed Segment from its .idx sidecar and
// opens its read fd. shippedAt comes from the DB row (the idx is written
// at seal, before ship); everything else is read from the sidecar.
func loadSealedFromIdx(dir string, seq uint64, plane blockstore.Plane, shippedAt int64, logger *zap.Logger) (*Segment, error) {
	s := newSegment(dir, seq, plane, logger)
	data, err := os.ReadFile(s.idxPath)
	if err != nil {
		return nil, fmt.Errorf("logstore: read %s idx %d: %w", plane, seq, err)
	}
	var raw idxFileJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("logstore: parse %s idx %d: %w", plane, seq, err)
	}
	if raw.Seq != seq {
		return nil, fmt.Errorf("logstore: %s idx seq %d != filename %d", plane, raw.Seq, seq)
	}
	for _, b := range raw.Blocks {
		c, err := cid.Decode(b.CID)
		if err != nil {
			return nil, fmt.Errorf("logstore: %s idx bad cid %q: %w", plane, b.CID, err)
		}
		s.index[c] = blockstore.BlockLoc{Offset: b.Offset, Length: b.Length}
		s.seen.Add(c)
	}
	sha, err := hexDecode(raw.SHA256)
	if err != nil {
		return nil, fmt.Errorf("logstore: %s idx bad sha %q: %w", plane, raw.SHA256, err)
	}
	s.sha = sha
	s.size = raw.SizeByte
	s.sealedAt = raw.SealedAt
	s.state = StateSealed
	s.shippedAt = shippedAt

	for _, o := range raw.OpRoots {
		c, err := cid.Decode(o.Root)
		if err != nil {
			return nil, fmt.Errorf("logstore: %s idx bad root %q: %w", plane, o.Root, err)
		}
		s.opRoots = append(s.opRoots, blockstore.OpRoot{Bucket: o.Bucket, Root: c})
	}

	ro, err := os.Open(s.carPath)
	if err != nil {
		return nil, fmt.Errorf("logstore: open sealed %s car %d: %w", plane, seq, err)
	}
	s.fdRO = ro
	return s, nil
}

// rebuildOpenFromDisk reconstructs an in-memory open Segment from a
// segment that was open at crash time (CAR scanned + truncated at any
// torn trailing frame; .ops replayed for the catalog plane). The returned
// segment is StateOpen with its fd at EOF; the caller is expected to
// immediately seal() it — recovery never resumes appending.
func rebuildOpenFromDisk(dir string, seq uint64, plane blockstore.Plane, logger *zap.Logger) (*Segment, error) {
	s := newSegment(dir, seq, plane, logger)
	scan, err := cars.ScanFile(s.carPath)
	if err != nil && !errors.Is(err, cars.ErrTorn) {
		return nil, fmt.Errorf("logstore: scan recovered %s car %d: %w", plane, seq, err)
	}
	if errors.Is(err, cars.ErrTorn) {
		if terr := os.Truncate(s.carPath, scan.LastGoodEnd); terr != nil {
			return nil, fmt.Errorf("logstore: truncate torn %s car %d: %w", plane, seq, terr)
		}
		logger.Warn("logstore: truncated torn trailing frame",
			zap.Stringer("plane", plane),
			zap.Uint64("seq", seq),
			zap.Int64("truncated_at", scan.LastGoodEnd))
	}
	s.size = scan.LastGoodEnd
	for _, f := range scan.Frames {
		c := f.Block.Cid()
		s.index[c] = blockstore.BlockLoc{Offset: f.Offset, Length: f.Length}
		s.seen.Add(c)
	}
	fd, err := os.OpenFile(s.carPath, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("logstore: reopen %s car %d: %w", plane, seq, err)
	}
	if _, err := fd.Seek(s.size, io.SeekStart); err != nil {
		_ = fd.Close()
		return nil, fmt.Errorf("logstore: seek %s car %d: %w", plane, seq, err)
	}
	s.fdRW = fd
	s.state = StateOpen

	if s.hasOps() {
		ops, oerr := readAllOps(s.opsPath())
		if oerr != nil {
			_ = fd.Close()
			return nil, fmt.Errorf("logstore: read ops %d: %w", seq, oerr)
		}
		s.opRoots = ops
		opsFD, oerr := os.OpenFile(s.opsPath(), os.O_RDWR|os.O_CREATE, 0o644)
		if oerr != nil {
			_ = fd.Close()
			return nil, fmt.Errorf("logstore: reopen ops %d: %w", seq, oerr)
		}
		if _, serr := opsFD.Seek(0, io.SeekEnd); serr != nil {
			_ = fd.Close()
			_ = opsFD.Close()
			return nil, fmt.Errorf("logstore: seek ops %d: %w", seq, serr)
		}
		s.opsFD = opsFD
	}

	return s, nil
}

// === ops sidecar codec ===
//
// Each record is a 4-byte big-endian length prefix followed by a minimal
// CBOR-encoded payload: a 2-element array [bucket: text, root: cid bytes].

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
