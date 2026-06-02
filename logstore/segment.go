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

// placeholderRoot is the placeholder CAR header root. Each segment
// is multi-rooted by intent; the per-op roots live in the .ops
// sidecar (and in-memory OpRoots), not the CAR header.
var placeholderRoot = cid.NewCidV1(cid.Raw, []byte{0x00, 0x00})

// Segment is one log file. Open segments accept appends; sealed
// segments are read-only.
//
// Concurrency model: Append is serialized by Store.appMu. Reads
// (Lookup + ReadAt against fdRO) and seal/finalize use Segment-level
// locks so they don't block appenders unnecessarily.
type Segment struct {
	seq    uint64
	dir    string
	logger *zap.Logger

	// stateMu guards state, sealedAt, sha256, opRoots, sizeBytes,
	// index, seen, fdRW, and fdRO. RLock for reads (lookups, opRoots
	// access); Lock for mutating any of the above.
	stateMu sync.RWMutex

	state    State
	sealedAt int64
	sha256   []byte

	sizeBytes int64
	// index maps each block's CID to its on-disk byte position
	// inside the segment's CAR. Updated on append (after a successful
	// fsync) and rebuilt on recovery from either the .idx sidecar or
	// a fresh CAR scan.
	index map[cid.Cid]blockstore.BlockLoc
	// seen is the dedup gate consulted by append. CIDs that have
	// already landed in this segment are skipped before
	// cars.WriteBlocksAt is called, so duplicate bytes are never
	// written to disk and never shipped to Forge. Always kept in
	// sync with index's key set.
	seen    *cid.Set
	opRoots []blockstore.OpRoot

	// fdRW is the append/read file descriptor for an open segment.
	// Closed at seal.
	fdRW *os.File
	// opsFD is the append-only ops sidecar (open segment only).
	// Closed at seal.
	opsFD *os.File
	// fdRO is the read-only descriptor used to serve Get after seal
	// (and before, when the open fdRW exists). For open segments we
	// use fdRW for reads via ReadAt; fdRO is opened lazily at seal
	// time so reads after seal don't need to reopen on every Get.
	fdRO *os.File
}

// Seq returns the segment's identifier.
func (s *Segment) Seq() uint64 { return s.seq }

// State reports the current lifecycle state.
func (s *Segment) State() State {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.state
}

// Size reports the current on-disk byte size of the CAR file.
func (s *Segment) Size() int64 {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.sizeBytes
}

// SHA256 returns the seal-time sha256 of the CAR file. Empty for
// open segments.
func (s *Segment) SHA256() []byte {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	out := make([]byte, len(s.sha256))
	copy(out, s.sha256)
	return out
}

// SealedAt returns the seal-time unix-seconds timestamp. Zero for
// open segments.
func (s *Segment) SealedAt() int64 {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.sealedAt
}

// OpRoots returns a copy of the per-batch (bucket, root) records.
// Safe to call from any goroutine.
func (s *Segment) OpRoots() []blockstore.OpRoot {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	out := make([]blockstore.OpRoot, len(s.opRoots))
	copy(out, s.opRoots)
	return out
}

// BlockPositions returns a copy of the cid → on-disk-position
// table for the segment's CAR. Populated at append time and
// rebuilt on recovery from either the .idx sidecar or a fresh CAR
// scan. Used by the flush path to build a ShardedDagIndexView
// without rescanning the file. Safe to call from any goroutine.
func (s *Segment) BlockPositions() map[cid.Cid]blockstore.BlockLoc {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	out := make(map[cid.Cid]blockstore.BlockLoc, len(s.index))
	for c, loc := range s.index {
		out[c] = loc
	}
	return out
}

// CARPath returns the absolute path to the segment's CAR file.
func (s *Segment) CARPath() string { return filepath.Join(s.dir, carName(s.seq)) }

// OpsPath returns the absolute path to the segment's ops sidecar.
func (s *Segment) OpsPath() string { return filepath.Join(s.dir, opsName(s.seq)) }

// IdxPath returns the absolute path to the segment's idx sidecar.
func (s *Segment) IdxPath() string { return filepath.Join(s.dir, idxName(s.seq)) }

func carName(seq uint64) string { return fmt.Sprintf("seg-%020d.car", seq) }
func opsName(seq uint64) string { return fmt.Sprintf("seg-%020d.ops", seq) }
func idxName(seq uint64) string { return fmt.Sprintf("seg-%020d.idx", seq) }

// createOpenSegment creates a brand-new segment in the open state:
// initializes the CAR file with a header, opens the ops sidecar,
// and records the row in Meta.
func createOpenSegment(ctx context.Context, dir string, seq uint64, meta Meta, logger *zap.Logger) (*Segment, error) {
	carPath := filepath.Join(dir, carName(seq))
	opsPath := filepath.Join(dir, opsName(seq))

	carFile, err := os.OpenFile(carPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("logstore: open car %d: %w", seq, err)
	}
	hdrLen, err := cars.WriteHeader(carFile, []cid.Cid{placeholderRoot})
	if err != nil {
		_ = carFile.Close()
		_ = os.Remove(carPath)
		return nil, fmt.Errorf("logstore: write header %d: %w", seq, err)
	}
	if err := carFile.Sync(); err != nil {
		_ = carFile.Close()
		_ = os.Remove(carPath)
		return nil, fmt.Errorf("logstore: sync header %d: %w", seq, err)
	}

	opsFile, err := os.OpenFile(opsPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		_ = carFile.Close()
		_ = os.Remove(carPath)
		return nil, fmt.Errorf("logstore: open ops %d: %w", seq, err)
	}

	if err := meta.InsertSegmentOpen(ctx, seq); err != nil {
		_ = carFile.Close()
		_ = opsFile.Close()
		_ = os.Remove(carPath)
		_ = os.Remove(opsPath)
		return nil, err
	}

	return &Segment{
		seq:       seq,
		dir:       dir,
		logger:    logger,
		state:     StateOpen,
		sizeBytes: hdrLen,
		index:     map[cid.Cid]blockstore.BlockLoc{},
		seen:      cid.NewSet(),
		fdRW:      carFile,
		opsFD:     opsFile,
	}, nil
}

// append writes the given blocks + opRoot to disk and updates the
// in-memory index. fsyncs both files before returning. Caller must
// hold Store.appMu.
//
// Block-level dedup: every block is checked against s.seen before
// writing. CIDs already present in this segment are skipped, so a
// duplicate body chunk or MST node landing across two PUTs in the
// same segment never hits the CAR file twice and never ships to
// Forge twice. The op-root record is appended unconditionally —
// even an all-duplicate batch still represents a real bucket-Root
// advance and must be replayed by the flusher.
func (s *Segment) append(blocks []block.Block, opRoot blockstore.OpRoot) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if s.state != StateOpen || s.fdRW == nil {
		return errors.New("logstore: segment not open for append")
	}

	// Filter out CIDs we've already written into this segment. We
	// don't mutate s.seen yet — only after the file write succeeds —
	// so a fsync error doesn't poison the dedup state.
	fresh := make([]block.Block, 0, len(blocks))
	for _, blk := range blocks {
		if s.seen.Has(blk.Cid()) {
			continue
		}
		fresh = append(fresh, blk)
	}

	var positions []cars.BlockPosition
	if len(fresh) > 0 {
		var err error
		positions, err = cars.WriteBlocksAt(s.fdRW, s.sizeBytes, fresh)
		if err != nil {
			return fmt.Errorf("logstore: append blocks seg %d: %w", s.seq, err)
		}
	}

	// Append the op-root record to the ops sidecar regardless of
	// whether any new bytes were written to the CAR.
	opsRec, err := encodeOpRecord(opRoot)
	if err != nil {
		return fmt.Errorf("logstore: encode oprec seg %d: %w", s.seq, err)
	}
	if _, err := s.opsFD.Write(opsRec); err != nil {
		return fmt.Errorf("logstore: write ops seg %d: %w", s.seq, err)
	}

	// fsync both files in parallel. The CAR fsync is a fast no-op
	// when len(fresh) == 0 (nothing written since the last sync) but
	// we issue it anyway to keep the durability contract uniform.
	var wg sync.WaitGroup
	var carErr, opsErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		carErr = s.fdRW.Sync()
	}()
	go func() {
		defer wg.Done()
		opsErr = s.opsFD.Sync()
	}()
	wg.Wait()
	if carErr != nil {
		return fmt.Errorf("logstore: fsync car seg %d: %w", s.seq, carErr)
	}
	if opsErr != nil {
		return fmt.Errorf("logstore: fsync ops seg %d: %w", s.seq, opsErr)
	}

	// Commit the dedup state and the position table together.
	for i, blk := range fresh {
		s.seen.Add(blk.Cid())
		s.index[blk.Cid()] = blockstore.BlockLoc{Offset: positions[i].Offset, Length: positions[i].Length}
	}
	if n := len(positions); n > 0 {
		end := int64(positions[n-1].Offset) + int64(positions[n-1].Length)
		if end > s.sizeBytes {
			s.sizeBytes = end
		}
	}
	s.opRoots = append(s.opRoots, opRoot)
	return nil
}

// seal closes the open fds, hashes the CAR, writes the .idx sidecar,
// and updates Meta. After this returns, the segment is in
// StateSealed and safe to be flushed.
func (s *Segment) seal(ctx context.Context, meta Meta) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if s.state != StateOpen {
		// Idempotent: already sealed.
		return nil
	}

	// Final fsync before close (defensive — append already fsyncs).
	if err := s.fdRW.Sync(); err != nil {
		return fmt.Errorf("logstore: pre-seal fsync car %d: %w", s.seq, err)
	}
	if err := s.opsFD.Sync(); err != nil {
		return fmt.Errorf("logstore: pre-seal fsync ops %d: %w", s.seq, err)
	}
	if err := s.fdRW.Close(); err != nil {
		return fmt.Errorf("logstore: close car %d: %w", s.seq, err)
	}
	s.fdRW = nil
	if err := s.opsFD.Close(); err != nil {
		return fmt.Errorf("logstore: close ops %d: %w", s.seq, err)
	}
	s.opsFD = nil

	// Compute CAR sha256 by streaming the file.
	sum, err := hashFile(s.CARPath())
	if err != nil {
		return fmt.Errorf("logstore: hash %d: %w", s.seq, err)
	}
	s.sha256 = sum
	s.sealedAt = time.Now().Unix()
	s.state = StateSealed

	// Write idx sidecar (atomic via tmp+rename).
	if err := s.writeIdxLocked(); err != nil {
		return fmt.Errorf("logstore: write idx %d: %w", s.seq, err)
	}

	// Persist sealed state in Postgres.
	if err := meta.MarkSegmentSealed(ctx, s.seq, s.sealedAt, s.sizeBytes, s.sha256, s.opRoots); err != nil {
		return fmt.Errorf("logstore: mark sealed %d: %w", s.seq, err)
	}

	// Open the read-only fd that will serve Get from now on.
	roFD, err := os.Open(s.CARPath())
	if err != nil {
		return fmt.Errorf("logstore: open ro car %d: %w", s.seq, err)
	}
	s.fdRO = roFD

	return nil
}

// retire closes any open fd and unlinks the segment's files. Safe to
// call after MarkFlushed; the caller must guarantee no other
// goroutine still holds a reference for reads.
func (s *Segment) retire() error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

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

	for _, name := range []string{s.CARPath(), s.OpsPath(), s.IdxPath()} {
		if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("logstore: unlink %s: %w", name, err)
		}
	}
	return nil
}

// get returns the block at the given CID, or blockstore.ErrNotFound. Safe for
// concurrent callers.
func (s *Segment) get(_ context.Context, c cid.Cid) (block.Block, error) {
	s.stateMu.RLock()
	loc, ok := s.index[c]
	fd := s.fdRO
	if fd == nil {
		fd = s.fdRW
	}
	s.stateMu.RUnlock()
	if !ok {
		return nil, blockstore.ErrNotFound
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

// writeIdxLocked persists the idx sidecar. Caller must hold stateMu
// in write mode and have already populated sha256/sealedAt.
func (s *Segment) writeIdxLocked() error {
	type idxBlock struct {
		CID    string `json:"cid"`
		Offset uint64 `json:"offset"`
		Length uint64 `json:"length"`
	}
	type idxOpRoot struct {
		Bucket string `json:"bucket"`
		Root   string `json:"root"`
	}
	type idxFile struct {
		Seq       uint64      `json:"seq"`
		SizeBytes int64       `json:"size_bytes"`
		SHA256    string      `json:"sha256_hex"`
		SealedAt  int64       `json:"sealed_at"`
		Blocks    []idxBlock  `json:"blocks"`
		OpRoots   []idxOpRoot `json:"op_roots"`
	}

	blocks := make([]idxBlock, 0, len(s.index))
	for c, loc := range s.index {
		blocks = append(blocks, idxBlock{
			CID:    c.String(),
			Offset: loc.Offset,
			Length: loc.Length,
		})
	}
	opRoots := make([]idxOpRoot, len(s.opRoots))
	for i, opr := range s.opRoots {
		opRoots[i] = idxOpRoot{Bucket: opr.Bucket, Root: opr.Root.String()}
	}

	body := idxFile{
		Seq:       s.seq,
		SizeBytes: s.sizeBytes,
		SHA256:    fmt.Sprintf("%x", s.sha256),
		SealedAt:  s.sealedAt,
		Blocks:    blocks,
		OpRoots:   opRoots,
	}
	data, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.IdxPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.IdxPath())
}

// loadSealedFromIdx hydrates a Segment in the StateSealed state from
// its on-disk .idx sidecar. Used at startup. Returns (nil, error) on
// any malformed sidecar; the caller can fall back to a CAR scan.
func loadSealedFromIdx(dir string, seq uint64, logger *zap.Logger) (*Segment, error) {
	idxPath := filepath.Join(dir, idxName(seq))
	data, err := os.ReadFile(idxPath)
	if err != nil {
		return nil, fmt.Errorf("logstore: read idx %d: %w", seq, err)
	}
	var raw struct {
		Seq       uint64 `json:"seq"`
		SizeBytes int64  `json:"size_bytes"`
		SHA256    string `json:"sha256_hex"`
		SealedAt  int64  `json:"sealed_at"`
		Blocks    []struct {
			CID    string `json:"cid"`
			Offset uint64 `json:"offset"`
			Length uint64 `json:"length"`
		} `json:"blocks"`
		OpRoots []struct {
			Bucket string `json:"bucket"`
			Root   string `json:"root"`
		} `json:"op_roots"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("logstore: parse idx %d: %w", seq, err)
	}
	if raw.Seq != seq {
		return nil, fmt.Errorf("logstore: idx seq %d does not match filename %d", raw.Seq, seq)
	}
	idx := make(map[cid.Cid]blockstore.BlockLoc, len(raw.Blocks))
	seen := cid.NewSet()
	for _, b := range raw.Blocks {
		c, err := cid.Decode(b.CID)
		if err != nil {
			return nil, fmt.Errorf("logstore: idx bad cid %q: %w", b.CID, err)
		}
		idx[c] = blockstore.BlockLoc{Offset: b.Offset, Length: b.Length}
		seen.Add(c)
	}
	ops := make([]blockstore.OpRoot, len(raw.OpRoots))
	for i, o := range raw.OpRoots {
		c, err := cid.Decode(o.Root)
		if err != nil {
			return nil, fmt.Errorf("logstore: idx bad root %q: %w", o.Root, err)
		}
		ops[i] = blockstore.OpRoot{Bucket: o.Bucket, Root: c}
	}
	sha, err := hexDecode(raw.SHA256)
	if err != nil {
		return nil, fmt.Errorf("logstore: idx bad sha %q: %w", raw.SHA256, err)
	}

	carFD, err := os.Open(filepath.Join(dir, carName(seq)))
	if err != nil {
		return nil, fmt.Errorf("logstore: open sealed car %d: %w", seq, err)
	}
	return &Segment{
		seq:       seq,
		dir:       dir,
		logger:    logger,
		state:     StateSealed,
		sealedAt:  raw.SealedAt,
		sha256:    sha,
		sizeBytes: raw.SizeBytes,
		index:     idx,
		seen:      seen,
		opRoots:   ops,
		fdRO:      carFD,
	}, nil
}

// loadFlushedFromIdx is loadSealedFromIdx but yields StateFlushed.
// Used to pick up retained segments at startup.
func loadFlushedFromIdx(dir string, seq uint64, flushedAt int64, logger *zap.Logger) (*Segment, error) {
	seg, err := loadSealedFromIdx(dir, seq, logger)
	if err != nil {
		return nil, err
	}
	seg.state = StateFlushed
	_ = flushedAt // kept for future use; not stored on Segment today.
	return seg, nil
}

// rebuildOpenFromDisk takes a torn or sidecar-less open segment on
// disk (the segment was open at crash time) and reconstructs an
// in-memory Segment ready to be sealed. It scans the CAR (truncating
// any torn last frame) and replays the .ops file.
//
// The returned segment is in StateOpen with its fds repositioned at
// EOF; the caller is expected to immediately call seal() to retire
// it cleanly. We do not resume appending to a recovered open
// segment — every restart starts a fresh segment for the next ops.
func rebuildOpenFromDisk(dir string, seq uint64, logger *zap.Logger) (*Segment, error) {
	carPath := filepath.Join(dir, carName(seq))
	scan, err := cars.ScanFile(carPath)
	if err != nil && !errors.Is(err, cars.ErrTorn) {
		return nil, fmt.Errorf("logstore: scan recovered car %d: %w", seq, err)
	}
	if errors.Is(err, cars.ErrTorn) {
		if terr := os.Truncate(carPath, scan.LastGoodEnd); terr != nil {
			return nil, fmt.Errorf("logstore: truncate torn car %d: %w", seq, terr)
		}
		logger.Warn("logstore: truncated torn trailing frame in segment",
			zap.Uint64("seq", seq),
			zap.Int64("truncated_at", scan.LastGoodEnd))
	}

	idx := make(map[cid.Cid]blockstore.BlockLoc, len(scan.Frames))
	seen := cid.NewSet()
	var size int64 = scan.LastGoodEnd
	for _, f := range scan.Frames {
		c := f.Block.Cid()
		idx[c] = blockstore.BlockLoc{Offset: f.Offset, Length: f.Length}
		seen.Add(c)
	}

	opsPath := filepath.Join(dir, opsName(seq))
	ops, err := readAllOps(opsPath)
	if err != nil {
		return nil, fmt.Errorf("logstore: read ops %d: %w", seq, err)
	}

	carFD, err := os.OpenFile(carPath, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("logstore: reopen car %d: %w", seq, err)
	}
	if _, err := carFD.Seek(size, io.SeekStart); err != nil {
		_ = carFD.Close()
		return nil, fmt.Errorf("logstore: seek car %d: %w", seq, err)
	}
	opsFD, err := os.OpenFile(opsPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		_ = carFD.Close()
		return nil, fmt.Errorf("logstore: reopen ops %d: %w", seq, err)
	}
	if _, err := opsFD.Seek(0, io.SeekEnd); err != nil {
		_ = carFD.Close()
		_ = opsFD.Close()
		return nil, fmt.Errorf("logstore: seek ops %d: %w", seq, err)
	}
	return &Segment{
		seq:       seq,
		dir:       dir,
		logger:    logger,
		state:     StateOpen,
		sizeBytes: size,
		index:     idx,
		seen:      seen,
		opRoots:   ops,
		fdRW:      carFD,
		opsFD:     opsFD,
	}, nil
}

// === ops sidecar codec ===
//
// Each record is a 4-byte big-endian length prefix followed by a
// minimal CBOR-encoded payload: a 2-element array
// [bucket: text, root: cid bytes]. We use array form rather than a
// map to keep the encoding compact and order-independent of map
// iteration.

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
