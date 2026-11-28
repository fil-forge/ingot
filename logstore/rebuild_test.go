package logstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fil-forge/libforge/testutil"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
)

// shipRecorder is a FlushFunc source that records the seqs it shipped. With
// refuse set every flush fails, so nothing ships.
type shipRecorder struct {
	mu     sync.Mutex
	seqs   map[uint64]bool
	refuse bool
}

func (r *shipRecorder) forBucket(string) logstore.FlushFunc {
	return func(_ context.Context, seg *logstore.Segment) (multihash.Multihash, error) {
		if r.refuse {
			return nil, errors.New("flush refused")
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.seqs == nil {
			r.seqs = map[uint64]bool{}
		}
		r.seqs[seg.Seq()] = true
		return multihash.Multihash("rebuild-test-index"), nil
	}
}

func (r *shipRecorder) shipped() map[uint64]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[uint64]bool{}
	for s := range r.seqs {
		out[s] = true
	}
	return out
}

func openRebuildManager(t *testing.T, dir string, meta logstore.Meta, ship *shipRecorder) (*logstore.Manager, error) {
	t.Helper()
	return openManagerShipping(t, dir, meta, ship, true)
}

// openManagerShipping opens a manager as openRebuildManager does, whose seals
// ship only when shipping is set.
func openManagerShipping(t *testing.T, dir string, meta logstore.Meta, ship *shipRecorder, shipping bool) (*logstore.Manager, error) {
	t.Helper()
	ships := &queueShips{}
	m, err := logstore.OpenManager(context.Background(), logstore.ManagerConfig{
		Dir:  dir,
		Meta: meta,
		// A small SealBytes makes the rebuild split across segments.
		Catalog:  logstore.PlaneConfig{SealBytes: 8 << 10, SealAge: time.Hour, Ship: true, Retain: 100},
		FlushFor: ship.forBucket,
		Logger:   zaptest.NewLogger(t),
	})
	if err == nil {
		t.Cleanup(func() { _ = m.Close(context.Background()) })
		if shipping {
			ships.start(t, m)
		}
	}
	return m, err
}

// queueShips stands in for queue-bucket.ship: each seal it hears of ships the
// bucket's log, retried until it succeeds or the test ends.
type queueShips struct {
	m   *logstore.Manager
	ctx context.Context
	wg  sync.WaitGroup
}

// start ships the buckets reported while the manager opened, and every one
// reported from now on. The ships stop before the manager closes.
func (q *queueShips) start(t *testing.T, m *logstore.Manager) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		q.wg.Wait()
	})
	q.m, q.ctx = m, ctx
	m.OnSealed(q.ship)
}

// ship runs one bucket's ship until it succeeds. q.mu is held.
func (q *queueShips) ship(bucket string) {
	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		for q.m.ShipBucket(q.ctx, bucket) != nil && q.ctx.Err() == nil {
			time.Sleep(10 * time.Millisecond)
		}
	}()
}

// pgMeta is the segment metadata a rebuild runs against: the Postgres
// registry on a fresh database, which also serves the journal reads the
// tests check.
type pgMeta struct {
	*registry.Postgres
	pool *pgxpool.Pool
}

func newPGMeta(t *testing.T) *pgMeta {
	t.Helper()
	pool := pgtest.New(t)
	return &pgMeta{Postgres: registry.NewPostgres(pool), pool: pool}
}

// commitJournaled records what a commit of blocks under segment leaves: their
// journal rows, and the bucket's root advanced to the last of them.
func (m *pgMeta) commitJournaled(t *testing.T, bucket string, segment uint64, blocks []block.Block) {
	t.Helper()
	ctx := context.Background()
	tx, err := m.pool.Begin(ctx)
	require.NoError(t, err, "begin commit")
	defer func() { _ = tx.Rollback(ctx) }()
	for _, b := range blocks {
		_, err := tx.Exec(ctx,
			`INSERT INTO ingot.catalog_journal (seq, bucket, cid, data) VALUES ($1, $2, $3, $4)`,
			int64(segment), bucket, b.Cid().Bytes(), b.RawData())
		require.NoError(t, err, "journal block")
	}
	_, err = tx.Exec(ctx, `UPDATE ingot.buckets SET root_cid = $1 WHERE name = $2`,
		blocks[len(blocks)-1].Cid().Bytes(), bucket)
	require.NoError(t, err, "advance root")
	require.NoError(t, tx.Commit(ctx), "commit")
}

// JournaledBlocks returns the journal's blocks under segment, by CID.
func (m *pgMeta) JournaledBlocks(t *testing.T, segment uint64) map[string][]byte {
	t.Helper()
	rows, err := m.pool.Query(context.Background(), `SELECT cid, data FROM ingot.catalog_journal WHERE seq = $1`, int64(segment))
	require.NoError(t, err, "read journal")
	defer rows.Close()
	out := map[string][]byte{}
	for rows.Next() {
		var c, data []byte
		require.NoError(t, rows.Scan(&c, &data), "read journal")
		out[string(c)] = data
	}
	require.NoError(t, rows.Err(), "read journal")
	return out
}

// journalSegment opens segment rows for bucket in mem and journals n blocks
// under it through a root commit, as a write does, without writing any
// segment file: the state a lost data volume leaves behind.
func journalSegment(t *testing.T, mem *pgMeta, bucket string, n int) (uint64, []block.Block) {
	t.Helper()
	return journalSegmentPadded(t, mem, bucket, n, 64)
}

// journalSegmentPadded is journalSegment with pad bytes of filler in each
// block.
func journalSegmentPadded(t *testing.T, mem *pgMeta, bucket string, n, pad int) (uint64, []block.Block) {
	t.Helper()
	ctx := context.Background()
	seq, err := mem.NextSegmentSeq(ctx)
	require.NoError(t, err, "NextSegmentSeq")
	require.NoError(t, mem.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq, bucket), "InsertSegmentOpen")
	blocks := make([]block.Block, n)
	for i := range blocks {
		blocks[i] = block.NewBlock([]byte(fmt.Sprintf("%s/seg-%d/block-%04d/%s", bucket, seq, i, strings.Repeat("x", pad))))
	}
	mem.commitJournaled(t, bucket, seq, blocks)
	return seq, blocks
}

func requireReadable(t *testing.T, m *logstore.Manager, blocks []block.Block) {
	t.Helper()
	for _, b := range blocks {
		got, err := m.Get(context.Background(), b.Cid())
		require.NoError(t, err, "Get %s after the rebuild", b.Cid())
		require.Equal(t, b.RawData(), got.RawData(), "Get %s returned other bytes", b.Cid())
	}
}

// onDiskSeqs returns the seqs of the segment CARs in bucket's catalog
// directory.
func onDiskSeqs(t *testing.T, dir, bucket string) map[uint64]bool {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, bucket, blockstore.PlaneCatalog.String()))
	require.NoError(t, err, "ReadDir")
	out := map[uint64]bool{}
	for _, e := range entries {
		stem, ok := strings.CutSuffix(strings.TrimPrefix(e.Name(), "seg-"), ".car")
		if !ok {
			continue
		}
		seq, err := strconv.ParseUint(stem, 10, 64)
		require.NoError(t, err, "parse %s", e.Name())
		out[seq] = true
	}
	return out
}

// TestRebuild_WipedVolume: with the data volume gone, the blocks the journal
// holds for the bucket's open and sealed segments are rebuilt into new
// segments, readable at once, with the journal rows re-filed under them and
// shipped from them. The lost open segment's row goes; the lost sealed one's
// is retired, as recovery treats any fileless row.
func TestRebuild_WipedVolume(t *testing.T) {
	ctx := context.Background()
	mem := newPGMeta(t)
	require.NoError(t, mem.Create(ctx, "bk", testutil.RandomDID(t), registry.CreateState{Tenant: testutil.RandomDID(t)}), "Create")
	sealedSeq, sealedBlocks := journalSegment(t, mem, "bk", 10)
	require.NoError(t, mem.MarkSegmentSealed(ctx, blockstore.PlaneCatalog, sealedSeq, time.Now().Unix(), 1, make([]byte, 32), nil), "MarkSegmentSealed")
	// More than two journal pages, so the rebuild pages and splits.
	openSeq, openBlocks := journalSegment(t, mem, "bk", 600)

	dir := t.TempDir()
	ship := &shipRecorder{}
	m, err := openRebuildManager(t, dir, mem, ship)
	require.NoError(t, err, "OpenManager")
	requireReadable(t, m, append(sealedBlocks, openBlocks...))

	for _, lost := range []uint64{sealedSeq, openSeq} {
		rows := mem.JournaledBlocks(t, lost)
		require.Empty(t, rows, "%d journal rows still under lost segment %d", len(rows), lost)
	}
	live, err := mem.ListSegments(ctx, blockstore.PlaneCatalog, "bk")
	require.NoError(t, err, "ListSegments")
	for _, r := range live {
		require.NotEqual(t, sealedSeq, r.Seq, "lost segment %d still has a live row (%v)", r.Seq, r.State)
		require.NotEqual(t, openSeq, r.Seq, "lost segment %d still has a live row (%v)", r.Seq, r.State)
	}
	retired, err := mem.ListRetiredSegments(ctx, blockstore.PlaneCatalog, "bk")
	require.NoError(t, err, "ListRetiredSegments")
	require.Len(t, retired, 1, "retired rows = %+v, want the lost sealed segment %d", retired, sealedSeq)
	require.Equal(t, sealedSeq, retired[0].Seq, "retired rows = %+v, want the lost sealed segment %d", retired, sealedSeq)

	rebuilt := onDiskSeqs(t, dir, "bk")
	require.GreaterOrEqual(t, len(rebuilt), 2, "rebuilt into %d segments, want the small SealBytes to split it", len(rebuilt))
	rows, err := mem.ListSegments(ctx, blockstore.PlaneCatalog, "bk")
	require.NoError(t, err, "ListSegments")
	for _, r := range rows {
		// The blocks are small, so no segment needs to pass SealBytes: one
		// seals once the next block would carry it past.
		require.LessOrEqual(t, r.Size, int64(8<<10), "rebuilt segment %d is %d bytes, past SealBytes (8 KiB)", r.Seq, r.Size)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		stats, err := mem.CatalogJournalStats(ctx)
		require.NoError(t, err, "CatalogJournalStats")
		shipped := ship.shipped()
		all := true
		for seq := range rebuilt {
			all = all && shipped[seq]
		}
		if all && stats.Blocks == 0 {
			break
		}
		require.False(t, time.Now().After(deadline), "rebuilt segments %v, shipped %v, %d journal rows left", rebuilt, shipped, stats.Blocks)
		time.Sleep(20 * time.Millisecond)
	}
}

// TestRebuild_SegmentsStayUnderSealBytes: a segment already holding blocks
// seals rather than take one past SealBytes, here the first block of the
// next lost segment.
func TestRebuild_SegmentsStayUnderSealBytes(t *testing.T) {
	ctx := context.Background()
	mem := newPGMeta(t)
	require.NoError(t, mem.Create(ctx, "bk", testutil.RandomDID(t), registry.CreateState{Tenant: testutil.RandomDID(t)}), "Create")
	// Each lost segment's blocks fill most of a rebuilt one, so the next
	// lost segment's first block doesn't fit after them.
	_, first := journalSegmentPadded(t, mem, "bk", 7, 1000)
	_, second := journalSegmentPadded(t, mem, "bk", 7, 1000)

	m, err := openRebuildManager(t, t.TempDir(), mem, &shipRecorder{})
	require.NoError(t, err, "OpenManager")
	requireReadable(t, m, append(first, second...))
	rows, err := mem.ListSegments(ctx, blockstore.PlaneCatalog, "bk")
	require.NoError(t, err, "ListSegments")
	for _, r := range rows {
		require.LessOrEqual(t, r.Size, int64(8<<10), "rebuilt segment %d is %d bytes, past SealBytes (8 KiB)", r.Seq, r.Size)
	}
}

// TestRebuild_JournalOnlyBucket: a bucket whose lost segment's row is already
// gone (an older recovery deleted it) is found from the journal alone.
func TestRebuild_JournalOnlyBucket(t *testing.T) {
	ctx := context.Background()
	mem := newPGMeta(t)
	require.NoError(t, mem.Create(ctx, "bk", testutil.RandomDID(t), registry.CreateState{Tenant: testutil.RandomDID(t)}), "Create")
	seq, blocks := journalSegment(t, mem, "bk", 5)
	require.NoError(t, mem.DeleteSegment(ctx, blockstore.PlaneCatalog, seq), "DeleteSegment")
	b, _ := mem.ListSegmentBuckets(ctx, blockstore.PlaneCatalog)
	require.Empty(t, b, "segment buckets = %v, want none; the test proves nothing", b)

	m, err := openRebuildManager(t, t.TempDir(), mem, &shipRecorder{})
	require.NoError(t, err, "OpenManager")
	requireReadable(t, m, blocks)
}

// failSecondMove fails the second MoveJournal, as a crash between a page's
// fsync and its journal move would leave things.
type failSecondMove struct {
	*pgMeta
	calls int
}

func (f *failSecondMove) MoveJournal(ctx context.Context, bucket string, from, to uint64, first, last cid.Cid) error {
	if f.calls++; f.calls == 2 {
		return errors.New("injected move failure")
	}
	return f.pgMeta.MoveJournal(ctx, bucket, from, to, first, last)
}

// TestRebuild_ResumesAfterFailure: a rebuild that fails partway leaves the
// unmoved rows under the lost segment and an open segment behind. Each later
// start force-seals the open segment the one before it left and rebuilds
// from where it stopped, ending with every block readable and every journal
// row under a segment that is on disk.
func TestRebuild_ResumesAfterFailure(t *testing.T) {
	ctx := context.Background()
	mem := newPGMeta(t)
	require.NoError(t, mem.Create(ctx, "bk", testutil.RandomDID(t), registry.CreateState{Tenant: testutil.RandomDID(t)}), "Create")
	lost, blocks := journalSegment(t, mem, "bk", 600)
	dir := t.TempDir()

	// Two starts fail partway, each leaving an open segment behind.
	for range 2 {
		_, err := openRebuildManager(t, dir, &failSecondMove{pgMeta: mem}, &shipRecorder{})
		require.Error(t, err, "OpenManager with a failing move succeeded; the test proves nothing")
	}
	rows := mem.JournaledBlocks(t, lost)
	require.NotEmpty(t, rows, "%d of %d rows under the lost segment after the failure, want some moved and some not", len(rows), len(blocks))
	require.NotEqual(t, len(blocks), len(rows), "%d of %d rows under the lost segment after the failure, want some moved and some not", len(rows), len(blocks))

	m, err := openRebuildManager(t, dir, mem, &shipRecorder{})
	require.NoError(t, err, "OpenManager after the failure")
	requireReadable(t, m, blocks)
	require.Empty(t, mem.JournaledBlocks(t, lost), "journal rows still under the lost segment")
	onDisk := onDiskSeqs(t, dir, "bk")
	segs, err := mem.JournalSegments(ctx, "bk")
	require.NoError(t, err, "JournalSegments")
	for _, seq := range segs {
		require.True(t, onDisk[seq], "journal rows under segment %d, which is not on disk (on disk: %v)", seq, onDisk)
	}
}

// TestOpenManager_SeqPastDisk: a Meta restored from behind the disk knows
// none of its segments, so its allocator would hand their seqs out again.
// Opening the manager moves it past every seq on disk, and restores each
// sealed segment's row from its .idx.
func TestOpenManager_SeqPastDisk(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	ahead := newPGMeta(t)
	m, err := openRebuildManager(t, dir, ahead, &shipRecorder{})
	require.NoError(t, err, "OpenManager")
	// Each append outgrows SealBytes; wait for its seal so every block lands
	// in a segment of its own.
	var written []block.Block
	for i := range 3 {
		b := block.NewBlock([]byte(fmt.Sprintf("written-%d/%s", i, strings.Repeat("y", 9<<10))))
		_, err := m.AppendBatch(ctx, []block.Block{b}, blockstore.OpRoot{Bucket: "bk", Root: b.Cid()})
		require.NoError(t, err, "AppendBatch")
		written = append(written, b)
		deadline := time.Now().Add(5 * time.Second)
		for {
			idx, _ := filepath.Glob(filepath.Join(dir, "bk", blockstore.PlaneCatalog.String(), "seg-*.idx"))
			if len(idx) > i {
				break
			}
			require.False(t, time.Now().After(deadline), "append %d never sealed", i)
			time.Sleep(10 * time.Millisecond)
		}
	}
	require.NoError(t, m.Close(ctx), "Close")
	var top uint64
	for seq := range onDiskSeqs(t, dir, "bk") {
		top = max(top, seq)
	}
	require.NotZero(t, top, "no segment on disk; the test proves nothing")

	behind := newPGMeta(t)
	m2, err := openRebuildManager(t, dir, behind, &shipRecorder{})
	require.NoError(t, err, "OpenManager over a Meta behind the disk")
	requireReadable(t, m2, written)
	rows, err := behind.ListSegments(ctx, blockstore.PlaneCatalog, "bk")
	require.NoError(t, err, "ListSegments")
	require.Len(t, rows, 3, "%d rows restored, want one per sealed segment on disk (3)", len(rows))
	for _, r := range rows {
		require.Equal(t, logstore.StateSealed, r.State, "restored row %d is %v (sha %x), want sealed from its .idx", r.Seq, r.State, r.SHA256)
		require.NotEmpty(t, r.SHA256, "restored row %d is %v (sha %x), want sealed from its .idx", r.Seq, r.State, r.SHA256)
	}
	next, err := behind.NextSegmentSeq(ctx)
	require.NoError(t, err, "NextSegmentSeq")
	require.Greater(t, next, top, "NextSegmentSeq = %d after opening over segments up to %d", next, top)
}

// sealedSegments writes one block per segment into bucket "bk" under dir,
// through a manager over mem, and returns the blocks once every segment has
// sealed, none shipped, and the manager is closed.
func sealedSegments(t *testing.T, dir string, mem *pgMeta, n int) []block.Block {
	t.Helper()
	ctx := context.Background()
	m, err := openManagerShipping(t, dir, mem, &shipRecorder{}, false)
	require.NoError(t, err, "OpenManager")
	var written []block.Block
	for i := range n {
		b := block.NewBlock([]byte(fmt.Sprintf("sealed-%d/%s", i, strings.Repeat("z", 9<<10))))
		_, err := m.AppendBatch(ctx, []block.Block{b}, blockstore.OpRoot{Bucket: "bk", Root: b.Cid()})
		require.NoError(t, err, "AppendBatch")
		written = append(written, b)
		deadline := time.Now().Add(5 * time.Second)
		for {
			idx, _ := filepath.Glob(filepath.Join(dir, "bk", blockstore.PlaneCatalog.String(), "seg-*.idx"))
			if len(idx) > i {
				break
			}
			require.False(t, time.Now().After(deadline), "append %d never sealed", i)
			time.Sleep(10 * time.Millisecond)
		}
	}
	require.NoError(t, m.Close(ctx), "Close")
	return written
}

func sortedSeqs(seqs map[uint64]bool) []uint64 {
	var out []uint64
	for s := range seqs {
		out = append(out, s)
	}
	slices.Sort(out)
	return out
}

// TestRecover_MetaBehindAnOpenRow: Meta restored to when segment A was still
// open, while on disk A has sealed and B has opened since. A's row comes up to
// its .idx, B is rebuilt from its CAR and force-sealed, and both read back.
func TestRecover_MetaBehindAnOpenRow(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	written := sealedSegments(t, dir, newPGMeta(t), 2)
	seqs := sortedSeqs(onDiskSeqs(t, dir, "bk"))
	a, b := seqs[0], seqs[1]
	// B was still open at the crash: no .idx.
	require.NoError(t, os.Remove(filepath.Join(dir, "bk", blockstore.PlaneCatalog.String(), fmt.Sprintf("seg-%020d.idx", b))), "remove B's .idx")
	behind := newPGMeta(t)
	require.NoError(t, behind.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, a, "bk"), "InsertSegmentOpen")

	m, err := openRebuildManager(t, dir, behind, &shipRecorder{})
	require.NoError(t, err, "OpenManager over a Meta behind an open row")
	requireReadable(t, m, written)
	rows, err := behind.ListSegments(ctx, blockstore.PlaneCatalog, "bk")
	require.NoError(t, err, "ListSegments")
	sealed := map[uint64]bool{}
	for _, r := range rows {
		if r.State == logstore.StateSealed && len(r.SHA256) > 0 {
			sealed[r.Seq] = true
		}
	}
	require.True(t, sealed[a], "sealed rows %v, want A (%d) brought up to its .idx and B (%d) force-sealed", sealed, a, b)
	require.True(t, sealed[b], "sealed rows %v, want A (%d) brought up to its .idx and B (%d) force-sealed", sealed, a, b)
}

// TestRecover_RetiredFilesReappear: a retire whose unlink never reached the
// disk leaves a retired segment's files behind. Recovery removes them and
// keeps the retired row as it was: it is DeleteBucket's record of the blobs
// the segment registered.
func TestRecover_RetiredFilesReappear(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mem := newPGMeta(t)
	sealedSegments(t, dir, mem, 1)
	seq := sortedSeqs(onDiskSeqs(t, dir, "bk"))[0]
	index := multihash.Multihash("retired-index")
	require.NoError(t, mem.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seq, time.Now().Unix(), index, -1, nil), "MarkSegmentShipped")
	require.NoError(t, mem.RetireSegment(ctx, blockstore.PlaneCatalog, seq, time.Now().Unix()), "RetireSegment")

	_, err := openRebuildManager(t, dir, mem, &shipRecorder{})
	require.NoError(t, err, "OpenManager")
	left := onDiskSeqs(t, dir, "bk")
	require.False(t, left[seq], "the retired segment's CAR is still on disk")
	retired, err := mem.ListRetiredSegments(ctx, blockstore.PlaneCatalog, "bk")
	require.NoError(t, err, "ListRetiredSegments")
	require.Len(t, retired, 1, "retired rows = %+v, want segment %d with its index digest kept", retired, seq)
	require.Equal(t, seq, retired[0].Seq, "retired rows = %+v, want segment %d with its index digest kept", retired, seq)
	require.Equal(t, string(index), string(retired[0].IndexDigest), "retired rows = %+v, want segment %d with its index digest kept", retired, seq)
	live, _ := mem.ListSegments(ctx, blockstore.PlaneCatalog, "bk")
	require.Empty(t, live, "live rows")
}

// TestRecover_UnshippedRetiredFileReappears: a segment that never shipped
// was retired because its file was missing, and the file is back. It may hold
// the only copy of its blocks, so recovery keeps it, restores its row and
// ships it.
func TestRecover_UnshippedRetiredFileReappears(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mem := newPGMeta(t)
	written := sealedSegments(t, dir, mem, 1)
	seq := sortedSeqs(onDiskSeqs(t, dir, "bk"))[0]
	// Sealed but never shipped, whatever the manager above got to.
	rows, err := mem.ListSegments(ctx, blockstore.PlaneCatalog, "bk")
	require.NoError(t, err, "ListSegments = %+v, %v; want the one sealed row", rows, err)
	require.Len(t, rows, 1, "ListSegments = %+v, %v; want the one sealed row", rows, err)
	row := rows[0]
	row.ShippedAt, row.IndexDigest = 0, nil
	require.NoError(t, mem.RehydrateSegment(ctx, row), "RehydrateSegment")
	require.NoError(t, mem.RetireSegment(ctx, blockstore.PlaneCatalog, seq, time.Now().Unix()), "RetireSegment")

	ship := &shipRecorder{}
	m, err := openRebuildManager(t, dir, mem, ship)
	require.NoError(t, err, "OpenManager")
	requireReadable(t, m, written)
	retired, _ := mem.ListRetiredSegments(ctx, blockstore.PlaneCatalog, "bk")
	require.Empty(t, retired, "retired rows = %+v, want the segment's row restored", retired)
	live, _ := mem.ListSegments(ctx, blockstore.PlaneCatalog, "bk")
	require.Len(t, live, 1, "live rows = %+v, want segment %d restored as sealed", live, seq)
	require.Equal(t, seq, live[0].Seq, "live rows = %+v, want segment %d restored as sealed", live, seq)
	require.Equal(t, logstore.StateSealed, live[0].State, "live rows = %+v, want segment %d restored as sealed", live, seq)
	deadline := time.Now().Add(5 * time.Second)
	for !ship.shipped()[seq] {
		require.False(t, time.Now().After(deadline), "segment %d never shipped", seq)
		time.Sleep(20 * time.Millisecond)
	}
}

// TestRecover_UnshippedRetiredCARWithoutIdx: the same, with only the CAR
// back (recovery removed the sidecars as strays when the CAR went missing).
// It is rebuilt from the CAR and ships under its retired row; once it has
// shipped, the next start removes its files like any retired segment's.
func TestRecover_UnshippedRetiredCARWithoutIdx(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mem := newPGMeta(t)
	written := sealedSegments(t, dir, mem, 1)
	seq := sortedSeqs(onDiskSeqs(t, dir, "bk"))[0]
	rows, err := mem.ListSegments(ctx, blockstore.PlaneCatalog, "bk")
	require.NoError(t, err, "ListSegments = %+v, %v; want the one sealed row", rows, err)
	require.Len(t, rows, 1, "ListSegments = %+v, %v; want the one sealed row", rows, err)
	row := rows[0]
	row.ShippedAt, row.IndexDigest = 0, nil
	require.NoError(t, mem.RehydrateSegment(ctx, row), "RehydrateSegment")
	require.NoError(t, mem.RetireSegment(ctx, blockstore.PlaneCatalog, seq, time.Now().Unix()), "RetireSegment")
	for _, name := range []string{fmt.Sprintf("seg-%020d.idx", seq), fmt.Sprintf("seg-%020d.ops", seq)} {
		require.NoError(t, os.Remove(filepath.Join(dir, "bk", blockstore.PlaneCatalog.String(), name)), "remove %s", name)
	}

	ship := &shipRecorder{}
	m, err := openRebuildManager(t, dir, mem, ship)
	require.NoError(t, err, "OpenManager")
	requireReadable(t, m, written)
	// Wait for the retired row to record the ship, not for the flush: the
	// ship writes the row after the flush returns, on queueShips's
	// goroutine, which Close doesn't wait for, and the reopen below decides
	// from the row.
	deadline := time.Now().Add(5 * time.Second)
	for {
		retired, err := mem.ListRetiredSegments(ctx, blockstore.PlaneCatalog, "bk")
		require.NoError(t, err, "ListRetiredSegments")
		if len(retired) == 1 && retired[0].Seq == seq && retired[0].ShippedAt != 0 {
			break
		}
		if time.Now().After(deadline) {
			require.FailNowf(t, "segment never shipped", "segment %d (flushed: %v, retired rows: %+v)", seq, ship.shipped()[seq], retired)
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NoError(t, m.Close(ctx), "Close")

	_, err = openRebuildManager(t, dir, mem, &shipRecorder{})
	require.NoError(t, err, "OpenManager again")
	require.False(t, onDiskSeqs(t, dir, "bk")[seq], "segment %d's CAR is still on disk after it shipped under its retired row", seq)
	retired, err := mem.ListRetiredSegments(ctx, blockstore.PlaneCatalog, "bk")
	require.NoError(t, err, "ListRetiredSegments")
	require.Len(t, retired, 1, "retired rows = %+v, want segment %d retired and shipped", retired, seq)
	require.Equal(t, seq, retired[0].Seq, "retired rows = %+v, want segment %d retired and shipped", retired, seq)
	require.NotZero(t, retired[0].ShippedAt, "retired rows = %+v, want segment %d retired and shipped", retired, seq)
}

// TestRecover_UnreadableIdx: a row-less CAR whose .idx didn't survive (a
// rename that never reached the disk) is rebuilt from the CAR instead of
// failing the start.
func TestRecover_UnreadableIdx(t *testing.T) {
	dir := t.TempDir()
	written := sealedSegments(t, dir, newPGMeta(t), 1)
	seq := sortedSeqs(onDiskSeqs(t, dir, "bk"))[0]
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bk", blockstore.PlaneCatalog.String(), fmt.Sprintf("seg-%020d.idx", seq)), nil, 0o644), "truncate .idx")
	m, err := openRebuildManager(t, dir, newPGMeta(t), &shipRecorder{})
	require.NoError(t, err, "OpenManager over an unreadable .idx")
	requireReadable(t, m, written)
}

// TestRecover_SealedRowWithoutIdx: a sealed row whose .idx is missing or
// unreadable is rebuilt from its CAR rather than failing the start. A
// shipped one isn't shipped again; an unshipped one ships.
func TestRecover_SealedRowWithoutIdx(t *testing.T) {
	for _, tc := range []struct {
		name    string
		shipped bool
		idx     []byte // nil removes the .idx
	}{
		{"missing, unshipped", false, nil},
		{"missing, shipped", true, nil},
		{"empty, unshipped", false, []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			mem := newPGMeta(t)
			written := sealedSegments(t, dir, mem, 1)
			seq := sortedSeqs(onDiskSeqs(t, dir, "bk"))[0]
			rows, err := mem.ListSegments(ctx, blockstore.PlaneCatalog, "bk")
			require.NoError(t, err, "ListSegments = %+v, %v; want the one sealed row", rows, err)
			require.Len(t, rows, 1, "ListSegments = %+v, %v; want the one sealed row", rows, err)
			require.Equal(t, logstore.StateSealed, rows[0].State, "ListSegments = %+v, %v; want the one sealed row", rows, err)
			row := rows[0]
			row.ShippedAt, row.IndexDigest = 0, nil
			if tc.shipped {
				row.ShippedAt = time.Now().Unix()
			}
			require.NoError(t, mem.RehydrateSegment(ctx, row), "RehydrateSegment")
			idx := filepath.Join(dir, "bk", blockstore.PlaneCatalog.String(), fmt.Sprintf("seg-%020d.idx", seq))
			if tc.idx == nil {
				err = os.Remove(idx)
			} else {
				err = os.WriteFile(idx, tc.idx, 0o644)
			}
			require.NoError(t, err, "break .idx")

			ship := &shipRecorder{}
			m, err := openRebuildManager(t, dir, mem, ship)
			require.NoError(t, err, "OpenManager over a sealed row without its .idx")
			requireReadable(t, m, written)
			_, err = os.Stat(idx)
			require.NoError(t, err, "no fresh .idx after the rebuild")
			if tc.shipped {
				time.Sleep(200 * time.Millisecond)
				require.False(t, ship.shipped()[seq], "shipped segment %d was shipped again", seq)
				return
			}
			deadline := time.Now().Add(5 * time.Second)
			for !ship.shipped()[seq] {
				require.False(t, time.Now().After(deadline), "segment %d never shipped", seq)
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}

// TestRecover_SealedRowWithoutIdxOrOps: with the .ops sidecar gone too, the
// rebuilt segment takes its op-roots from the row, so the fresh .idx carries
// them.
func TestRecover_SealedRowWithoutIdxOrOps(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mem := newPGMeta(t)
	written := sealedSegments(t, dir, mem, 1)
	seq := sortedSeqs(onDiskSeqs(t, dir, "bk"))[0]
	rows, err := mem.ListSegments(ctx, blockstore.PlaneCatalog, "bk")
	require.NoError(t, err, "ListSegments = %+v, %v; want one sealed row with op-roots", rows, err)
	require.Len(t, rows, 1, "ListSegments = %+v, %v; want one sealed row with op-roots", rows, err)
	require.NotEmpty(t, rows[0].OpRoots, "ListSegments = %+v, %v; want one sealed row with op-roots", rows, err)
	catalog := filepath.Join(dir, "bk", blockstore.PlaneCatalog.String())
	for _, name := range []string{fmt.Sprintf("seg-%020d.idx", seq), fmt.Sprintf("seg-%020d.ops", seq)} {
		require.NoError(t, os.Remove(filepath.Join(catalog, name)), "remove %s", name)
	}

	m, err := openRebuildManager(t, dir, mem, &shipRecorder{})
	require.NoError(t, err, "OpenManager")
	requireReadable(t, m, written)
	data, err := os.ReadFile(filepath.Join(catalog, fmt.Sprintf("seg-%020d.idx", seq)))
	require.NoError(t, err, "read the fresh .idx")
	var idx struct {
		OpRoots []json.RawMessage `json:"op_roots"`
	}
	require.NoError(t, json.Unmarshal(data, &idx), "parse the fresh .idx")
	require.Len(t, idx.OpRoots, len(rows[0].OpRoots), "fresh .idx op-roots")
}

// TestRecover_SealedRowDamagedCAR: a sealed row whose .idx is gone and whose
// CAR is short of the size it sealed at is damaged, not torn. Rebuilding it
// would ship a segment missing blocks and drop their journal rows, so it is
// set aside and recovered as a lost file: rebuilt from the journal when it
// never shipped, left to Forge when it did. With nothing journaled for an
// unshipped one, the start fails and the file stays.
func TestRecover_SealedRowDamagedCAR(t *testing.T) {
	for _, tc := range []struct {
		name               string
		shipped, journaled bool
		wantErr            bool
	}{
		{name: "unshipped, journaled", journaled: true},
		{name: "shipped", shipped: true},
		{name: "unshipped, nothing journaled", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			mem := newPGMeta(t)
			written := sealedSegments(t, dir, mem, 1)
			seq := sortedSeqs(onDiskSeqs(t, dir, "bk"))[0]
			rows, err := mem.ListSegments(ctx, blockstore.PlaneCatalog, "bk")
			require.NoError(t, err, "ListSegments = %+v, %v; want the one sealed row", rows, err)
			require.Len(t, rows, 1, "ListSegments = %+v, %v; want the one sealed row", rows, err)
			require.Equal(t, logstore.StateSealed, rows[0].State, "ListSegments = %+v, %v; want the one sealed row", rows, err)
			row := rows[0]
			row.ShippedAt, row.IndexDigest = 0, nil
			if tc.shipped {
				row.ShippedAt = time.Now().Unix()
			}
			require.NoError(t, mem.RehydrateSegment(ctx, row), "RehydrateSegment")
			if tc.journaled {
				require.NoError(t, mem.Create(ctx, "bk", testutil.RandomDID(t), registry.CreateState{Tenant: testutil.RandomDID(t)}), "Create bk")
				mem.commitJournaled(t, "bk", seq, written)
			}
			catalog := filepath.Join(dir, "bk", blockstore.PlaneCatalog.String())
			car := filepath.Join(catalog, fmt.Sprintf("seg-%020d.car", seq))
			require.NoError(t, os.Remove(filepath.Join(catalog, fmt.Sprintf("seg-%020d.idx", seq))), "remove .idx")
			require.NoError(t, os.Truncate(car, row.Size-100), "cut the CAR short")

			ship := &shipRecorder{}
			m, err := openRebuildManager(t, dir, mem, ship)
			if tc.wantErr {
				require.Error(t, err, "OpenManager over a damaged, unjournaled sealed CAR succeeded")
				info, serr := os.Stat(car)
				require.NoError(t, serr, "the damaged CAR was moved or changed")
				require.Equal(t, row.Size-100, info.Size(), "the damaged CAR was moved or changed")
				return
			}
			require.NoError(t, err, "OpenManager")
			require.False(t, onDiskSeqs(t, dir, "bk")[seq], "segment %d's damaged CAR is still in place", seq)
			_, err = os.Stat(car + ".damaged")
			require.NoError(t, err, "the damaged CAR wasn't set aside")
			_, err = os.Stat(filepath.Join(catalog, fmt.Sprintf("seg-%020d.ops", seq)))
			require.ErrorIs(t, err, fs.ErrNotExist, "the damaged segment's .ops is still there")
			retired, err := mem.ListRetiredSegments(ctx, blockstore.PlaneCatalog, "bk")
			require.NoError(t, err, "retired rows = %+v, %v", retired, err)
			require.Len(t, retired, 1, "retired rows = %+v, %v", retired, err)
			require.Equal(t, seq, retired[0].Seq, "retired rows = %+v, %v", retired, err)
			if tc.journaled {
				requireReadable(t, m, written)
			}
		})
	}
}

// TestRecover_SeveralOpenSegments: two row-less CARs with no .idx, both open
// at the crash as far as recovery can tell (Meta restored from behind the
// disk), are both rebuilt and force-sealed rather than failing the start.
func TestRecover_SeveralOpenSegments(t *testing.T) {
	dir := t.TempDir()
	written := sealedSegments(t, dir, newPGMeta(t), 2)
	for seq := range onDiskSeqs(t, dir, "bk") {
		require.NoError(t, os.Remove(filepath.Join(dir, "bk", blockstore.PlaneCatalog.String(), fmt.Sprintf("seg-%020d.idx", seq))), "remove .idx")
	}
	m, err := openRebuildManager(t, dir, newPGMeta(t), &shipRecorder{})
	require.NoError(t, err, "OpenManager over two open segments")
	requireReadable(t, m, written)
}
