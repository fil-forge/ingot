package registry_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/taskrunner"
	"github.com/fil-forge/libforge/testutil"
)

// TestCatalogJournal checks the catalog journal against a real Postgres:
// the rows a commit writes, the rows a ship deletes, and the lock order that
// keeps a commit racing its segment's ship from deadlocking or leaving rows
// behind.
func TestCatalogJournal(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	r := registry.NewPostgres(pool)
	require.NoError(t, r.Create(ctx, "jb", testutil.RandomDID(t), registry.CreateState{Tenant: testutil.RandomDID(t)}), "Create")

	openSegment := func(t *testing.T) uint64 {
		t.Helper()
		seq, err := r.NextSegmentSeq(ctx)
		require.NoError(t, err, "NextSegmentSeq")
		require.NoError(t, r.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq, "jb"), "InsertSegmentOpen")
		return seq
	}
	// commit swaps the bucket's root to a fresh CID, journaling blocks under
	// seg, and returns the new root.
	commit := func(t *testing.T, seg uint64, blocks ...block.Block) (cid.Cid, error) {
		t.Helper()
		st, err := r.Get(ctx, "jb")
		require.NoError(t, err, "Get")
		next := liveCid(t, time.Now().String()+blocks[0].Cid().String())
		return next, r.CommitRoot(ctx, "jb", st.Root, next, registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: blocks},
		})
	}
	journaled := func(t *testing.T, seg uint64) map[string]string {
		t.Helper()
		rows, err := pool.Query(ctx, `SELECT cid, data, bucket FROM ingot.catalog_journal WHERE seq = $1`, int64(seg))
		require.NoError(t, err, "query journal")
		defer rows.Close()
		out := map[string]string{}
		for rows.Next() {
			var c, data []byte
			var bucket string
			require.NoError(t, rows.Scan(&c, &data, &bucket), "scan journal")
			require.Equal(t, "jb", bucket, "journal row for bucket %q", bucket)
			out[string(c)] = string(data)
		}
		require.NoError(t, rows.Err(), "journal rows")
		return out
	}

	t.Run("commit journals, ship clears, late commit journals nothing", func(t *testing.T) {
		seg := openSegment(t)
		a, b := block.NewBlock([]byte("node-a")), block.NewBlock([]byte("manifest-b"))
		_, err := commit(t, seg, a, b, a)
		require.NoError(t, err, "commit")
		got := journaled(t, seg)
		require.Len(t, got, 2, "journal")
		require.Equal(t, "node-a", got[string(a.Cid().Bytes())], "journal")
		require.Equal(t, "manifest-b", got[string(b.Cid().Bytes())], "journal")
		st, err := r.CatalogJournalStats(ctx)
		require.NoError(t, err, "CatalogJournalStats")
		require.Equal(t, int64(2), st.Blocks, "stats = %+v, want 2 blocks, their bytes and an oldest time", st)
		require.Equal(t, int64(len("node-a")+len("manifest-b")), st.Bytes, "stats = %+v, want 2 blocks, their bytes and an oldest time", st)
		require.False(t, st.Oldest.IsZero(), "stats = %+v, want 2 blocks, their bytes and an oldest time", st)

		// A commit that loses the CAS journals nothing.
		err = r.CommitRoot(ctx, "jb", liveCid(t, "not-the-root"), liveCid(t, "next"), registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: []block.Block{block.NewBlock([]byte("lost"))}},
		})
		require.ErrorIs(t, err, registry.ErrConflict, "conflicting commit")
		require.Len(t, journaled(t, seg), 2, "journal after a refused commit")

		require.NoError(t, r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seg, time.Now().Unix(), nil, -1, nil), "MarkSegmentShipped")
		require.Empty(t, journaled(t, seg), "journal after ship")
		_, err = commit(t, seg, block.NewBlock([]byte("late")))
		require.NoError(t, err, "late commit")
		require.Empty(t, journaled(t, seg), "journal after a commit into a shipped segment")
	})

	t.Run("rows outlive their segment row but not their bucket", func(t *testing.T) {
		seg := openSegment(t)
		_, err := commit(t, seg, block.NewBlock([]byte("kept")))
		require.NoError(t, err, "commit")
		require.NoError(t, r.DeleteSegment(ctx, blockstore.PlaneCatalog, seg), "DeleteSegment")
		got := journaled(t, seg)
		require.Len(t, got, 1, "journal after deleting the segment row")
		// A segment whose row is gone takes no new rows.
		_, err = commit(t, seg, block.NewBlock([]byte("orphan")))
		require.NoError(t, err, "commit into a segment with no row")
		require.Len(t, journaled(t, seg), 1, "journal after a commit into a segment with no row, want only the first block")
	})

	// Recovery retires a sealed segment whose CAR is gone rather than
	// deleting its row. The segment never shipped, so its journal rows are
	// the only copy of its blocks and must survive the retirement.
	t.Run("retiring a sealed, unshipped segment keeps its rows", func(t *testing.T) {
		seg := openSegment(t)
		_, err := commit(t, seg, block.NewBlock([]byte("sealed-lost")))
		require.NoError(t, err, "commit")
		now := time.Now().Unix()
		require.NoError(t, r.MarkSegmentSealed(ctx, blockstore.PlaneCatalog, seg, now, 10, make([]byte, 32), nil), "MarkSegmentSealed")
		require.NoError(t, r.RetireSegment(ctx, blockstore.PlaneCatalog, seg, now), "RetireSegment")
		got := journaled(t, seg)
		require.Len(t, got, 1, "journal after retiring the unshipped segment")
	})

	// newBucket creates a bucket with a segment open for it.
	// openSegmentFor opens a further segment row for an existing bucket.
	openSegmentFor := func(t *testing.T, bucket string) uint64 {
		t.Helper()
		seq, err := r.NextSegmentSeq(ctx)
		require.NoError(t, err, "NextSegmentSeq")
		require.NoError(t, r.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq, bucket), "InsertSegmentOpen")
		return seq
	}
	// journaledFor counts the journal rows under seg, whatever their bucket.
	journaledFor := func(t *testing.T, seg uint64) []string {
		t.Helper()
		rows, err := pool.Query(ctx, `SELECT cid FROM ingot.catalog_journal WHERE seq = $1`, int64(seg))
		require.NoError(t, err, "query journal")
		out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
			var c []byte
			err := row.Scan(&c)
			return string(c), err
		})
		require.NoError(t, err, "journal rows")
		return out
	}

	newBucket := func(t *testing.T, name string) uint64 {
		t.Helper()
		require.NoError(t, r.Create(ctx, name, testutil.RandomDID(t), registry.CreateState{Tenant: testutil.RandomDID(t)}), "Create %s", name)
		seq, err := r.NextSegmentSeq(ctx)
		require.NoError(t, err, "NextSegmentSeq")
		require.NoError(t, r.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq, name), "InsertSegmentOpen")
		return seq
	}

	// Journaling runs before the CAS, so the journal's foreign key is the
	// first thing to meet a deleted bucket. The commit still reports it as
	// one.
	t.Run("a commit to a deleted bucket is ErrNotFound", func(t *testing.T) {
		seg := newBucket(t, "jb-gone")
		st, gerr := r.Get(ctx, "jb-gone")
		require.NoError(t, gerr)
		require.NoError(t, r.Delete(ctx, "jb-gone", st.Space), "Delete")
		err := r.CommitRoot(ctx, "jb-gone", cid.Undef, liveCid(t, "gone-root"), registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: []block.Block{block.NewBlock([]byte("gone"))}},
		})
		require.ErrorIs(t, err, registry.ErrNotFound, "commit to a deleted bucket")
	})

	// A bucket's deletion locks its row and then cascades into its journal
	// rows. A ship of one of its segments, waiting on that row, must not
	// already hold the journal rows, or the two deadlock. The deleting
	// transaction takes the bucket row first, so the ship blocks on it, and
	// only then deletes the bucket.
	t.Run("a ship racing its bucket's deletion doesn't deadlock", func(t *testing.T) {
		seg := newBucket(t, "jb-del")
		root := liveCid(t, "del-root")
		require.NoError(t, r.CommitRoot(ctx, "jb-del", cid.Undef, root, registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: []block.Block{block.NewBlock([]byte("del"))}},
		}), "commit")
		deleter, err := pool.Begin(ctx)
		require.NoError(t, err, "begin deleter")
		defer func() { _ = deleter.Rollback(ctx) }()
		_, err = deleter.Exec(ctx, `SELECT 1 FROM ingot.buckets WHERE name = 'jb-del' FOR UPDATE`)
		require.NoError(t, err, "lock bucket row")

		shipDone := make(chan error, 1)
		go func() {
			shipDone <- r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seg, time.Now().Unix(), nil, -1,
				[]blockstore.OpRoot{{Bucket: "jb-del", Root: root}})
		}()
		waitForLockWaiters(t, pool, 1)

		_, err = deleter.Exec(ctx, `DELETE FROM ingot.buckets WHERE name = 'jb-del'`)
		require.NoError(t, err, "delete bucket")
		require.NoError(t, deleter.Commit(ctx), "commit deletion")
		select {
		case err := <-shipDone:
			require.NoError(t, err, "ship")
		case <-time.After(10 * time.Second):
			require.FailNow(t, "ship did not finish")
		}
		var left int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ingot.catalog_journal WHERE seq = $1`, int64(seg)).Scan(&left), "count journal")
		require.Zero(t, left, "%d journal rows left for the deleted bucket", left)
	})

	// The same race when the bucket's root has moved on since the segment's
	// op-root, so the forge-root advance's guard matches nothing and locks no
	// bucket row: the ship still waits for the deletion before it touches the
	// journal rows the deletion cascades into.
	t.Run("a ship with a stale guard still waits for its bucket's deletion", func(t *testing.T) {
		seg := newBucket(t, "jb-stale")
		old, next := liveCid(t, "stale-old"), liveCid(t, "stale-next")
		require.NoError(t, r.CommitRoot(ctx, "jb-stale", cid.Undef, old, registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: []block.Block{block.NewBlock([]byte("stale"))}},
		}), "commit")
		require.NoError(t, r.CASRoot(ctx, "jb-stale", old, next), "move the root on")
		deleter, err := pool.Begin(ctx)
		require.NoError(t, err, "begin deleter")
		defer func() { _ = deleter.Rollback(ctx) }()
		_, err = deleter.Exec(ctx, `SELECT 1 FROM ingot.buckets WHERE name = 'jb-stale' FOR UPDATE`)
		require.NoError(t, err, "lock bucket row")

		shipDone := make(chan error, 1)
		go func() {
			shipDone <- r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seg, time.Now().Unix(), nil, -1,
				[]blockstore.OpRoot{{Bucket: "jb-stale", Root: old}})
		}()
		waitForLockWaiters(t, pool, 1)

		_, err = deleter.Exec(ctx, `DELETE FROM ingot.buckets WHERE name = 'jb-stale'`)
		require.NoError(t, err, "delete bucket")
		require.NoError(t, deleter.Commit(ctx), "commit deletion")
		select {
		case err := <-shipDone:
			require.NoError(t, err, "ship")
		case <-time.After(10 * time.Second):
			require.FailNow(t, "ship did not finish")
		}
	})

	// EmptyRoot deletes the bucket's journal rows as a deletion does, so a
	// ship of one of the bucket's segments waits for it on the bucket row,
	// also when the ship's guard is stale and its advance locks nothing: the
	// two deletes would otherwise lock the same journal rows in different
	// orders and could deadlock. A third transaction holding one journal row
	// pauses EmptyRoot after it has taken the bucket row; the ship must then
	// be queued on that row, not on the journal.
	t.Run("a ship with a stale guard waits for EmptyRoot", func(t *testing.T) {
		seg := newBucket(t, "jb-emptying")
		old := liveCid(t, "emptying-old")
		require.NoError(t, r.CommitRoot(ctx, "jb-emptying", cid.Undef, old, registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: []block.Block{block.NewBlock([]byte("emptying-a"))}},
		}), "commit")
		cur := liveCid(t, "emptying-cur")
		require.NoError(t, r.CommitRoot(ctx, "jb-emptying", old, cur, registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: []block.Block{block.NewBlock([]byte("emptying-b"))}},
		}), "second commit")
		holder, err := pool.Begin(ctx)
		require.NoError(t, err, "begin holder")
		defer func() { _ = holder.Rollback(ctx) }()
		_, err = holder.Exec(ctx,
			`SELECT 1 FROM ingot.catalog_journal WHERE seq = $1 LIMIT 1 FOR UPDATE`, int64(seg))
		require.NoError(t, err, "hold a journal row")

		emptyDone := make(chan error, 1)
		go func() { emptyDone <- r.EmptyRoot(ctx, "jb-emptying", cur) }()
		waitForLockedQuery(t, pool, "DELETE FROM ingot.catalog_journal WHERE bucket%")

		shipDone := make(chan error, 1)
		go func() {
			shipDone <- r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seg, time.Now().Unix(), nil, -1,
				[]blockstore.OpRoot{{Bucket: "jb-emptying", Root: old}})
		}()
		q := waitForLockedQuery(t, pool, "%FOR KEY SHARE%", "DELETE FROM ingot.catalog_journal WHERE seq%")
		require.Contains(t, q, "FOR KEY SHARE", "the ship reached the journal rows while EmptyRoot was deleting them (waiting on %q)", q)

		require.NoError(t, holder.Commit(ctx), "release the journal row")
		for name, done := range map[string]chan error{"EmptyRoot": emptyDone, "ship": shipDone} {
			select {
			case err := <-done:
				require.NoError(t, err, "%s", name)
			case <-time.After(10 * time.Second):
				require.FailNow(t, name+" did not finish")
			}
		}
		var shipped bool
		require.NoError(t, pool.QueryRow(ctx, `SELECT shipped_at IS NOT NULL FROM ingot.segments WHERE seq = $1`, int64(seg)).Scan(&shipped), "read the segment")
		require.True(t, shipped, "the ship didn't stamp its segment")
	})

	// DeleteBucket's first write empties the root, clears the forge root and
	// drops the bucket's journaled blocks, in one transaction. A stale expect
	// changes none of it, and another bucket's rows stay.
	t.Run("emptying the root drops the bucket's journal rows", func(t *testing.T) {
		seg := newBucket(t, "jb-empty")
		otherSeg := newBucket(t, "jb-other")
		root := liveCid(t, "empty-root")
		require.NoError(t, r.CommitRoot(ctx, "jb-empty", cid.Undef, root, registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: []block.Block{block.NewBlock([]byte("emptied"))}},
		}), "commit")
		require.NoError(t, r.CommitRoot(ctx, "jb-other", cid.Undef, liveCid(t, "other-root"), registry.RootCommit{
			Journal: blockstore.Appended{Segment: otherSeg, Blocks: []block.Block{block.NewBlock([]byte("other"))}},
		}), "commit other")
		require.NoError(t, r.SetForgeRoot(ctx, "jb-empty", root), "SetForgeRoot")
		count := func(t *testing.T, bucket string) int {
			t.Helper()
			var n int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ingot.catalog_journal WHERE bucket = $1`, bucket).Scan(&n), "count journal")
			return n
		}

		err := r.EmptyRoot(ctx, "jb-empty", liveCid(t, "stale"))
		require.ErrorIs(t, err, registry.ErrConflict, "EmptyRoot with a stale")
		// After a refused EmptyRoot both roots and the journal row are kept.
		st, err := r.Get(ctx, "jb-empty")
		require.NoError(t, err, "Get after a refused EmptyRoot")
		require.True(t, st.Root.Equals(root), "root after a refused EmptyRoot = %s, want %s", st.Root, root)
		require.True(t, st.ForgeRoot.Equals(root), "forge root after a refused EmptyRoot = %s, want %s", st.ForgeRoot, root)
		require.Equal(t, 1, count(t, "jb-empty"), "journal rows after a refused EmptyRoot")

		require.NoError(t, r.EmptyRoot(ctx, "jb-empty", root), "EmptyRoot")
		st, err = r.Get(ctx, "jb-empty")
		require.NoError(t, err, "Get")
		require.False(t, st.Root.Defined(), "root %s, forge root %s after EmptyRoot; want both empty", st.Root, st.ForgeRoot)
		require.False(t, st.ForgeRoot.Defined(), "root %s, forge root %s after EmptyRoot; want both empty", st.Root, st.ForgeRoot)
		n := count(t, "jb-empty")
		require.Zero(t, n, "journal rows left for the emptied bucket")
		require.Equal(t, 1, count(t, "jb-other"), "journal rows for the other bucket")

		// A retried DeleteBucket empties an empty root again.
		require.NoError(t, r.EmptyRoot(ctx, "jb-empty", cid.Undef), "EmptyRoot on an empty root")
		err = r.EmptyRoot(ctx, "jb-missing", cid.Undef)
		require.ErrorIs(t, err, registry.ErrNotFound, "EmptyRoot on a missing bucket")
	})

	// What recovery reads and writes to rebuild a lost segment from the
	// journal: the buckets and segments the journal files blocks under, a
	// segment's blocks paged in CID order, and a range of them moved onto the
	// segment they were rebuilt into, keeping each row's age.
	t.Run("rebuild reads", func(t *testing.T) {
		seg := newBucket(t, "jb-rebuild")
		var blocks []block.Block
		for i := range 5 {
			blocks = append(blocks, block.NewBlock([]byte(fmt.Sprintf("rebuild-%d", i))))
		}
		require.NoError(t, r.CommitRoot(ctx, "jb-rebuild", cid.Undef, liveCid(t, "rebuild-root"), registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: blocks},
		}), "commit")
		buckets, err := r.JournalBuckets(ctx)
		require.NoError(t, err, "JournalBuckets")
		require.Contains(t, buckets, "jb-rebuild", "JournalBuckets")
		segs, err := r.JournalSegments(ctx, "jb-rebuild")
		require.NoError(t, err, "JournalSegments")
		require.Equal(t, []uint64{seg}, segs, "JournalSegments")

		var paged []string
		for after := cid.Undef; ; {
			page, err := r.ReadJournal(ctx, seg, after, 2)
			require.NoError(t, err, "ReadJournal")
			for _, b := range page {
				paged = append(paged, string(b.Cid().Bytes()))
			}
			if len(page) < 2 {
				break
			}
			after = page[len(page)-1].Cid()
		}
		require.Len(t, paged, len(blocks), "paged blocks")
		require.True(t, slices.IsSorted(paged), "paged blocks not in CID order")
	})

	t.Run("a journal move re-files a range and keeps its age", func(t *testing.T) {
		from := newBucket(t, "jb-move")
		var blocks []block.Block
		for i := range 5 {
			blocks = append(blocks, block.NewBlock([]byte(fmt.Sprintf("move-%d", i))))
		}
		require.NoError(t, r.CommitRoot(ctx, "jb-move", cid.Undef, liveCid(t, "move-root"), registry.RootCommit{
			Journal: blockstore.Appended{Segment: from, Blocks: blocks},
		}), "commit")
		slices.SortFunc(blocks, func(a, b block.Block) int { return bytes.Compare(a.Cid().Bytes(), b.Cid().Bytes()) })
		to := openSegmentFor(t, "jb-move")
		var before time.Time
		require.NoError(t, pool.QueryRow(ctx, `SELECT created_at FROM ingot.catalog_journal WHERE seq = $1 AND cid = $2`,
			int64(from), blocks[1].Cid().Bytes()).Scan(&before), "read created_at")

		require.NoError(t, r.MoveJournal(ctx, "jb-move", from, to, blocks[0].Cid(), blocks[2].Cid()), "MoveJournal")
		got := journaledFor(t, from)
		require.Len(t, got, 2, "rows left under the source, want the 2 outside the range")
		require.Len(t, journaledFor(t, to), 3, "rows under the target, want the 3 in the range")
		var after time.Time
		require.NoError(t, pool.QueryRow(ctx, `SELECT created_at FROM ingot.catalog_journal WHERE seq = $1 AND cid = $2`,
			int64(to), blocks[1].Cid().Bytes()).Scan(&after), "read moved created_at")
		require.True(t, after.Equal(before), "moved row's created_at = %v, want %v", after, before)

		// A target that has shipped since takes nothing; the source still goes.
		require.NoError(t, r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, to, time.Now().Unix(), nil, -1, nil), "MarkSegmentShipped")
		require.NoError(t, r.MoveJournal(ctx, "jb-move", from, to, blocks[3].Cid(), blocks[4].Cid()), "MoveJournal to a shipped segment")
		n := len(journaledFor(t, from)) + len(journaledFor(t, to))
		require.Zero(t, n, "%d rows left after a move onto a shipped segment", n)
		require.Error(t, r.MoveJournal(ctx, "jb-move", from, 1<<62, blocks[0].Cid(), blocks[4].Cid()), "MoveJournal onto a segment with no row succeeded")
	})

	// Recovery over real Postgres: a manager opened on an empty directory
	// rebuilds the bucket's journaled blocks into new segments, reads them,
	// and leaves every journal row under a segment that is on disk. It opens
	// every bucket in the database, so it runs after the subtests that need
	// theirs as they left them.
	t.Run("a manager over a wiped volume rebuilds from the journal", func(t *testing.T) {
		seg := newBucket(t, "jb-wiped")
		var blocks []block.Block
		for i := range 300 {
			blocks = append(blocks, block.NewBlock([]byte(fmt.Sprintf("wiped-%03d-%s", i, strings.Repeat("w", 64)))))
		}
		require.NoError(t, r.CommitRoot(ctx, "jb-wiped", cid.Undef, liveCid(t, "wiped-root"), registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: blocks},
		}), "commit")
		dir := t.TempDir()
		m, err := logstore.OpenManager(ctx, logstore.ManagerConfig{
			Dir:     dir,
			Meta:    r,
			Catalog: logstore.PlaneConfig{SealBytes: 8 << 10, SealAge: time.Hour},
			Logger:  zaptest.NewLogger(t),
		})
		require.NoError(t, err, "OpenManager")
		defer func() { _ = m.Close(ctx) }()
		for _, b := range blocks {
			_, err = m.Get(ctx, b.Cid())
			require.NoError(t, err, "Get %s after the rebuild", b.Cid())
		}
		got := journaledFor(t, seg)
		require.Empty(t, got, "%d journal rows still under the lost segment", len(got))
		segs, err := r.JournalSegments(ctx, "jb-wiped")
		require.NoError(t, err, "journal segments after the rebuild = %v (err %v), want the rows split across rebuilt segments", segs, err)
		require.GreaterOrEqual(t, len(segs), 2, "journal segments after the rebuild = %v (err %v), want the rows split across rebuilt segments", segs, err)
		for _, s := range segs {
			_, err = os.Stat(filepath.Join(dir, "jb-wiped", "catalog", fmt.Sprintf("seg-%020d.car", s)))
			require.NoError(t, err, "journal rows under segment %d, whose CAR isn't on disk", s)
		}
	})

	t.Run("the segment seq moves past disk and never back", func(t *testing.T) {
		start, err := r.NextSegmentSeq(ctx)
		require.NoError(t, err, "NextSegmentSeq")
		require.NoError(t, r.EnsureSegmentSeqAbove(ctx, start+10), "EnsureSegmentSeqAbove")
		next, err := r.NextSegmentSeq(ctx)
		require.NoError(t, err, "NextSegmentSeq after moving past %d", start+10)
		require.Equal(t, start+11, next, "NextSegmentSeq after moving past %d", start+10)
		require.NoError(t, r.EnsureSegmentSeqAbove(ctx, start), "EnsureSegmentSeqAbove below")
		next, err = r.NextSegmentSeq(ctx)
		require.NoError(t, err, "NextSegmentSeq after a lower EnsureSegmentSeqAbove")
		require.Equal(t, start+12, next, "NextSegmentSeq after a lower EnsureSegmentSeqAbove")
	})
}

// waitForLockedQuery waits until a backend in the database is blocked on a
// lock running a statement that matches one of the LIKE patterns, and returns
// that statement. Unlike waitForLockWaiters it can't be satisfied by another
// package's tests waiting on a lock in the same database.
func waitForLockedQuery(t *testing.T, pool *pgxpool.Pool, patterns ...string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var query string
		err := pool.QueryRow(context.Background(), `
			SELECT query FROM pg_stat_activity
			 WHERE wait_event_type = 'Lock' AND datname = current_database()
			   AND query LIKE ANY ($1)
			 LIMIT 1`, patterns).Scan(&query)
		if err == nil {
			return query
		}
		require.ErrorIs(t, err, pgx.ErrNoRows, "pg_stat_activity")
		time.Sleep(10 * time.Millisecond)
	}
	require.FailNow(t, fmt.Sprintf("no backend blocked on a lock running %q", patterns))
	return ""
}

// waitForLockWaiters waits until at least n backends are blocked on a lock.
func waitForLockWaiters(t *testing.T, pool *pgxpool.Pool, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database()`).Scan(&waiting), "pg_stat_activity")
		if waiting >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.FailNow(t, fmt.Sprintf("fewer than %d backends blocked on a lock", n))
}

// TestTakeoverGuardFailsOnAPartialTakeover: an expiry that deletes one of a
// commit's pending references while the take-over's delete runs leaves it
// fewer rows than references, and the guard fails rather than letting the
// commit claim a blob whose reference has gone.
func TestTakeoverGuardFailsOnAPartialTakeover(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	space := testutil.RandomDID(t)
	refs := []taskrunner.TaskID{uuid.New(), uuid.New()}
	for i, ref := range refs {
		_, err := pool.Exec(ctx, `INSERT INTO ingot.blob_pending_refs (add_task, expire_task, space, digest, deadline)
			VALUES ($1, $2, $3, $4, now() + interval '1 hour')`, ref, uuid.New(), space.String(), []byte{byte(i)})
		require.NoError(t, err)
	}
	// The expiry: deletes one reference and holds its lock until released.
	expiry, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = expiry.Rollback(ctx) }()
	_, err = expiry.Exec(ctx, `DELETE FROM ingot.blob_pending_refs WHERE add_task = $1`, refs[1])
	require.NoError(t, err)

	guard := registry.TakeoverGuard(refs)
	done := make(chan int64, 1)
	errs := make(chan error, 1)
	go func() {
		tag, err := pool.Exec(ctx, guard.SQL, guard.Args...)
		if err != nil {
			errs <- err
			return
		}
		done <- tag.RowsAffected()
	}()
	waitForLockWaiters(t, pool, 1) // the guard reached the locked row
	require.NoError(t, expiry.Commit(ctx))
	select {
	case err := <-errs:
		require.NoError(t, err, "guard")
	case n := <-done:
		require.Zero(t, n, "guard affected rows after a partial take-over, want 0 so the commit rolls back")
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the guard never finished")
	}
}
