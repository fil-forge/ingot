package registry_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/migrations"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/testutil"
)

// TestCatalogJournal_Live checks the catalog journal against a real Postgres:
// the rows a commit writes, the rows a ship deletes, and the lock order that
// keeps a commit racing its segment's ship from deadlocking or leaving rows
// behind. Skipped unless INGOT_TEST_DSN names a reachable database (see
// TestPostgresStores_Live).
func TestCatalogJournal_Live(t *testing.T) {
	dsn := os.Getenv("INGOT_TEST_DSN")
	if dsn == "" {
		t.Skip("set INGOT_TEST_DSN to run the live catalog journal test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	if err := migrations.Up(ctx, pool, zaptest.NewLogger(t)); err != nil {
		t.Fatalf("migrations.Up: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE ingot.catalog_journal, ingot.segments, ingot.buckets CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	r := registry.NewPostgres(pool)
	if err := r.Create(ctx, "jb", testutil.RandomDID(t), registry.CreateState{Tenant: testutil.RandomDID(t)}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	openSegment := func(t *testing.T) uint64 {
		t.Helper()
		seq, err := r.NextSegmentSeq(ctx)
		if err != nil {
			t.Fatalf("NextSegmentSeq: %v", err)
		}
		if err := r.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq, "jb"); err != nil {
			t.Fatalf("InsertSegmentOpen: %v", err)
		}
		return seq
	}
	// commit swaps the bucket's root to a fresh CID, journaling blocks under
	// seg, and returns the new root.
	commit := func(t *testing.T, seg uint64, blocks ...block.Block) (cid.Cid, error) {
		t.Helper()
		st, err := r.Get(ctx, "jb")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		next := liveCid(t, time.Now().String()+blocks[0].Cid().String())
		return next, r.CommitRoot(ctx, "jb", st.Root, next, registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: blocks},
		})
	}
	journaled := func(t *testing.T, seg uint64) map[string]string {
		t.Helper()
		rows, err := pool.Query(ctx, `SELECT cid, data, bucket FROM ingot.catalog_journal WHERE seq = $1`, int64(seg))
		if err != nil {
			t.Fatalf("query journal: %v", err)
		}
		defer rows.Close()
		out := map[string]string{}
		for rows.Next() {
			var c, data []byte
			var bucket string
			if err := rows.Scan(&c, &data, &bucket); err != nil {
				t.Fatalf("scan journal: %v", err)
			}
			if bucket != "jb" {
				t.Fatalf("journal row for bucket %q, want jb", bucket)
			}
			out[string(c)] = string(data)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("journal rows: %v", err)
		}
		return out
	}

	t.Run("commit journals, ship clears, late commit journals nothing", func(t *testing.T) {
		seg := openSegment(t)
		a, b := block.NewBlock([]byte("node-a")), block.NewBlock([]byte("manifest-b"))
		if _, err := commit(t, seg, a, b, a); err != nil {
			t.Fatalf("commit: %v", err)
		}
		got := journaled(t, seg)
		if len(got) != 2 || got[string(a.Cid().Bytes())] != "node-a" || got[string(b.Cid().Bytes())] != "manifest-b" {
			t.Fatalf("journal = %v, want both blocks once", got)
		}
		st, err := r.CatalogJournalStats(ctx)
		if err != nil {
			t.Fatalf("CatalogJournalStats: %v", err)
		}
		if st.Blocks != 2 || st.Bytes != int64(len("node-a")+len("manifest-b")) || st.Oldest.IsZero() {
			t.Fatalf("stats = %+v, want 2 blocks, their bytes and an oldest time", st)
		}

		// A commit that loses the CAS journals nothing.
		if err := r.CommitRoot(ctx, "jb", liveCid(t, "not-the-root"), liveCid(t, "next"), registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: []block.Block{block.NewBlock([]byte("lost"))}},
		}); !errors.Is(err, registry.ErrConflict) {
			t.Fatalf("conflicting commit = %v, want ErrConflict", err)
		}
		if got := journaled(t, seg); len(got) != 2 {
			t.Fatalf("journal after a refused commit = %v, want the 2 blocks", got)
		}

		if err := r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seg, time.Now().Unix(), nil, nil); err != nil {
			t.Fatalf("MarkSegmentShipped: %v", err)
		}
		if got := journaled(t, seg); len(got) != 0 {
			t.Fatalf("journal after ship = %v, want empty", got)
		}
		if _, err := commit(t, seg, block.NewBlock([]byte("late"))); err != nil {
			t.Fatalf("late commit: %v", err)
		}
		if got := journaled(t, seg); len(got) != 0 {
			t.Fatalf("journal after a commit into a shipped segment = %v, want empty", got)
		}
	})

	t.Run("rows outlive their segment row but not their bucket", func(t *testing.T) {
		seg := openSegment(t)
		if _, err := commit(t, seg, block.NewBlock([]byte("kept"))); err != nil {
			t.Fatalf("commit: %v", err)
		}
		if err := r.DeleteSegment(ctx, blockstore.PlaneCatalog, seg); err != nil {
			t.Fatalf("DeleteSegment: %v", err)
		}
		if got := journaled(t, seg); len(got) != 1 {
			t.Fatalf("journal after deleting the segment row = %v, want the block", got)
		}
		// A segment whose row is gone takes no new rows.
		if _, err := commit(t, seg, block.NewBlock([]byte("orphan"))); err != nil {
			t.Fatalf("commit into a segment with no row: %v", err)
		}
		if got := journaled(t, seg); len(got) != 1 {
			t.Fatalf("journal after a commit into a segment with no row = %v, want only the first block", got)
		}
	})

	// Recovery retires a sealed segment whose CAR is gone rather than
	// deleting its row. The segment never shipped, so its journal rows are
	// the only copy of its blocks and must survive the retirement.
	t.Run("retiring a sealed, unshipped segment keeps its rows", func(t *testing.T) {
		seg := openSegment(t)
		if _, err := commit(t, seg, block.NewBlock([]byte("sealed-lost"))); err != nil {
			t.Fatalf("commit: %v", err)
		}
		now := time.Now().Unix()
		if err := r.MarkSegmentSealed(ctx, blockstore.PlaneCatalog, seg, now, 10, make([]byte, 32), nil); err != nil {
			t.Fatalf("MarkSegmentSealed: %v", err)
		}
		if err := r.RetireSegment(ctx, blockstore.PlaneCatalog, seg, now); err != nil {
			t.Fatalf("RetireSegment: %v", err)
		}
		if got := journaled(t, seg); len(got) != 1 {
			t.Fatalf("journal after retiring the unshipped segment = %v, want the block", got)
		}
	})

	// A commit paused mid-transaction, holding its segment's share lock,
	// while the segment's ship waits on that lock: both finish, without a
	// deadlock, and the ship deletes the commit's rows. The pause comes from
	// another transaction holding the bucket row: the journal insert's
	// foreign-key check on that row waits for it, after the segment's lock is
	// taken.
	t.Run("a commit racing its segment's ship leaves no rows", func(t *testing.T) {
		seg := openSegment(t)
		st, err := r.Get(ctx, "jb")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		holder, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin holder: %v", err)
		}
		defer func() { _ = holder.Rollback(ctx) }()
		if _, err := holder.Exec(ctx, `SELECT 1 FROM ingot.buckets WHERE name = 'jb' FOR UPDATE`); err != nil {
			t.Fatalf("hold bucket row: %v", err)
		}

		next := liveCid(t, "raced-root")
		commitDone := make(chan error, 1)
		go func() {
			commitDone <- r.CommitRoot(ctx, "jb", st.Root, next, registry.RootCommit{
				Journal: blockstore.Appended{Segment: seg, Blocks: []block.Block{block.NewBlock([]byte("raced"))}},
			})
		}()
		waitForLockWaiters(t, pool, 1)

		shipDone := make(chan error, 1)
		go func() {
			shipDone <- r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seg, time.Now().Unix(), nil,
				[]blockstore.OpRoot{{Bucket: "jb", Root: st.Root}, {Bucket: "jb", Root: next}})
		}()
		waitForLockWaiters(t, pool, 2)

		if err := holder.Commit(ctx); err != nil {
			t.Fatalf("release bucket row: %v", err)
		}
		for name, done := range map[string]chan error{"commit": commitDone, "ship": shipDone} {
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("%s did not finish", name)
			}
		}
		if got := journaled(t, seg); len(got) != 0 {
			t.Fatalf("journal after the raced ship = %v, want empty", got)
		}
		after, err := r.Get(ctx, "jb")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !after.Root.Equals(next) || !after.ForgeRoot.Equals(next) {
			t.Fatalf("root %s, forge root %s; want both %s", after.Root, after.ForgeRoot, next)
		}
	})

	// newBucket creates a bucket with a segment open for it.
	// openSegmentFor opens a further segment row for an existing bucket.
	openSegmentFor := func(t *testing.T, bucket string) uint64 {
		t.Helper()
		seq, err := r.NextSegmentSeq(ctx)
		if err != nil {
			t.Fatalf("NextSegmentSeq: %v", err)
		}
		if err := r.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq, bucket); err != nil {
			t.Fatalf("InsertSegmentOpen: %v", err)
		}
		return seq
	}
	// journaledFor counts the journal rows under seg, whatever their bucket.
	journaledFor := func(t *testing.T, seg uint64) []string {
		t.Helper()
		rows, err := pool.Query(ctx, `SELECT cid FROM ingot.catalog_journal WHERE seq = $1`, int64(seg))
		if err != nil {
			t.Fatalf("query journal: %v", err)
		}
		out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
			var c []byte
			err := row.Scan(&c)
			return string(c), err
		})
		if err != nil {
			t.Fatalf("journal rows: %v", err)
		}
		return out
	}

	newBucket := func(t *testing.T, name string) uint64 {
		t.Helper()
		if err := r.Create(ctx, name, testutil.RandomDID(t), registry.CreateState{Tenant: testutil.RandomDID(t)}); err != nil {
			t.Fatalf("Create %s: %v", name, err)
		}
		seq, err := r.NextSegmentSeq(ctx)
		if err != nil {
			t.Fatalf("NextSegmentSeq: %v", err)
		}
		if err := r.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq, name); err != nil {
			t.Fatalf("InsertSegmentOpen: %v", err)
		}
		return seq
	}

	// Journaling runs before the CAS, so the journal's foreign key is the
	// first thing to meet a deleted bucket. The commit still reports it as
	// one.
	t.Run("a commit to a deleted bucket is ErrNotFound", func(t *testing.T) {
		seg := newBucket(t, "jb-gone")
		if err := r.Delete(ctx, "jb-gone"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		err := r.CommitRoot(ctx, "jb-gone", cid.Undef, liveCid(t, "gone-root"), registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: []block.Block{block.NewBlock([]byte("gone"))}},
		})
		if !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("commit to a deleted bucket = %v, want ErrNotFound", err)
		}
	})

	// A bucket's deletion locks its row and then cascades into its journal
	// rows. A ship of one of its segments, waiting on that row, must not
	// already hold the journal rows, or the two deadlock. The deleting
	// transaction takes the bucket row first, so the ship blocks on it, and
	// only then deletes the bucket.
	t.Run("a ship racing its bucket's deletion doesn't deadlock", func(t *testing.T) {
		seg := newBucket(t, "jb-del")
		root := liveCid(t, "del-root")
		if err := r.CommitRoot(ctx, "jb-del", cid.Undef, root, registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: []block.Block{block.NewBlock([]byte("del"))}},
		}); err != nil {
			t.Fatalf("commit: %v", err)
		}
		deleter, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin deleter: %v", err)
		}
		defer func() { _ = deleter.Rollback(ctx) }()
		if _, err := deleter.Exec(ctx, `SELECT 1 FROM ingot.buckets WHERE name = 'jb-del' FOR UPDATE`); err != nil {
			t.Fatalf("lock bucket row: %v", err)
		}

		shipDone := make(chan error, 1)
		go func() {
			shipDone <- r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seg, time.Now().Unix(), nil,
				[]blockstore.OpRoot{{Bucket: "jb-del", Root: root}})
		}()
		waitForLockWaiters(t, pool, 1)

		if _, err := deleter.Exec(ctx, `DELETE FROM ingot.buckets WHERE name = 'jb-del'`); err != nil {
			t.Fatalf("delete bucket: %v", err)
		}
		if err := deleter.Commit(ctx); err != nil {
			t.Fatalf("commit deletion: %v", err)
		}
		select {
		case err := <-shipDone:
			if err != nil {
				t.Fatalf("ship: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("ship did not finish")
		}
		var left int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM ingot.catalog_journal WHERE seq = $1`, int64(seg)).Scan(&left); err != nil {
			t.Fatalf("count journal: %v", err)
		}
		if left != 0 {
			t.Fatalf("%d journal rows left for the deleted bucket, want 0", left)
		}
	})

	// The same race when the bucket's root has moved on since the segment's
	// op-root, so the forge-root advance's guard matches nothing and locks no
	// bucket row: the ship still waits for the deletion before it touches the
	// journal rows the deletion cascades into.
	t.Run("a ship with a stale guard still waits for its bucket's deletion", func(t *testing.T) {
		seg := newBucket(t, "jb-stale")
		old, next := liveCid(t, "stale-old"), liveCid(t, "stale-next")
		if err := r.CommitRoot(ctx, "jb-stale", cid.Undef, old, registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: []block.Block{block.NewBlock([]byte("stale"))}},
		}); err != nil {
			t.Fatalf("commit: %v", err)
		}
		if err := r.CASRoot(ctx, "jb-stale", old, next); err != nil {
			t.Fatalf("move the root on: %v", err)
		}
		deleter, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin deleter: %v", err)
		}
		defer func() { _ = deleter.Rollback(ctx) }()
		if _, err := deleter.Exec(ctx, `SELECT 1 FROM ingot.buckets WHERE name = 'jb-stale' FOR UPDATE`); err != nil {
			t.Fatalf("lock bucket row: %v", err)
		}

		shipDone := make(chan error, 1)
		go func() {
			shipDone <- r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seg, time.Now().Unix(), nil,
				[]blockstore.OpRoot{{Bucket: "jb-stale", Root: old}})
		}()
		waitForLockWaiters(t, pool, 1)

		if _, err := deleter.Exec(ctx, `DELETE FROM ingot.buckets WHERE name = 'jb-stale'`); err != nil {
			t.Fatalf("delete bucket: %v", err)
		}
		if err := deleter.Commit(ctx); err != nil {
			t.Fatalf("commit deletion: %v", err)
		}
		select {
		case err := <-shipDone:
			if err != nil {
				t.Fatalf("ship: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("ship did not finish")
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
		if err := r.CommitRoot(ctx, "jb-emptying", cid.Undef, old, registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: []block.Block{block.NewBlock([]byte("emptying-a"))}},
		}); err != nil {
			t.Fatalf("commit: %v", err)
		}
		cur := liveCid(t, "emptying-cur")
		if err := r.CommitRoot(ctx, "jb-emptying", old, cur, registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: []block.Block{block.NewBlock([]byte("emptying-b"))}},
		}); err != nil {
			t.Fatalf("second commit: %v", err)
		}
		holder, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin holder: %v", err)
		}
		defer func() { _ = holder.Rollback(ctx) }()
		if _, err := holder.Exec(ctx,
			`SELECT 1 FROM ingot.catalog_journal WHERE seq = $1 LIMIT 1 FOR UPDATE`, int64(seg)); err != nil {
			t.Fatalf("hold a journal row: %v", err)
		}

		emptyDone := make(chan error, 1)
		go func() { emptyDone <- r.EmptyRoot(ctx, "jb-emptying", cur) }()
		waitForLockedQuery(t, pool, "DELETE FROM ingot.catalog_journal WHERE bucket%")

		shipDone := make(chan error, 1)
		go func() {
			shipDone <- r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seg, time.Now().Unix(), nil,
				[]blockstore.OpRoot{{Bucket: "jb-emptying", Root: old}})
		}()
		if q := waitForLockedQuery(t, pool, "%FOR KEY SHARE%", "DELETE FROM ingot.catalog_journal WHERE seq%"); !strings.Contains(q, "FOR KEY SHARE") {
			t.Fatalf("the ship reached the journal rows while EmptyRoot was deleting them (waiting on %q)", q)
		}

		if err := holder.Commit(ctx); err != nil {
			t.Fatalf("release the journal row: %v", err)
		}
		for name, done := range map[string]chan error{"EmptyRoot": emptyDone, "ship": shipDone} {
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("%s did not finish", name)
			}
		}
		var shipped bool
		if err := pool.QueryRow(ctx, `SELECT shipped_at IS NOT NULL FROM ingot.segments WHERE seq = $1`, int64(seg)).Scan(&shipped); err != nil {
			t.Fatalf("read the segment: %v", err)
		}
		if !shipped {
			t.Fatal("the ship didn't stamp its segment")
		}
	})

	// DeleteBucket's first write empties the root, clears the forge root and
	// drops the bucket's journaled blocks, in one transaction. A stale expect
	// changes none of it, and another bucket's rows stay.
	t.Run("emptying the root drops the bucket's journal rows", func(t *testing.T) {
		seg := newBucket(t, "jb-empty")
		otherSeg := newBucket(t, "jb-other")
		root := liveCid(t, "empty-root")
		if err := r.CommitRoot(ctx, "jb-empty", cid.Undef, root, registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: []block.Block{block.NewBlock([]byte("emptied"))}},
		}); err != nil {
			t.Fatalf("commit: %v", err)
		}
		if err := r.CommitRoot(ctx, "jb-other", cid.Undef, liveCid(t, "other-root"), registry.RootCommit{
			Journal: blockstore.Appended{Segment: otherSeg, Blocks: []block.Block{block.NewBlock([]byte("other"))}},
		}); err != nil {
			t.Fatalf("commit other: %v", err)
		}
		if err := r.SetForgeRoot(ctx, "jb-empty", root); err != nil {
			t.Fatalf("SetForgeRoot: %v", err)
		}
		count := func(t *testing.T, bucket string) int {
			t.Helper()
			var n int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM ingot.catalog_journal WHERE bucket = $1`, bucket).Scan(&n); err != nil {
				t.Fatalf("count journal: %v", err)
			}
			return n
		}

		if err := r.EmptyRoot(ctx, "jb-empty", liveCid(t, "stale")); !errors.Is(err, registry.ErrConflict) {
			t.Fatalf("EmptyRoot with a stale expect = %v, want ErrConflict", err)
		}
		if st, err := r.Get(ctx, "jb-empty"); err != nil || !st.Root.Equals(root) || !st.ForgeRoot.Equals(root) || count(t, "jb-empty") != 1 {
			t.Fatalf("after a refused EmptyRoot: state %+v (err %v), %d journal rows; want both roots and the row kept",
				st, err, count(t, "jb-empty"))
		}

		if err := r.EmptyRoot(ctx, "jb-empty", root); err != nil {
			t.Fatalf("EmptyRoot: %v", err)
		}
		st, err := r.Get(ctx, "jb-empty")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if st.Root.Defined() || st.ForgeRoot.Defined() {
			t.Fatalf("root %s, forge root %s after EmptyRoot; want both empty", st.Root, st.ForgeRoot)
		}
		if n := count(t, "jb-empty"); n != 0 {
			t.Fatalf("%d journal rows left for the emptied bucket, want 0", n)
		}
		if n := count(t, "jb-other"); n != 1 {
			t.Fatalf("%d journal rows for the other bucket, want 1", n)
		}

		// A retried DeleteBucket empties an empty root again.
		if err := r.EmptyRoot(ctx, "jb-empty", cid.Undef); err != nil {
			t.Fatalf("EmptyRoot on an empty root: %v", err)
		}
		if err := r.EmptyRoot(ctx, "jb-missing", cid.Undef); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("EmptyRoot on a missing bucket = %v, want ErrNotFound", err)
		}
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
		if err := r.CommitRoot(ctx, "jb-rebuild", cid.Undef, liveCid(t, "rebuild-root"), registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: blocks},
		}); err != nil {
			t.Fatalf("commit: %v", err)
		}
		buckets, err := r.JournalBuckets(ctx)
		if err != nil || !slices.Contains(buckets, "jb-rebuild") {
			t.Fatalf("JournalBuckets = %v (err %v), want jb-rebuild among them", buckets, err)
		}
		if segs, err := r.JournalSegments(ctx, "jb-rebuild"); err != nil || !slices.Equal(segs, []uint64{seg}) {
			t.Fatalf("JournalSegments = %v (err %v), want [%d]", segs, err, seg)
		}

		var paged []string
		for after := cid.Undef; ; {
			page, err := r.ReadJournal(ctx, seg, after, 2)
			if err != nil {
				t.Fatalf("ReadJournal: %v", err)
			}
			for _, b := range page {
				paged = append(paged, string(b.Cid().Bytes()))
			}
			if len(page) < 2 {
				break
			}
			after = page[len(page)-1].Cid()
		}
		if len(paged) != len(blocks) || !slices.IsSorted(paged) {
			t.Fatalf("paged %d blocks (sorted %v), want all %d in CID order", len(paged), slices.IsSorted(paged), len(blocks))
		}
	})

	t.Run("a journal move re-files a range and keeps its age", func(t *testing.T) {
		from := newBucket(t, "jb-move")
		var blocks []block.Block
		for i := range 5 {
			blocks = append(blocks, block.NewBlock([]byte(fmt.Sprintf("move-%d", i))))
		}
		if err := r.CommitRoot(ctx, "jb-move", cid.Undef, liveCid(t, "move-root"), registry.RootCommit{
			Journal: blockstore.Appended{Segment: from, Blocks: blocks},
		}); err != nil {
			t.Fatalf("commit: %v", err)
		}
		slices.SortFunc(blocks, func(a, b block.Block) int { return bytes.Compare(a.Cid().Bytes(), b.Cid().Bytes()) })
		to := openSegmentFor(t, "jb-move")
		var before time.Time
		if err := pool.QueryRow(ctx, `SELECT created_at FROM ingot.catalog_journal WHERE seq = $1 AND cid = $2`,
			int64(from), blocks[1].Cid().Bytes()).Scan(&before); err != nil {
			t.Fatalf("read created_at: %v", err)
		}

		if err := r.MoveJournal(ctx, "jb-move", from, to, blocks[0].Cid(), blocks[2].Cid()); err != nil {
			t.Fatalf("MoveJournal: %v", err)
		}
		if got := journaledFor(t, from); len(got) != 2 {
			t.Fatalf("%d rows left under the source, want the 2 outside the range", len(got))
		}
		if got := journaledFor(t, to); len(got) != 3 {
			t.Fatalf("%d rows under the target, want the 3 in the range", len(got))
		}
		var after time.Time
		if err := pool.QueryRow(ctx, `SELECT created_at FROM ingot.catalog_journal WHERE seq = $1 AND cid = $2`,
			int64(to), blocks[1].Cid().Bytes()).Scan(&after); err != nil {
			t.Fatalf("read moved created_at: %v", err)
		}
		if !after.Equal(before) {
			t.Fatalf("moved row's created_at = %v, want %v", after, before)
		}

		// A target that has shipped since takes nothing; the source still goes.
		if err := r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, to, time.Now().Unix(), nil, nil); err != nil {
			t.Fatalf("MarkSegmentShipped: %v", err)
		}
		if err := r.MoveJournal(ctx, "jb-move", from, to, blocks[3].Cid(), blocks[4].Cid()); err != nil {
			t.Fatalf("MoveJournal to a shipped segment: %v", err)
		}
		if n := len(journaledFor(t, from)) + len(journaledFor(t, to)); n != 0 {
			t.Fatalf("%d rows left after a move onto a shipped segment, want 0", n)
		}
		if err := r.MoveJournal(ctx, "jb-move", from, 1<<62, blocks[0].Cid(), blocks[4].Cid()); err == nil {
			t.Fatal("MoveJournal onto a segment with no row succeeded")
		}
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
		if err := r.CommitRoot(ctx, "jb-wiped", cid.Undef, liveCid(t, "wiped-root"), registry.RootCommit{
			Journal: blockstore.Appended{Segment: seg, Blocks: blocks},
		}); err != nil {
			t.Fatalf("commit: %v", err)
		}
		dir := t.TempDir()
		m, err := logstore.OpenManager(ctx, logstore.ManagerConfig{
			Dir:     dir,
			Meta:    r,
			Catalog: logstore.PlaneConfig{SealBytes: 8 << 10, SealAge: time.Hour},
			Logger:  zaptest.NewLogger(t),
		})
		if err != nil {
			t.Fatalf("OpenManager: %v", err)
		}
		defer func() { _ = m.Close(ctx) }()
		for _, b := range blocks {
			if _, err := m.Get(ctx, b.Cid()); err != nil {
				t.Fatalf("Get %s after the rebuild: %v", b.Cid(), err)
			}
		}
		if got := journaledFor(t, seg); len(got) != 0 {
			t.Fatalf("%d journal rows still under the lost segment", len(got))
		}
		segs, err := r.JournalSegments(ctx, "jb-wiped")
		if err != nil || len(segs) < 2 {
			t.Fatalf("journal segments after the rebuild = %v (err %v), want the rows split across rebuilt segments", segs, err)
		}
		for _, s := range segs {
			if _, err := os.Stat(filepath.Join(dir, "jb-wiped", "catalog", fmt.Sprintf("seg-%020d.car", s))); err != nil {
				t.Fatalf("journal rows under segment %d, whose CAR isn't on disk: %v", s, err)
			}
		}
	})

	t.Run("the segment seq moves past disk and never back", func(t *testing.T) {
		start, err := r.NextSegmentSeq(ctx)
		if err != nil {
			t.Fatalf("NextSegmentSeq: %v", err)
		}
		if err := r.EnsureSegmentSeqAbove(ctx, start+10); err != nil {
			t.Fatalf("EnsureSegmentSeqAbove: %v", err)
		}
		if next, err := r.NextSegmentSeq(ctx); err != nil || next != start+11 {
			t.Fatalf("NextSegmentSeq after moving past %d = %d (err %v)", start+10, next, err)
		}
		if err := r.EnsureSegmentSeqAbove(ctx, start); err != nil {
			t.Fatalf("EnsureSegmentSeqAbove below: %v", err)
		}
		if next, err := r.NextSegmentSeq(ctx); err != nil || next != start+12 {
			t.Fatalf("NextSegmentSeq after a lower EnsureSegmentSeqAbove = %d (err %v), want %d", next, err, start+12)
		}
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
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("pg_stat_activity: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no backend blocked on a lock running %q", patterns)
	return ""
}

// waitForLockWaiters waits until at least n backends are blocked on a lock.
func waitForLockWaiters(t *testing.T, pool *pgxpool.Pool, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		if err := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database()`).Scan(&waiting); err != nil {
			t.Fatalf("pg_stat_activity: %v", err)
		}
		if waiting >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("fewer than %d backends blocked on a lock", n)
}
