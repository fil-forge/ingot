package registry_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
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
