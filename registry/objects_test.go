package registry_test

import (
	"context"
	"testing"
	"time"

	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/registry"
)

// objectFixture is a registry holding one bucket, whose objects a test counts
// as a commit would.
type objectFixture struct {
	pool  *pgxpool.Pool
	r     *registry.Postgres
	name  string
	space did.DID
}

func newObjectFixture(t *testing.T) *objectFixture {
	t.Helper()
	pool := pgtest.New(t)
	f := &objectFixture{pool: pool, r: registry.NewPostgres(pool), name: "objects-bucket", space: testutil.RandomDID(t)}
	require.NoError(t, f.r.Create(context.Background(), f.name, f.space, registry.CreateState{Tenant: testutil.RandomDID(t)}), "create bucket")
	return f
}

// count applies a change's object counts, in one transaction as its commit
// does.
func (f *objectFixture) count(t *testing.T, added, removed int64) {
	t.Helper()
	ctx := context.Background()
	tx, err := f.pool.Begin(ctx)
	require.NoError(t, err, "begin")
	defer func() { _ = tx.Rollback(ctx) }()
	for _, st := range registry.ObjectCountWrites(f.name, added, removed) {
		_, err = tx.Exec(ctx, st.SQL, st.Args...)
		require.NoError(t, err, "object count write")
	}
	require.NoError(t, tx.Commit(ctx), "commit")
}

// age moves the space's counts back by d, as if they had been written then.
func (f *objectFixture) age(t *testing.T, d time.Duration) {
	t.Helper()
	_, err := f.pool.Exec(context.Background(),
		`UPDATE ingot.object_usage SET hour = hour - $2::interval WHERE space = $1`,
		f.space.String(), d.String())
	require.NoError(t, err, "age counts")
}

func (f *objectFixture) due(t *testing.T) []registry.ObjectCount {
	t.Helper()
	due, err := f.r.DueObjects(context.Background(), 100)
	require.NoError(t, err, "DueObjects")
	return due
}

func (f *objectFixture) acknowledge(t *testing.T, counts ...registry.ObjectCount) {
	t.Helper()
	require.NoError(t, f.r.AcknowledgeObjects(context.Background(), counts), "AcknowledgeObjects")
}

// totals strips the hours from counts, which are the database's own.
func totals(counts []registry.ObjectCount) [][2]int64 {
	out := [][2]int64{}
	for _, c := range counts {
		out = append(out, [2]int64{c.Added, c.Removed})
	}
	return out
}

func wantTotals(t *testing.T, got []registry.ObjectCount, want ...[2]int64) {
	t.Helper()
	if want == nil {
		want = [][2]int64{}
	}
	require.Equal(t, want, totals(got), "object counts")
}

// A change adds to the bucket's totals, and the hour's count holds the totals
// as the latest change in it left them.
func TestObjectCountWritesKeepTheBucketsTotalsByHour(t *testing.T) {
	f := newObjectFixture(t)

	f.count(t, 3, 0)
	f.count(t, 2, 1)
	due := f.due(t)
	wantTotals(t, due, [2]int64{5, 1})
	require.Equal(t, f.space, due[0].Space, "counted for space")
	require.True(t, due[0].Hour.Equal(due[0].Hour.Truncate(time.Hour)), "counted in hour %s, want the current whole hour", due[0].Hour)
	require.LessOrEqual(t, time.Since(due[0].Hour), 2*time.Hour, "counted in hour %s, want the current whole hour", due[0].Hour)

	// A change in a later hour starts that hour's count from the totals so
	// far, and leaves the earlier hour's as it was.
	f.age(t, time.Hour)
	f.count(t, 0, 4)
	wantTotals(t, f.due(t), [2]int64{5, 1}, [2]int64{5, 5})

	// A change that counts nothing writes nothing.
	w := registry.ObjectCountWrites(f.name, 0, 0)
	require.Empty(t, w, "a change of no objects has %d writes", len(w))
}

// A bucket that is gone is counted nowhere, and what its space has counted
// stays for the reporter.
func TestObjectCountWritesForAMissingBucketWriteNothing(t *testing.T) {
	f := newObjectFixture(t)
	f.count(t, 2, 2)
	require.NoError(t, f.r.Delete(context.Background(), f.name, f.space), "delete bucket")
	f.count(t, 1, 0)
	wantTotals(t, f.due(t), [2]int64{2, 2})
}

func TestAcknowledgedObjectsAreDueAgainWhenTheyGrow(t *testing.T) {
	f := newObjectFixture(t)
	f.count(t, 3, 1)
	first := f.due(t)
	f.acknowledge(t, first...)
	wantTotals(t, f.due(t))

	f.count(t, 0, 1)
	wantTotals(t, f.due(t), [2]int64{3, 2})
}

// A count that grew between the read and the acknowledgement stays due, and
// an older report acknowledged late changes nothing.
func TestAcknowledgeObjectsKeepsTheLargestReported(t *testing.T) {
	f := newObjectFixture(t)
	f.count(t, 3, 0)
	older := f.due(t)
	f.count(t, 1, 2)
	newer := f.due(t)

	f.acknowledge(t, older...)
	wantTotals(t, f.due(t), [2]int64{4, 2})
	f.acknowledge(t, newer...)
	wantTotals(t, f.due(t))

	f.acknowledge(t, older...)
	wantTotals(t, f.due(t))
	b, err := f.r.ObjectBacklog(context.Background())
	require.NoError(t, err, "ObjectBacklog")
	require.Zero(t, b.Counts, "backlog %+v", b)
	require.True(t, b.Oldest.IsZero(), "backlog %+v, want none", b)
}

func TestObjectBacklogCountsWhatIsDue(t *testing.T) {
	f := newObjectFixture(t)
	before := time.Now().Add(-time.Minute)
	f.count(t, 1, 0)
	f.age(t, time.Hour)
	f.count(t, 1, 0)

	b, err := f.r.ObjectBacklog(context.Background())
	require.NoError(t, err, "ObjectBacklog")
	require.Equal(t, int64(2), b.Counts, "backlog %+v, want 2 counts due since about now", b)
	require.False(t, b.Oldest.Before(before), "backlog %+v, want 2 counts due since about now", b)
	require.False(t, b.Oldest.After(time.Now().Add(time.Minute)), "backlog %+v, want 2 counts due since about now", b)
}

// Only counts that are up to date and were acknowledged before the cutoff are
// pruned, and the bucket's totals carry on from where they were.
func TestPruneObjects(t *testing.T) {
	f := newObjectFixture(t)
	ctx := context.Background()
	f.count(t, 4, 1)
	f.acknowledge(t, f.due(t)...)
	f.age(t, time.Hour)
	f.count(t, 1, 0)

	n, err := f.r.PruneObjects(ctx, time.Now().Add(-time.Hour))
	require.NoError(t, err, "prune before the cutoff")
	require.Zero(t, n, "pruned before the cutoff")
	// The acknowledged count goes; the one still due stays.
	n, err = f.r.PruneObjects(ctx, time.Now().Add(time.Hour))
	require.NoError(t, err, "prune")
	require.Equal(t, int64(1), n, "pruned")
	wantTotals(t, f.due(t), [2]int64{5, 1})

	f.acknowledge(t, f.due(t)...)
	n, err = f.r.PruneObjects(ctx, time.Now().Add(time.Hour))
	require.NoError(t, err, "prune")
	require.Equal(t, int64(1), n, "pruned")
	f.count(t, 0, 2)
	wantTotals(t, f.due(t), [2]int64{5, 3})
}

// TestBucketTotalsSumTheBucketsObjects: the totals count every bucket and
// what each holds now, gained less lost, and a deleted bucket leaves both.
func TestBucketTotalsSumTheBucketsObjects(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	r := registry.NewPostgres(pool)
	wantBucketTotals := func(want registry.BucketTotals, msg string) {
		t.Helper()
		got, err := r.BucketTotals(ctx)
		require.NoError(t, err, "BucketTotals")
		require.Equal(t, want, got, "totals: %s", msg)
	}
	wantBucketTotals(registry.BucketTotals{}, "no buckets")

	first := &objectFixture{pool: pool, r: r, name: "first", space: testutil.RandomDID(t)}
	second := &objectFixture{pool: pool, r: r, name: "second", space: testutil.RandomDID(t)}
	for _, f := range []*objectFixture{first, second} {
		require.NoError(t, r.Create(ctx, f.name, f.space, registry.CreateState{Tenant: testutil.RandomDID(t)}), "create bucket %s", f.name)
	}
	wantBucketTotals(registry.BucketTotals{Buckets: 2}, "two empty buckets")

	first.count(t, 5, 2)
	second.count(t, 4, 0)
	wantBucketTotals(registry.BucketTotals{Buckets: 2, Objects: 7}, "3 in one bucket and 4 in the other")

	require.NoError(t, r.Delete(ctx, second.name, second.space), "delete bucket")
	wantBucketTotals(registry.BucketTotals{Buckets: 1, Objects: 3}, "the deleted bucket's objects have gone with it")
}
