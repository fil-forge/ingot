package registry_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
	"github.com/jackc/pgx/v5/pgxpool"

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
	if err := f.r.Create(context.Background(), f.name, f.space, registry.CreateState{Tenant: testutil.RandomDID(t)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return f
}

// count applies a change's object counts, in one transaction as its commit
// does.
func (f *objectFixture) count(t *testing.T, added, removed int64) {
	t.Helper()
	ctx := context.Background()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, st := range registry.ObjectCountWrites(f.name, added, removed) {
		if _, err := tx.Exec(ctx, st.SQL, st.Args...); err != nil {
			t.Fatalf("object count write: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// age moves the space's counts back by d, as if they had been written then.
func (f *objectFixture) age(t *testing.T, d time.Duration) {
	t.Helper()
	if _, err := f.pool.Exec(context.Background(),
		`UPDATE ingot.object_usage SET hour = hour - $2::interval WHERE space = $1`,
		f.space.String(), d.String()); err != nil {
		t.Fatalf("age counts: %v", err)
	}
}

func (f *objectFixture) due(t *testing.T) []registry.ObjectCount {
	t.Helper()
	due, err := f.r.DueObjects(context.Background(), 100)
	if err != nil {
		t.Fatalf("DueObjects: %v", err)
	}
	return due
}

func (f *objectFixture) acknowledge(t *testing.T, counts ...registry.ObjectCount) {
	t.Helper()
	if err := f.r.AcknowledgeObjects(context.Background(), counts); err != nil {
		t.Fatalf("AcknowledgeObjects: %v", err)
	}
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
	if !reflect.DeepEqual(totals(got), want) {
		t.Fatalf("object counts:\n got %v\nwant %v", totals(got), want)
	}
}

// A change adds to the bucket's totals, and the hour's count holds the totals
// as the latest change in it left them.
func TestObjectCountWritesKeepTheBucketsTotalsByHour(t *testing.T) {
	f := newObjectFixture(t)

	f.count(t, 3, 0)
	f.count(t, 2, 1)
	due := f.due(t)
	wantTotals(t, due, [2]int64{5, 1})
	if due[0].Space != f.space {
		t.Fatalf("counted for space %s, want %s", due[0].Space, f.space)
	}
	if !due[0].Hour.Equal(due[0].Hour.Truncate(time.Hour)) || time.Since(due[0].Hour) > 2*time.Hour {
		t.Fatalf("counted in hour %s, want the current whole hour", due[0].Hour)
	}

	// A change in a later hour starts that hour's count from the totals so
	// far, and leaves the earlier hour's as it was.
	f.age(t, time.Hour)
	f.count(t, 0, 4)
	wantTotals(t, f.due(t), [2]int64{5, 1}, [2]int64{5, 5})

	// A change that counts nothing writes nothing.
	if w := registry.ObjectCountWrites(f.name, 0, 0); len(w) != 0 {
		t.Fatalf("a change of no objects has %d writes", len(w))
	}
}

// A bucket that is gone is counted nowhere, and what its space has counted
// stays for the reporter.
func TestObjectCountWritesForAMissingBucketWriteNothing(t *testing.T) {
	f := newObjectFixture(t)
	f.count(t, 2, 2)
	if err := f.r.Delete(context.Background(), f.name, f.space); err != nil {
		t.Fatalf("delete bucket: %v", err)
	}
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
	if err != nil {
		t.Fatalf("ObjectBacklog: %v", err)
	}
	if b.Counts != 0 || !b.Oldest.IsZero() {
		t.Fatalf("backlog %+v, want none", b)
	}
}

func TestObjectBacklogCountsWhatIsDue(t *testing.T) {
	f := newObjectFixture(t)
	before := time.Now().Add(-time.Minute)
	f.count(t, 1, 0)
	f.age(t, time.Hour)
	f.count(t, 1, 0)

	b, err := f.r.ObjectBacklog(context.Background())
	if err != nil {
		t.Fatalf("ObjectBacklog: %v", err)
	}
	if b.Counts != 2 || b.Oldest.Before(before) || b.Oldest.After(time.Now().Add(time.Minute)) {
		t.Fatalf("backlog %+v, want 2 counts due since about now", b)
	}
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

	if n, err := f.r.PruneObjects(ctx, time.Now().Add(-time.Hour)); err != nil || n != 0 {
		t.Fatalf("pruned %d (%v) before the cutoff, want 0", n, err)
	}
	// The acknowledged count goes; the one still due stays.
	if n, err := f.r.PruneObjects(ctx, time.Now().Add(time.Hour)); err != nil || n != 1 {
		t.Fatalf("pruned %d (%v), want 1", n, err)
	}
	wantTotals(t, f.due(t), [2]int64{5, 1})

	f.acknowledge(t, f.due(t)...)
	if n, err := f.r.PruneObjects(ctx, time.Now().Add(time.Hour)); err != nil || n != 1 {
		t.Fatalf("pruned %d (%v), want 1", n, err)
	}
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
		if err != nil {
			t.Fatalf("BucketTotals: %v", err)
		}
		if got != want {
			t.Fatalf("totals = %+v, want %+v: %s", got, want, msg)
		}
	}
	wantBucketTotals(registry.BucketTotals{}, "no buckets")

	first := &objectFixture{pool: pool, r: r, name: "first", space: testutil.RandomDID(t)}
	second := &objectFixture{pool: pool, r: r, name: "second", space: testutil.RandomDID(t)}
	for _, f := range []*objectFixture{first, second} {
		if err := r.Create(ctx, f.name, f.space, registry.CreateState{Tenant: testutil.RandomDID(t)}); err != nil {
			t.Fatalf("create bucket %s: %v", f.name, err)
		}
	}
	wantBucketTotals(registry.BucketTotals{Buckets: 2}, "two empty buckets")

	first.count(t, 5, 2)
	second.count(t, 4, 0)
	wantBucketTotals(registry.BucketTotals{Buckets: 2, Objects: 7}, "3 in one bucket and 4 in the other")

	if err := r.Delete(ctx, second.name, second.space); err != nil {
		t.Fatalf("delete bucket: %v", err)
	}
	wantBucketTotals(registry.BucketTotals{Buckets: 1, Objects: 3}, "the deleted bucket's objects have gone with it")
}
