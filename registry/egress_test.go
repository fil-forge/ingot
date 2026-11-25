package registry_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/testutil"
)

var (
	egressHour0 = time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	egressHour1 = egressHour0.Add(time.Hour)
)

func egressStore(t *testing.T) *registry.Postgres {
	t.Helper()
	return registry.NewPostgres(pgtest.New(t))
}

func addEgress(t *testing.T, r *registry.Postgres, counts ...registry.EgressCount) {
	t.Helper()
	if err := r.AddEgress(context.Background(), counts); err != nil {
		t.Fatalf("AddEgress: %v", err)
	}
}

func dueEgress(t *testing.T, r *registry.Postgres, limit int) []registry.EgressCount {
	t.Helper()
	due, err := r.DueEgress(context.Background(), limit)
	if err != nil {
		t.Fatalf("DueEgress: %v", err)
	}
	return due
}

func acknowledgeEgress(t *testing.T, r *registry.Postgres, counts ...registry.EgressCount) {
	t.Helper()
	if err := r.AcknowledgeEgress(context.Background(), counts); err != nil {
		t.Fatalf("AcknowledgeEgress: %v", err)
	}
}

func egressBacklog(t *testing.T, r *registry.Postgres) registry.EgressBacklog {
	t.Helper()
	b, err := r.EgressBacklog(context.Background())
	if err != nil {
		t.Fatalf("EgressBacklog: %v", err)
	}
	return b
}

func wantDue(t *testing.T, got []registry.EgressCount, want ...registry.EgressCount) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("due egress:\n got %v\nwant %v", got, want)
	}
}

// Adds to one space and hour accumulate, from one call or several, as the
// instances sharing a database add into the same rows.
func TestAddEgressAccumulates(t *testing.T) {
	r := egressStore(t)
	space := testutil.RandomDID(t)

	addEgress(t, r,
		registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 10},
		registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 5},
		registry.EgressCount{Space: space, Hour: egressHour1, Bytes: 7},
		registry.EgressCount{Space: space, Hour: egressHour1, Bytes: 0},
	)
	addEgress(t, r, registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 100})

	wantDue(t, dueEgress(t, r, 10),
		registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 115},
		registry.EgressCount{Space: space, Hour: egressHour1, Bytes: 7},
	)
}

func TestAddEgressIgnoresNothingAndRefusesANegativeCount(t *testing.T) {
	r := egressStore(t)
	space := testutil.RandomDID(t)

	addEgress(t, r)
	addEgress(t, r, registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 0})
	wantDue(t, dueEgress(t, r, 10))

	if err := r.AddEgress(context.Background(), []registry.EgressCount{{Space: space, Hour: egressHour0, Bytes: -1}}); err == nil {
		t.Fatal("AddEgress accepted a negative count")
	}
}

// Due counts come oldest hour first, within the limit, so a backlog is worked
// through in order across several reports.
func TestDueEgressIsOldestFirstAndLimited(t *testing.T) {
	r := egressStore(t)
	a, b := testutil.RandomDID(t), testutil.RandomDID(t)
	if a.String() > b.String() {
		a, b = b, a
	}
	addEgress(t, r,
		registry.EgressCount{Space: b, Hour: egressHour1, Bytes: 4},
		registry.EgressCount{Space: b, Hour: egressHour0, Bytes: 2},
		registry.EgressCount{Space: a, Hour: egressHour0, Bytes: 1},
	)

	wantDue(t, dueEgress(t, r, 2),
		registry.EgressCount{Space: a, Hour: egressHour0, Bytes: 1},
		registry.EgressCount{Space: b, Hour: egressHour0, Bytes: 2},
	)
	wantDue(t, dueEgress(t, r, 10),
		registry.EgressCount{Space: a, Hour: egressHour0, Bytes: 1},
		registry.EgressCount{Space: b, Hour: egressHour0, Bytes: 2},
		registry.EgressCount{Space: b, Hour: egressHour1, Bytes: 4},
	)
}

// An acknowledged count is no longer due, until more is sent in its hour,
// when it is due again with its new total.
func TestAcknowledgedEgressIsDueAgainWhenItGrows(t *testing.T) {
	r := egressStore(t)
	space := testutil.RandomDID(t)
	addEgress(t, r, registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 10})

	acknowledgeEgress(t, r, dueEgress(t, r, 10)...)
	wantDue(t, dueEgress(t, r, 10))
	if b := egressBacklog(t, r); b.Counts != 0 || !b.Oldest.IsZero() {
		t.Fatalf("backlog after acknowledging everything: %+v", b)
	}

	addEgress(t, r, registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 5})
	wantDue(t, dueEgress(t, r, 10), registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 15})
}

// A count that grew between being read and being acknowledged stays due: the
// service holds the total that was sent to it, not the row's.
func TestAcknowledgeLeavesACountThatGrewDue(t *testing.T) {
	r := egressStore(t)
	space := testutil.RandomDID(t)
	addEgress(t, r, registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 10})
	read := dueEgress(t, r, 10)
	before := egressBacklog(t, r)

	addEgress(t, r, registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 5})
	acknowledgeEgress(t, r, read...)

	wantDue(t, dueEgress(t, r, 10), registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 15})
	// It has been due since before the report went out, and its age says so.
	after := egressBacklog(t, r)
	if after.Counts != 1 || !after.Oldest.Equal(before.Oldest) {
		t.Fatalf("backlog: got %+v, want one count due since %s", after, before.Oldest)
	}

	// Acknowledging the total it reached settles it.
	acknowledgeEgress(t, r, dueEgress(t, r, 10)...)
	wantDue(t, dueEgress(t, r, 10))
}

func TestEgressBacklogCountsWhatIsDue(t *testing.T) {
	r := egressStore(t)
	a, b := testutil.RandomDID(t), testutil.RandomDID(t)
	if got := egressBacklog(t, r); got.Counts != 0 || !got.Oldest.IsZero() {
		t.Fatalf("empty backlog: %+v", got)
	}

	start := time.Now().Add(-time.Minute)
	addEgress(t, r,
		registry.EgressCount{Space: a, Hour: egressHour0, Bytes: 1},
		registry.EgressCount{Space: b, Hour: egressHour0, Bytes: 1},
	)
	got := egressBacklog(t, r)
	if got.Counts != 2 {
		t.Fatalf("backlog counts: got %d, want 2", got.Counts)
	}
	if got.Oldest.Before(start) || got.Oldest.After(time.Now().Add(time.Minute)) {
		t.Fatalf("backlog oldest %s is not around now", got.Oldest)
	}

	acknowledgeEgress(t, r, registry.EgressCount{Space: a, Hour: egressHour0, Bytes: 1})
	if got := egressBacklog(t, r); got.Counts != 1 {
		t.Fatalf("backlog counts after one acknowledgement: got %d, want 1", got.Counts)
	}
}

// Pruning takes only counts that are up to date and were acknowledged long
// enough ago; a count still due is never pruned, however old.
func TestPruneEgress(t *testing.T) {
	r := egressStore(t)
	settled, due := testutil.RandomDID(t), testutil.RandomDID(t)
	addEgress(t, r,
		registry.EgressCount{Space: settled, Hour: egressHour0, Bytes: 10},
		registry.EgressCount{Space: due, Hour: egressHour0, Bytes: 20},
	)
	acknowledgeEgress(t, r, registry.EgressCount{Space: settled, Hour: egressHour0, Bytes: 10})

	prune := func(before time.Time) int64 {
		t.Helper()
		n, err := r.PruneEgress(context.Background(), before)
		if err != nil {
			t.Fatalf("PruneEgress: %v", err)
		}
		return n
	}
	if n := prune(time.Now().Add(-time.Hour)); n != 0 {
		t.Fatalf("pruned %d counts acknowledged a moment ago", n)
	}
	if n := prune(time.Now().Add(time.Hour)); n != 1 {
		t.Fatalf("pruned %d counts, want the 1 that is settled", n)
	}
	wantDue(t, dueEgress(t, r, 10), registry.EgressCount{Space: due, Hour: egressHour0, Bytes: 20})

}
