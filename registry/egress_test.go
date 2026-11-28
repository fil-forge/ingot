package registry_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/testutil"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, r.AddEgress(context.Background(), counts), "AddEgress")
}

func dueEgress(t *testing.T, r *registry.Postgres, limit int) []registry.EgressCount {
	t.Helper()
	due, err := r.DueEgress(context.Background(), limit)
	require.NoError(t, err, "DueEgress")
	return due
}

func acknowledgeEgress(t *testing.T, r *registry.Postgres, counts ...registry.EgressCount) {
	t.Helper()
	require.NoError(t, r.AcknowledgeEgress(context.Background(), counts), "AcknowledgeEgress")
}

func egressBacklog(t *testing.T, r *registry.Postgres) registry.EgressBacklog {
	t.Helper()
	b, err := r.EgressBacklog(context.Background())
	require.NoError(t, err, "EgressBacklog")
	return b
}

func wantDue(t *testing.T, got []registry.EgressCount, want ...registry.EgressCount) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	require.Equal(t, want, got, "due egress")
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

	require.Error(t, r.AddEgress(context.Background(), []registry.EgressCount{{Space: space, Hour: egressHour0, Bytes: -1}}), "AddEgress accepted a negative count")
}

// Due counts come oldest hour first, within the limit, so a backlog is worked
// through in order across several reports. Spaces within one hour come in no
// promised order: the database sorts them by its own collation.
func TestDueEgressIsOldestFirstAndLimited(t *testing.T) {
	r := egressStore(t)
	a, b := testutil.RandomDID(t), testutil.RandomDID(t)
	addEgress(t, r,
		registry.EgressCount{Space: b, Hour: egressHour1, Bytes: 4},
		registry.EgressCount{Space: b, Hour: egressHour0, Bytes: 2},
		registry.EgressCount{Space: a, Hour: egressHour0, Bytes: 1},
	)

	wantDueOldestFirst(t, dueEgress(t, r, 2),
		registry.EgressCount{Space: a, Hour: egressHour0, Bytes: 1},
		registry.EgressCount{Space: b, Hour: egressHour0, Bytes: 2},
	)
	wantDueOldestFirst(t, dueEgress(t, r, 10),
		registry.EgressCount{Space: a, Hour: egressHour0, Bytes: 1},
		registry.EgressCount{Space: b, Hour: egressHour0, Bytes: 2},
		registry.EgressCount{Space: b, Hour: egressHour1, Bytes: 4},
	)
}

// wantDueOldestFirst checks got is in hour order and holds the same counts as
// want, ignoring the order of spaces within an hour.
func wantDueOldestFirst(t *testing.T, got []registry.EgressCount, want ...registry.EgressCount) {
	t.Helper()
	for i := 1; i < len(got); i++ {
		require.False(t, got[i].Hour.Before(got[i-1].Hour), "due egress out of hour order:\n got %v", got)
	}
	byHourThenSpace := func(c []registry.EgressCount) []registry.EgressCount {
		c = slices.Clone(c)
		slices.SortFunc(c, func(x, y registry.EgressCount) int {
			if n := x.Hour.Compare(y.Hour); n != 0 {
				return n
			}
			return strings.Compare(x.Space.String(), y.Space.String())
		})
		return c
	}
	wantDue(t, byHourThenSpace(got), byHourThenSpace(want)...)
}

// An acknowledged count is no longer due, until more is sent in its hour,
// when it is due again with its new total.
func TestAcknowledgedEgressIsDueAgainWhenItGrows(t *testing.T) {
	r := egressStore(t)
	space := testutil.RandomDID(t)
	addEgress(t, r, registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 10})

	acknowledgeEgress(t, r, dueEgress(t, r, 10)...)
	wantDue(t, dueEgress(t, r, 10))
	b := egressBacklog(t, r)
	require.Zero(t, b.Counts, "backlog after acknowledging everything: %+v", b)
	require.True(t, b.Oldest.IsZero(), "backlog after acknowledging everything: %+v", b)

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
	require.Equal(t, int64(1), after.Counts, "backlog: got %+v, want one count due since %s", after, before.Oldest)
	require.True(t, after.Oldest.Equal(before.Oldest), "backlog: got %+v, want one count due since %s", after, before.Oldest)

	// Acknowledging the total it reached settles it.
	acknowledgeEgress(t, r, dueEgress(t, r, 10)...)
	wantDue(t, dueEgress(t, r, 10))
}

// Two instances may report one count at different totals, and the lower may
// be acknowledged last. The service keeps the larger, so the row stays as the
// larger acknowledgement left it.
func TestAcknowledgeAtALowerTotalChangesNothing(t *testing.T) {
	r := egressStore(t)
	space := testutil.RandomDID(t)
	addEgress(t, r, registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 150})
	acknowledgeEgress(t, r, registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 150})
	acknowledgeEgress(t, r, registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 100})
	wantDue(t, dueEgress(t, r, 10))

	// The same while the row is still due: the larger total stays recorded.
	addEgress(t, r, registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 50})
	acknowledgeEgress(t, r, registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 100})
	wantDue(t, dueEgress(t, r, 10), registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 200})
	acknowledgeEgress(t, r, registry.EgressCount{Space: space, Hour: egressHour0, Bytes: 200})
	wantDue(t, dueEgress(t, r, 10))
}

func TestEgressBacklogCountsWhatIsDue(t *testing.T) {
	r := egressStore(t)
	a, b := testutil.RandomDID(t), testutil.RandomDID(t)
	got := egressBacklog(t, r)
	require.Zero(t, got.Counts, "empty backlog")
	require.True(t, got.Oldest.IsZero(), "empty backlog: %+v", got)

	start := time.Now().Add(-time.Minute)
	addEgress(t, r,
		registry.EgressCount{Space: a, Hour: egressHour0, Bytes: 1},
		registry.EgressCount{Space: b, Hour: egressHour0, Bytes: 1},
	)
	got = egressBacklog(t, r)
	require.Equal(t, int64(2), got.Counts, "backlog counts")
	require.False(t, got.Oldest.Before(start), "backlog oldest %s is not around now", got.Oldest)
	require.False(t, got.Oldest.After(time.Now().Add(time.Minute)), "backlog oldest %s is not around now", got.Oldest)

	acknowledgeEgress(t, r, registry.EgressCount{Space: a, Hour: egressHour0, Bytes: 1})
	require.Equal(t, int64(1), egressBacklog(t, r).Counts, "backlog counts after one acknowledgement")
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
		require.NoError(t, err, "PruneEgress")
		return n
	}
	n := prune(time.Now().Add(-time.Hour))
	require.Zero(t, n, "pruned counts acknowledged a moment ago")
	require.Equal(t, int64(1), prune(time.Now().Add(time.Hour)), "pruned counts, want the 1 that is settled")
	wantDue(t, dueEgress(t, r, 10), registry.EgressCount{Space: due, Hour: egressHour0, Bytes: 20})

}
