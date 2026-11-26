package registry

import (
	"context"
	"fmt"
	"time"

	"github.com/fil-forge/ucantone/did"
)

// ObjectCount is the objects one bucket's space has gained and lost over its
// life, as of one hour.
type ObjectCount struct {
	// Space is the bucket's space.
	Space did.DID
	// Hour is the start of the hour, in UTC.
	Hour time.Time
	// Added is the objects the space had ever gained.
	Added int64
	// Removed is the objects the space had ever lost.
	Removed int64
}

// ObjectBacklog is the object counts waiting to be reported.
type ObjectBacklog struct {
	// Counts is how many counts have changed since they were last
	// acknowledged.
	Counts int64
	// Oldest is when the longest-waiting of them became due. It is the zero
	// time when Counts is zero.
	Oldest time.Time
}

// ObjectUsageStore holds each space's object counts by the hour, and which of
// them the auth service has acknowledged.
//
// A bucket counts every version of every key, delete markers included, and
// every part of a multipart upload still in progress, as two totals over its
// life: the objects it has gained and the objects it has lost. The writes that change them ([ObjectCountWrites]) copy the totals to
// the current hour's count. Both only grow, and the service keeps the largest
// of each it is sent for an hour. So a count is due for reporting whenever it
// differs from the one last acknowledged, and reporting one twice, or two
// instances reporting it in either order, does no harm.
type ObjectUsageStore interface {
	// DueObjects returns up to limit counts that have changed since they were
	// last acknowledged, oldest hour first.
	DueObjects(ctx context.Context, limit int) ([]ObjectCount, error)
	// AcknowledgeObjects records that the service holds each count's totals.
	// A count that has grown since it was read stays due. Totals below the
	// ones already acknowledged change nothing: the service holds the larger.
	AcknowledgeObjects(ctx context.Context, reported []ObjectCount) error
	// PruneObjects deletes the counts that are up to date and were last
	// acknowledged before the given time, and returns how many it deleted.
	PruneObjects(ctx context.Context, before time.Time) (int64, error)
	// ObjectBacklog returns how many object counts are waiting to be
	// reported.
	ObjectBacklog(ctx context.Context) (ObjectBacklog, error)
}

// UsageStore holds the usage counts the gateway reports to the auth service.
type UsageStore interface {
	EgressUsageStore
	ObjectUsageStore
}

var _ UsageStore = (*Postgres)(nil)

// ObjectCountWrites returns the writes that add to bucket name's totals of
// objects added and removed, and copy the totals to the current hour's usage
// count, for the task that changes the bucket's objects to commit with the
// change. It returns none when both are zero.
//
// The hour is the database's, so instances sharing it agree on which hour a
// change falls in. A bucket already gone is counted nowhere: its space's
// counts stand as its last change left them.
func ObjectCountWrites(name string, added, removed int64) []Statement {
	return objectCountWrites("name", name, added, removed)
}

// SpaceObjectCountWrites is [ObjectCountWrites] for the bucket whose space is
// space, for a change that knows its bucket by the space it began in, such as
// one to a multipart upload: a bucket created later under the same name is
// another space, and is not counted for it.
func SpaceObjectCountWrites(space did.DID, added, removed int64) []Statement {
	return objectCountWrites("space", space.String(), added, removed)
}

// objectCountWrites counts for the bucket whose column, name or space, holds
// value.
func objectCountWrites(column, value string, added, removed int64) []Statement {
	if added == 0 && removed == 0 {
		return nil
	}
	return []Statement{
		{
			SQL: `UPDATE ingot.buckets
			         SET objects_added = objects_added + $2, objects_removed = objects_removed + $3
			       WHERE ` + column + ` = $1`,
			Args: []any{value, added, removed},
		},
		{
			// The totals only grow and this transaction holds the bucket's
			// row, so the hour's count is behind them. The condition keeps
			// a count that is not from being marked due.
			SQL: `INSERT INTO ingot.object_usage AS u (space, hour, added, removed, dirty_since)
			      SELECT space, date_trunc('hour', now(), 'UTC'), objects_added, objects_removed, now()
			        FROM ingot.buckets WHERE ` + column + ` = $1
			      ON CONFLICT (space, hour) DO UPDATE
			      SET added = GREATEST(u.added, EXCLUDED.added),
			          removed = GREATEST(u.removed, EXCLUDED.removed),
			          dirty_since = COALESCE(u.dirty_since, now())
			      WHERE u.added < EXCLUDED.added OR u.removed < EXCLUDED.removed`,
			Args: []any{value},
		},
	}
}

func (r *Postgres) DueObjects(ctx context.Context, limit int) ([]ObjectCount, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT space, hour, added, removed
		FROM ingot.object_usage
		WHERE added <> added_reported OR removed <> removed_reported
		ORDER BY hour, space
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("registry: due objects: %w", err)
	}
	defer rows.Close()
	var due []ObjectCount
	for rows.Next() {
		var (
			space string
			c     ObjectCount
		)
		if err := rows.Scan(&space, &c.Hour, &c.Added, &c.Removed); err != nil {
			return nil, fmt.Errorf("registry: due objects: %w", err)
		}
		if c.Space, err = did.Parse(space); err != nil {
			return nil, fmt.Errorf("registry: due objects: space %q: %w", space, err)
		}
		c.Hour = c.Hour.UTC()
		due = append(due, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: due objects: %w", err)
	}
	return due, nil
}

func (r *Postgres) AcknowledgeObjects(ctx context.Context, reported []ObjectCount) error {
	if len(reported) == 0 {
		return nil
	}
	spaces := make([]string, len(reported))
	hours := make([]time.Time, len(reported))
	added := make([]int64, len(reported))
	removed := make([]int64, len(reported))
	for i, c := range reported {
		spaces[i] = c.Space.String()
		hours[i] = c.Hour.UTC()
		added[i] = c.Added
		removed[i] = c.Removed
	}
	// A row that grew after it was read keeps its dirty_since: it has been
	// due since before this report went out. The totals of one report are
	// recorded together, and only when neither is behind what is already
	// acknowledged, so another instance's older report, acknowledged late,
	// leaves the row alone.
	_, err := r.pool.Exec(ctx, `
		UPDATE ingot.object_usage AS u
		SET added_reported = c.added,
		    removed_reported = c.removed,
		    reported_at = now(),
		    dirty_since = CASE WHEN u.added = c.added AND u.removed = c.removed THEN NULL ELSE u.dirty_since END
		FROM unnest($1::text[], $2::timestamptz[], $3::bigint[], $4::bigint[]) AS c(space, hour, added, removed)
		WHERE u.space = c.space AND u.hour = c.hour
		  AND c.added >= u.added_reported AND c.removed >= u.removed_reported
		  AND (c.added > u.added_reported OR c.removed > u.removed_reported)
		  AND c.added <= u.added AND c.removed <= u.removed
	`, spaces, hours, added, removed)
	if err != nil {
		return fmt.Errorf("registry: acknowledge objects: %w", err)
	}
	return nil
}

func (r *Postgres) PruneObjects(ctx context.Context, before time.Time) (int64, error) {
	tag, err := r.pool.Exec(ctx, `
		DELETE FROM ingot.object_usage
		WHERE added = added_reported AND removed = removed_reported AND reported_at < $1
	`, before.UTC())
	if err != nil {
		return 0, fmt.Errorf("registry: prune objects: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (r *Postgres) ObjectBacklog(ctx context.Context) (ObjectBacklog, error) {
	var (
		b      ObjectBacklog
		oldest *time.Time
	)
	err := r.pool.QueryRow(ctx, `
		SELECT count(*), min(dirty_since)
		FROM ingot.object_usage
		WHERE added <> added_reported OR removed <> removed_reported
	`).Scan(&b.Counts, &oldest)
	if err != nil {
		return ObjectBacklog{}, fmt.Errorf("registry: object backlog: %w", err)
	}
	if oldest != nil {
		b.Oldest = oldest.UTC()
	}
	return b, nil
}
