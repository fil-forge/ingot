package registry

import (
	"context"
	"fmt"
	"time"

	"github.com/fil-forge/ucantone/did"
)

// EgressCount is the bytes of object data sent to clients from one bucket's
// space in one hour.
type EgressCount struct {
	// Space is the bucket's space.
	Space did.DID
	// Hour is the start of the hour, in UTC.
	Hour time.Time
	// Bytes is the bytes sent. In [EgressUsageStore.AddEgress] it is what
	// was sent since the last add; everywhere else it is the hour's total.
	Bytes int64
}

// EgressBacklog is the egress waiting to be reported.
type EgressBacklog struct {
	// Counts is how many counts have changed since they were last
	// acknowledged.
	Counts int64
	// Oldest is when the longest-waiting of them became due. It is the zero
	// time when Counts is zero.
	Oldest time.Time
}

// EgressUsageStore holds the egress GetObject sent, by space and hour, and
// which of it the auth service has acknowledged.
//
// The counts are totals the service replaces what it holds with, so a count
// is due for reporting whenever it differs from the one last acknowledged,
// and reporting one twice does no harm.
type EgressUsageStore interface {
	// AddEgress adds what was sent to each count's hour. Instances sharing a
	// database add into the same rows. A count of zero bytes is ignored.
	AddEgress(ctx context.Context, sent []EgressCount) error
	// DueEgress returns up to limit counts that have changed since they were
	// last acknowledged, each with its hour's total, oldest hour first.
	DueEgress(ctx context.Context, limit int) ([]EgressCount, error)
	// AcknowledgeEgress records that the service holds each count's total.
	// A count that has grown since it was read stays due.
	AcknowledgeEgress(ctx context.Context, reported []EgressCount) error
	// PruneEgress deletes the counts that are up to date and were last
	// acknowledged before the given time, and returns how many it deleted.
	PruneEgress(ctx context.Context, before time.Time) (int64, error)
	// EgressBacklog returns how much egress is waiting to be reported.
	EgressBacklog(ctx context.Context) (EgressBacklog, error)
}

var _ EgressUsageStore = (*Postgres)(nil)

func (r *Postgres) AddEgress(ctx context.Context, sent []EgressCount) error {
	spaces := make([]string, 0, len(sent))
	hours := make([]time.Time, 0, len(sent))
	bytes := make([]int64, 0, len(sent))
	for _, c := range sent {
		if c.Bytes == 0 {
			continue
		}
		if c.Bytes < 0 {
			return fmt.Errorf("registry: add egress: negative count for %s", c.Space)
		}
		spaces = append(spaces, c.Space.String())
		hours = append(hours, c.Hour.UTC())
		bytes = append(bytes, c.Bytes)
	}
	if len(spaces) == 0 {
		return nil
	}
	// Summed first: one statement cannot update a row twice, and two counts
	// may name the same space and hour.
	_, err := r.pool.Exec(ctx, `
		INSERT INTO ingot.egress_usage AS u (space, hour, bytes_sent, dirty_since)
		SELECT c.space, c.hour, SUM(c.bytes), now()
		FROM unnest($1::text[], $2::timestamptz[], $3::bigint[]) AS c(space, hour, bytes)
		GROUP BY c.space, c.hour
		ON CONFLICT (space, hour) DO UPDATE
		SET bytes_sent = u.bytes_sent + EXCLUDED.bytes_sent,
		    dirty_since = COALESCE(u.dirty_since, now())
	`, spaces, hours, bytes)
	if err != nil {
		return fmt.Errorf("registry: add egress: %w", err)
	}
	return nil
}

func (r *Postgres) DueEgress(ctx context.Context, limit int) ([]EgressCount, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT space, hour, bytes_sent
		FROM ingot.egress_usage
		WHERE bytes_sent <> bytes_reported
		ORDER BY hour, space
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("registry: due egress: %w", err)
	}
	defer rows.Close()
	var due []EgressCount
	for rows.Next() {
		var (
			space string
			c     EgressCount
		)
		if err := rows.Scan(&space, &c.Hour, &c.Bytes); err != nil {
			return nil, fmt.Errorf("registry: due egress: %w", err)
		}
		if c.Space, err = did.Parse(space); err != nil {
			return nil, fmt.Errorf("registry: due egress: space %q: %w", space, err)
		}
		c.Hour = c.Hour.UTC()
		due = append(due, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: due egress: %w", err)
	}
	return due, nil
}

func (r *Postgres) AcknowledgeEgress(ctx context.Context, reported []EgressCount) error {
	if len(reported) == 0 {
		return nil
	}
	spaces := make([]string, len(reported))
	hours := make([]time.Time, len(reported))
	bytes := make([]int64, len(reported))
	for i, c := range reported {
		spaces[i] = c.Space.String()
		hours[i] = c.Hour.UTC()
		bytes[i] = c.Bytes
	}
	// A row that grew after it was read keeps its dirty_since: it has been
	// due since before this report went out.
	_, err := r.pool.Exec(ctx, `
		UPDATE ingot.egress_usage AS u
		SET bytes_reported = c.bytes,
		    reported_at = now(),
		    dirty_since = CASE WHEN u.bytes_sent = c.bytes THEN NULL ELSE u.dirty_since END
		FROM unnest($1::text[], $2::timestamptz[], $3::bigint[]) AS c(space, hour, bytes)
		WHERE u.space = c.space AND u.hour = c.hour
	`, spaces, hours, bytes)
	if err != nil {
		return fmt.Errorf("registry: acknowledge egress: %w", err)
	}
	return nil
}

func (r *Postgres) PruneEgress(ctx context.Context, before time.Time) (int64, error) {
	tag, err := r.pool.Exec(ctx, `
		DELETE FROM ingot.egress_usage
		WHERE bytes_sent = bytes_reported AND reported_at < $1
	`, before.UTC())
	if err != nil {
		return 0, fmt.Errorf("registry: prune egress: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (r *Postgres) EgressBacklog(ctx context.Context) (EgressBacklog, error) {
	var (
		b      EgressBacklog
		oldest *time.Time
	)
	err := r.pool.QueryRow(ctx, `
		SELECT count(*), min(dirty_since)
		FROM ingot.egress_usage
		WHERE bytes_sent <> bytes_reported
	`).Scan(&b.Counts, &oldest)
	if err != nil {
		return EgressBacklog{}, fmt.Errorf("registry: egress backlog: %w", err)
	}
	if oldest != nil {
		b.Oldest = oldest.UTC()
	}
	return b, nil
}
