package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/ingot/queue/spaceshardqueue"
	"github.com/fil-forge/ingot/taskrunner"
)

// ingotSchema is the Postgres schema ingot's migrations and task queues
// create their tables in.
const ingotSchema = "ingot"

func newFootprintCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "footprint",
		Short: "Print what this node keeps in Postgres and on disk, and how its blob releases keep up, as JSON",
		Long: `Print what this node keeps in Postgres and on its disk, and how its blob
releases keep up, as JSON:

  time                     when the report was taken
  postgres.database_bytes  the database postgres_dsn names, on disk
  postgres.wal_bytes       the server's write-ahead log position, in bytes:
                           two reports' difference is the WAL written
                           between them, by every database on the server
  postgres.schema_bytes    the sum of the tables' total_bytes
  postgres.tables          each table in the ingot schema: total_bytes, with
                           its indexes, split into table_bytes (TOAST
                           included) and index_bytes; and the live and dead
                           rows Postgres's statistics estimate
  disk.entries             each entry at the top of data_dir: how many
                           regular files it holds and their apparent size in
                           bytes
  disk.total               the same, over every entry
  releases                 the releases of blobs nothing in a space references
                           any more, queued by the change that dropped the
                           last reference. One a DELETE or an overwrite
                           queued waits out the release grace; others can be
                           due at once:
    released               those that removed a blob, of the finished ones
                           the queue keeps (for its retention of finished
                           tasks, a day by default): a rolling window, not a
                           running total
    seconds                their time from queued to finished, any grace
                           included: p50, p90, p99 and max, or null when no
                           release removed a blob
    lag_seconds            their time from due to finished, how far the
                           queue ran behind: the same quantiles
    pending                those not yet finished
    due                    pending ones past their due time, running or
                           waiting
    oldest_due_seconds     how long past its due time the oldest due one is
    failed                 those that failed for good, kept until resubmitted

A catalog loaded by ingot-preload queues no releases: it writes each object
once, and the only objects it deletes have no body. Delete or overwrite
objects with bodies to time them.

Shipped catalog segments are counted by "ingot catalog usage". The command
only reads, and can run beside a live daemon: a file removed while it runs is
left out.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(cfgFile)
			if err != nil {
				return err
			}
			if cfg.DataDir == "" || cfg.PostgresDSN == "" {
				return errors.New("footprint needs data_dir and postgres_dsn")
			}
			pool, err := openPool(cmd.Context(), cfg.PostgresDSN)
			if err != nil {
				return err
			}
			defer pool.Close()
			report, err := footprint(cmd.Context(), pool, cfg.DataDir)
			if err != nil {
				return err
			}
			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetIndent("", "  ")
			return enc.Encode(report)
		},
	}
}

// footprintReport is what the node keeps and how its releases keep up. The
// command's help text defines each field.
type footprintReport struct {
	Time     time.Time         `json:"time"`
	Postgres postgresFootprint `json:"postgres"`
	Disk     diskFootprint     `json:"disk"`
	Releases releaseFootprint  `json:"releases"`
}

// postgresFootprint is the database's size, the server's WAL position, and
// each table of the ingot schema.
type postgresFootprint struct {
	DatabaseBytes int64            `json:"database_bytes"`
	WALBytes      int64            `json:"wal_bytes"`
	SchemaBytes   int64            `json:"schema_bytes"`
	Tables        []tableFootprint `json:"tables"`
}

// tableFootprint is one table's bytes and estimated rows.
type tableFootprint struct {
	Name       string `json:"name"`
	TotalBytes int64  `json:"total_bytes"`
	TableBytes int64  `json:"table_bytes"`
	IndexBytes int64  `json:"index_bytes"`
	LiveRows   int64  `json:"live_rows"`
	DeadRows   int64  `json:"dead_rows"`
}

// diskFootprint is each entry at the top of the data directory, and their
// sum.
type diskFootprint struct {
	Entries []diskEntry `json:"entries"`
	Total   diskFigures `json:"total"`
}

// diskEntry is one entry at the top of the data directory.
type diskEntry struct {
	Name string `json:"name"`
	diskFigures
}

// diskFigures counts regular files and their apparent size.
type diskFigures struct {
	Files int64 `json:"files"`
	Bytes int64 `json:"bytes"`
}

// releaseFootprint is the queue-space-shard release tasks, by how they ended
// or what they wait for.
type releaseFootprint struct {
	Released int64 `json:"released"`
	// Seconds and LagSeconds are nil when no finished release removed a
	// blob.
	Seconds          *releaseSeconds `json:"seconds"`
	LagSeconds       *releaseSeconds `json:"lag_seconds"`
	Pending          int64           `json:"pending"`
	Due              int64           `json:"due"`
	OldestDueSeconds float64         `json:"oldest_due_seconds"`
	Failed           int64           `json:"failed"`
}

// releaseSeconds are quantiles of a duration, in seconds.
type releaseSeconds struct {
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

// footprint measures what the node keeps in the database pool reaches and
// under dataDir.
func footprint(ctx context.Context, pool *pgxpool.Pool, dataDir string) (footprintReport, error) {
	report := footprintReport{Time: time.Now().UTC()}
	var err error
	if report.Postgres, err = postgresUsage(ctx, pool); err != nil {
		return report, err
	}
	if report.Disk, err = diskUsage(dataDir); err != nil {
		return report, err
	}
	if report.Releases, err = releaseUsage(ctx, pool); err != nil {
		return report, err
	}
	return report, nil
}

// postgresUsage reads the database's size, the WAL position and the ingot
// schema's tables.
func postgresUsage(ctx context.Context, pool *pgxpool.Pool) (postgresFootprint, error) {
	var p postgresFootprint
	if err := pool.QueryRow(ctx, `SELECT pg_database_size(current_database()),
		pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint`).Scan(&p.DatabaseBytes, &p.WALBytes); err != nil {
		return p, fmt.Errorf("database size: %w", err)
	}
	rows, err := pool.Query(ctx, `SELECT c.relname,
		pg_total_relation_size(c.oid), pg_table_size(c.oid), pg_indexes_size(c.oid),
		coalesce(s.n_live_tup, 0), coalesce(s.n_dead_tup, 0)
	FROM pg_class c
	JOIN pg_namespace n ON n.oid = c.relnamespace
	LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
	WHERE n.nspname = $1 AND c.relkind = 'r'
	ORDER BY c.relname`, ingotSchema)
	if err != nil {
		return p, fmt.Errorf("table sizes: %w", err)
	}
	p.Tables, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (tableFootprint, error) {
		var t tableFootprint
		err := row.Scan(&t.Name, &t.TotalBytes, &t.TableBytes, &t.IndexBytes, &t.LiveRows, &t.DeadRows)
		return t, err
	})
	if err != nil {
		return p, fmt.Errorf("table sizes: %w", err)
	}
	for _, t := range p.Tables {
		p.SchemaBytes += t.TotalBytes
	}
	return p, nil
}

// diskUsage counts the regular files under each entry at the top of dataDir.
// A file or directory removed while the walk runs is left out.
func diskUsage(dataDir string) (diskFootprint, error) {
	d := diskFootprint{Entries: []diskEntry{}}
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return d, fmt.Errorf("list %s: %w", dataDir, err)
	}
	for _, e := range entries {
		var f diskFigures
		err := filepath.WalkDir(filepath.Join(dataDir, e.Name()), func(path string, de fs.DirEntry, err error) error {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			if !de.Type().IsRegular() {
				return nil
			}
			info, err := de.Info()
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			f.Files++
			f.Bytes += info.Size()
			return nil
		})
		if err != nil {
			return d, fmt.Errorf("walk %s: %w", dataDir, err)
		}
		d.Entries = append(d.Entries, diskEntry{Name: e.Name(), diskFigures: f})
		d.Total.Files += f.Files
		d.Total.Bytes += f.Bytes
	}
	return d, nil
}

// releaseUsage reads the release tasks of the queue-space-shard queue.
func releaseUsage(ctx context.Context, pool *pgxpool.Pool) (releaseFootprint, error) {
	tasks := taskrunner.TasksTable(ingotSchema, spaceshardqueue.Name)
	var (
		r         releaseFootprint
		took, lag [4]*float64
	)
	if err := pool.QueryRow(ctx, `SELECT count(*), `+quantiles("took")+`, `+quantiles("lag")+`
	FROM (SELECT extract(epoch FROM finished - created)::float8 AS took,
			extract(epoch FROM finished - coalesce(not_before, created))::float8 AS lag
		FROM `+tasks+`
		WHERE action = $1 AND state = 'succeeded' AND result = $2) released`,
		spaceshardqueue.ReleaseAction, []byte(spaceshardqueue.ReleasedResult),
	).Scan(&r.Released, &took[0], &took[1], &took[2], &took[3], &lag[0], &lag[1], &lag[2], &lag[3]); err != nil {
		return r, fmt.Errorf("finished releases: %w", err)
	}
	r.Seconds, r.LagSeconds = seconds(took), seconds(lag)
	if err := pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE state = 'pending'),
		count(*) FILTER (WHERE state = 'pending' AND coalesce(not_before, created) <= now()),
		coalesce(extract(epoch FROM now() - min(coalesce(not_before, created))
			FILTER (WHERE state = 'pending' AND coalesce(not_before, created) <= now())), 0)::float8,
		count(*) FILTER (WHERE state = 'failed')
	FROM `+tasks+` WHERE action = $1`, spaceshardqueue.ReleaseAction,
	).Scan(&r.Pending, &r.Due, &r.OldestDueSeconds, &r.Failed); err != nil {
		return r, fmt.Errorf("unfinished releases: %w", err)
	}
	return r, nil
}

// quantiles is the SQL for the p50, p90, p99 and max of col, in that order.
func quantiles(col string) string {
	return fmt.Sprintf(`percentile_cont(0.5) WITHIN GROUP (ORDER BY %[1]s),
		percentile_cont(0.9) WITHIN GROUP (ORDER BY %[1]s),
		percentile_cont(0.99) WITHIN GROUP (ORDER BY %[1]s),
		max(%[1]s)`, col)
}

// seconds is the quantiles quantiles selected, or nil over no rows.
func seconds(q [4]*float64) *releaseSeconds {
	if q[3] == nil {
		return nil
	}
	return &releaseSeconds{P50: *q[0], P90: *q[1], P99: *q[2], Max: *q[3]}
}
