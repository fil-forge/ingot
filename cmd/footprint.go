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
  releases                 the releases of blobs nothing in a space references
                           any more, queued by the change that dropped the
                           last reference, such as a DELETE or an overwrite:
    released               those that removed a blob, of the finished ones
                           the queue still keeps
    seconds                their time from queued to finished, the release
                           grace included: p50, p90, p99 and max
    pending                those not yet finished
    due                    pending ones past their grace, running or waiting
    oldest_due_seconds     how long past its grace the oldest due one is
    failed                 those that failed for good

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

type footprintReport struct {
	Time     time.Time         `json:"time"`
	Postgres postgresFootprint `json:"postgres"`
	Disk     diskFootprint     `json:"disk"`
	Releases releaseFootprint  `json:"releases"`
}

type postgresFootprint struct {
	DatabaseBytes int64            `json:"database_bytes"`
	WALBytes      int64            `json:"wal_bytes"`
	SchemaBytes   int64            `json:"schema_bytes"`
	Tables        []tableFootprint `json:"tables"`
}

type tableFootprint struct {
	Name       string `json:"name"`
	TotalBytes int64  `json:"total_bytes"`
	TableBytes int64  `json:"table_bytes"`
	IndexBytes int64  `json:"index_bytes"`
	LiveRows   int64  `json:"live_rows"`
	DeadRows   int64  `json:"dead_rows"`
}

type diskFootprint struct {
	Entries []diskEntry `json:"entries"`
	Total   diskFigures `json:"total"`
}

type diskEntry struct {
	Name string `json:"name"`
	diskFigures
}

type diskFigures struct {
	Files int64 `json:"files"`
	Bytes int64 `json:"bytes"`
}

type releaseFootprint struct {
	Released int64 `json:"released"`
	// Seconds is nil when no finished release removed a blob.
	Seconds          *releaseSeconds `json:"seconds"`
	Pending          int64           `json:"pending"`
	Due              int64           `json:"due"`
	OldestDueSeconds float64         `json:"oldest_due_seconds"`
	Failed           int64           `json:"failed"`
}

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
		r                  releaseFootprint
		p50, p90, p99, top *float64
	)
	if err := pool.QueryRow(ctx, `SELECT count(*),
		percentile_cont(0.5) WITHIN GROUP (ORDER BY s),
		percentile_cont(0.9) WITHIN GROUP (ORDER BY s),
		percentile_cont(0.99) WITHIN GROUP (ORDER BY s),
		max(s)
	FROM (SELECT extract(epoch FROM finished - created)::float8 AS s FROM `+tasks+`
		WHERE action = $1 AND state = 'succeeded' AND result = $2) released`,
		spaceshardqueue.ReleaseAction, []byte(spaceshardqueue.ReleasedResult),
	).Scan(&r.Released, &p50, &p90, &p99, &top); err != nil {
		return r, fmt.Errorf("finished releases: %w", err)
	}
	if top != nil {
		r.Seconds = &releaseSeconds{P50: *p50, P90: *p90, P99: *p99, Max: *top}
	}
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
