package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/spf13/cobra"

	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
)

func newCatalogCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "catalog",
		Short: "Inspect the buckets' catalogs",
	}
	c.AddCommand(newCatalogUsageCmd())
	return c
}

func newCatalogUsageCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "usage",
		Short: "Print each bucket's catalog storage on Forge, dead on arrival and live, as JSON",
		Long: `Print each bucket's catalog storage as JSON, labeled by the bucket's space
DID, and the totals over every bucket:

  forge_bytes            shipped catalog segments, retired ones included
  dead_on_arrival_bytes  their blocks already dead when they shipped
  unmeasured_bytes       shipped segments with no dead-on-arrival figure
  local_bytes            the catalog segment files on this node's disk
  live_bytes             blocks the bucket's root reaches, over local segments
  live_blocks            how many such blocks
  missing_blocks         catalog blocks the root reaches that no local
                         segment holds; any makes live_bytes a lower bound

The first three come from Postgres. The rest read every local catalog
segment into memory, so the command suits a test stack that keeps all of
them (a large catalog_plane.retain), such as a benchmark run, rather than a
production node. It only reads, and can run beside a live daemon.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(cfgFile)
			if err != nil {
				return err
			}
			if cfg.DataDir == "" || cfg.PostgresDSN == "" {
				return errors.New("catalog usage needs data_dir and postgres_dsn")
			}
			pool, err := openPool(cmd.Context(), cfg.PostgresDSN)
			if err != nil {
				return err
			}
			defer pool.Close()
			report, err := catalogUsage(cmd.Context(), registry.NewPostgres(pool), filepath.Join(cfg.DataDir, "segments"))
			if err != nil {
				return err
			}
			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetIndent("", "  ")
			return enc.Encode(report)
		},
	}
}

// catalogFigures are one bucket's catalog measurements, or their sum over
// every bucket. The command's help text defines each.
type catalogFigures struct {
	ForgeBytes         int64 `json:"forge_bytes"`
	DeadOnArrivalBytes int64 `json:"dead_on_arrival_bytes"`
	UnmeasuredBytes    int64 `json:"unmeasured_bytes"`
	LocalBytes         int64 `json:"local_bytes"`
	LiveBytes          int64 `json:"live_bytes"`
	LiveBlocks         int64 `json:"live_blocks"`
	MissingBlocks      int64 `json:"missing_blocks"`
}

func (f *catalogFigures) add(o catalogFigures) {
	f.ForgeBytes += o.ForgeBytes
	f.DeadOnArrivalBytes += o.DeadOnArrivalBytes
	f.UnmeasuredBytes += o.UnmeasuredBytes
	f.LocalBytes += o.LocalBytes
	f.LiveBytes += o.LiveBytes
	f.LiveBlocks += o.LiveBlocks
	f.MissingBlocks += o.MissingBlocks
}

// bucketCatalogUsage is one bucket's figures. The bucket goes by its space
// DID: its name is customer data.
type bucketCatalogUsage struct {
	Space string `json:"space"`
	catalogFigures
}

type catalogUsageReport struct {
	Buckets []bucketCatalogUsage `json:"buckets"`
	Total   catalogFigures       `json:"total"`
}

// catalogUsageSource is what the report reads from Postgres.
type catalogUsageSource interface {
	registry.CatalogUsageStore
	Get(ctx context.Context, name string) (*registry.State, error)
}

// catalogUsage measures every bucket that has shipped catalog segments or a
// catalog log under segmentsDir. A bucket whose row is gone (deleted, with
// its log not yet removed) is left out.
func catalogUsage(ctx context.Context, src catalogUsageSource, segmentsDir string) (catalogUsageReport, error) {
	report := catalogUsageReport{Buckets: []bucketCatalogUsage{}}
	shipped, err := src.CatalogUsage(ctx)
	if err != nil {
		return report, err
	}
	byName := make(map[string]registry.CatalogUsage, len(shipped))
	for _, u := range shipped {
		byName[u.Bucket] = u
	}
	names := make(map[string]struct{}, len(shipped))
	for name := range byName {
		names[name] = struct{}{}
	}
	entries, err := os.ReadDir(segmentsDir)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return report, fmt.Errorf("list %s: %w", segmentsDir, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			names[e.Name()] = struct{}{}
		}
	}

	for name := range names {
		st, err := src.Get(ctx, name)
		if errors.Is(err, registry.ErrNotFound) {
			continue
		}
		if err != nil {
			return report, fmt.Errorf("bucket %q: %w", name, err)
		}
		local, err := logstore.MeasureLocalCatalog(segmentsDir, name, st.Root)
		if err != nil {
			return report, err
		}
		u := byName[name]
		b := bucketCatalogUsage{
			Space: st.Space.String(),
			catalogFigures: catalogFigures{
				ForgeBytes:         u.Bytes,
				DeadOnArrivalBytes: u.DeadOnArrivalBytes,
				UnmeasuredBytes:    u.UnmeasuredBytes,
				LocalBytes:         local.FileBytes,
				LiveBytes:          local.LiveBytes,
				LiveBlocks:         local.LiveBlocks,
				MissingBlocks:      local.MissingBlocks,
			},
		}
		report.Buckets = append(report.Buckets, b)
		report.Total.add(b.catalogFigures)
	}
	sort.Slice(report.Buckets, func(i, j int) bool { return report.Buckets[i].Space < report.Buckets[j].Space })
	return report, nil
}
