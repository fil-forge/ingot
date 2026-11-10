// Command ingot-preload loads an ingot gateway's catalog to estate scale
// through its S3 API, for measuring the catalog there. See
// github.com/fil-forge/ingot/internal/preload.
//
// It is a plain S3 client: it needs the gateway's endpoint and region and an
// access key that may create buckets and write to them, taken from the AWS
// SDK's usual sources (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or a
// shared config profile such as the one smelt's `make s3-key` writes). It
// never takes a secret as a flag.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/fil-forge/ingot/internal/build"
	"github.com/fil-forge/ingot/internal/preload"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := newRootCmd().ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "ingot-preload:", err)
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "ingot-preload",
		Short:         "Load an ingot gateway's catalog to estate scale, for measuring it",
		Version:       build.Version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(newLoadCmd())
	return root
}

func newLoadCmd() *cobra.Command {
	var (
		client      preload.ClientConfig
		buckets     int
		bucketNames string
		cfg         preload.Config
		realMinMiB  int64
		realMaxMiB  int64
		metricsPath string
	)
	c := &cobra.Command{
		Use:   "load",
		Short: "Write objects until each bucket holds --until of them",
		Long: `Write objects until each bucket holds --until bulk objects, plus one real
object per --real-every bulk objects.

Bulk objects are zero-byte: each runs the gateway's whole commit, seal and
ship without uploading a blob. Real objects have pseudo-random bodies of
--real-min-mib to --real-max-mib MiB, the size of the pack server's packs.
Every object's key is --key-prefix followed by 64 hex digits, and every PUT is
create-only (If-None-Match: *) with an x-amz-checksum-sha256 header, as the
pack server writes them.

Buckets that do not exist are created, and their default encryption set to
--encryption ("none" needs the gateway's encryption.allow_none).

Progress goes to the --state file every --checkpoint-interval, and on exit.
Running load again with the same --state, --seed and --key-prefix resumes:
raise --until to load past a test point. A report goes to stdout and to the
--metrics file, as a line of JSON, every --report-interval.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg.Buckets = make([]string, buckets)
			for i := range buckets {
				cfg.Buckets[i] = preload.BucketName(bucketNames, i)
			}
			cfg.RealMinSize = realMinMiB << 20
			cfg.RealMaxSize = realMaxMiB << 20
			cfg.Progress = cmd.OutOrStdout()
			if metricsPath != "" {
				f, err := os.OpenFile(metricsPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
				if err != nil {
					return err
				}
				defer f.Close()
				cfg.Metrics = f
			}
			client.Conns = buckets*cfg.Workers + cfg.RealWorkers
			s3c, err := preload.NewClient(cmd.Context(), client)
			if err != nil {
				return err
			}
			_, err = preload.Load(cmd.Context(), s3c, cfg)
			if errors.Is(err, context.Canceled) && !errors.Is(err, preload.ErrStateNotSaved) {
				return fmt.Errorf("interrupted; progress saved in %s", cfg.StatePath)
			}
			return err
		},
	}
	f := c.Flags()
	f.StringVar(&client.Endpoint, "endpoint", "", "the gateway's URL (default: the profile's endpoint_url)")
	f.StringVar(&client.Region, "region", "", "the gateway's region (default: the profile's or AWS_REGION)")
	f.StringVar(&client.Profile, "profile", "", "shared config profile with the access key (default: the AWS SDK's default chain)")

	f.IntVar(&buckets, "buckets", 150, "number of buckets")
	f.StringVar(&bucketNames, "bucket-prefix", "preload-", "bucket names: this followed by a three-digit index")
	f.StringVar(&cfg.KeyPrefix, "key-prefix", "", "prefix of every object key")
	f.StringVar(&cfg.Seed, "seed", "preload", "seed of every key and body")
	f.Uint64Var(&cfg.Until, "until", 0, "bulk objects each bucket ends with (required)")
	f.IntVar(&cfg.Workers, "workers", 2, "bulk PUTs in flight per bucket")

	f.Uint64Var(&cfg.RealEvery, "real-every", 0, "write one real object per this many bulk objects in each bucket; 0 for none")
	f.Int64Var(&realMinMiB, "real-min-mib", 128, "smallest real object, in MiB")
	f.Int64Var(&realMaxMiB, "real-max-mib", 240, "largest real object, in MiB")
	f.IntVar(&cfg.RealWorkers, "real-workers", 4, "real PUTs in flight across all buckets")

	f.StringVar(&cfg.Encryption, "encryption", "none", `default encryption set on each new bucket; "" leaves the gateway's`)
	f.StringVar(&cfg.StatePath, "state", "preload-state.json", "state file to resume from and record progress in")
	f.StringVar(&metricsPath, "metrics", "preload-metrics.jsonl", "file the JSON reports are appended to; empty for none")
	f.DurationVar(&cfg.CheckpointInterval, "checkpoint-interval", 10*time.Second, "how often the state file is written")
	f.DurationVar(&cfg.ReportInterval, "report-interval", 10*time.Second, "how often progress is reported")
	f.IntVar(&cfg.MaxAttempts, "max-attempts", 10, "attempts at a PUT that fails for a passing reason before the load stops")
	f.DurationVar(&cfg.AttemptTimeout, "attempt-timeout", 10*time.Minute, "how long one attempt at a PUT may take before it is tried again; 0 for no limit")
	_ = c.MarkFlagRequired("until")
	return c
}
