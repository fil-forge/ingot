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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
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
	root.AddCommand(newLoadCmd(), newValidateCmd())
	return root
}

// addClientFlags adds to cmd the flags that say which gateway to talk to.
func addClientFlags(cmd *cobra.Command, c *preload.ClientConfig) {
	f := cmd.Flags()
	f.StringVar(&c.Endpoint, "endpoint", "", "the gateway's URL (default: the profile's endpoint_url)")
	f.StringVar(&c.Region, "region", "", "the gateway's region (default: the profile's or AWS_REGION)")
	f.StringVar(&c.Profile, "profile", "", "shared config profile with the access key (default: the AWS SDK's default chain)")
}

// bucketNames returns the names of n buckets starting with prefix.
func bucketNames(prefix string, n int) []string {
	names := make([]string, n)
	for i := range n {
		names[i] = preload.BucketName(prefix, i)
	}
	return names
}

func newLoadCmd() *cobra.Command {
	var (
		client       preload.ClientConfig
		buckets      int
		bucketPrefix string
		cfg          preload.Config
		realMinMiB   int64
		realMaxMiB   int64
		metricsPath  string
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

Buckets that do not exist are created. Every bucket the --state file has not
set up, an existing one included, gets --encryption as its default encryption
("none" needs the gateway's encryption.allow_none), so a new --state over
buckets an earlier load filled changes theirs part way through: use a new
--bucket-prefix with a new --state.

Progress goes to the --state file every --checkpoint-interval, and on exit.
Running load again with the same --state, --seed, --key-prefix and
--encryption resumes: raise --until to load past a test point. A report goes
to stdout and to the --metrics file, as a line of JSON, every
--report-interval.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg.Buckets = bucketNames(bucketPrefix, buckets)
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
	addClientFlags(c, &client)
	f := c.Flags()

	f.IntVar(&buckets, "buckets", 150, "number of buckets")
	f.StringVar(&bucketPrefix, "bucket-prefix", "preload-", "bucket names: this followed by a three-digit index")
	f.StringVar(&cfg.KeyPrefix, "key-prefix", "", "prefix of every object key")
	f.StringVar(&cfg.Seed, "seed", "preload", "seed of every key and body")
	f.Uint64Var(&cfg.Until, "until", 0, "bulk objects each bucket ends with (required)")
	f.IntVar(&cfg.Workers, "workers", 2, "bulk PUTs in flight per bucket")

	f.Uint64Var(&cfg.RealEvery, "real-every", 0, "write one real object per this many bulk objects in each bucket; 0 for none")
	f.Int64Var(&realMinMiB, "real-min-mib", 128, "smallest real object, in MiB")
	f.Int64Var(&realMaxMiB, "real-max-mib", 240, "largest real object, in MiB")
	f.IntVar(&cfg.RealWorkers, "real-workers", 4, "real PUTs in flight across all buckets")

	f.StringVar(&cfg.Encryption, "encryption", "none", `default encryption set on each bucket the state file has not set up; "" leaves the gateway's`)
	f.StringVar(&cfg.StatePath, "state", "preload-state.json", "state file to resume from and record progress in")
	f.StringVar(&metricsPath, "metrics", "preload-metrics.jsonl", "file the JSON reports are appended to; empty for none")
	f.DurationVar(&cfg.CheckpointInterval, "checkpoint-interval", 10*time.Second, "how often the state file is written")
	f.DurationVar(&cfg.ReportInterval, "report-interval", 10*time.Second, "how often progress is reported")
	f.IntVar(&cfg.MaxAttempts, "max-attempts", 10, "attempts at a PUT that fails for a passing reason before the load stops")
	f.DurationVar(&cfg.AttemptTimeout, "attempt-timeout", 10*time.Minute, "how long one attempt at a PUT may take before it is tried again; 0 for no limit")
	_ = c.MarkFlagRequired("until")
	return c
}

func newValidateCmd() *cobra.Command {
	var (
		client       preload.ClientConfig
		buckets      int
		bucketPrefix string
		steps        []string
		rangeKiB     int64
		reportPath   string
		cfg          preload.ValidateConfig
	)
	c := &cobra.Command{
		Use:   "validate",
		Short: "Time and check reads, listings, writes and deletes against a loaded catalog",
		Long: `Time and check requests against the buckets a load filled, reading which
objects the load wrote from its --state file. The steps run in this order:

  get    reads a --range-kib range of every real object, and checks its bytes
  head   reads the metadata of --samples bulk objects per bucket
  list   lists the first --list-buckets buckets in full, and checks the keys
         are in order and number at least those the load recorded
  write  writes --samples new zero-byte objects per bucket the way the load
         does, deletes them, and checks they are gone

It deletes only the objects it writes. --sample picks which objects and
ranges are read: run once just after restarting the gateway, then again with
the same --sample and --steps get,head, to time cold reads and then warm
ones. Use the --seed and --key-prefix the load used.

The report goes to stdout a step at a time and to the --report file as a
line of JSON. A request that failed or answered wrongly is counted there,
and makes the command exit non-zero once it has finished.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg.Buckets = bucketNames(bucketPrefix, buckets)
			if len(steps) > 0 {
				cfg.Steps = steps
			}
			cfg.RangeSize = rangeKiB << 10
			cfg.Progress = cmd.OutOrStdout()
			client.Conns = cfg.Workers
			s3c, err := preload.NewClient(cmd.Context(), client)
			if err != nil {
				return err
			}
			r, err := preload.Validate(cmd.Context(), s3c, cfg)
			if r != nil {
				if werr := appendReport(reportPath, r); werr != nil {
					return errors.Join(err, werr)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "%s: done in %.0fs. Buckets: %d, holding %d to %d bulk objects each. Failed or wrong: %d.\n",
					r.Label, r.ElapsedS, r.Point.Buckets, r.Point.MinBulk, r.Point.MaxBulk, r.Failures)
				for _, s := range r.FailureSample {
					fmt.Fprintln(cmd.OutOrStdout(), "  "+s)
				}
			}
			if err != nil {
				return err
			}
			if r.Failures > 0 {
				return fmt.Errorf("%d requests failed or answered wrongly", r.Failures)
			}
			return nil
		},
	}
	addClientFlags(c, &client)
	f := c.Flags()

	f.IntVar(&buckets, "buckets", 150, "number of buckets")
	f.StringVar(&bucketPrefix, "bucket-prefix", "preload-", "bucket names: this followed by a three-digit index")
	f.StringVar(&cfg.KeyPrefix, "key-prefix", "", "the load's key prefix")
	f.StringVar(&cfg.Seed, "seed", "preload", "the load's seed")
	f.StringVar(&cfg.StatePath, "state", "preload-state.json", "the load's state file, which is only read")

	f.StringVar(&cfg.Label, "label", "", "name of this run in the report, such as 0.6M-cold (required)")
	f.StringVar(&cfg.Sample, "sample", "sample", "picks the objects and ranges read; the same value reads the same ones")
	f.StringSliceVar(&steps, "steps", nil, "steps to run, of "+strings.Join(preload.ValidateSteps, ",")+" (default all)")
	f.IntVar(&cfg.Samples, "samples", 20, "bulk objects read, and new objects written and deleted, per bucket")
	f.Int64Var(&rangeKiB, "range-kib", 1024, "length of each ranged read of a real object, in KiB")
	f.IntVar(&cfg.ListBuckets, "list-buckets", 3, "buckets listed in full, from the first")
	f.IntVar(&cfg.Workers, "workers", 8, "requests in flight")
	f.DurationVar(&cfg.RequestTimeout, "request-timeout", 5*time.Minute, "how long one request may take before it counts as failed; 0 for no limit")
	f.StringVar(&reportPath, "report", "preload-validate.jsonl", "file the JSON report is appended to; empty for none")
	_ = c.MarkFlagRequired("label")
	return c
}

// appendReport appends r to path as a line of JSON; an empty path does
// nothing.
func appendReport(path string, r *preload.ValidateReport) error {
	if path == "" {
		return nil
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
