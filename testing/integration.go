// Package testing wires the upstream versitygw integration suite
// (github.com/versity/versitygw/tests/integration) against a running
// ingot S3 listener. Callers own server lifecycle and pass connection
// details in via Config; this package only selects which upstream
// group functions to run. Each group prints its own per-test results
// to stdout; Run additionally returns a Result summarizing how many
// cases passed and failed so Go tests can fail a *testing.T when the
// suite reports any failures (see RunT).
package testing

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/versity/versitygw/tests/integration"
)

// Config addresses the ingot S3 listener under test.
type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string

	// Region must match the listener's configured region. Empty
	// defaults to "us-east-1".
	Region string

	// Parallel queues each Run-mode test on TestState's worker
	// pool instead of running serially. Sync-mode tests still
	// run after the parallel batch drains.
	Parallel bool

	// HostStyle uses host-style bucket addressing (bucket.host)
	// instead of path-style (host/bucket).
	HostStyle bool

	// VersioningEnabled tells the suite the bucket-versioning
	// feature is on; group functions branch on this flag.
	VersioningEnabled bool

	// SkipTLSVerify accepts self-signed certs.
	SkipTLSVerify bool
}

// Suite is an ordered list of upstream group functions. Each takes a
// *integration.TestState and dispatches its individual tests via
// ts.Run / ts.Sync. Compose ad-hoc suites by listing
// integration.TestXxx values directly:
//
//	testing.Run(ctx, cfg, testing.Suite{
//	    integration.TestCreateBucket,
//	    integration.TestPutObject,
//	})
type Suite []func(*integration.TestState)

// Result summarizes a single suite Run. Counts are deltas — the
// versitygw counters are package-level atomics shared across every
// caller in the process, so Run snapshots them on entry and reports
// the difference.
type Result struct {
	// Ran is the number of individual case functions that started.
	Ran uint32
	// Passed is the number that ended in passF.
	Passed uint32
	// Failed is the number that ended in failF.
	Failed uint32

	// PassedCases / FailedCases name the individual cases by their versitygw
	// test name (e.g. "PutObject_conditional_writes"), recovered by capturing the
	// suite's stdout — the upstream counters expose only totals. They let a caller
	// gate per-case (e.g. assert the forge-mode failures are a subset of the
	// in-process baseline's, catching forge-specific regressions a count
	// comparison would miss). Empty if stdout capture failed.
	PassedCases []string
	FailedCases []string
}

// Err returns a non-nil error if any case failed. Use this when the
// caller is not a *testing.T (e.g., a CLI runner). For Go tests,
// prefer RunT which propagates failures into t.Errorf directly.
func (r Result) Err() error {
	if r.Failed > 0 {
		return fmt.Errorf("integration suite: %d of %d cases failed", r.Failed, r.Ran)
	}
	return nil
}

// runMu serializes concurrent Run calls so the global versitygw
// counters can be sampled before/after one Run without interleaving
// with another. Two parallel Run calls in the same process would
// otherwise contaminate each other's deltas.
var runMu sync.Mutex

// Run drives suite against a fresh TestState bound to ctx and c.
// Blocks until queued (Run-mode) and deferred (Sync-mode) tests
// complete, then returns this run's case counts.
func Run(ctx context.Context, c Config, suite Suite) Result {
	runMu.Lock()
	defer runMu.Unlock()

	ranBefore := integration.RunCount.Load()
	passedBefore := integration.PassCount.Load()
	failedBefore := integration.FailCount.Load()

	// The upstream group functions print each case's RUN/PASS/FAIL line to
	// os.Stdout and bump package atomics — they expose no structured per-case
	// result. Tee os.Stdout through a pipe for the duration of the run so we can
	// recover the case names (live output is preserved via the MultiWriter).
	// runMu already serializes Run calls, so the global os.Stdout swap is safe.
	var captured bytes.Buffer
	orig := os.Stdout
	pr, pw, pipeErr := os.Pipe()
	copyDone := make(chan struct{})
	if pipeErr == nil {
		os.Stdout = pw
		go func() {
			_, _ = io.Copy(io.MultiWriter(orig, &captured), pr)
			close(copyDone)
		}()
	}

	ts := integration.NewTestState(ctx, newS3Conf(c), c.Parallel)
	for _, group := range suite {
		group(ts)
	}
	ts.Wait()

	if pipeErr == nil {
		os.Stdout = orig
		_ = pw.Close()
		<-copyDone
		_ = pr.Close()
	}

	passed, failed := parseCaseLines(captured.String())
	return Result{
		Ran:         integration.RunCount.Load() - ranBefore,
		Passed:      integration.PassCount.Load() - passedBefore,
		Failed:      integration.FailCount.Load() - failedBefore,
		PassedCases: passed,
		FailedCases: failed,
	}
}

var ansiRe = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// parseCaseLines recovers per-case names from captured suite output. The upstream
// printers emit "PASS <name>" and "FAIL <name>: <error>" (wrapped in ANSI color);
// the name is the token up to the first colon. Teardown-failure lines lack the
// "FAIL " prefix and are ignored (they don't increment the fail counter).
func parseCaseLines(output string) (passed, failed []string) {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(ansiRe.ReplaceAllString(line, ""))
		switch {
		case strings.HasPrefix(line, "FAIL "):
			failed = append(failed, caseName(line[len("FAIL "):]))
		case strings.HasPrefix(line, "PASS "):
			passed = append(passed, caseName(line[len("PASS "):]))
		}
	}
	return passed, failed
}

func caseName(s string) string {
	if i := strings.IndexByte(s, ':'); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(s)
}

// RunT is the Go-test-friendly form of Run. On any failure the
// returned Result is also reported via t.Errorf so `go test` exits
// non-zero. The per-case FAIL lines printed by versitygw are
// captured in t's log output, so the test author sees exactly which
// cases failed without RunT having to summarize them.
func RunT(t *testing.T, c Config, suite Suite) Result {
	t.Helper()
	r := Run(t.Context(), c, suite)
	if r.Failed > 0 {
		t.Errorf("integration suite: %d of %d cases failed (see test output for per-case details)", r.Failed, r.Ran)
	}
	return r
}

func newS3Conf(c Config) *integration.S3Conf {
	region := c.Region
	if region == "" {
		region = "us-east-1"
	}
	opts := []integration.Option{
		integration.WithEndpoint(c.Endpoint),
		integration.WithAccess(c.AccessKey),
		integration.WithSecret(c.SecretKey),
		integration.WithRegion(region),
		integration.WithTLSStatus(c.SkipTLSVerify),
	}
	if c.HostStyle {
		opts = append(opts, integration.WithHostStyle())
	}
	if c.VersioningEnabled {
		opts = append(opts, integration.WithVersioningEnabled())
	}
	return integration.NewS3Conf(opts...)
}

// Smoke is the minimum subset that should pass on a working listener:
// bucket lifecycle plus single-object CRUD.
var Smoke = Suite{
	integration.TestCreateBucket,
	integration.TestHeadBucket,
	integration.TestListBuckets,
	integration.TestPutObject,
	integration.TestGetObject,
	integration.TestHeadObject,
	integration.TestDeleteObject,
	integration.TestDeleteBucket,
}

// CRUD covers Smoke plus listing, multi-delete, copy, and the
// GetObjectAttributes surface. Stays inside features that don't
// require multipart, versioning, ACL, policy, CORS, lock, or tagging.
var CRUD = Suite{
	integration.TestCreateBucket,
	integration.TestHeadBucket,
	integration.TestListBuckets,
	integration.TestDeleteBucket,
	integration.TestPutObject,
	integration.TestHeadObject,
	integration.TestGetObject,
	integration.TestGetObjectAttributes,
	integration.TestListObjects,
	integration.TestListObjectsV2,
	integration.TestCopyObject,
	integration.TestDeleteObject,
	integration.TestDeleteObjects,
}

// Multipart covers the multipart-upload group set.
var Multipart = Suite{
	integration.TestCreateMultipartUpload,
	integration.TestUploadPart,
	integration.TestUploadPartCopy,
	integration.TestListParts,
	integration.TestListMultipartUploads,
	integration.TestAbortMultipartUpload,
	integration.TestCompleteMultipartUpload,
}

// Tagging covers object and bucket tagging APIs.
var Tagging = Suite{
	integration.TestPutBucketTagging,
	integration.TestGetBucketTagging,
	integration.TestDeleteBucketTagging,
	integration.TestPutObjectTagging,
	integration.TestGetObjectTagging,
	integration.TestDeleteObjectTagging,
}

// ObjectLock covers retention, legal hold, lock config, and
// WORM-protection groups.
var ObjectLock = Suite{
	integration.TestPutObjectLockConfiguration,
	integration.TestGetObjectLockConfiguration,
	integration.TestPutObjectRetention,
	integration.TestGetObjectRetention,
	integration.TestPutObjectLegalHold,
	integration.TestGetObjectLegalHold,
	integration.TestWORMProtection,
}

// Versioning runs the version-aware group. Set
// Config.VersioningEnabled = true.
var Versioning = Suite{
	integration.TestVersioning,
	integration.TestVersioningDisabled,
	integration.TestListObjectVersions_VD,
}

// Auth runs sigv4 + presigned-URL authentication groups.
var Auth = Suite{
	integration.TestAuthentication,
	integration.TestPresignedAuthentication,
}

// Full is the upstream TestFullFlow rolled-up suite — the
// "how-far-from-full-compatibility" gauge. Expect noisy failures
// until ingot closes the gaps tracked by the focused suites above.
var Full = Suite{integration.TestFullFlow}
