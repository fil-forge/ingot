package testing

import (
	"context"
	"testing"

	"github.com/versity/versitygw/tests/integration"
)

// TestRun_CapturesCaseNames validates end-to-end that Run recovers per-case
// names from the suite's stdout (the os.Stdout tee + parseCaseLines) and that
// the recovered names match the upstream pass/fail counters one-for-one. Without
// this, a broken capture would silently return empty case sets — turning the
// forge conformance gate (forge failures ⊆ baseline failures) into a vacuous
// pass. Runs in-process against the harness (no Docker); the CreateBucket group
// has both passing and out-of-scope-failing cases, so both sets are non-empty.
func TestRun_CapturesCaseNames(t *testing.T) {
	ctx := context.Background()
	h, err := StartHarness(ctx)
	if err != nil {
		t.Fatalf("start harness: %v", err)
	}
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	res := Run(ctx, Config{
		Endpoint:  h.Endpoint,
		AccessKey: h.AccessKey,
		SecretKey: h.SecretKey,
		Region:    h.Region,
	}, Suite{integration.TestCreateBucket})

	if len(res.PassedCases) == 0 && len(res.FailedCases) == 0 {
		t.Fatal("Run captured no case names — os.Stdout capture is broken (the conformance gate would pass vacuously)")
	}
	if int(res.Passed) != len(res.PassedCases) {
		t.Errorf("passed counter=%d but captured %d names: %v", res.Passed, len(res.PassedCases), res.PassedCases)
	}
	if int(res.Failed) != len(res.FailedCases) {
		t.Errorf("failed counter=%d but captured %d names: %v", res.Failed, len(res.FailedCases), res.FailedCases)
	}
}
