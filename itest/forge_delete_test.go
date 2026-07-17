//go:build itest

package itest

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	ingottest "github.com/fil-forge/ingot/testing"
	"github.com/fil-forge/smelt/pkg/stack"
)

// TestForgeDeleteReleasesNetworkBlob is the delete-finality regression gate
// (FIL-588): DeleteObject must release the blob on the network, not just drop
// registry rows. The chain under test is ingot's reference index (claims→0 ⇒
// RemoveBlob) → forgeclient /blob/remove → sprue (deregister + forward) →
// piri (claim release; deferred physical deletion once the PDP aggregate
// root retires on-chain).
//
// The published piri/sprue images predate the /blob/remove handlers, so this
// test injects working-tree builds of both and skips when they aren't
// provided:
//
//	(cd ../../piri  && CGO_ENABLED=0 GOOS=linux go build -o /tmp/piri  ./cmd)
//	(cd ../../sprue && CGO_ENABLED=0 GOOS=linux go build -o /tmp/sprue ./cmd/main.go)
//	ITEST_PIRI_BIN=/tmp/piri ITEST_SPRUE_BIN=/tmp/sprue \
//	  go test -tags itest ./itest -run TestForgeDeleteReleasesNetworkBlob -v -timeout 900s
//
// Once images with the handlers publish, drop the env gate and the binary
// injection.
func TestForgeDeleteReleasesNetworkBlob(t *testing.T) {
	piriBin := os.Getenv("ITEST_PIRI_BIN")
	sprueBin := os.Getenv("ITEST_SPRUE_BIN")
	if piriBin == "" || sprueBin == "" {
		t.Skip("requires piri+sprue builds with the /blob/remove chain: set ITEST_PIRI_BIN and ITEST_SPRUE_BIN (see test doc comment)")
	}

	ctx := t.Context()
	const email = "test@example.com"

	// The Curio-based piri requires a Postgres node (harmonydb) and, until
	// the published localdev image carries the mockrpc Ticket fix, an
	// overridable blockchain image.
	stackOpts := []stack.Option{
		stack.WithPiriNodes(stack.PiriNodeConfig{Postgres: true}),
		stack.WithServiceBinary("piri", piriBin),
		stack.WithServiceBinary("upload", sprueBin),
	}
	if img := os.Getenv("ITEST_BLOCKCHAIN_IMAGE"); img != "" {
		stackOpts = append(stackOpts, stack.WithBlockchainImage(img))
	}
	s, ingotEndpoint := forgeStack(t, stackOpts...)
	ingotSelfProvision(t, ctx, s, email)
	cfg := forgeConfig(ingotEndpoint)

	const bucket, key = "delete-bucket", "obj"

	// Unique deterministic body, large enough to be a real blob on piri.
	data := make([]byte, 512*1024)
	for i := range data {
		data[i] = byte(i*13 + 11)
	}

	if err := ingottest.CreateBucket(ctx, cfg, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if err := ingottest.PutBytes(ctx, cfg, bucket, key, data); err != nil {
		t.Fatalf("put object: %v", err)
	}

	// Precondition: the blob really lives on piri — wipe the spool and prove
	// the GET re-fetches from the network (same tier the eviction test pins).
	if out, errOut, err := s.Exec(ctx, "ingot", "sh", "-c", "rm -rf /data/spool"); err != nil {
		t.Fatalf("evict spool: %v (stdout=%s stderr=%s)", err, out, errOut)
	}
	if got, err := ingottest.GetBytes(ctx, cfg, bucket, key); err != nil || len(got) != len(data) {
		t.Fatalf("read-through from piri before delete: err=%v len=%d", err, len(got))
	}

	if err := ingottest.DeleteObject(ctx, cfg, bucket, key); err != nil {
		t.Fatalf("delete object: %v", err)
	}

	// The object is gone from the gateway.
	if _, err := ingottest.GetBytes(ctx, cfg, bucket, key); err == nil {
		t.Fatalf("GET after delete succeeded, want NoSuchKey")
	}

	// And the release traversed the network: piri's /blob/remove handler ran
	// and, with the last claim gone, queued the piece for removal. Byte
	// release is fully asynchronous — ingot's releaseBlobs is best-effort
	// post-commit, and piri's removal sweep (PDPRemoveSweep, 30s ticks)
	// re-verifies claims and pipeline state before finalizing — so poll the
	// provider's logs through to the finalization line.
	waitForPiriLog(t, ctx, s, "/blob/remove", 2*time.Minute)
	waitForPiriLog(t, ctx, s, "queueing piece removal", 2*time.Minute)
	waitForPiriLog(t, ctx, s, "finalized piece removal", 3*time.Minute)
	t.Logf("delete finality OK: /blob/remove executed on piri and the sweep finalized the byte release")
}

// waitForPiriLog polls piri-0's container logs until substr appears.
func waitForPiriLog(t *testing.T, ctx context.Context, s *stack.Stack, substr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		logs, err := s.Logs(ctx, "piri-0")
		if err == nil && strings.Contains(logs, substr) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context done waiting for piri log %q: %v", substr, ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	t.Fatalf("piri-0 logs never contained %q within %s", substr, timeout)
}
