//go:build itest

package itest

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/tests/integration"

	ingottest "github.com/fil-forge/ingot/testing"
	"github.com/fil-forge/smelt/pkg/clients/guppy"
	"github.com/fil-forge/smelt/pkg/stack"
)

// This file is the shared harness for the integration tests: they boot the
// full smelt Forge stack (sprue + piri + indexer + postgres + ...) via
// smelt's Go SDK and mount the WORKING TREE's ingot binary over the published
// image, so every S3 call exercises this checkout's code against the real
// network path. Requires Docker; runs behind the itest build tag:
//
//	make itest

// ingotConfigPath is where smelt's ingot system definition mounts the daemon
// config inside the container.
const ingotConfigPath = "/etc/ingot/config.yaml"

// forgeCreds are the root S3 credentials from smelt's
// systems/ingot/config/config.yaml.
const (
	forgeAccessKey = "ingot"
	forgeSecretKey = "ingotsecret"
	forgeRegion    = "us-east-1"
)

// TestMain sweeps containers/volumes leaked by prior crashed itest runs (same
// smeltery- project prefix as any smelt-SDK stack). Best-effort: a missing
// docker only matters once a test actually boots a stack.
func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	if err := stack.CleanupLeaked(ctx); err != nil {
		log.Printf("itest: pre-test sweep warning: %v", err)
	}
	cancel()
	os.Exit(m.Run())
}

var (
	buildOnce   sync.Once
	builtBinary string
	buildErr    error
)

// localIngotBinary compiles the working tree's ingot daemon once per test
// run as a static linux binary suitable for bind-mounting over the published
// image's /usr/bin/ingot. GOARCH follows the test host so the binary matches
// the Docker host's container platform; GOWORK=off matches the Makefile
// convention (the parent go.work may declare a newer Go than the toolchain).
func localIngotBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "ingot-itest-bin-")
		if err != nil {
			buildErr = err
			return
		}
		out := filepath.Join(dir, "ingot")
		cmd := exec.Command("go", "build", "-o", out, "./cmd/ingot")
		cmd.Dir = ".." // tests run in itest/; build from the module root
		cmd.Env = append(os.Environ(),
			"CGO_ENABLED=0",
			"GOOS=linux",
			"GOARCH="+runtime.GOARCH,
			"GOWORK=off",
		)
		if outb, err := cmd.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("go build ./cmd/ingot: %v\n%s", err, outb)
			return
		}
		builtBinary = out
	})
	if buildErr != nil {
		t.Fatalf("build local ingot binary: %v", buildErr)
	}
	return builtBinary
}

// forgeStack boots the smelt stack with the working tree's ingot injected
// (plus any extra stack options), waits for ingot's S3 listener, and returns
// the stack and ingot's host endpoint. Callers that upload must provision
// first (ingotSelfProvision). The stack lives until the calling test —
// including all of its subtests — completes; share it across categories by
// booting in a parent test and passing the conf into t.Run subtests.
func forgeStack(t *testing.T, extra ...stack.Option) (*stack.Stack, string) {
	t.Helper()
	t.Logf("booting the smelt Forge stack (~1-2 min; first run also compiles ingot and pulls images)")
	opts := append([]stack.Option{
		stack.WithPiriNodes(stack.PiriNodeConfig{}),
		stack.WithServiceBinary("ingot", localIngotBinary(t)),
	}, extra...)
	s := stack.MustNewStack(t, opts...)
	endpoint := s.IngotEndpoint()
	waitHTTPOK(t, endpoint+"/health", 2*time.Minute)
	t.Logf("forge-mode ingot S3 endpoint: %s", endpoint)
	return s, endpoint
}

// withSmallBlobConfig mounts testdata/config-smallblob.yaml over the ingot
// container's config — smelt's default forge config with max_blob_size
// lowered to 64 KiB, for the blob-split scenarios.
func withSmallBlobConfig() stack.Option {
	return stack.WithServiceConfig("ingot", "testdata/config-smallblob.yaml")
}

// forgeConfig is the roundtrip-helper config for a stack-deployed ingot.
func forgeConfig(endpoint string) ingottest.Config {
	return ingottest.Config{
		Endpoint:  endpoint,
		AccessKey: forgeAccessKey,
		SecretKey: forgeSecretKey,
		Region:    forgeRegion,
	}
}

// forgeS3Conf is the upstream-suite config for a stack-deployed ingot, for
// invoking versitygw cases directly.
func forgeS3Conf(endpoint string) *integration.S3Conf {
	return ingottest.NewS3Conf(forgeConfig(endpoint))
}

// waitHTTPOK polls url until it returns 2xx or the timeout elapses.
func waitHTTPOK(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("%s not healthy after %s", url, timeout)
}

// ingotLoginViaEmail logs the ingot agent in as email — guppy-free: the
// blocking `ingot login` CLI runs in the ingot container and the validation
// link is clicked from inside that same container.
func ingotLoginViaEmail(t *testing.T, ctx context.Context, s *stack.Stack, email string) {
	t.Helper()
	if err := guppy.LoginViaEmail(ctx, s, "ingot", email,
		"ingot", "--config", ingotConfigPath, "login", email); err != nil {
		t.Fatalf("ingot login via email: %v", err)
	}
}

// ingotSelfProvision has ingot provision its own space to email on sprue
// (login + `space generate --provision-to`), no guppy involved.
func ingotSelfProvision(t *testing.T, ctx context.Context, s *stack.Stack, email string) {
	t.Helper()
	ingotLoginViaEmail(t, ctx, s, email)
	if out, errOut, err := s.Exec(ctx, "ingot",
		"ingot", "--config", ingotConfigPath, "space", "generate", "--provision-to", email); err != nil {
		t.Fatalf("ingot space generate: %v (stdout=%s stderr=%s)", err, out, errOut)
	}
}

// spoolBlobCount counts the body blobs in the ingot container's spool,
// ignoring in-progress temp files. Used to prove object bodies are spooled by
// digest (the data-plane inversion), not journaled into the log.
func spoolBlobCount(t *testing.T, ctx context.Context, s *stack.Stack) int {
	t.Helper()
	out, errOut, err := s.Exec(ctx, "ingot", "sh", "-c",
		`find /data/spool -maxdepth 1 -type f ! -name '.tmp*' 2>/dev/null | wc -l`)
	if err != nil {
		t.Fatalf("count spool blobs: %v (stderr=%s)", err, errOut)
	}
	n, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("parse spool count %q: %v", out, err)
	}
	return n
}

// --- raw-SDK helpers for the scenario tests (ported from the old
// in-process suite; same construction the upstream integration cases use) ---

func sdkClient(conf *integration.S3Conf) *s3.Client {
	return conf.GetClient()
}

func quotedMD5(b []byte) string {
	sum := md5.Sum(b)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// patternBytes returns n deterministic bytes — a cheap way to make a payload
// whose every byte position is verifiable after a round trip. The (i>>16) term
// mixes in the 64 KiB-blob index so each coarse-split blob has distinct content
// (a purely position-linear pattern would make equal-length blobs identical and
// dedup them in the spool).
func patternBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + 7 + (i>>16)*101)
	}
	return b
}

func getBody(t *testing.T, ctx context.Context, cl *s3.Client, bucket, key, rangeHdr string) []byte {
	t.Helper()
	in := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if rangeHdr != "" {
		in.Range = aws.String(rangeHdr)
	}
	out, err := cl.GetObject(ctx, in)
	if err != nil {
		t.Fatalf("GetObject %s (range %q): %v", key, rangeHdr, err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read body %s: %v", key, err)
	}
	return b
}
