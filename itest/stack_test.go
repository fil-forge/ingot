//go:build itest

package itest

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
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
	"github.com/fil-forge/versitygw/tests/integration"

	ingottest "github.com/fil-forge/ingot/testing"
	"github.com/fil-forge/smelt/pkg/stack"
)

// This file is the shared harness for the integration tests: they boot the
// full smelt Forge stack (sprue + piri + indexer + postgres + ...) via
// smelt's Go SDK and mount the WORKING TREE's ingot binary over the published
// image, so every S3 call exercises this checkout's code against the real
// network path. Requires Docker; runs behind the itest build tag:
//
//	make itest

// forgeRegion must match smelt's ingot config (systems/ingot/config/
// config.yaml) and the provider region hilt's post_start hook registers
// ingot under (INGOT_REGION) — tenants are provisioned per region.
const forgeRegion = "us-west-1"

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
// the stack and ingot's host endpoint. Callers must provision a hilt tenant
// first (hiltProvisionTenant) and sign with its credentials. The stack lives until the calling test —
// including all of its subtests — completes; share it across categories by
// booting in a parent test and passing the conf into t.Run subtests.
func forgeStack(t *testing.T, extra ...stack.Option) (*stack.Stack, string) {
	t.Helper()
	t.Logf("booting the smelt Forge stack (~1-2 min; first run also compiles ingot and pulls images)")
	opts := []stack.Option{
		// Postgres-backed piri: piri:main's curio PDP pipeline refuses
		// sqlite ("curio PDP pipeline requires Postgres") as of 2026-07-24.
		stack.WithPiriNodes(stack.PiriNodeConfig{Postgres: true}),
		stack.WithServiceBinary("ingot", localIngotBinary(t)),
	}
	// Local-dev escape hatches: run against upload-service (sprue) / piri
	// images the registry doesn't have yet — e.g. built from an unmerged
	// branch. Unset (CI) uses the published defaults. These exist because the
	// hilt integration made the forge stack cross-service: hilt mints did:plc
	// tenant spaces, so both sprue (bucket create / catalog ship) and piri
	// (the /content/retrieve read tier) must be able to resolve did:plc — a
	// capability that lands in those services branch-by-branch.
	if img := os.Getenv("INGOT_ITEST_UPLOAD_IMAGE"); img != "" {
		t.Logf("using upload-service image override: %s", img)
		opts = append(opts, stack.WithUploadImage(img))
	}
	if img := os.Getenv("INGOT_ITEST_PIRI_IMAGE"); img != "" {
		t.Logf("using piri image override: %s", img)
		opts = append(opts, stack.WithPiriImage(img))
	}
	// Same idea one step earlier in the pipeline: mount a locally-built piri
	// binary (linux, static) over the image's /usr/bin/piri — validates an
	// unreleased piri/ucantone change with no image build at all.
	if bin := os.Getenv("INGOT_ITEST_PIRI_BINARY"); bin != "" {
		t.Logf("using piri binary override: %s", bin)
		opts = append(opts, stack.WithPiriBinary(bin))
	}
	opts = append(opts, extra...)
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

// forgeConfig is the roundtrip-helper config for a stack-deployed ingot,
// signing with the given hilt-issued tenant credentials.
func forgeConfig(endpoint, accessKey, secretKey string) ingottest.Config {
	return ingottest.Config{
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Region:    forgeRegion,
	}
}

// forgeS3Conf is the upstream-suite config for a stack-deployed ingot, for
// invoking versitygw cases directly.
func forgeS3Conf(endpoint, accessKey, secretKey string) *integration.S3Conf {
	return ingottest.NewS3Conf(forgeConfig(endpoint, accessKey, secretKey))
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

// hiltAllPermissions is every S3 permission hilt recognizes (its pkg/s3perm
// map). The conformance cases exercise the whole S3 surface, so the test
// tenant's access key carries them all.
var hiltAllPermissions = []string{
	"s3:GetObject",
	"s3:GetObjectVersion",
	"s3:GetObjectRetention",
	"s3:GetObjectLegalHold",
	"s3:ListBucket",
	"s3:ListBucketVersions",
	"s3:PutObject",
	"s3:PutObjectRetention",
	"s3:PutObjectLegalHold",
	"s3:DeleteObject",
	"s3:DeleteObjectVersion",
	"s3:CreateBucket",
	"s3:ListAllMyBuckets",
	"s3:DeleteBucket",
	"s3:AbortMultipartUpload",
	"s3:ListMultipartUploadParts",
	"s3:ListBucketMultipartUploads",
}

// hiltProvisionTenant provisions tenantID in hilt with an all-permission
// access key and returns the S3 credentials tests sign with. This is the
// forge-mode onboarding path: ingot no longer self-provisions a space (the
// login/space CLI is gone) — hilt owns tenancy, mints the tenant's did:plc
// space, registers it as a customer with the upload service, and issues the
// SigV4 keys ingot authorizes against via /s3/request/authorize.
//
// The Tenant API is reached with curl inside the hilt container (so no host
// port mapping is needed), authenticated with smelt's local-dev partner key.
// The tenant's region must match the provider region hilt's post_start hook
// registered ingot under (forgeRegion).
func hiltProvisionTenant(t *testing.T, ctx context.Context, s *stack.Stack, tenantID string) (accessKey, secretKey string) {
	t.Helper()
	const partnerAuth = "Authorization: Bearer dev-partner-key"

	if out, errOut, err := s.Exec(ctx, "hilt", "curl", "-sS", "-f", "-X", "PUT",
		"http://localhost:80/tenants/"+tenantID,
		"-H", partnerAuth, "-H", "Content-Type: application/json",
		"-d", fmt.Sprintf(`{"region":%q}`, forgeRegion)); err != nil {
		t.Fatalf("hilt provision tenant %q: %v (stdout=%s stderr=%s)", tenantID, err, out, errOut)
	}

	keyReq, err := json.Marshal(map[string]any{
		"name":        "itest",
		"permissions": hiltAllPermissions,
	})
	if err != nil {
		t.Fatalf("marshal access-key request: %v", err)
	}
	out, errOut, err := s.Exec(ctx, "hilt", "curl", "-sS", "-f", "-X", "POST",
		"http://localhost:80/tenants/"+tenantID+"/access-keys",
		"-H", partnerAuth, "-H", "Content-Type: application/json",
		"-d", string(keyReq))
	if err != nil {
		t.Fatalf("hilt create access key for %q: %v (stdout=%s stderr=%s)", tenantID, err, out, errOut)
	}
	var created struct {
		AccessKeyID     string `json:"accessKeyId"`
		SecretAccessKey string `json:"secretAccessKey"`
	}
	if err := json.Unmarshal([]byte(out), &created); err != nil {
		t.Fatalf("parse access-key response %q: %v", out, err)
	}
	if created.AccessKeyID == "" || created.SecretAccessKey == "" {
		t.Fatalf("hilt returned incomplete credentials: %s", out)
	}
	t.Logf("hilt tenant %q provisioned; access key %s", tenantID, created.AccessKeyID)
	return created.AccessKeyID, created.SecretAccessKey
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
