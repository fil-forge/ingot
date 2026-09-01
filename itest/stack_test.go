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
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
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
	if img := os.Getenv("INGOT_ITEST_HILT_IMAGE"); img != "" {
		t.Logf("using hilt image override: %s", img)
		opts = append(opts, stack.WithHiltImage(img))
	}
	// Same idea one step earlier in the pipeline: mount a locally-built piri
	// binary (linux, static) over the image's /usr/bin/piri — validates an
	// unreleased piri/ucantone change with no image build at all.
	if bin := os.Getenv("INGOT_ITEST_PIRI_BINARY"); bin != "" {
		t.Logf("using piri binary override: %s", bin)
		opts = append(opts, stack.WithPiriBinary(bin))
	}
	// And for hilt: validates an ingot change against an unreleased hilt
	// (e.g. a new field in the authorize response) before its image publishes.
	if bin := os.Getenv("INGOT_ITEST_HILT_BINARY"); bin != "" {
		t.Logf("using hilt binary override: %s", bin)
		opts = append(opts, stack.WithServiceBinary("hilt", bin))
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

// withMultipartTTLConfig mounts testdata/config-mpttl.yaml —
// multipart_session_ttl at 30s so the abandoned-session sweeper reaps an
// unfinished upload within a test's budget. Dedicated stacks only: the low
// TTL also reaps completed sessions.
func withMultipartTTLConfig() stack.Option {
	return stack.WithServiceConfig("ingot", "testdata/config-mpttl.yaml")
}

// ingotSQL runs one SQL statement against ingot's Postgres and returns the
// bare psql output (rows, newline-separated). Digests round-trip as hex:
// encode(digest,'hex') out, decode('<hex>','hex') in.
func ingotSQL(t *testing.T, ctx context.Context, s *stack.Stack, q string) string {
	t.Helper()
	out, errOut, err := s.Exec(ctx, "ingot-postgres", "psql", "-U", "ingot", "-d", "ingot", "-tAc", q)
	if err != nil {
		t.Fatalf("ingot sql %q: %v (stderr=%s)", q, err, errOut)
	}
	return strings.TrimSpace(out)
}

// countRowsForDigests counts the rows of table whose digest column matches
// any of the hex-encoded digests.
func countRowsForDigests(t *testing.T, ctx context.Context, s *stack.Stack, table, digestCol string, hexDigests []string) int {
	t.Helper()
	if len(hexDigests) == 0 {
		t.Fatalf("countRowsForDigests: no digests given")
	}
	terms := make([]string, len(hexDigests))
	for i, h := range hexDigests {
		terms[i] = fmt.Sprintf("decode('%s','hex')", h)
	}
	q := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s IN (%s)", table, digestCol, strings.Join(terms, ","))
	n, err := strconv.Atoi(ingotSQL(t, ctx, s, q))
	if err != nil {
		t.Fatalf("parse count from %s: %v", table, err)
	}
	return n
}

// objectBlobDigestsHex reads a committed object's body-blob digests
// (hex-encoded ciphertext multihashes) from ingot's reference index.
func objectBlobDigestsHex(t *testing.T, ctx context.Context, s *stack.Stack, bucket, key string) []string {
	t.Helper()
	q := fmt.Sprintf(`SELECT DISTINCT encode(digest,'hex') FROM ingot.blob_refs WHERE bucket = '%s' AND object_key = '%s'`, bucket, key)
	out := ingotSQL(t, ctx, s, q)
	var digests []string
	for _, line := range strings.Split(out, "\n") {
		if line != "" {
			digests = append(digests, line)
		}
	}
	if len(digests) == 0 {
		t.Fatalf("no blob_refs rows for %s/%s", bucket, key)
	}
	return digests
}

// partBlobDigestsHex reads one uploaded part's stored blob digests from
// ingot's Postgres as hex — the form that round-trips SQL (the base58 twin,
// partBlobDigests, matches piri's log format instead). Capture BEFORE abort
// or supersede: both destroy the multipart_parts rows.
func partBlobDigestsHex(t *testing.T, ctx context.Context, s *stack.Stack, uploadID string, partNumber int32) []string {
	t.Helper()
	q := fmt.Sprintf(
		`SELECT encode(d, 'hex') FROM ingot.multipart_parts, unnest(blob_digests) AS d WHERE upload_id = '%s' AND part_number = %d`,
		uploadID, partNumber)
	out := ingotSQL(t, ctx, s, q)
	var digests []string
	for _, line := range strings.Split(out, "\n") {
		if line != "" {
			digests = append(digests, line)
		}
	}
	if len(digests) == 0 {
		t.Fatalf("no blob digests recorded for upload %s part %d", uploadID, partNumber)
	}
	return digests
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

// forgeS3ConfVersioned is forgeS3Conf with the upstream suite's versioned
// mode on: teardown empties buckets via ListObjectVersions + per-version
// deletes (a plain DeleteObject on a versioned bucket only stacks delete
// markers). Used by the Versioning conformance categories.
func forgeS3ConfVersioned(endpoint, accessKey, secretKey string) *integration.S3Conf {
	cfg := forgeConfig(endpoint, accessKey, secretKey)
	cfg.VersioningEnabled = true
	return ingottest.NewS3Conf(cfg)
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
	// Multipart operations, first-class in hilt since fil-forge/hilt#35:
	// AbortMultipartUpload carries the blob.Abort + blob.Remove delegations
	// the abort leg invokes; the two List ops are catalog reads.
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

// spoolBlobPaths lists the body-blob files in the ingot container's spool
// (full paths, in-progress temp files excluded). Diffing two listings around
// a PUT identifies the envelope(s) that PUT spooled — the filename is the
// ciphertext digest, so it cannot be computed from the plaintext.
func spoolBlobPaths(t *testing.T, ctx context.Context, s *stack.Stack) map[string]bool {
	t.Helper()
	out, errOut, err := s.Exec(ctx, "ingot", "sh", "-c",
		`find /data/spool -maxdepth 1 -type f ! -name '.tmp*' 2>/dev/null`)
	if err != nil {
		t.Fatalf("list spool blobs: %v (stderr=%s)", err, errOut)
	}
	paths := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line != "" {
			paths[line] = true
		}
	}
	return paths
}

// newSpoolPaths returns the paths in after that are not in before.
func newSpoolPaths(before, after map[string]bool) []string {
	var added []string
	for p := range after {
		if !before[p] {
			added = append(added, p)
		}
	}
	return added
}

// corruptSpoolFileTail overwrites 16 bytes of the spooled envelope at path,
// tailOffset bytes from its end, with zeros — a byte-level tamper inside the
// final ciphertext chunk (the envelope's tail is STREAM ciphertext; 16
// random bytes are all-zero with probability 2^-128). Fails if the file
// content did not change.
func corruptSpoolFileTail(t *testing.T, ctx context.Context, s *stack.Stack, path string, tailOffset int64) {
	t.Helper()
	script := fmt.Sprintf(`
		f=%q
		size=$(wc -c < "$f")
		before=$(md5sum "$f")
		dd if=/dev/zero of="$f" bs=1 seek=$((size-%d)) count=16 conv=notrunc 2>/dev/null
		after=$(md5sum "$f")
		[ "$before" != "$after" ] || { echo "file unchanged" >&2; exit 1; }
	`, path, tailOffset)
	if out, errOut, err := s.Exec(ctx, "ingot", "sh", "-c", script); err != nil {
		t.Fatalf("corrupt spool file %s: %v (stdout=%s stderr=%s)", path, err, out, errOut)
	}
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

// --- raw-SDK helpers for the scenario tests (ported from the old
// in-process suite; same construction the upstream integration cases use) ---

func sdkClient(conf *integration.S3Conf) *s3.Client {
	return conf.GetClient()
}

// bigObjectClient is sdkClient minus the upstream suite's short per-request
// HTTP timeout, for requests that stream gigabytes before any response
// headers arrive (the 5 GiB max-part test). Same path-style addressing and
// single-attempt retry policy as forgeS3Conf.
func bigObjectClient(t *testing.T, endpoint, accessKey, secretKey string) *s3.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(forgeRegion),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		awsconfig.WithHTTPClient(&http.Client{}), // no Timeout: cancellation is the test context's job
		awsconfig.WithRetryMaxAttempts(1),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
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
		b[i] = patternByteAt(int64(i))
	}
	return b
}

// patternByteAt is patternBytes' formula at a single index, for payloads too
// large to materialize (patternReader) and for spot-checking ranges of them
// (patternRange) without holding the whole object.
func patternByteAt(i int64) byte {
	return byte(i*31 + 7 + (i>>16)*101)
}

// patternRange returns the pattern's bytes for the inclusive range
// [start, end] — the expected body of a ranged GET against a pattern object.
func patternRange(start, end int64) []byte {
	b := make([]byte, end-start+1)
	for i := range b {
		b[i] = patternByteAt(start + int64(i))
	}
	return b
}

// patternReader streams size bytes of the pattern without allocating them.
// It is seekable so the AWS SDK can rewind it for signing and retries.
type patternReader struct {
	off, size int64
}

func newPatternReader(size int64) *patternReader { return &patternReader{size: size} }

func (r *patternReader) Read(p []byte) (int, error) {
	if r.off >= r.size {
		return 0, io.EOF
	}
	n := int64(len(p))
	if rem := r.size - r.off; rem < n {
		n = rem
	}
	for i := int64(0); i < n; i++ {
		p[i] = patternByteAt(r.off + i)
	}
	r.off += n
	return int(n), nil
}

func (r *patternReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.off + offset
	case io.SeekEnd:
		abs = r.size + offset
	default:
		return 0, fmt.Errorf("patternReader.Seek: invalid whence %d", whence)
	}
	if abs < 0 {
		return 0, fmt.Errorf("patternReader.Seek: negative position %d", abs)
	}
	r.off = abs
	return abs, nil
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
