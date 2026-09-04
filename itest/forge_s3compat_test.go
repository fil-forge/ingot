//go:build itest

package itest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
	s3tests "github.com/cloud-portable/s3tests/packages/go"
	"github.com/cloud-portable/s3tests/packages/go/report"
	"github.com/cloud-portable/s3tests/packages/go/report/gotest"
	"github.com/cloud-portable/s3tests/packages/go/report/html"
)

// TestForgeS3Compat runs the cloud-portable S3 compatibility corpus (via the
// alanshaw/s3tests runner) against a forge-mode ingot on the smelt stack. It
// reports each vector live through Go's testing output (one t.Run subtest per
// vector, via the gotest reporter) AND writes an HTML report. A single run of
// the corpus feeds both: the result stream is tee'd through gotest (live
// console) while each result is captured for the HTML render at the end.
//
// Because gotest maps a failing vector onto a failing subtest, this test
// reports FAIL when any vector fails — ingot does not implement the whole S3
// surface, so expect failures; they are the compatibility signal, not a
// regression. The HTML report and the summary tally are written regardless
// (a failing subtest does not abort the parent).
//
// Gated behind INGOT_S3COMPAT=1 (checked before the stack boots) so the
// normal `make itest` run and CI never spend the time. Env knobs:
//
//	INGOT_S3COMPAT=1                 enable the test
//	INGOT_S3COMPAT_OUT=<path>        report file (default itest/ingot-s3compat.html)
//	INGOT_S3COMPAT_GROUPS=a,b        restrict to feature groups (default: all)
//	INGOT_S3COMPAT_TAGS=tier-1       restrict to vectors carrying a tag
//	INGOT_S3COMPAT_CONCURRENCY=8     vectors run in parallel (default 4)
//
//	INGOT_S3COMPAT=1 GOWORK=off go test -tags itest ./itest \
//	  -run TestForgeS3Compat -v -timeout 1800s
func TestForgeS3Compat(t *testing.T) {
	if os.Getenv("INGOT_S3COMPAT") == "" {
		t.Skip("set INGOT_S3COMPAT=1 to run the S3 compatibility report (boots the stack, runs the whole corpus)")
	}
	ctx := t.Context()

	s, endpoint := forgeStack(t)
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "s3compat")

	concurrency := 4
	if v := os.Getenv("INGOT_S3COMPAT_CONCURRENCY"); v != "" {
		if n, err := fmt.Sscanf(v, "%d", &concurrency); n != 1 || err != nil || concurrency < 1 {
			t.Fatalf("INGOT_S3COMPAT_CONCURRENCY = %q, want a positive integer", v)
		}
	}

	runner, err := s3tests.New(s3tests.Config{
		Endpoint:    endpoint,
		Region:      forgeRegion,
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		Concurrency: concurrency,
		// $credential vectors (corpus handle "alt") need a second identity:
		// provision another hilt tenant per handle and hand back its SigV4
		// keys. The runner calls this lazily, once per handle, possibly from
		// a worker goroutine — so it uses the error-returning provisioning
		// path, never the t.Fatalf helper. CanonicalID/DisplayName are left
		// empty: ingot models tenant ownership as a did:plc, not an S3
		// canonical user id, so the ACL-grant vectors that interpolate those
		// won't pass regardless — but every other cross-identity vector now
		// runs instead of reporting blocked.
		ProvisionCredential: func(ctx context.Context, handle string) (s3tests.Credential, error) {
			ak, sk, err := hiltProvisionTenantErr(ctx, s, "s3compat-"+handle)
			if err != nil {
				return s3tests.Credential{}, err
			}
			return s3tests.Credential{AccessKeyID: ak, SecretAccessKey: sk}, nil
		},
	})
	if err != nil {
		t.Fatalf("s3tests.New: %v", err)
	}

	vectors, err := s3tests.Vectors()
	if err != nil {
		t.Fatalf("load corpus vectors: %v", err)
	}
	var filters []s3tests.FilterFunc
	properties := map[string]string{}
	if groups := os.Getenv("INGOT_S3COMPAT_GROUPS"); groups != "" {
		filters = append(filters, s3tests.Groups(strings.Split(groups, ",")...))
		properties["groups"] = groups
	}
	if tags := os.Getenv("INGOT_S3COMPAT_TAGS"); tags != "" {
		filters = append(filters, s3tests.Tags(strings.Split(tags, ",")...))
		properties["tags"] = tags
	}
	filters = append(filters, s3tests.ExcludeTags("quirk:not-aws"))
	selected := s3tests.ApplyFilters(vectors, filters...)
	if len(selected) == 0 {
		t.Fatalf("no vectors selected (groups=%q tags=%q)", os.Getenv("INGOT_S3COMPAT_GROUPS"), os.Getenv("INGOT_S3COMPAT_TAGS"))
	}

	outPath := os.Getenv("INGOT_S3COMPAT_OUT")
	if outPath == "" {
		outPath = "ingot-s3compat.html"
	}
	abs, err := filepath.Abs(outPath)
	if err != nil {
		t.Fatalf("resolve output path %q: %v", outPath, err)
	}
	f, err := os.Create(outPath)
	if err != nil {
		t.Fatalf("create report %q: %v", outPath, err)
	}
	defer func() { _ = f.Close() }()

	t.Logf("running %d vectors (corpus %s) against %s → %s", len(selected), runner.CorpusVersion(), endpoint, abs)

	// One run of the corpus feeds both reporters. gotest.Run drives the live
	// console output (a t.Run subtest per vector, printed as each completes);
	// the tee captures every result as it passes through, so the HTML render
	// below replays the same run without executing it twice.
	collected := make([]s3tests.VectorResult, 0, len(selected))
	raw := runner.Run(
		ctx,
		selected,
		s3tests.Skip("Bucket ACLs are not supported", s3tests.IDs("bucket-0022", "bucket-0023")),
		s3tests.Skip("PutBucketOwnershipControls are not supported", s3tests.IDs("bucket-0026")),
		s3tests.Skip("us-east-1 legacy 200-on-recreate; the target signs us-west-1 (see bucket-0039)", s3tests.Tags("quirk:us-east-1-legacy")),
	)
	gotest.Run(t, func(yield func(s3tests.VectorResult) bool) {
		for v := range raw {
			collected = append(collected, v)
			if !yield(v) {
				return
			}
		}
	})

	if err := html.Write(f, slices.Values(collected), report.Meta{
		CorpusVersion: runner.CorpusVersion(),
		Target:        "Ingot (smelt forge stack)",
		Properties:    properties,
		GeneratedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("write HTML report: %v", err)
	}

	counts := map[s3tests.Outcome]int{}
	for _, v := range collected {
		counts[v.Outcome]++
	}
	t.Logf("S3 compatibility report written to %s", abs)
	t.Logf("corpus %s: %d pass, %d fail, %d blocked, %d skipped (of %d)",
		runner.CorpusVersion(),
		counts[s3tests.Pass], counts[s3tests.Fail], counts[s3tests.Blocked], counts[s3tests.Skipped], len(collected))
}
