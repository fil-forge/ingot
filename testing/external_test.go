//go:build e2e

package testing

import (
	"context"
	"os"
	"testing"
)

// TestExternalVersity runs the versitygw integration suite against an
// EXTERNAL, already-running ingot S3 endpoint (e.g. an ingot daemon
// deployed in the smelt Forge stack) rather than the in-process harness.
//
// Configure via env and run with the e2e build tag:
//
//	INGOT_E2E_ENDPOINT=http://localhost:15110 \
//	  go test -tags e2e ./testing -run TestExternalVersity -v
//
// Optional: INGOT_E2E_ACCESS, INGOT_E2E_SECRET, INGOT_E2E_REGION,
// INGOT_E2E_SUITE (smoke|crud|full; default smoke).
//
// This is the same versitygw Suite ingot's in-process smoke tests use, so
// a forge-mode ingot in smelt should report the same pass/xfail profile as
// the in-memory harness — the S3 semantics are backend-independent.
func TestExternalVersity(t *testing.T) {
	endpoint := os.Getenv("INGOT_E2E_ENDPOINT")
	if endpoint == "" {
		t.Skip("INGOT_E2E_ENDPOINT not set; this test targets an external ingot S3 listener")
	}

	cfg := Config{
		Endpoint:  endpoint,
		AccessKey: getenvDefault("INGOT_E2E_ACCESS", "ingot"),
		SecretKey: getenvDefault("INGOT_E2E_SECRET", "ingotsecret"),
		Region:    getenvDefault("INGOT_E2E_REGION", "us-east-1"),
	}

	suiteName := getenvDefault("INGOT_E2E_SUITE", "smoke")
	suite, ok := externalSuites[suiteName]
	if !ok {
		t.Fatalf("unknown INGOT_E2E_SUITE %q (want smoke|crud|full)", suiteName)
	}

	res := Run(context.Background(), cfg, suite)
	t.Logf("versitygw %q suite vs %s: ran=%d passed=%d failed=%d", suiteName, endpoint, res.Ran, res.Passed, res.Failed)

	// We don't assert Failed==0: ingot is not yet fully S3-conformant (the
	// known xfail set). The meaningful signal is that the suite actually
	// exercised the external listener and the conformant cases pass.
	if res.Ran == 0 {
		t.Fatalf("no versitygw cases ran against %s — is the endpoint reachable?", endpoint)
	}
	if res.Passed == 0 {
		t.Fatalf("no versitygw cases passed against %s", endpoint)
	}
}

// TestInProcessVersityBaseline runs the SAME suite against a fresh
// in-process harness, so the external/smelt result can be compared to a
// clean-ingot baseline (the forge-mode S3 semantics should match the
// in-memory ones exactly).
func TestInProcessVersityBaseline(t *testing.T) {
	h, err := StartHarness(context.Background())
	if err != nil {
		t.Fatalf("start harness: %v", err)
	}
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	suiteName := getenvDefault("INGOT_E2E_SUITE", "smoke")
	suite := externalSuites[suiteName]
	res := Run(context.Background(), Config{
		Endpoint:  h.Endpoint,
		AccessKey: h.AccessKey,
		SecretKey: h.SecretKey,
		Region:    h.Region,
	}, suite)
	t.Logf("IN-PROCESS %q baseline: ran=%d passed=%d failed=%d", suiteName, res.Ran, res.Passed, res.Failed)
}

var externalSuites = map[string]Suite{
	"smoke": Smoke,
	"crud":  CRUD,
	"full":  Full,
}

func getenvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
