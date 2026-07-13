//go:build itest

package itest

import (
	"bytes"
	"strings"
	"testing"

	ingottest "github.com/fil-forge/ingot/testing"
)

// TestForgeNativeProvision proves ingot needs no guppy: it drives
// `ingot login` and `ingot space generate --provision-to` inside the ingot
// container (the access-request email confirmed via smtp4dev), then
// round-trips a PUT/GET through the forge-mode listener to confirm the full
// ship path (sprue -> piri -> indexer) works on a space ingot provisioned
// itself.
//
//	go test -tags itest ./itest -run TestForgeNativeProvision -v -timeout 900s
func TestForgeNativeProvision(t *testing.T) {
	ctx := t.Context()
	const email = "test@example.com"

	s, ingotEndpoint := forgeStack(t)

	// 1. ingot logs itself in (no guppy).
	ingotLoginViaEmail(t, ctx, s, email)

	// 2. ingot provisions its own space (no guppy): generate reuses the
	// daemon's /data/space.key, provisions it to the account on sprue, and
	// grants access. Provisioning is server-side, so the running daemon needs
	// no restart.
	out, errOut, err := s.Exec(ctx, "ingot",
		"ingot", "--config", ingotConfigPath, "space", "generate", "--provision-to", email)
	if err != nil {
		t.Fatalf("ingot space generate: %v (stdout=%s stderr=%s)", err, out, errOut)
	}
	if !strings.Contains(out+errOut, "Generated space:") {
		t.Fatalf("ingot space generate: unexpected output stdout=%q stderr=%q", out, errOut)
	}
	t.Logf("ingot space generate ok:\nstdout=%s\nstderr=%s", out, errOut)

	// 3. PUT/GET round-trip against the forge-mode ingot — exercises the full
	// blob ship path (sprue -> piri -> indexer) on the natively-provisioned
	// space. The exhaustive S3 surface is TestForgeVersity's job.
	cfg := forgeConfig(ingotEndpoint)
	const bucket, key = "native-provision", "obj"
	data := patternBytes(512 << 10)
	if err := ingottest.CreateBucket(ctx, cfg, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if err := ingottest.PutBytes(ctx, cfg, bucket, key, data); err != nil {
		t.Fatalf("put object: %v", err)
	}
	got, err := ingottest.GetBytes(ctx, cfg, bucket, key)
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(got), len(data))
	}
	t.Logf("self-provisioned round-trip OK: %d bytes through sprue/piri", len(data))
}
