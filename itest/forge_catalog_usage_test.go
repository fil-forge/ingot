//go:build itest

package itest

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	ingottest "github.com/fil-forge/ingot/testing"
	"github.com/fil-forge/smelt/pkg/stack"
)

// TestForgeCatalogUsage runs `ingot catalog usage` inside the running Ingot
// container, as a benchmark run does at its end, and checks its figures for
// a bucket whose catalog segments have all shipped and stayed on disk: bytes
// on Forge, the local segment files, and a live walk that finds every block
// it needs.
//
//	go test -tags itest ./itest -run TestForgeCatalogUsage -v -timeout 900s
func TestForgeCatalogUsage(t *testing.T) {
	ctx := t.Context()

	s, ingotEndpoint := forgeStack(t, stack.WithServiceConfig("ingot", "testdata/config-catalogusage.yaml"))
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "catalogusage")
	cfg := forgeConfig(ingotEndpoint, accessKey, secretKey)

	const bucket = "catalog-usage-bucket"
	if err := ingottest.CreateBucket(ctx, cfg, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	// Overwrites leave superseded catalog blocks behind, some within one
	// segment and some across segments.
	for round := range 3 {
		for i := range 4 {
			key := fmt.Sprintf("obj-%d", i)
			if err := ingottest.PutBytes(ctx, cfg, bucket, key, patternBytes(16<<10+round*100+i)); err != nil {
				t.Fatalf("put %s: %v", key, err)
			}
		}
		time.Sleep(3 * time.Second) // past seal_age, so the rounds land in different segments
	}

	waitFor(t, 2*time.Minute, "every catalog segment to ship", func() bool {
		time.Sleep(2 * time.Second)
		pending := ingotSQL(t, ctx, s,
			"SELECT count(*) FROM ingot.segments WHERE bucket = '"+bucket+"' AND shipped_at IS NULL AND size_bytes > 0")
		shipped := ingotSQL(t, ctx, s,
			"SELECT count(*) FROM ingot.segments WHERE bucket = '"+bucket+"' AND shipped_at IS NOT NULL")
		p, _ := strconv.Atoi(pending)
		n, _ := strconv.Atoi(shipped)
		return p == 0 && n > 1
	})
	space := ingotSQL(t, ctx, s, "SELECT space FROM ingot.buckets WHERE name = '"+bucket+"'")

	out, errOut, err := s.Exec(ctx, "ingot", "/usr/bin/ingot", "catalog", "usage", "--config", "/etc/ingot/config.yaml")
	if err != nil {
		t.Fatalf("ingot catalog usage: %v (stderr=%s)", err, errOut)
	}
	type figures struct {
		ForgeBytes         int64 `json:"forge_bytes"`
		DeadOnArrivalBytes int64 `json:"dead_on_arrival_bytes"`
		UnmeasuredBytes    int64 `json:"unmeasured_bytes"`
		LocalBytes         int64 `json:"local_bytes"`
		LiveBytes          int64 `json:"live_bytes"`
		LiveBlocks         int64 `json:"live_blocks"`
		MissingBlocks      int64 `json:"missing_blocks"`
	}
	var report struct {
		Buckets []struct {
			Space string `json:"space"`
			figures
		} `json:"buckets"`
		Total figures `json:"total"`
	}
	if err := json.Unmarshal([]byte(out), &report); err != nil {
		t.Fatalf("parse the report: %v\n%s", err, out)
	}
	t.Logf("catalog usage: %+v", report.Total)

	if len(report.Buckets) != 1 || report.Buckets[0].Space != space {
		t.Fatalf("buckets = %+v, want one, for space %s", report.Buckets, space)
	}
	got := report.Buckets[0].figures
	if got != report.Total {
		t.Fatalf("total %+v differs from the one bucket's %+v", report.Total, got)
	}
	switch {
	case got.ForgeBytes == 0:
		t.Fatal("no catalog bytes on Forge after every segment shipped")
	case got.UnmeasuredBytes != 0:
		t.Fatalf("%d bytes shipped unmeasured; every segment here shipped after the measurement existed", got.UnmeasuredBytes)
	case got.LocalBytes < got.ForgeBytes:
		t.Fatalf("local segment files hold %d bytes, fewer than the %d on Forge, with nothing retired", got.LocalBytes, got.ForgeBytes)
	case got.LiveBlocks == 0 || got.LiveBytes > got.LocalBytes:
		t.Fatalf("live walk found %d blocks of %d bytes over %d local bytes", got.LiveBlocks, got.LiveBytes, got.LocalBytes)
	case got.MissingBlocks != 0:
		t.Fatalf("live walk missed %d catalog blocks with every segment on disk", got.MissingBlocks)
	}
}
