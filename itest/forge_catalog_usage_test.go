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
// container, as a benchmark run does at its end, with the tenant's access key
// in its environment, and checks its figures for a bucket some of whose
// catalog segments have shipped and retired: bytes on Forge, the local
// segment files, and a live walk that reads the retired segments it needs
// from Forge and finds every block. Without the key, the same walk misses
// the retired blocks.
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
	// Objects written once, whose manifests stay in the first segment, then
	// rounds of overwrites past seal_age, so later segments hold superseded
	// blocks and enough segments ship that the first retires.
	for i := range 4 {
		key := fmt.Sprintf("kept-%d", i)
		if err := ingottest.PutBytes(ctx, cfg, bucket, key, patternBytes(16<<10+i)); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	time.Sleep(3 * time.Second)
	const rounds = 10
	for round := range rounds {
		for i := range 4 {
			key := fmt.Sprintf("obj-%d", i)
			if err := ingottest.PutBytes(ctx, cfg, bucket, key, patternBytes(16<<10+round*100+i)); err != nil {
				t.Fatalf("put %s: %v", key, err)
			}
		}
		time.Sleep(3 * time.Second)
	}

	waitFor(t, 2*time.Minute, "every catalog segment to ship and the oldest to retire", func() bool {
		time.Sleep(2 * time.Second)
		pending := ingotSQL(t, ctx, s,
			"SELECT count(*) FROM ingot.segments WHERE bucket = '"+bucket+"' AND shipped_at IS NULL AND size_bytes > 0")
		shipped := ingotSQL(t, ctx, s,
			"SELECT count(*) FROM ingot.segments WHERE bucket = '"+bucket+"' AND shipped_at IS NOT NULL")
		p, _ := strconv.Atoi(pending)
		n, _ := strconv.Atoi(shipped)
		return p == 0 && n > len(catalogSegments(t, s, bucket))
	})
	space := ingotSQL(t, ctx, s, "SELECT space FROM ingot.buckets WHERE name = '"+bucket+"'")

	type figures struct {
		ForgeBytes         int64 `json:"forge_bytes"`
		DeadOnArrivalBytes int64 `json:"dead_on_arrival_bytes"`
		UnmeasuredBytes    int64 `json:"unmeasured_bytes"`
		LocalBytes         int64 `json:"local_bytes"`
		LiveBytes          int64 `json:"live_bytes"`
		LiveBlocks         int64 `json:"live_blocks"`
		MissingBlocks      int64 `json:"missing_blocks"`
		FetchedSegments    int64 `json:"fetched_segments"`
	}
	type report struct {
		Buckets []struct {
			Space string `json:"space"`
			figures
		} `json:"buckets"`
		Total figures `json:"total"`
	}
	// usage runs the command, with env (NAME=value) set for it. The key
	// travels on the test container's command line, which a throwaway test
	// key may; a benchmark run passes it through the environment instead.
	usage := func(env ...string) figures {
		t.Helper()
		args := append(append([]string{"env"}, env...), "/usr/bin/ingot", "catalog", "usage", "--config", "/etc/ingot/config.yaml")
		out, errOut, err := s.Exec(ctx, "ingot", args...)
		if err != nil {
			t.Fatalf("ingot catalog usage: %v (stderr=%s)", err, errOut)
		}
		var r report
		if err := json.Unmarshal([]byte(out), &r); err != nil {
			t.Fatalf("parse the report: %v\n%s", err, out)
		}
		if len(r.Buckets) != 1 || r.Buckets[0].Space != space {
			t.Fatalf("buckets = %+v, want one, for space %s", r.Buckets, space)
		}
		if r.Buckets[0].figures != r.Total {
			t.Fatalf("total %+v differs from the one bucket's %+v", r.Total, r.Buckets[0].figures)
		}
		return r.Total
	}

	got := usage("AWS_ACCESS_KEY_ID="+accessKey, "AWS_SECRET_ACCESS_KEY="+secretKey)
	t.Logf("catalog usage: %+v", got)
	switch {
	case got.ForgeBytes == 0:
		t.Fatal("no catalog bytes on Forge after every segment shipped")
	case got.UnmeasuredBytes != 0:
		t.Fatalf("%d bytes shipped unmeasured; every segment here shipped after the measurement existed", got.UnmeasuredBytes)
	case got.LocalBytes >= got.ForgeBytes:
		t.Fatalf("local segment files hold %d bytes, no fewer than the %d on Forge, with segments retired", got.LocalBytes, got.ForgeBytes)
	case got.FetchedSegments == 0:
		t.Fatal("the walk read no retired segment from Forge; the kept objects' manifests are in the first one")
	case got.LiveBlocks == 0 || got.LiveBytes > got.ForgeBytes:
		t.Fatalf("live walk found %d blocks of %d bytes over %d bytes on Forge", got.LiveBlocks, got.LiveBytes, got.ForgeBytes)
	case got.MissingBlocks != 0:
		t.Fatalf("live walk missed %d catalog blocks with Forge to read them from", got.MissingBlocks)
	}

	without := usage()
	t.Logf("catalog usage without a key: %+v", without)
	if without.MissingBlocks == 0 || without.FetchedSegments != 0 || without.LiveBlocks >= got.LiveBlocks {
		t.Fatalf("without a key the walk found %+v, want missing blocks, no fetch and fewer live blocks than %d", without, got.LiveBlocks)
	}
}
