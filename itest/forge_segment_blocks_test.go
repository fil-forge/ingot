//go:build itest

package itest

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	ingottest "github.com/fil-forge/ingot/testing"
	"github.com/fil-forge/ingot/uploader"
	"github.com/fil-forge/smelt/pkg/stack"
)

// TestForgeSegmentBlockLimit writes more catalog blocks to one bucket than a
// shard index can list, all inside one seal window, and checks that the
// segment sealed full rather than growing past the limit, and shipped.
//
//	go test -tags itest ./itest -run TestForgeSegmentBlockLimit -v -timeout 900s
func TestForgeSegmentBlockLimit(t *testing.T) {
	ctx := t.Context()

	s, ingotEndpoint := forgeStack(t, stack.WithServiceConfig("ingot", "testdata/config-sealblocks.yaml"))
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "sealblocks")
	cfg := forgeConfig(ingotEndpoint, accessKey, secretKey)

	const bucket = "seal-blocks-bucket"
	if err := ingottest.CreateBucket(ctx, cfg, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	// A PUT writes at least six catalog blocks, so these fill one segment.
	const puts = 1500
	var wg sync.WaitGroup
	errs := make(chan error, puts)
	slots := make(chan struct{}, 16)
	for i := range puts {
		wg.Add(1)
		slots <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			if err := ingottest.PutBytes(ctx, cfg, bucket, fmt.Sprintf("obj-%04d", i), nil); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		t.Fatalf("put: %v", err)
	}

	waitFor(t, 3*time.Minute, "the full segment to ship with its index", func() bool {
		time.Sleep(2 * time.Second)
		return ingotSQL(t, ctx, s, "SELECT count(*) FROM ingot.segments WHERE bucket = '"+bucket+
			"' AND shipped_at IS NOT NULL AND index_digest IS NOT NULL") != "0"
	})
	space := ingotSQL(t, ctx, s, "SELECT space FROM ingot.buckets WHERE name = '"+bucket+"'")
	largest, err := strconv.Atoi(ingotSQL(t, ctx, s, "SELECT COALESCE(max(n), 0) FROM (SELECT count(*) AS n FROM ingot.shard_inclusions WHERE space = '"+
		space+"' GROUP BY shard_digest) AS shards"))
	if err != nil {
		t.Fatalf("count the shipped segment's blocks: %v", err)
	}
	// The segment sealed before the first commit that did not fit.
	if largest <= uploader.MaxIndexedBlocks-64 || largest > uploader.MaxIndexedBlocks {
		t.Fatalf("the largest shipped segment holds %d blocks, want just under or at %d", largest, uploader.MaxIndexedBlocks)
	}
}
