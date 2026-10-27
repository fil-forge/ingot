//go:build itest

package itest

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	ingottest "github.com/fil-forge/ingot/testing"
	"github.com/fil-forge/smelt/pkg/stack"
)

// TestForgeRebuildAfterLostVolume loses Ingot's data volume mid-ingest and
// checks that Ingot recovers from Postgres alone: every acknowledged object
// reads back and lists, and the catalog segments it rebuilds from the
// journal ship.
//
// The config never seals the catalog on age within the test, so every write's
// catalog blocks sit in the open segment, unshipped, when the test wipes
// /data and stops Ingot. The journal is then their only copy. The compose
// file's restart policy brings Ingot back, and recovery rebuilds the lost
// segment from the journal. Its rows move onto the rebuilt segments, which
// ship once a write after the restart hands Ingot ship authority again.
//
//	go test -tags itest ./itest -run TestForgeRebuildAfterLostVolume -v -timeout 900s
func TestForgeRebuildAfterLostVolume(t *testing.T) {
	ctx := t.Context()

	s, ingotEndpoint := forgeStack(t, stack.WithServiceConfig("ingot", "testdata/config-lostvolume.yaml"))
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "lostvolume")
	cfg := forgeConfig(ingotEndpoint, accessKey, secretKey)

	const bucket = "lost-volume-bucket"
	if err := ingottest.CreateBucket(ctx, cfg, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	objects := map[string][]byte{}
	for i := range 5 {
		key := fmt.Sprintf("obj-%d", i)
		body := patternBytes(32<<10 + i)
		if err := ingottest.PutBytes(ctx, cfg, bucket, key, body); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		objects[key] = body
	}

	lost := ingotSQL(t, ctx, s, "SELECT string_agg(DISTINCT seq::text, ',') FROM ingot.catalog_journal WHERE bucket = '"+bucket+"'")
	if lost == "" {
		t.Fatal("no journaled catalog blocks before the wipe; the test proves nothing")
	}
	t.Logf("segments journaled before the wipe: %s", lost)

	// Wipe the volume under the running process, then stop it. A SIGTERM is
	// what PID 1 in the container honors; its shutdown can't save anything,
	// since the files are already gone.
	if _, errOut, err := s.Exec(ctx, "ingot", "sh", "-c", "rm -rf /data/*"); err != nil {
		t.Fatalf("wipe /data: %v (stderr=%s)", err, errOut)
	}
	_, _, _ = s.Exec(ctx, "ingot", "sh", "-c", "kill -TERM 1")
	waitFor(t, 2*time.Minute, "ingot to go down and come back", func() bool {
		time.Sleep(2 * time.Second)
		out, _, err := s.Exec(ctx, "ingot", "sh", "-c", "test -d /data/segments && echo up")
		return err == nil && bytes.Contains([]byte(out), []byte("up"))
	})
	// Test mode publishes Ingot on an ephemeral host port, which a restart
	// can move: read it again.
	ingotEndpoint = s.IngotEndpoint()
	cfg = forgeConfig(ingotEndpoint, accessKey, secretKey)
	waitHTTPOK(t, ingotEndpoint+"/health", 2*time.Minute)

	for key, want := range objects {
		got, err := ingottest.GetBytes(ctx, cfg, bucket, key)
		if err != nil {
			t.Fatalf("get %s after the rebuild: %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("get %s after the rebuild: %d bytes, want %d", key, len(got), len(want))
		}
	}
	keys, err := ingottest.ListKeys(ctx, cfg, bucket)
	if err != nil {
		t.Fatalf("list after the rebuild: %v", err)
	}
	for key := range objects {
		if !slices.Contains(keys, key) {
			t.Fatalf("list after the rebuild = %v, missing %s", keys, key)
		}
	}

	lostSeqs := ingotSQL(t, ctx, s, "SELECT count(*) FROM ingot.catalog_journal WHERE seq IN ("+lost+")")
	if n, _ := strconv.Atoi(lostSeqs); n != 0 {
		t.Fatalf("%d journal rows still under the lost segments %s", n, lost)
	}

	// A write after the restart captures ship authority for the space; the
	// retry sweep then ships the rebuilt segments, and their journal rows go.
	if err := ingottest.PutBytes(ctx, cfg, bucket, "after-restart", patternBytes(1<<10)); err != nil {
		t.Fatalf("put after the restart: %v", err)
	}
	waitFor(t, 4*time.Minute, "the rebuilt segments to ship", func() bool {
		time.Sleep(5 * time.Second)
		unshipped := ingotSQL(t, ctx, s,
			"SELECT count(*) FROM ingot.segments WHERE bucket = '"+bucket+"' AND state = 'sealed' AND shipped_at IS NULL AND retired_at IS NULL")
		n, _ := strconv.Atoi(unshipped)
		return n == 0
	})
}
