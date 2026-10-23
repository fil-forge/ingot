package s3frontend

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
)

// newJournalingBackend is a backend whose log ships: a segment seals once it
// is 200ms old, and its ship waits until the test calls the returned release
// func, so the test can read the journal before the ship and after it.
func newJournalingBackend(t *testing.T) (*Backend, *inmem.MemStore, *logstore.Store, func()) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	mem := inmem.NewMemStore()
	cache, err := blockstore.NewBlobCache(filepath.Join(dir, "cache"))
	require.NoError(t, err)
	spool, err := blockstore.NewSpool(filepath.Join(dir, "spool"))
	require.NoError(t, err)
	gate := make(chan struct{})
	log, err := logstore.Open(ctx, logstore.Config{
		Dir:    filepath.Join(dir, "segments"),
		Bucket: "bk",
		Meta:   mem,
		Catalog: logstore.PlaneConfig{
			Ship: true, SealBytes: 1 << 30, SealAge: 200 * time.Millisecond, Retain: 6,
			Flush: func(ctx context.Context, _ *logstore.Segment) (multihash.Multihash, error) {
				select {
				case <-gate:
					return nil, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		},
		Logger: zaptest.NewLogger(t),
	})
	require.NoError(t, err)
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(func() {
		release()
		_ = log.Close(ctx)
	})

	b := New(Deps{
		Authority:       mem,
		Registry:        mem,
		Intents:         mem,
		Locations:       mem,
		GC:              mem,
		Multipart:       mem,
		Parks:           mem,
		Reads:           blockstore.NewLayered(blockstore.LocalBlobs{Cache: cache, Spool: spool}, log, inmem.NopBaseReader{}),
		Log:             log,
		Spool:           spool,
		Cache:           cache,
		Uploader:        inmem.NopUploader{},
		Deferred:        inmem.NopUploader{},
		Remover:         &recordingRemover{},
		Registrar:       &recordingRegistrar{},
		UploadRegs:      mem,
		EncParams:       mem,
		RegionKeys:      testRegionKeys(t),
		TenantKeys:      testTenantKeys(),
		PendingReleases: mem,
	})
	require.NoError(t, mem.Create(ctx, "bk", did.Undef, registry.CreateState{}))
	return b, mem, log, release
}

// TestCommitJournalsItsCatalogBlocks: a PUT journals the catalog blocks it
// appended to the log, byte for byte, under the segment it appended them to,
// and shipping that segment clears them.
func TestCommitJournalsItsCatalogBlocks(t *testing.T) {
	ctx := context.Background()
	b, mem, log, release := newJournalingBackend(t)

	putObjV(t, b, "a", []byte("hello"))

	segs, err := mem.ListSegments(ctx, blockstore.PlaneCatalog, "bk")
	require.NoError(t, err)
	require.Len(t, segs, 1, "the PUT opened one segment")
	rows := mem.JournaledBlocks(segs[0].Seq)

	st, err := mem.Get(ctx, "bk")
	require.NoError(t, err)
	for _, c := range []cid.Cid{st.Root, currentManifestCID(t, b, "a")} {
		require.Contains(t, rows, string(c.Bytes()), "the new root and the manifest are journaled")
	}
	for k, data := range rows {
		c, err := cid.Cast([]byte(k))
		require.NoError(t, err)
		blk, err := log.Get(ctx, c)
		require.NoError(t, err, "every journaled block is one the log holds")
		require.True(t, bytes.Equal(blk.RawData(), data), "journaled bytes match the log's for %s", c)
	}

	// Let the segment ship.
	release()
	require.Eventually(t, func() bool {
		stats, err := mem.CatalogJournalStats(ctx)
		return err == nil && stats.Blocks == 0
	}, 5*time.Second, 20*time.Millisecond, "shipping the segment clears its journal")
}
