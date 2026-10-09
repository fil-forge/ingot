package registry_test

import (
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/migrations"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/testutil"
)

// releaseStartStore is what the release start and cancel need of a store.
type releaseStartStore interface {
	registry.PendingReleaseStore
	registry.BlobRefStore
	registry.MultipartStore
}

// TestReleaseStartLive runs the release start and cancel against the
// in-memory store and, when INGOT_TEST_DSN is set, against Postgres (the
// Live suffix puts it in CI's live-Postgres job).
func TestReleaseStartLive(t *testing.T) {
	t.Run("inmem", func(t *testing.T) {
		runReleaseStartSuite(t, inmem.NewMemStore())
	})
	t.Run("postgres", func(t *testing.T) {
		dsn := os.Getenv("INGOT_TEST_DSN")
		if dsn == "" {
			t.Skip("set INGOT_TEST_DSN to run the release start against Postgres")
		}
		ctx := t.Context()
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		t.Cleanup(pool.Close)
		if err := migrations.Up(ctx, pool, zaptest.NewLogger(t)); err != nil {
			t.Fatalf("migrations.Up: %v", err)
		}
		if _, err := pool.Exec(ctx, `TRUNCATE ingot.blob_refs, ingot.blob_release_intents,
			ingot.multipart_sessions, ingot.multipart_parts CASCADE`); err != nil {
			t.Fatalf("truncate: %v", err)
		}
		runReleaseStartSuite(t, registry.NewPostgres(pool))
	})
}

func runReleaseStartSuite(t *testing.T, st releaseStartStore) {
	ctx := t.Context()
	space := testutil.RandomDID(t)
	enqueue := func(name string) multihash.Multihash {
		t.Helper()
		d := evictDigest(t, "start:"+name)
		if err := st.EnqueueRelease(ctx, space, d, time.Now()); err != nil {
			t.Fatalf("EnqueueRelease: %v", err)
		}
		return d
	}
	started := func(d multihash.Multihash) bool {
		t.Helper()
		rows, err := st.ListReleasesBySpace(ctx, space)
		if err != nil {
			t.Fatalf("ListReleasesBySpace: %v", err)
		}
		for _, pr := range rows {
			if string(pr.Digest) == string(d) {
				return !pr.StartedAt.IsZero()
			}
		}
		t.Fatalf("no record for %x", d)
		return false
	}

	t.Run("no record is cancelled", func(t *testing.T) {
		got, err := st.StartRelease(ctx, space, evictDigest(t, "start:none"))
		if err != nil || got != registry.ReleaseCancelled {
			t.Fatalf("StartRelease = %v, %v; want ReleaseCancelled", got, err)
		}
	})

	t.Run("a pending record starts, and starts again on retry", func(t *testing.T) {
		d := enqueue("pending")
		for range 2 {
			got, err := st.StartRelease(ctx, space, d)
			if err != nil || got != registry.ReleaseStarted {
				t.Fatalf("StartRelease = %v, %v; want ReleaseStarted", got, err)
			}
			if !started(d) {
				t.Fatalf("record not marked started")
			}
		}
		// Enqueued again (a later drop of the same digest), it stays started.
		if err := st.EnqueueRelease(ctx, space, d, time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("EnqueueRelease: %v", err)
		}
		if !started(d) {
			t.Fatalf("re-enqueue cleared the started mark")
		}
	})

	t.Run("a claimed blob is not started", func(t *testing.T) {
		d := enqueue("claimed")
		if err := st.AddBlobClaim(ctx, registry.BlobClaim{Digest: d, Bucket: "bk", ObjectKey: "k", VersionID: "v", Space: space}); err != nil {
			t.Fatalf("AddBlobClaim: %v", err)
		}
		got, err := st.StartRelease(ctx, space, d)
		if err != nil || got != registry.ReleaseClaimed {
			t.Fatalf("StartRelease = %v, %v; want ReleaseClaimed", got, err)
		}
		if started(d) {
			t.Fatalf("a refused start marked the record")
		}
	})

	t.Run("a blob a live session of the space names is not started", func(t *testing.T) {
		d := enqueue("referenced")
		if err := st.CreateSession(ctx, registry.MultipartSession{UploadID: "rs-1", Bucket: "bk", ObjectKey: "k", Space: space, State: registry.SessionOpen}); err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
		if err := st.PutPart(ctx, registry.MultipartPart{UploadID: "rs-1", PartNumber: 1, ETagMD5: make([]byte, 16), Size: 7, BlobDigests: []multihash.Multihash{d}, State: registry.PartParked}); err != nil {
			t.Fatalf("PutPart: %v", err)
		}
		got, err := st.StartRelease(ctx, space, d)
		if err != nil || got != registry.ReleaseReferenced {
			t.Fatalf("StartRelease = %v, %v; want ReleaseReferenced", got, err)
		}
		// Another space's session does not count.
		other := testutil.RandomDID(t)
		if err := st.EnqueueRelease(ctx, other, d, time.Now()); err != nil {
			t.Fatalf("EnqueueRelease: %v", err)
		}
		got, err = st.StartRelease(ctx, other, d)
		if err != nil || got != registry.ReleaseStarted {
			t.Fatalf("StartRelease in another space = %v, %v; want ReleaseStarted", got, err)
		}
	})

	t.Run("a write cancels a pending record and waits out a started one", func(t *testing.T) {
		got, err := st.CancelRelease(ctx, space, evictDigest(t, "start:absent"))
		if err != nil || got != registry.ReleaseNone {
			t.Fatalf("CancelRelease(absent) = %v, %v; want ReleaseNone", got, err)
		}
		pending := enqueue("cancel-pending")
		got, err = st.CancelRelease(ctx, space, pending)
		if err != nil || got != registry.ReleaseCancelledByWrite {
			t.Fatalf("CancelRelease(pending) = %v, %v; want ReleaseCancelledByWrite", got, err)
		}
		if s, err := st.StartRelease(ctx, space, pending); err != nil || s != registry.ReleaseCancelled {
			t.Fatalf("StartRelease after cancel = %v, %v; want ReleaseCancelled", s, err)
		}
		running := enqueue("cancel-started")
		if s, err := st.StartRelease(ctx, space, running); err != nil || s != registry.ReleaseStarted {
			t.Fatalf("StartRelease = %v, %v", s, err)
		}
		got, err = st.CancelRelease(ctx, space, running)
		if err != nil || got != registry.ReleaseInProgress {
			t.Fatalf("CancelRelease(started) = %v, %v; want ReleaseInProgress", got, err)
		}
		if !started(running) {
			t.Fatalf("a refused cancel removed the record")
		}
	})
}
