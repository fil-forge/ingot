package registry_test

import (
	"context"
	"errors"
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
	"github.com/fil-forge/ucantone/did"
)

// releaseStartStore is what the release start and cancel need of a store.
type releaseStartStore interface {
	registry.PendingReleaseStore
	registry.BlobRefStore
	registry.MultipartStore
	registry.IntentStore
	registry.LocationStore
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
	// locate records the provider as holding d for sp: what a claim requires.
	locate := func(sp did.DID, d multihash.Multihash) {
		t.Helper()
		if err := st.PutLocation(ctx, registry.BlobLocation{Space: sp, Digest: d, Provider: "did:key:p", URL: "http://p/blob", Size: 7}); err != nil {
			t.Fatalf("PutLocation: %v", err)
		}
	}
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

	t.Run("a started record starts again despite a part recorded since", func(t *testing.T) {
		// The release owns the blob from its start; a part recorded after
		// that belongs to a write waiting for the release to finish, and
		// must not hold the retry back.
		d := enqueue("retry")
		if s, err := st.StartRelease(ctx, space, d); err != nil || s != registry.ReleaseStarted {
			t.Fatalf("StartRelease = %v, %v", s, err)
		}
		if err := st.CreateSession(ctx, registry.MultipartSession{UploadID: "rs-retry", Bucket: "bk", ObjectKey: "k", Space: space, State: registry.SessionOpen}); err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
		if err := st.PutPart(ctx, registry.MultipartPart{UploadID: "rs-retry", PartNumber: 1, ETagMD5: make([]byte, 16), Size: 7, BlobDigests: []multihash.Multihash{d}, State: registry.PartParked}); err != nil {
			t.Fatalf("PutPart: %v", err)
		}
		if s, err := st.StartRelease(ctx, space, d); err != nil || s != registry.ReleaseStarted {
			t.Fatalf("StartRelease retry with a later part = %v, %v; want ReleaseStarted", s, err)
		}
	})

	t.Run("a claimed blob is not started", func(t *testing.T) {
		d := enqueue("claimed")
		locate(space, d)
		if err := st.AddBlobClaim(ctx, registry.BlobClaim{Digest: d, Bucket: "bk", ObjectKey: "k", VersionID: "v", Space: space}); err != nil {
			t.Fatalf("AddBlobClaim: %v", err)
		}
		got, err := st.StartRelease(ctx, space, d)
		if err != nil || got != registry.ReleaseClaimed {
			t.Fatalf("StartRelease = %v, %v; want ReleaseClaimed", got, err)
		}
		// Stale, and gone: deleted under the lock, so no caller deletes a
		// replacement by key afterwards.
		for _, pr := range mustListReleases(t, st, space) {
			if string(pr.Digest) == string(d) {
				t.Fatalf("a stale record survived its start: %+v", pr)
			}
		}
	})

	t.Run("the intent delete counts the intents other spaces keep", func(t *testing.T) {
		d := enqueue("last-out")
		other := testutil.RandomDID(t)
		for _, sp := range []did.DID{space, other} {
			if err := st.PutIntent(ctx, registry.UploadIntent{Space: sp, Digest: d, LocalPath: "/spool/x", Size: 7, State: registry.IntentPublished}); err != nil {
				t.Fatalf("PutIntent: %v", err)
			}
		}
		remaining, err := st.DeleteIntentAndRelease(ctx, space, d)
		if err != nil || remaining != 1 {
			t.Fatalf("DeleteIntentAndRelease with another space's intent = %d, %v; want 1", remaining, err)
		}
		remaining, err = st.DeleteIntentAndRelease(ctx, other, d)
		if err != nil || remaining != 0 {
			t.Fatalf("DeleteIntentAndRelease of the last intent = %d, %v; want 0", remaining, err)
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

	t.Run("a claim needs the location", func(t *testing.T) {
		// A committed body blob is located by its commit; a claim with no
		// location is one whose blob a release removed since the write
		// settled it.
		d := evictDigest(t, "start:unlocated")
		claim := registry.BlobClaim{Digest: d, Bucket: "bk", ObjectKey: "k", VersionID: "v", Space: space}
		if err := st.AddBlobClaim(ctx, claim); !errors.Is(err, registry.ErrReleaseInProgress) {
			t.Fatalf("AddBlobClaim with no location err = %v, want ErrReleaseInProgress", err)
		}
		locate(space, d)
		if err := st.AddBlobClaim(ctx, claim); err != nil {
			t.Fatalf("AddBlobClaim once located: %v", err)
		}
	})

	t.Run("a claim is refused while a release is started", func(t *testing.T) {
		d := enqueue("claim-during")
		claim := registry.BlobClaim{Digest: d, Bucket: "bk", ObjectKey: "k", VersionID: "v", Space: space}
		locate(space, d)
		if s, err := st.StartRelease(ctx, space, d); err != nil || s != registry.ReleaseStarted {
			t.Fatalf("StartRelease = %v, %v", s, err)
		}
		if err := st.AddBlobClaim(ctx, claim); !errors.Is(err, registry.ErrReleaseInProgress) {
			t.Fatalf("AddBlobClaim during a started release err = %v, want ErrReleaseInProgress", err)
		}
		if n, _ := st.CountClaims(ctx, space, d); n != 0 {
			t.Fatalf("a refused claim was recorded: %d", n)
		}
		// Another space's release does not refuse this space's claim.
		other := registry.BlobClaim{Digest: d, Bucket: "bk2", ObjectKey: "k", VersionID: "v", Space: testutil.RandomDID(t)}
		locate(other.Space, d)
		if err := st.AddBlobClaim(ctx, other); err != nil {
			t.Fatalf("AddBlobClaim in another space: %v", err)
		}
		// Once the release has run, the claim lands.
		if err := st.DeleteRelease(ctx, space, d); err != nil {
			t.Fatalf("DeleteRelease: %v", err)
		}
		if err := st.AddBlobClaim(ctx, claim); err != nil {
			t.Fatalf("AddBlobClaim after the release: %v", err)
		}
	})

	t.Run("a generation's claims land together or not at all", func(t *testing.T) {
		free := evictDigest(t, "start:free")
		going := enqueue("claims-going")
		locate(space, free)
		locate(space, going)
		if s, err := st.StartRelease(ctx, space, going); err != nil || s != registry.ReleaseStarted {
			t.Fatalf("StartRelease = %v, %v", s, err)
		}
		claims := []registry.BlobClaim{
			{Digest: free, Bucket: "bk", ObjectKey: "k", VersionID: "v", Space: space},
			{Digest: going, Bucket: "bk", ObjectKey: "k", VersionID: "v", Space: space},
		}
		if err := st.AddBlobClaims(ctx, claims); !errors.Is(err, registry.ErrReleaseInProgress) {
			t.Fatalf("AddBlobClaims with one digest under a started release err = %v, want ErrReleaseInProgress", err)
		}
		for _, d := range []multihash.Multihash{free, going} {
			if n, _ := st.CountClaims(ctx, space, d); n != 0 {
				t.Fatalf("a refused batch recorded a claim on %x", d)
			}
		}
		if err := st.DeleteRelease(ctx, space, going); err != nil {
			t.Fatalf("DeleteRelease: %v", err)
		}
		if err := st.AddBlobClaims(ctx, claims); err != nil {
			t.Fatalf("AddBlobClaims after the release: %v", err)
		}
		for _, d := range []multihash.Multihash{free, going} {
			if n, _ := st.CountClaims(ctx, space, d); n != 1 {
				t.Fatalf("claims on %x after the batch = %d, want 1", d, n)
			}
		}
	})
}

func mustListReleases(t *testing.T, st registry.PendingReleaseStore, space did.DID) []registry.PendingRelease {
	t.Helper()
	rows, err := st.ListReleasesBySpace(context.Background(), space)
	if err != nil {
		t.Fatalf("ListReleasesBySpace: %v", err)
	}
	return rows
}
