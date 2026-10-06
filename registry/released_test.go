package registry_test

import (
	"os"
	"reflect"
	"sort"
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

// releasedStore is what the released-pass query needs of a store.
type releasedStore interface {
	registry.IntentStore
	registry.LocationStore
	registry.BlobRefStore
	registry.PendingReleaseStore
	registry.MultipartStore
}

// TestListReleasedPublishedLive runs the released-pass query against the
// in-memory store and, when INGOT_TEST_DSN is set, against Postgres (the Live
// suffix puts it in CI's live-Postgres job).
func TestListReleasedPublishedLive(t *testing.T) {
	t.Run("inmem", func(t *testing.T) {
		runReleasedSuite(t, inmem.NewMemStore())
	})
	t.Run("postgres", func(t *testing.T) {
		dsn := os.Getenv("INGOT_TEST_DSN")
		if dsn == "" {
			t.Skip("set INGOT_TEST_DSN to run the released-pass query against Postgres")
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
		if _, err := pool.Exec(ctx, `TRUNCATE ingot.upload_intents, ingot.blob_locations, ingot.blob_refs,
			ingot.blob_release_intents, ingot.multipart_sessions, ingot.multipart_parts CASCADE`); err != nil {
			t.Fatalf("truncate: %v", err)
		}
		runReleasedSuite(t, registry.NewPostgres(pool))
	})
}

func runReleasedSuite(t *testing.T, st releasedStore) {
	ctx := t.Context()
	space := testutil.RandomDID(t)
	published := func(name string) multihash.Multihash {
		t.Helper()
		d := evictDigest(t, "released:"+name)
		if err := st.PutIntent(ctx, registry.UploadIntent{Digest: d, LocalPath: "/spool/x", Size: 7, State: registry.IntentPublished}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		return d
	}

	released := published("released")
	located := published("located")
	if err := st.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: located, Provider: "did:web:piri", URL: "https://piri/blob", Size: 7}); err != nil {
		t.Fatalf("PutLocation: %v", err)
	}
	claimed := published("claimed")
	if err := st.AddBlobClaim(ctx, registry.BlobClaim{Digest: claimed, Bucket: "bk", ObjectKey: "k", VersionID: "v", Space: space}); err != nil {
		t.Fatalf("AddBlobClaim: %v", err)
	}
	pending := published("pending")
	if err := st.EnqueueRelease(ctx, space, pending, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("EnqueueRelease: %v", err)
	}
	inPart := published("in-part")
	if err := st.CreateSession(ctx, registry.MultipartSession{UploadID: "u1", Bucket: "bk", ObjectKey: "k", Space: space, State: registry.SessionOpen}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := st.PutPart(ctx, registry.MultipartPart{UploadID: "u1", PartNumber: 1, ETagMD5: make([]byte, 16), Size: 7, BlobDigests: []multihash.Multihash{inPart}, State: registry.PartParked}); err != nil {
		t.Fatalf("PutPart: %v", err)
	}
	// A part only protects its blob while its session is live.
	var inDeadParts []multihash.Multihash
	for _, state := range []string{registry.SessionCompleted, registry.SessionAborting} {
		d := published("in-" + state + "-part")
		id := "u-" + state
		if err := st.CreateSession(ctx, registry.MultipartSession{UploadID: id, Bucket: "bk", ObjectKey: "k-" + state, Space: space, State: registry.SessionOpen}); err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
		if err := st.PutPart(ctx, registry.MultipartPart{UploadID: id, PartNumber: 1, ETagMD5: make([]byte, 16), Size: 7, BlobDigests: []multihash.Multihash{d}, State: registry.PartParked}); err != nil {
			t.Fatalf("PutPart: %v", err)
		}
		if state == registry.SessionCompleted {
			if ok, err := st.LatchSession(ctx, id, registry.SessionOpen, registry.SessionCompleting); err != nil || !ok {
				t.Fatalf("LatchSession: %v, %v", ok, err)
			}
			if ok, err := st.CompleteSession(ctx, id, "etag", "v"); err != nil || !ok {
				t.Fatalf("CompleteSession: %v, %v", ok, err)
			}
		} else if ok, err := st.LatchSession(ctx, id, registry.SessionOpen, state); err != nil || !ok {
			t.Fatalf("LatchSession: %v, %v", ok, err)
		}
		inDeadParts = append(inDeadParts, d)
	}
	accepted := evictDigest(t, "released:accepted")
	if err := st.PutIntent(ctx, registry.UploadIntent{Digest: accepted, LocalPath: "/spool/x", Size: 7, State: registry.IntentAccepted}); err != nil {
		t.Fatalf("PutIntent: %v", err)
	}
	hexes := func(ds []multihash.Multihash) []string {
		var out []string
		for _, d := range ds {
			out = append(out, d.HexString())
		}
		sort.Strings(out)
		return out
	}
	listed := func() []string {
		t.Helper()
		got, err := st.ListReleasedPublished(ctx, nil, 100)
		if err != nil {
			t.Fatalf("ListReleasedPublished: %v", err)
		}
		var ds []multihash.Multihash
		for _, in := range got {
			ds = append(ds, in.Digest)
		}
		return hexes(ds)
	}

	want := hexes(append([]multihash.Multihash{released}, inDeadParts...))
	if got := listed(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ListReleasedPublished = %v, want %v", got, want)
	}

	// One at a time from a cursor, the pages come in digest order and cover
	// the same rows once each.
	var paged []string
	var after multihash.Multihash
	for {
		page, err := st.ListReleasedPublished(ctx, after, 1)
		if err != nil {
			t.Fatalf("ListReleasedPublished(after %x): %v", after, err)
		}
		if len(page) == 0 {
			break
		}
		after = page[0].Digest
		paged = append(paged, after.HexString())
	}
	if !reflect.DeepEqual(paged, want) {
		t.Fatalf("paged one at a time = %v, want %v in digest order", paged, want)
	}

	// The delete rechecks the conditions: a named blob keeps its intent.
	for _, d := range []multihash.Multihash{located, claimed, pending, inPart, accepted} {
		if ok, err := st.DeleteReleasedPublished(ctx, d); err != nil || ok {
			t.Fatalf("DeleteReleasedPublished(%s) = %v, %v; want false", d.HexString(), ok, err)
		}
		if _, err := st.GetIntent(ctx, d); err != nil {
			t.Fatalf("intent %s after a refused delete: %v", d.HexString(), err)
		}
	}
	for _, d := range append([]multihash.Multihash{released}, inDeadParts...) {
		if ok, err := st.DeleteReleasedPublished(ctx, d); err != nil || !ok {
			t.Fatalf("DeleteReleasedPublished(%s) = %v, %v; want true", d.HexString(), ok, err)
		}
	}
	if got := listed(); len(got) != 0 {
		t.Fatalf("after deleting the released intents: %v, want none", got)
	}
}
