package registry_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/migrations"
	"github.com/fil-forge/ingot/registry"
)

func liveCid(t *testing.T, s string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte(s), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	return cid.NewCidV1(cid.DagCBOR, mh)
}

// TestPostgresStores_Live exercises the SQL-backed store methods against a
// real Postgres (jsonb, bytea[], ON CONFLICT, the latch's RowsAffected
// semantics — none of which the in-memory fake can validate). Skipped unless
// INGOT_TEST_DSN names a reachable database:
//
//	INGOT_TEST_DSN=postgres://postgres:pw@127.0.0.1:55432/ingot \
//	  GOWORK=off go test ./registry/ -run Live -v
func TestPostgresStores_Live(t *testing.T) {
	dsn := os.Getenv("INGOT_TEST_DSN")
	if dsn == "" {
		t.Skip("set INGOT_TEST_DSN to run the live Postgres store test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	if err := migrations.Up(ctx, pool, zaptest.NewLogger(t)); err != nil {
		t.Fatalf("migrations.Up: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`TRUNCATE ingot.blob_refs, ingot.upload_intents, ingot.blob_locations,
		 ingot.multipart_sessions, ingot.multipart_parts, ingot.gc_candidates, ingot.buckets CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	r := registry.NewPostgres(pool)
	digest := []byte{0x12, 0x20, 0xab, 0xcd} // binary, to exercise bytea round-trips

	t.Run("bucket space defaults empty", func(t *testing.T) {
		if err := r.Create(ctx, "b", time.Now().Unix()); err != nil {
			t.Fatalf("Create: %v", err)
		}
		st, err := r.Get(ctx, "b")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if st.Space != "" {
			t.Fatalf("space = %q, want empty default", st.Space)
		}
	})

	t.Run("blob claims count to zero", func(t *testing.T) {
		// The blob_refs PK is (digest, bucket, object_key, version_id); space is
		// denormalized, so a given (bucket, key, version) belongs to one space
		// (a bucket has one space). A second space therefore implies a second
		// bucket — bucket "b2" below, not a re-keyed "b".
		const space = "did:space:1"
		add := func(bucket, key, sp string) {
			if err := r.AddBlobClaim(ctx, registry.BlobClaim{Digest: digest, Bucket: bucket, ObjectKey: key, VersionID: registry.NullVersionID, Space: sp}); err != nil {
				t.Fatalf("AddBlobClaim: %v", err)
			}
		}
		add("b", "k1", space)
		add("b", "k2", space)
		add("b", "k1", space) // ON CONFLICT DO NOTHING — does not inflate the count
		add("b2", "k1", "did:space:2")
		if n, _ := r.CountClaims(ctx, space, digest); n != 2 {
			t.Fatalf("count space1 = %d, want 2", n)
		}
		if n, _ := r.CountClaims(ctx, "did:space:2", digest); n != 1 {
			t.Fatalf("count space2 = %d, want 1", n)
		}
		if err := r.DeleteBlobClaim(ctx, digest, "b", "k1", registry.NullVersionID); err != nil {
			t.Fatalf("DeleteBlobClaim: %v", err)
		}
		if err := r.DeleteBlobClaim(ctx, digest, "b", "k2", registry.NullVersionID); err != nil {
			t.Fatalf("DeleteBlobClaim: %v", err)
		}
		if n, _ := r.CountClaims(ctx, space, digest); n != 0 {
			t.Fatalf("count after release = %d, want 0", n)
		}
	})

	t.Run("intent lifecycle", func(t *testing.T) {
		if err := r.PutIntent(ctx, registry.UploadIntent{Digest: digest, LocalPath: "/spool/x", Size: 9, State: registry.IntentSpooled, Bucket: "b"}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		if err := r.SetIntentState(ctx, digest, registry.IntentParked); err != nil {
			t.Fatalf("SetIntentState: %v", err)
		}
		got, err := r.GetIntent(ctx, digest)
		if err != nil || got.State != registry.IntentParked || got.Bucket != "b" {
			t.Fatalf("GetIntent = %+v, err %v", got, err)
		}
		parked, _ := r.ListIntentsByState(ctx, registry.IntentParked)
		if len(parked) != 1 {
			t.Fatalf("parked = %d, want 1", len(parked))
		}
		if err := r.SetIntentState(ctx, []byte("missing"), registry.IntentParked); err != registry.ErrNotFound {
			t.Fatalf("SetIntentState missing = %v, want ErrNotFound", err)
		}
		if err := r.DeleteIntent(ctx, digest); err != nil {
			t.Fatalf("DeleteIntent: %v", err)
		}
		if _, err := r.GetIntent(ctx, digest); err != registry.ErrNotFound {
			t.Fatalf("GetIntent after delete = %v, want ErrNotFound", err)
		}
	})

	t.Run("location round trip", func(t *testing.T) {
		// Unencrypted blob: the FEE wrap columns store as NULL and read back zero.
		if err := r.PutLocation(ctx, registry.BlobLocation{Space: "s", Digest: digest, Provider: "did:piri", URL: "http://piri/b", Size: 100}); err != nil {
			t.Fatalf("PutLocation: %v", err)
		}
		loc, err := r.GetLocation(ctx, "s", digest)
		if err != nil || loc.URL != "http://piri/b" || loc.Size != 100 {
			t.Fatalf("GetLocation = %+v, err %v", loc, err)
		}
		if loc.RegionWrappedCEK != nil || loc.BaseNonce != nil || loc.ProtectedHeader != nil ||
			loc.RegionKeyVersion != "" || loc.TenantRecipientKID != "" || loc.ChunkSize != 0 {
			t.Fatalf("unencrypted location read back wrap material (NULL round-trip): %+v", loc)
		}
		if err := r.DeleteLocation(ctx, "s", digest); err != nil {
			t.Fatalf("DeleteLocation: %v", err)
		}
		if _, err := r.GetLocation(ctx, "s", digest); err != registry.ErrNotFound {
			t.Fatalf("GetLocation after delete = %v, want ErrNotFound", err)
		}
	})

	t.Run("location FEE wrap material", func(t *testing.T) {
		// Binary bytea (with an embedded NUL) exercises real byte round-trips.
		encDigest := []byte{0x00, 0x01, 0x02, 0xff}
		enc := registry.BlobLocation{
			Space: "s", Digest: encDigest, Provider: "did:piri", URL: "http://piri/enc", Size: 4096,
			RegionWrappedCEK:   []byte{0x00, 0xde, 0xad, 0xbe, 0xef},
			RegionKeyVersion:   "region-v1",
			TenantRecipientKID: "did:key:tenant#wrap",
			BaseNonce:          []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07},
			ChunkSize:          65536,
			ProtectedHeader:    []byte{0xa1, 0x00, 0x18, 0x20},
		}
		if err := r.PutLocation(ctx, enc); err != nil {
			t.Fatalf("PutLocation (encrypted): %v", err)
		}
		got, err := r.GetLocation(ctx, "s", encDigest)
		if err != nil {
			t.Fatalf("GetLocation (encrypted): %v", err)
		}
		if !bytes.Equal(got.RegionWrappedCEK, enc.RegionWrappedCEK) ||
			!bytes.Equal(got.BaseNonce, enc.BaseNonce) ||
			!bytes.Equal(got.ProtectedHeader, enc.ProtectedHeader) {
			t.Fatalf("bytea wrap material round-trip mismatch: %+v", got)
		}
		if got.RegionKeyVersion != "region-v1" || got.TenantRecipientKID != "did:key:tenant#wrap" || got.ChunkSize != 65536 {
			t.Fatalf("wrap scalar round-trip mismatch: %+v", got)
		}

		// Re-wrap in place: a PutLocation upsert swaps the CEK + key version.
		got.RegionWrappedCEK = []byte{0x11, 0x22, 0x33}
		got.RegionKeyVersion = "region-v2"
		if err := r.PutLocation(ctx, *got); err != nil {
			t.Fatalf("PutLocation (re-wrap): %v", err)
		}
		after, err := r.GetLocation(ctx, "s", encDigest)
		if err != nil || !bytes.Equal(after.RegionWrappedCEK, []byte{0x11, 0x22, 0x33}) || after.RegionKeyVersion != "region-v2" {
			t.Fatalf("re-wrap round-trip = %+v, err %v", after, err)
		}
		if after.ChunkSize != 65536 || !bytes.Equal(after.BaseNonce, enc.BaseNonce) {
			t.Fatalf("re-wrap disturbed non-wrap fields: %+v", after)
		}
		if err := r.DeleteLocation(ctx, "s", encDigest); err != nil {
			t.Fatalf("DeleteLocation: %v", err)
		}
	})

	t.Run("location partial FEE rejected", func(t *testing.T) {
		// A partial wrap set is rejected before it reaches SQL and leaves no row.
		d := []byte{0x77}
		partial := registry.BlobLocation{
			Space: "s", Digest: d, Provider: "did:piri", URL: "u", Size: 10,
			RegionWrappedCEK: []byte{0x01}, // the rest deliberately absent
		}
		if err := r.PutLocation(ctx, partial); !errors.Is(err, registry.ErrPartialFEE) {
			t.Fatalf("PutLocation(partial) = %v, want ErrPartialFEE", err)
		}
		if _, err := r.GetLocation(ctx, "s", d); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("partial FEE leaked a row: %v", err)
		}
	})

	t.Run("multipart session parts latch metadata", func(t *testing.T) {
		const id = "upl-1"
		meta := map[string]string{"x-amz-meta-foo": "bar"}
		if err := r.CreateSession(ctx, registry.MultipartSession{UploadID: id, Bucket: "b", ObjectKey: "k", ContentType: "text/plain", Metadata: meta}); err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
		if err := r.CreateSession(ctx, registry.MultipartSession{UploadID: id, Bucket: "b", ObjectKey: "k"}); err != registry.ErrExists {
			t.Fatalf("duplicate CreateSession = %v, want ErrExists", err)
		}
		s, err := r.GetSession(ctx, id)
		if err != nil || s.ContentType != "text/plain" || s.Metadata["x-amz-meta-foo"] != "bar" {
			t.Fatalf("GetSession = %+v, err %v (metadata jsonb round-trip)", s, err)
		}

		// bytea[] round trip + ordering.
		if err := r.PutPart(ctx, registry.MultipartPart{UploadID: id, PartNumber: 2, ETagMD5: []byte{0x02}, Size: 2, BlobDigests: [][]byte{{0xd2}}}); err != nil {
			t.Fatalf("PutPart 2: %v", err)
		}
		if err := r.PutPart(ctx, registry.MultipartPart{UploadID: id, PartNumber: 1, ETagMD5: []byte{0x01}, Size: 1, BlobDigests: [][]byte{{0xd1, 0xa}, {0xd1, 0xb}}}); err != nil {
			t.Fatalf("PutPart 1: %v", err)
		}
		parts, err := r.ListParts(ctx, id)
		if err != nil || len(parts) != 2 || parts[0].PartNumber != 1 || len(parts[0].BlobDigests) != 2 {
			t.Fatalf("ListParts = %+v, err %v", parts, err)
		}

		// single-winner latch
		won, err := r.LatchSession(ctx, id, registry.SessionOpen, registry.SessionCompleting)
		if err != nil || !won {
			t.Fatalf("Complete latch won=%v err=%v", won, err)
		}
		won, err = r.LatchSession(ctx, id, registry.SessionOpen, registry.SessionAborting)
		if err != nil || won {
			t.Fatalf("Abort latch after Complete won=%v err=%v, want won=false", won, err)
		}

		// delete cascades parts
		if err := r.DeleteSession(ctx, id); err != nil {
			t.Fatalf("DeleteSession: %v", err)
		}
		if after, _ := r.ListParts(ctx, id); len(after) != 0 {
			t.Fatalf("parts after session delete = %d, want 0 (cascade)", len(after))
		}
	})

	t.Run("gc candidate idempotent", func(t *testing.T) {
		if err := r.AddGCCandidate(ctx, digest, "b"); err != nil {
			t.Fatalf("AddGCCandidate: %v", err)
		}
		if err := r.AddGCCandidate(ctx, digest, "b"); err != nil {
			t.Fatalf("AddGCCandidate (dup): %v", err)
		}
	})

	t.Run("forge_root advance guarded on root", func(t *testing.T) {
		committed := liveCid(t, "fg-committed")
		stale := liveCid(t, "fg-stale")
		if err := r.Create(ctx, "fg", time.Now().Unix()); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := r.CASRoot(ctx, "fg", cid.Undef, committed); err != nil {
			t.Fatalf("CASRoot: %v", err)
		}
		seq, err := r.NextSegmentSeq(ctx)
		if err != nil {
			t.Fatalf("NextSegmentSeq: %v", err)
		}
		if err := r.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq); err != nil {
			t.Fatalf("InsertSegmentOpen: %v", err)
		}
		// Ship a segment whose op-roots include a stale root the bucket never
		// adopted; only the committed root may advance forge_root.
		if err := r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seq, time.Now().Unix(),
			[]blockstore.OpRoot{{Bucket: "fg", Root: stale}, {Bucket: "fg", Root: committed}}); err != nil {
			t.Fatalf("MarkSegmentShipped: %v", err)
		}
		st, err := r.Get(ctx, "fg")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !st.ForgeRoot.Equals(committed) {
			t.Fatalf("forge_root = %v, want committed root (stale op-root must be skipped)", st.ForgeRoot)
		}
	})
}
