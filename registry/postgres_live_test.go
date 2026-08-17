package registry_test

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/migrations"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
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
		 ingot.blob_encryption_params, ingot.multipart_sessions, ingot.multipart_parts,
		 ingot.gc_candidates, ingot.buckets CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	r := registry.NewPostgres(pool)
	seedBucket := func(t *testing.T, name string) {
		t.Helper()
		// space has no default (Create always supplies the DID Hilt
		// returns); the seed uses '' — the pre-space sentinel Get maps
		// to did.Undef.
		if _, err := pool.Exec(ctx, `INSERT INTO ingot.buckets (name, space) VALUES ($1, '')`, name); err != nil {
			t.Fatalf("seed bucket %q: %v", name, err)
		}
	}
	digest := []byte{0x12, 0x20, 0xab, 0xcd} // binary, to exercise bytea round-trips

	t.Run("bucket space defaults empty", func(t *testing.T) {
		seedBucket(t, "b")
		st, err := r.Get(ctx, "b")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if st.Space != did.Undef {
			t.Fatalf("space = %q, want empty default", st.Space)
		}
	})

	t.Run("blob claims count to zero", func(t *testing.T) {
		// The blob_refs PK is (digest, bucket, object_key, version_id); space is
		// denormalized, so a given (bucket, key, version) belongs to one space
		// (a bucket has one space). A second space therefore implies a second
		// bucket — bucket "b2" below, not a re-keyed "b".
		space := testutil.RandomDID(t)
		add := func(bucket, key string, sp did.DID) {
			if err := r.AddBlobClaim(ctx, registry.BlobClaim{Digest: digest, Bucket: bucket, ObjectKey: key, VersionID: registry.NullVersionID, Space: sp}); err != nil {
				t.Fatalf("AddBlobClaim: %v", err)
			}
		}
		add("b", "k1", space)
		add("b", "k2", space)
		add("b", "k1", space) // ON CONFLICT DO NOTHING — does not inflate the count
		add("b2", "k1", testutil.RandomDID(t))
		if n, _ := r.CountClaims(ctx, space, digest); n != 2 {
			t.Fatalf("count space1 = %d, want 2", n)
		}
		if n, _ := r.CountClaims(ctx, testutil.RandomDID(t), digest); n != 1 {
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
		space := testutil.RandomDID(t)
		if err := r.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: digest, Provider: "did:piri", URL: "http://piri/b", Size: 100}); err != nil {
			t.Fatalf("PutLocation: %v", err)
		}
		loc, err := r.GetLocation(ctx, space, digest)
		if err != nil || loc.URL != "http://piri/b" || loc.Size != 100 {
			t.Fatalf("GetLocation = %+v, err %v", loc, err)
		}
		if err := r.DeleteLocation(ctx, space, digest); err != nil {
			t.Fatalf("DeleteLocation: %v", err)
		}
		if _, err := r.GetLocation(ctx, space, digest); err != registry.ErrNotFound {
			t.Fatalf("GetLocation after delete = %v, want ErrNotFound", err)
		}
	})

	// liveFEEParams is a complete parameter set whose bytea values carry
	// embedded NULs and high bytes, to exercise real byte round-trips.
	liveFEEParams := func(space did.DID, d []byte) registry.BlobEncryptionParams {
		return registry.BlobEncryptionParams{
			Space:              space,
			Digest:             d,
			RegionWrappedCEK:   []byte{0x00, 0xde, 0xad, 0xbe, 0xef},
			RegionKeyVersion:   "region-v1",
			TenantRecipientKID: "did:key:tenant#wrap",
			HeaderLen:          212,
			BaseNonce:          []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07},
			ChunkSize:          65536,
			AAD:                []byte{0xa1, 0x00, 0x18, 0x20},
		}
	}

	t.Run("encryption params round trip", func(t *testing.T) {
		space := testutil.RandomDID(t)
		encDigest := []byte{0x00, 0x01, 0x02, 0xff}
		want := liveFEEParams(space, encDigest)
		if err := r.PutEncryptionParams(ctx, want); err != nil {
			t.Fatalf("PutEncryptionParams: %v", err)
		}
		got, err := r.GetEncryptionParams(ctx, space, encDigest)
		if err != nil {
			t.Fatalf("GetEncryptionParams: %v", err)
		}
		if !reflect.DeepEqual(*got, want) {
			t.Fatalf("GetEncryptionParams = %+v, want %+v", *got, want)
		}
	})

	t.Run("encryption params re-wrap in place", func(t *testing.T) {
		space := testutil.RandomDID(t)
		encDigest := []byte{0x03, 0x04}
		if err := r.PutEncryptionParams(ctx, liveFEEParams(space, encDigest)); err != nil {
			t.Fatalf("PutEncryptionParams: %v", err)
		}
		want := liveFEEParams(space, encDigest)
		want.RegionWrappedCEK = []byte{0x11, 0x22, 0x33}
		want.RegionKeyVersion = "region-v2"
		if err := r.PutEncryptionParams(ctx, want); err != nil {
			t.Fatalf("PutEncryptionParams (re-wrap): %v", err)
		}
		got, err := r.GetEncryptionParams(ctx, space, encDigest)
		if err != nil {
			t.Fatalf("GetEncryptionParams: %v", err)
		}
		if !reflect.DeepEqual(*got, want) {
			t.Fatalf("after re-wrap = %+v, want %+v", *got, want)
		}
	})

	t.Run("encryption params delete shreds", func(t *testing.T) {
		space := testutil.RandomDID(t)
		encDigest := []byte{0x05, 0x06}
		if err := r.PutEncryptionParams(ctx, liveFEEParams(space, encDigest)); err != nil {
			t.Fatalf("PutEncryptionParams: %v", err)
		}
		if err := r.DeleteEncryptionParams(ctx, space, encDigest); err != nil {
			t.Fatalf("DeleteEncryptionParams: %v", err)
		}
		if _, err := r.GetEncryptionParams(ctx, space, encDigest); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("GetEncryptionParams after delete = %v, want ErrNotFound", err)
		}
	})

	t.Run("incomplete encryption params rejected", func(t *testing.T) {
		// Rejected in Go, so the NOT NULL constraints are never reached.
		space := testutil.RandomDID(t)
		d := []byte{0x77}
		partial := liveFEEParams(space, d)
		partial.AAD = nil
		if err := r.PutEncryptionParams(ctx, partial); !errors.Is(err, registry.ErrInvalidEncryptionParams) {
			t.Fatalf("PutEncryptionParams(partial) = %v, want ErrInvalidEncryptionParams", err)
		}
		if _, err := r.GetEncryptionParams(ctx, space, d); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("incomplete params leaked a row: %v", err)
		}
	})

	t.Run("encryption params independent of location", func(t *testing.T) {
		// No foreign key and no cascade: the params outlive their location row,
		// so a caller removing a blob must delete from both tables.
		space := testutil.RandomDID(t)
		encDigest := []byte{0x08, 0x09}
		if err := r.PutEncryptionParams(ctx, liveFEEParams(space, encDigest)); err != nil {
			t.Fatalf("PutEncryptionParams: %v", err)
		}
		if err := r.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: encDigest, Provider: "did:piri", URL: "http://piri/enc", Size: 4096}); err != nil {
			t.Fatalf("PutLocation: %v", err)
		}
		if err := r.DeleteLocation(ctx, space, encDigest); err != nil {
			t.Fatalf("DeleteLocation: %v", err)
		}
		if _, err := r.GetEncryptionParams(ctx, space, encDigest); err != nil {
			t.Fatalf("DeleteLocation shredded the encryption params: %v", err)
		}
	})

	t.Run("park round trip", func(t *testing.T) {
		park := registry.BlobPark{
			Digest:        digest,
			AddTask:       []byte{0x01, 0x02},
			AcceptTask:    []byte{0x03, 0x04},
			PutInvocation: []byte("sealed-inv"),
			Size:          42,
		}
		if err := r.PutPark(ctx, park); err != nil {
			t.Fatalf("PutPark: %v", err)
		}
		got, err := r.GetPark(ctx, digest)
		if err != nil || string(got.PutInvocation) != "sealed-inv" || got.Size != 42 {
			t.Fatalf("GetPark = %+v, err %v", got, err)
		}
		// Upsert replaces in place.
		park.Size = 43
		if err := r.PutPark(ctx, park); err != nil {
			t.Fatalf("PutPark (upsert): %v", err)
		}
		if got, err := r.GetPark(ctx, digest); err != nil || got.Size != 43 {
			t.Fatalf("GetPark after upsert = %+v, err %v", got, err)
		}
		if err := r.DeletePark(ctx, digest); err != nil {
			t.Fatalf("DeletePark: %v", err)
		}
		if _, err := r.GetPark(ctx, digest); err != registry.ErrNotFound {
			t.Fatalf("GetPark after delete = %v, want ErrNotFound", err)
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

	t.Run("multipart listing sweeper and part refs", func(t *testing.T) {
		mk := func(id, key string) {
			t.Helper()
			if err := r.CreateSession(ctx, registry.MultipartSession{
				UploadID: id, Bucket: "b", ObjectKey: key,
				ContentEncoding: "testenc", ChecksumAlgorithm: "CRC32", ChecksumType: "FULL_OBJECT",
			}); err != nil {
				t.Fatalf("CreateSession %s: %v", id, err)
			}
		}
		mk("ls-2", "zeta")
		mk("ls-1", "alpha")
		mk("ls-3", "alpha") // same key, created after ls-1

		// New session columns round-trip.
		s, err := r.GetSession(ctx, "ls-1")
		if err != nil || s.ContentEncoding != "testenc" || s.ChecksumAlgorithm != "CRC32" ||
			s.ChecksumType != "FULL_OBJECT" || s.CreatedAt.IsZero() {
			t.Fatalf("GetSession new columns = %+v, err %v", s, err)
		}

		// ListSessions: (object_key, created_at, upload_id) order.
		sessions, err := r.ListSessions(ctx, "b")
		if err != nil || len(sessions) != 3 ||
			sessions[0].UploadID != "ls-1" || sessions[1].UploadID != "ls-3" || sessions[2].UploadID != "ls-2" {
			ids := make([]string, len(sessions))
			for i, x := range sessions {
				ids[i] = x.UploadID
			}
			t.Fatalf("ListSessions order = %v, err %v (want [ls-1 ls-3 ls-2])", ids, err)
		}

		// ListStaleSessions: cutoff in the past excludes them, future includes.
		if stale, err := r.ListStaleSessions(ctx, registry.SessionOpen, time.Now().Add(-time.Hour)); err != nil || len(stale) != 0 {
			t.Fatalf("ListStaleSessions past cutoff = %d, err %v (want 0)", len(stale), err)
		}
		if stale, err := r.ListStaleSessions(ctx, registry.SessionOpen, time.Now().Add(time.Hour)); err != nil || len(stale) != 3 {
			t.Fatalf("ListStaleSessions future cutoff = %d, err %v (want 3)", len(stale), err)
		}

		// CountPartRefs: bytea[] ANY-match across sessions, excluding one.
		shared := []byte{0xee, 0x01}
		if err := r.PutPart(ctx, registry.MultipartPart{UploadID: "ls-1", PartNumber: 1, ETagMD5: []byte{1}, Size: 1, BlobDigests: [][]byte{shared}}); err != nil {
			t.Fatalf("PutPart ls-1: %v", err)
		}
		if err := r.PutPart(ctx, registry.MultipartPart{UploadID: "ls-2", PartNumber: 1, ETagMD5: []byte{2}, Size: 1, BlobDigests: [][]byte{shared, {0xee, 0x02}}}); err != nil {
			t.Fatalf("PutPart ls-2: %v", err)
		}
		if n, err := r.CountPartRefs(ctx, shared, "ls-1"); err != nil || n != 1 {
			t.Fatalf("CountPartRefs(shared, exclude ls-1) = %d, err %v (want 1)", n, err)
		}
		if n, err := r.CountPartRefs(ctx, []byte{0xee, 0x02}, "ls-2"); err != nil || n != 0 {
			t.Fatalf("CountPartRefs(unique, exclude owner) = %d, err %v (want 0)", n, err)
		}

		// 'completed' passes the widened state CHECK constraint.
		if won, err := r.LatchSession(ctx, "ls-1", registry.SessionOpen, registry.SessionCompleted); err != nil || !won {
			t.Fatalf("latch to completed won=%v err=%v", won, err)
		}

		for _, id := range []string{"ls-1", "ls-2", "ls-3"} {
			_ = r.DeleteSession(ctx, id)
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
		seedBucket(t, "fg")
		if err := r.CASRoot(ctx, "fg", cid.Undef, committed); err != nil {
			t.Fatalf("CASRoot: %v", err)
		}
		seq, err := r.NextSegmentSeq(ctx)
		if err != nil {
			t.Fatalf("NextSegmentSeq: %v", err)
		}
		if err := r.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq, "fg"); err != nil {
			t.Fatalf("InsertSegmentOpen: %v", err)
		}
		// Ship a segment whose op-roots include a stale root the bucket never
		// adopted; only the committed root may advance forge_root.
		if err := r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seq, time.Now().Unix(), nil,
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
