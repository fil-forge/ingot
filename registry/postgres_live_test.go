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
		 ingot.gc_candidates, ingot.buckets, ingot.revocation_cursor,
		 ingot.blob_release_intents CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	r := registry.NewPostgres(pool)
	// seedBucket inserts a bucket row directly, bypassing Create, and returns
	// the space DID it generated. space and tenant have no default — Create
	// always supplies the DIDs Hilt returns — so the seed supplies them too,
	// with the tenant sentinel a pre-column row would carry.
	seedBucket := func(t *testing.T, name string) did.DID {
		t.Helper()
		space := testutil.RandomDID(t)
		if _, err := pool.Exec(ctx, `INSERT INTO ingot.buckets (name, space, tenant) VALUES ($1, $2, $3)`,
			name, space.String(), registry.UnknownTenant.String()); err != nil {
			t.Fatalf("seed bucket %q: %v", name, err)
		}
		return space
	}
	digest := []byte{0x12, 0x20, 0xab, 0xcd} // binary, to exercise bytea round-trips

	t.Run("bucket space round trips", func(t *testing.T) {
		space := seedBucket(t, "b")
		st, err := r.Get(ctx, "b")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if st.Space != space {
			t.Fatalf("space = %q, want %q", st.Space, space)
		}
		// The backfill sentinel reads back as the constant, no special case.
		if st.Tenant != registry.UnknownTenant {
			t.Fatalf("tenant = %q, want the sentinel %q", st.Tenant, registry.UnknownTenant)
		}
	})

	t.Run("bucket tenant round trips", func(t *testing.T) {
		tenant := testutil.RandomDID(t)
		if err := r.Create(ctx, "owned", testutil.RandomDID(t), registry.CreateState{Tenant: tenant}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		st, err := r.Get(ctx, "owned")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if st.Tenant != tenant {
			t.Fatalf("tenant = %q, want %q", st.Tenant, tenant)
		}
	})

	t.Run("create rejects an undefined tenant", func(t *testing.T) {
		if err := r.Create(ctx, "tenantless", testutil.RandomDID(t), registry.CreateState{}); err == nil {
			t.Fatal("Create without a tenant succeeded; the row would be unreadable")
		}
		if _, err := r.Get(ctx, "tenantless"); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("Get after rejected Create = %v, want ErrNotFound (no row written)", err)
		}
	})

	t.Run("blob claims count to zero", func(t *testing.T) {
		// The blob_refs PK is (digest, bucket, object_key, version_id); space is
		// denormalized, so a given (bucket, key, version) belongs to one space
		// (a bucket has one space). A second space therefore implies a second
		// bucket — bucket "b2" below, not a re-keyed "b".
		space := testutil.RandomDID(t)
		space2 := testutil.RandomDID(t)
		add := func(bucket, key string, sp did.DID) {
			if err := r.AddBlobClaim(ctx, registry.BlobClaim{Digest: digest, Bucket: bucket, ObjectKey: key, VersionID: registry.NullVersionID, Space: sp}); err != nil {
				t.Fatalf("AddBlobClaim: %v", err)
			}
		}
		add("b", "k1", space)
		add("b", "k2", space)
		add("b", "k1", space) // ON CONFLICT DO NOTHING — does not inflate the count
		add("b2", "k1", space2)
		if n, _ := r.CountClaims(ctx, space, digest); n != 2 {
			t.Fatalf("count space1 = %d, want 2", n)
		}
		if n, _ := r.CountClaims(ctx, space2, digest); n != 1 {
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

		// A claim publishes the digest's upload intent, and the state stays
		// once the claims are gone: it is the durable mark of a committed blob.
		pubDigest := multihash.Multihash([]byte{0x12, 0x20, 0xc1, 0xa1})
		if err := r.PutIntent(ctx, registry.UploadIntent{Digest: pubDigest, LocalPath: "/spool/x", Size: 1, State: registry.IntentAccepted, Bucket: "b"}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		if err := r.AddBlobClaim(ctx, registry.BlobClaim{Digest: pubDigest, Bucket: "b", ObjectKey: "kp", VersionID: registry.NullVersionID, Space: space}); err != nil {
			t.Fatalf("AddBlobClaim (publish): %v", err)
		}
		if in, err := r.GetIntent(ctx, pubDigest); err != nil || in.State != registry.IntentPublished {
			t.Fatalf("intent after claim = %+v, err %v (want published)", in, err)
		}
		if err := r.DeleteBlobClaim(ctx, pubDigest, "b", "kp", registry.NullVersionID); err != nil {
			t.Fatalf("DeleteBlobClaim (publish): %v", err)
		}
		if in, err := r.GetIntent(ctx, pubDigest); err != nil || in.State != registry.IntentPublished {
			t.Fatalf("intent after the claim dropped = %+v, err %v (want still published)", in, err)
		}
		// A claim on a digest with no intent row (a shipped segment) is fine.
		if err := r.AddBlobClaim(ctx, registry.BlobClaim{Digest: multihash.Multihash([]byte{0x12, 0x20, 0xc1, 0xa2}), Bucket: "b", ObjectKey: "kn", VersionID: registry.NullVersionID, Space: space}); err != nil {
			t.Fatalf("AddBlobClaim (no intent): %v", err)
		}
	})

	t.Run("pin claims require an existing claim, all or nothing", func(t *testing.T) {
		space := testutil.RandomDID(t)
		d1 := multihash.Multihash([]byte{0x12, 0x20, 0xb1, 0x01})
		d2 := multihash.Multihash([]byte{0x12, 0x20, 0xb1, 0x02})
		pin := func(ds ...multihash.Multihash) []registry.BlobClaim {
			var out []registry.BlobClaim
			for _, d := range ds {
				out = append(out, registry.BlobClaim{Digest: d, Bucket: "pb", ObjectKey: "copy", VersionID: "null#7", Space: space})
			}
			return out
		}
		if ok, err := r.PinBlobClaims(ctx, pin(d1)); err != nil || ok {
			t.Fatalf("pin with no claim to attach to: ok=%v err=%v, want refused", ok, err)
		}
		if err := r.AddBlobClaim(ctx, registry.BlobClaim{Digest: d1, Bucket: "pb", ObjectKey: "src", VersionID: "null#1", Space: space}); err != nil {
			t.Fatalf("AddBlobClaim: %v", err)
		}
		// d1 is claimed, d2 is not: nothing is recorded.
		if ok, err := r.PinBlobClaims(ctx, pin(d1, d2)); err != nil || ok {
			t.Fatalf("pin with one unclaimed digest: ok=%v err=%v, want refused", ok, err)
		}
		if n, _ := r.CountClaims(ctx, space, d1); n != 1 {
			t.Fatalf("refused pin left %d claims on d1, want the source's 1", n)
		}
		if ok, err := r.PinBlobClaims(ctx, pin(d1)); err != nil || !ok {
			t.Fatalf("pin beside a live claim: ok=%v err=%v, want recorded", ok, err)
		}
		if ok, err := r.PinBlobClaims(ctx, pin(d1)); err != nil || !ok {
			t.Fatalf("repeated pin: ok=%v err=%v, want idempotent true", ok, err)
		}
		if n, _ := r.CountClaims(ctx, space, d1); n != 2 {
			t.Fatalf("claims after pin = %d, want 2", n)
		}
		// A claim in another space does not qualify.
		other := testutil.RandomDID(t)
		if ok, err := r.PinBlobClaims(ctx, []registry.BlobClaim{{Digest: d1, Bucket: "pb2", ObjectKey: "copy", VersionID: "null#1", Space: other}}); err != nil || ok {
			t.Fatalf("pin against another space's claim: ok=%v err=%v, want refused", ok, err)
		}
		// Dropping the source's claim beside the pin enqueues nothing; dropping
		// the pin too does.
		if enq, err := r.DropClaimEnqueueRelease(ctx, d1, "pb", "src", "null#1", space, time.Now()); err != nil || enq {
			t.Fatalf("drop source beside pin: enqueued=%v err=%v, want false", enq, err)
		}
		if enq, err := r.DropClaimEnqueueRelease(ctx, d1, "pb", "copy", "null#7", space, time.Now()); err != nil || !enq {
			t.Fatalf("drop last claim: enqueued=%v err=%v, want true", enq, err)
		}
	})

	t.Run("drop claim enqueues release atomically", func(t *testing.T) {
		space := testutil.RandomDID(t)
		add := func(key string) {
			if err := r.AddBlobClaim(ctx, registry.BlobClaim{Digest: digest, Bucket: "rb", ObjectKey: key, VersionID: "null#1", Space: space}); err != nil {
				t.Fatalf("AddBlobClaim: %v", err)
			}
		}
		add("k1")
		add("k2")

		due := time.Now().Add(-time.Second) // already due
		enq, err := r.DropClaimEnqueueRelease(ctx, digest, "rb", "k1", "null#1", space, due)
		if err != nil {
			t.Fatalf("DropClaimEnqueueRelease (first): %v", err)
		}
		if enq {
			t.Fatalf("first drop enqueued a release while a claim remains")
		}
		enq, err = r.DropClaimEnqueueRelease(ctx, digest, "rb", "k2", "null#1", space, due)
		if err != nil {
			t.Fatalf("DropClaimEnqueueRelease (last): %v", err)
		}
		if !enq {
			t.Fatalf("last drop did not enqueue a release")
		}

		listed, err := r.ListDueReleases(ctx, time.Now(), 10)
		if err != nil {
			t.Fatalf("ListDueReleases: %v", err)
		}
		found := false
		for _, pr := range listed {
			if pr.Space == space && string(pr.Digest) == string(digest) {
				found = true
			}
		}
		if !found {
			t.Fatalf("enqueued release not listed as due: %v", listed)
		}

		// Upsert keeps the LATER not_before: re-enqueueing far in the future
		// pushes the intent out of the due window.
		if err := r.EnqueueRelease(ctx, space, digest, time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("EnqueueRelease (extend): %v", err)
		}
		listed, err = r.ListDueReleases(ctx, time.Now(), 10)
		if err != nil {
			t.Fatalf("ListDueReleases (after extend): %v", err)
		}
		for _, pr := range listed {
			if pr.Space == space && string(pr.Digest) == string(digest) {
				t.Fatalf("extended intent still listed as due")
			}
		}

		if err := r.DeleteRelease(ctx, space, digest); err != nil {
			t.Fatalf("DeleteRelease: %v", err)
		}

		// Bulk enqueue: one statement, repeated digests recorded once, the
		// upsert keeps an existing later not_before, and the records come
		// back as they stand so the caller sees that later time.
		d2 := multihash.Multihash([]byte{0xbb, 0x02})
		if err := r.EnqueueRelease(ctx, space, digest, time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("EnqueueRelease (future): %v", err)
		}
		recs, err := r.EnqueueReleases(ctx, space, []multihash.Multihash{digest, d2, d2}, time.Now().Add(-time.Second))
		if err != nil || len(recs) != 2 {
			t.Fatalf("EnqueueReleases = %d records, err %v (want 2)", len(recs), err)
		}
		for _, pr := range recs {
			if string(pr.Digest) == string(digest) && pr.NotBefore.Before(time.Now().Add(30*time.Minute)) {
				t.Fatalf("bulk enqueue returned an existing record with an earlier not_before: %v", pr.NotBefore)
			}
			if string(pr.Digest) == string(d2) && pr.NotBefore.After(time.Now()) {
				t.Fatalf("new record not due: %v", pr.NotBefore)
			}
		}
		all, err := r.ListReleasesBySpace(ctx, space)
		if err != nil || len(all) != 2 {
			t.Fatalf("ListReleasesBySpace = %d, err %v (want 2)", len(all), err)
		}
		for _, pr := range all {
			_ = r.DeleteRelease(ctx, space, pr.Digest)
		}
	})

	t.Run("delete intent and release record atomically", func(t *testing.T) {
		space := testutil.RandomDID(t)
		d := multihash.Multihash([]byte{0x12, 0x20, 0xda, 0x01})
		if err := r.PutIntent(ctx, registry.UploadIntent{Digest: d, LocalPath: "/spool/d", Size: 3, State: registry.IntentSpooled, Bucket: "b"}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		if err := r.EnqueueRelease(ctx, space, d, time.Now()); err != nil {
			t.Fatalf("EnqueueRelease: %v", err)
		}
		if err := r.DeleteIntentAndRelease(ctx, space, d); err != nil {
			t.Fatalf("DeleteIntentAndRelease: %v", err)
		}
		if _, err := r.GetIntent(ctx, d); err != registry.ErrNotFound {
			t.Fatalf("intent after the paired delete = %v, want ErrNotFound", err)
		}
		if rows, _ := r.ListReleasesBySpace(ctx, space); len(rows) != 0 {
			t.Fatalf("release records after the paired delete = %v, want none", rows)
		}
		// Idempotent: nothing left to delete is not an error.
		if err := r.DeleteIntentAndRelease(ctx, space, d); err != nil {
			t.Fatalf("repeated DeleteIntentAndRelease: %v", err)
		}
	})

	t.Run("intent lifecycle", func(t *testing.T) {
		if err := r.PutIntent(ctx, registry.UploadIntent{Digest: digest, LocalPath: "/spool/x", Size: 9, State: registry.IntentSpooled, Bucket: "b"}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		if err := r.SetIntentState(ctx, digest, registry.IntentUploading); err != nil {
			t.Fatalf("SetIntentState (uploading, the constraint admits it): %v", err)
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
			Space:            space,
			Digest:           d,
			RegionWrappedCEK: []byte{0xde, 0xad, 0x00, 0xbe, 0xef, 0xff},
			RegionKeyVersion: "region-kek-v1",
			HeaderLen:        212,
			BaseNonce:        []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07},
			ChunkSize:        65536,
			AAD:              []byte{0xa1, 0x00, 0x18, 0x20},
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

	t.Run("encryption params delete removes the row", func(t *testing.T) {
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

	t.Run("encryption params rewrap in place", func(t *testing.T) {
		// A rotation replaces only the wrapped CEK and its key version; the
		// parameters describing the unchanged ciphertext must survive.
		space := testutil.RandomDID(t)
		encDigest := []byte{0x0a, 0x0b}
		if err := r.PutEncryptionParams(ctx, liveFEEParams(space, encDigest)); err != nil {
			t.Fatalf("PutEncryptionParams: %v", err)
		}
		if err := r.RewrapEncryptionParams(ctx, space, encDigest, []byte{0xca, 0xfe, 0x00, 0x01}, "region-kek-v2"); err != nil {
			t.Fatalf("RewrapEncryptionParams: %v", err)
		}
		got, err := r.GetEncryptionParams(ctx, space, encDigest)
		if err != nil {
			t.Fatalf("GetEncryptionParams: %v", err)
		}
		want := liveFEEParams(space, encDigest)
		want.RegionWrappedCEK = []byte{0xca, 0xfe, 0x00, 0x01}
		want.RegionKeyVersion = "region-kek-v2"
		if !reflect.DeepEqual(*got, want) {
			t.Fatalf("after rewrap = %+v, want %+v", *got, want)
		}
	})

	t.Run("encryption params rewrap of a missing row is ErrNotFound", func(t *testing.T) {
		space := testutil.RandomDID(t)
		err := r.RewrapEncryptionParams(ctx, space, []byte{0x0c}, []byte{0x01}, "region-kek-v2")
		if !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("RewrapEncryptionParams(absent) = %v, want ErrNotFound", err)
		}
	})

	t.Run("incomplete encryption params rejected", func(t *testing.T) {
		// The column constraints are the invariant: a half-populated set never
		// reaches the table.
		space := testutil.RandomDID(t)
		d := []byte{0x77}
		for name, mutate := range map[string]func(*registry.BlobEncryptionParams){
			"nil AAD":           func(p *registry.BlobEncryptionParams) { p.AAD = nil },
			"nil wrapped CEK":   func(p *registry.BlobEncryptionParams) { p.RegionWrappedCEK = nil },
			"empty key version": func(p *registry.BlobEncryptionParams) { p.RegionKeyVersion = "" },
		} {
			partial := liveFEEParams(space, d)
			mutate(&partial)
			if err := r.PutEncryptionParams(ctx, partial); err == nil {
				t.Fatalf("PutEncryptionParams(%s) = nil, want a constraint error", name)
			}
			if _, err := r.GetEncryptionParams(ctx, space, d); !errors.Is(err, registry.ErrNotFound) {
				t.Fatalf("incomplete params (%s) leaked a row: %v", name, err)
			}
		}
		// A rewrap cannot blank out the key material either.
		encDigest := []byte{0x78}
		if err := r.PutEncryptionParams(ctx, liveFEEParams(space, encDigest)); err != nil {
			t.Fatalf("PutEncryptionParams: %v", err)
		}
		if err := r.RewrapEncryptionParams(ctx, space, encDigest, nil, "region-kek-v2"); err == nil {
			t.Fatal("RewrapEncryptionParams(nil CEK) = nil, want a constraint error")
		}
		if err := r.RewrapEncryptionParams(ctx, space, encDigest, []byte{0x01}, ""); err == nil {
			t.Fatal("RewrapEncryptionParams(empty version) = nil, want a constraint error")
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
		bSpace := testutil.RandomDID(t)
		if err := r.CreateSession(ctx, registry.MultipartSession{UploadID: id, Bucket: "b", ObjectKey: "k", Space: bSpace, ContentType: "text/plain", Metadata: meta}); err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
		if err := r.CreateSession(ctx, registry.MultipartSession{UploadID: id, Bucket: "b", ObjectKey: "k", Space: bSpace}); err != registry.ErrExists {
			t.Fatalf("duplicate CreateSession = %v, want ErrExists", err)
		}
		s, err := r.GetSession(ctx, id)
		if err != nil || s.ContentType != "text/plain" || s.Metadata["x-amz-meta-foo"] != "bar" || s.Space != bSpace {
			t.Fatalf("GetSession = %+v, err %v (metadata jsonb round-trip, space)", s, err)
		}
		// The space is required: a session without one is refused.
		if err := r.CreateSession(ctx, registry.MultipartSession{UploadID: id + "-nospace", Bucket: "b", ObjectKey: "k"}); err == nil {
			t.Fatal("CreateSession without a space was accepted")
		}
		// The bucket's space rides on the session, for teardown once the
		// bucket row is gone.
		space := testutil.RandomDID(t)
		if err := r.CreateSession(ctx, registry.MultipartSession{UploadID: id + "-space", Bucket: "b", ObjectKey: "k", Space: space}); err != nil {
			t.Fatalf("CreateSession (space): %v", err)
		}
		if got, err := r.GetSession(ctx, id+"-space"); err != nil || got.Space != space {
			t.Fatalf("GetSession space = %v, err %v (want %v)", got.Space, err, space)
		}
		_ = r.DeleteSession(ctx, id+"-space")

		// bytea[] round trip + ordering.
		if err := r.PutPart(ctx, registry.MultipartPart{UploadID: id, PartNumber: 2, ETagMD5: []byte{0x02}, Size: 2, BlobDigests: []multihash.Multihash{{0xd2}}}); err != nil {
			t.Fatalf("PutPart 2: %v", err)
		}
		if err := r.PutPart(ctx, registry.MultipartPart{UploadID: id, PartNumber: 1, ETagMD5: []byte{0x01}, Size: 1, BlobDigests: []multihash.Multihash{{0xd1, 0xa}, {0xd1, 0xb}}}); err != nil {
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
		// A part cannot land once the session has left 'open', nor on a
		// session that does not exist.
		if err := r.PutPart(ctx, registry.MultipartPart{UploadID: id, PartNumber: 3, ETagMD5: []byte{0x03}, Size: 3, BlobDigests: []multihash.Multihash{{0xd3}}}); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("PutPart on a completing session = %v, want ErrNotFound", err)
		}
		if err := r.PutPart(ctx, registry.MultipartPart{UploadID: "no-such-upload", PartNumber: 1, ETagMD5: []byte{0x01}, Size: 1, BlobDigests: []multihash.Multihash{{0xd1}}}); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("PutPart on a missing session = %v, want ErrNotFound", err)
		}
		if parts, _ := r.ListParts(ctx, id); len(parts) != 2 {
			t.Fatalf("parts after the refused writes = %d, want 2", len(parts))
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
		lsSpace := testutil.RandomDID(t)
		mk := func(id, key string) {
			t.Helper()
			if err := r.CreateSession(ctx, registry.MultipartSession{
				UploadID: id, Bucket: "b", ObjectKey: key, Space: lsSpace,
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
		if err := r.PutPart(ctx, registry.MultipartPart{UploadID: "ls-1", PartNumber: 1, ETagMD5: []byte{1}, Size: 1, BlobDigests: []multihash.Multihash{shared}}); err != nil {
			t.Fatalf("PutPart ls-1: %v", err)
		}
		if err := r.PutPart(ctx, registry.MultipartPart{UploadID: "ls-2", PartNumber: 1, ETagMD5: []byte{2}, Size: 1, BlobDigests: []multihash.Multihash{shared, {0xee, 0x02}}}); err != nil {
			t.Fatalf("PutPart ls-2: %v", err)
		}
		if n, err := r.CountPartRefs(ctx, shared, "ls-1"); err != nil || n != 1 {
			t.Fatalf("CountPartRefs(shared, exclude ls-1) = %d, err %v (want 1)", n, err)
		}
		if n, err := r.CountPartRefs(ctx, []byte{0xee, 0x02}, "ls-2"); err != nil || n != 0 {
			t.Fatalf("CountPartRefs(unique, exclude owner) = %d, err %v (want 0)", n, err)
		}

		// CountLivePartRefs: parts of open sessions count.
		if n, err := r.CountLivePartRefs(ctx, shared); err != nil || n != 2 {
			t.Fatalf("CountLivePartRefs(shared) = %d, err %v (want 2)", n, err)
		}

		// 'completed' passes the widened state CHECK constraint, and the
		// latch restarts the sweeper's clock: the row is not stale by a
		// past cutoff although it was created before it.
		if won, err := r.LatchSession(ctx, "ls-1", registry.SessionOpen, registry.SessionCompleted); err != nil || !won {
			t.Fatalf("latch to completed won=%v err=%v", won, err)
		}
		if s, err := r.GetSession(ctx, "ls-1"); err != nil || s.StateChangedAt.Before(s.CreatedAt) || s.StateChangedAt.IsZero() {
			t.Fatalf("StateChangedAt after latch = %v (created %v), err %v", s.StateChangedAt, s.CreatedAt, err)
		}
		if stale, err := r.ListStaleSessions(ctx, registry.SessionCompleted, time.Now().Add(-time.Minute)); err != nil || len(stale) != 0 {
			t.Fatalf("ListStaleSessions completed, past cutoff = %d, err %v (want 0: just latched)", len(stale), err)
		}
		if stale, err := r.ListStaleSessions(ctx, registry.SessionCompleted, time.Now().Add(time.Hour)); err != nil || len(stale) != 1 {
			t.Fatalf("ListStaleSessions completed, future cutoff = %d, err %v (want 1)", len(stale), err)
		}
		// A completed session's parts are no longer live; an aborting one's
		// are not either.
		if n, err := r.CountLivePartRefs(ctx, shared); err != nil || n != 1 {
			t.Fatalf("CountLivePartRefs(shared) after ls-1 completed = %d, err %v (want 1)", n, err)
		}
		if won, err := r.LatchSession(ctx, "ls-2", registry.SessionOpen, registry.SessionAborting); err != nil || !won {
			t.Fatalf("latch ls-2 to aborting won=%v err=%v", won, err)
		}
		if n, err := r.CountLivePartRefs(ctx, shared); err != nil || n != 0 {
			t.Fatalf("CountLivePartRefs(shared) after ls-2 aborting = %d, err %v (want 0)", n, err)
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

	t.Run("revocation cursor upsert round trip", func(t *testing.T) {
		if _, err := r.GetRevocationCursor(ctx); err != registry.ErrNotFound {
			t.Fatalf("GetRevocationCursor before put err = %v, want ErrNotFound", err)
		}
		first := registry.RevocationCursor{
			RecordedAt: time.Now().UTC().Truncate(time.Microsecond), // timestamptz is µs-precision
			Revoke:     liveCid(t, "revoked-1"),
		}
		if err := r.PutRevocationCursor(ctx, first); err != nil {
			t.Fatalf("PutRevocationCursor: %v", err)
		}
		got, err := r.GetRevocationCursor(ctx)
		if err != nil {
			t.Fatalf("GetRevocationCursor: %v", err)
		}
		if !got.RecordedAt.Equal(first.RecordedAt) || !got.Revoke.Equals(first.Revoke) {
			t.Fatalf("cursor = %+v, want %+v", got, first)
		}
		// The row is a single latch: a second put overwrites, never adds.
		second := registry.RevocationCursor{
			RecordedAt: first.RecordedAt.Add(time.Hour),
			Revoke:     liveCid(t, "revoked-2"),
		}
		if err := r.PutRevocationCursor(ctx, second); err != nil {
			t.Fatalf("PutRevocationCursor (upsert): %v", err)
		}
		got, err = r.GetRevocationCursor(ctx)
		if err != nil {
			t.Fatalf("GetRevocationCursor after upsert: %v", err)
		}
		if !got.RecordedAt.Equal(second.RecordedAt) || !got.Revoke.Equals(second.Revoke) {
			t.Fatalf("cursor after upsert = %+v, want %+v", got, second)
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
