package registry_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/migrations"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
)

// liveDigest is the sha256 multihash of s: a digest the batch reads decode.
func liveDigest(t *testing.T, s string) multihash.Multihash {
	t.Helper()
	d, err := multihash.Sum([]byte(s), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	return d
}

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
		 ingot.blob_release_intents, ingot.blob_streams CASCADE`); err != nil {
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
			if err := r.PutLocation(ctx, registry.BlobLocation{Space: sp, Digest: digest, Provider: "did:key:p", URL: "http://p/blob", Size: 1}); err != nil {
				t.Fatalf("PutLocation: %v", err)
			}
			mustClaim(t, r, registry.BlobClaim{Digest: digest, Bucket: bucket, ObjectKey: key, VersionID: registry.NullVersionID, Space: sp})
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
		drop(t, r, registry.ClaimDrop{Digest: digest, Bucket: "b", ObjectKey: "k1", VersionID: registry.NullVersionID, Space: space})
		drop(t, r, registry.ClaimDrop{Digest: digest, Bucket: "b", ObjectKey: "k2", VersionID: registry.NullVersionID, Space: space})
		if n, _ := r.CountClaims(ctx, space, digest); n != 0 {
			t.Fatalf("count after release = %d, want 0", n)
		}

		// A claim publishes the digest's upload intent, and the state stays
		// once the claims are gone: it is the durable mark of a committed blob.
		pubDigest := multihash.Multihash([]byte{0x12, 0x20, 0xc1, 0xa1})
		if err := r.PutIntent(ctx, registry.UploadIntent{Space: space, Digest: pubDigest, LocalPath: "/spool/x", Size: 1, State: registry.IntentAccepted, Bucket: "b"}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		if err := r.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: pubDigest, Provider: "did:key:p", URL: "http://p/blob", Size: 1}); err != nil {
			t.Fatalf("PutLocation: %v", err)
		}
		mustClaim(t, r, registry.BlobClaim{Digest: pubDigest, Bucket: "b", ObjectKey: "kp", VersionID: registry.NullVersionID, Space: space})
		if in, err := r.GetIntent(ctx, space, pubDigest); err != nil || in.State != registry.IntentPublished {
			t.Fatalf("intent after claim = %+v, err %v (want published)", in, err)
		}
		drop(t, r, registry.ClaimDrop{Digest: pubDigest, Bucket: "b", ObjectKey: "kp", VersionID: registry.NullVersionID, Space: space})
		if in, err := r.GetIntent(ctx, space, pubDigest); err != nil || in.State != registry.IntentPublished {
			t.Fatalf("intent after the claim dropped = %+v, err %v (want still published)", in, err)
		}
		// A claim on a digest with no intent row (a shipped segment) is fine.
		if err := r.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: multihash.Multihash([]byte{0x12, 0x20, 0xc1, 0xa2}), Provider: "did:key:p", URL: "http://p/blob", Size: 1}); err != nil {
			t.Fatalf("PutLocation: %v", err)
		}
		mustClaim(t, r, registry.BlobClaim{Digest: multihash.Multihash([]byte{0x12, 0x20, 0xc1, 0xa2}), Bucket: "b", ObjectKey: "kn", VersionID: registry.NullVersionID, Space: space})
	})

	t.Run("pin claims require an existing claim, all or nothing", func(t *testing.T) {
		space := testutil.RandomDID(t)
		d1 := multihash.Multihash([]byte{0x12, 0x20, 0xb1, 0x01})
		d2 := multihash.Multihash([]byte{0x12, 0x20, 0xb1, 0x02})
		pins := func(ds ...multihash.Multihash) []registry.BlobClaim {
			var out []registry.BlobClaim
			for _, d := range ds {
				out = append(out, registry.BlobClaim{Digest: d, Bucket: "pb", ObjectKey: "copy", VersionID: "null#7", Space: space})
			}
			return out
		}
		// A refused pin fails the commit, and the root stays where it was.
		before, _ := r.Get(ctx, "pb")
		if err := pin(t, r, pins(d1)); !errors.Is(err, registry.ErrClaimGone) {
			t.Fatalf("pin with no claim to attach to: err=%v, want ErrClaimGone", err)
		}
		if after, _ := r.Get(ctx, "pb"); before != nil && !after.Root.Equals(before.Root) {
			t.Fatalf("a refused pin advanced the root: %v -> %v", before.Root, after.Root)
		}
		if err := r.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: d1, Provider: "did:key:p", URL: "http://p/blob", Size: 1}); err != nil {
			t.Fatalf("PutLocation: %v", err)
		}
		mustClaim(t, r, registry.BlobClaim{Digest: d1, Bucket: "pb", ObjectKey: "src", VersionID: "null#1", Space: space})
		// d1 is claimed, d2 is not: nothing is recorded.
		if err := pin(t, r, pins(d1, d2)); !errors.Is(err, registry.ErrClaimGone) {
			t.Fatalf("pin with one unclaimed digest: err=%v, want ErrClaimGone", err)
		}
		if n, _ := r.CountClaims(ctx, space, d1); n != 1 {
			t.Fatalf("refused pin left %d claims on d1, want the source's 1", n)
		}
		if err := pin(t, r, pins(d1)); err != nil {
			t.Fatalf("pin beside a live claim: %v, want recorded", err)
		}
		if err := pin(t, r, pins(d1)); err != nil {
			t.Fatalf("repeated pin: %v, want idempotent", err)
		}
		if n, _ := r.CountClaims(ctx, space, d1); n != 2 {
			t.Fatalf("claims after pin = %d, want 2", n)
		}
		// A claim in another space does not qualify.
		other := testutil.RandomDID(t)
		if err := pin(t, r, []registry.BlobClaim{{Digest: d1, Bucket: "pb2", ObjectKey: "copy", VersionID: "null#1", Space: other}}); !errors.Is(err, registry.ErrClaimGone) {
			t.Fatalf("pin against another space's claim: err=%v, want ErrClaimGone", err)
		}
		// Dropping the source's claim beside the pin records no release;
		// dropping the pin too does.
		drop(t, r, registry.ClaimDrop{Digest: d1, Bucket: "pb", ObjectKey: "src", VersionID: "null#1", Space: space})
		if releaseRecorded(t, r, space, d1) {
			t.Fatalf("drop of the source beside the pin recorded a release")
		}
		drop(t, r, registry.ClaimDrop{Digest: d1, Bucket: "pb", ObjectKey: "copy", VersionID: "null#7", Space: space})
		if !releaseRecorded(t, r, space, d1) {
			t.Fatalf("drop of the last claim recorded no release")
		}
	})

	t.Run("drop claim enqueues release atomically", func(t *testing.T) {
		space := testutil.RandomDID(t)
		add := func(key string) {
			if err := r.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: digest, Provider: "did:key:p", URL: "http://p/blob", Size: 1}); err != nil {
				t.Fatalf("PutLocation: %v", err)
			}
			mustClaim(t, r, registry.BlobClaim{Digest: digest, Bucket: "rb", ObjectKey: key, VersionID: "null#1", Space: space})
		}
		add("k1")
		add("k2")

		// A zero grace (the commit helper's) makes the record due at once.
		drop(t, r, registry.ClaimDrop{Digest: digest, Bucket: "rb", ObjectKey: "k1", VersionID: "null#1", Space: space})
		if releaseRecorded(t, r, space, digest) {
			t.Fatalf("first drop recorded a release while a claim remains")
		}
		drop(t, r, registry.ClaimDrop{Digest: digest, Bucket: "rb", ObjectKey: "k2", VersionID: "null#1", Space: space})
		if !releaseRecorded(t, r, space, digest) {
			t.Fatalf("last drop recorded no release")
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
		if err := r.PutIntent(ctx, registry.UploadIntent{Space: space, Digest: d, LocalPath: "/spool/d", Size: 3, State: registry.IntentSpooled, Bucket: "b"}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		if err := r.EnqueueRelease(ctx, space, d, time.Now()); err != nil {
			t.Fatalf("EnqueueRelease: %v", err)
		}
		if _, err := r.DeleteIntentAndRelease(ctx, space, d); err != nil {
			t.Fatalf("DeleteIntentAndRelease: %v", err)
		}
		if _, err := r.GetIntent(ctx, space, d); err != registry.ErrNotFound {
			t.Fatalf("intent after the paired delete = %v, want ErrNotFound", err)
		}
		if rows, _ := r.ListReleasesBySpace(ctx, space); len(rows) != 0 {
			t.Fatalf("release records after the paired delete = %v, want none", rows)
		}
		// Idempotent: nothing left to delete is not an error.
		if _, err := r.DeleteIntentAndRelease(ctx, space, d); err != nil {
			t.Fatalf("repeated DeleteIntentAndRelease: %v", err)
		}
	})

	t.Run("intent lifecycle", func(t *testing.T) {
		space := testutil.RandomDID(t)
		if err := r.PutIntent(ctx, registry.UploadIntent{Space: space, Digest: digest, LocalPath: "/spool/x", Size: 9, State: registry.IntentSpooled, Bucket: "b"}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		if err := r.SetIntentState(ctx, space, digest, registry.IntentUploading); err != nil {
			t.Fatalf("SetIntentState (uploading, the constraint admits it): %v", err)
		}
		if err := r.SetIntentState(ctx, space, digest, registry.IntentParked); err != nil {
			t.Fatalf("SetIntentState: %v", err)
		}
		got, err := r.GetIntent(ctx, space, digest)
		if err != nil || got.State != registry.IntentParked || got.Bucket != "b" {
			t.Fatalf("GetIntent = %+v, err %v", got, err)
		}
		parked, _ := r.ListIntentsByState(ctx, registry.IntentParked)
		if len(parked) != 1 {
			t.Fatalf("parked = %d, want 1", len(parked))
		}
		if err := r.SetIntentState(ctx, space, []byte("missing"), registry.IntentParked); err != registry.ErrNotFound {
			t.Fatalf("SetIntentState missing = %v, want ErrNotFound", err)
		}
		if err := r.DeleteIntent(ctx, space, digest); err != nil {
			t.Fatalf("DeleteIntent: %v", err)
		}
		if _, err := r.GetIntent(ctx, space, digest); err != registry.ErrNotFound {
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

	t.Run("locations batch", func(t *testing.T) {
		// Only the space's own rows come back, keyed by digest; a digest
		// without a row is simply absent.
		space, other := testutil.RandomDID(t), testutil.RandomDID(t)
		a, b, missing := liveDigest(t, "loc a"), liveDigest(t, "loc b"), liveDigest(t, "loc missing")
		for _, loc := range []registry.BlobLocation{
			{Space: space, Digest: a, Provider: "did:piri", URL: "http://piri/a", Size: 10},
			{Space: space, Digest: b, Provider: "did:piri", URL: "http://piri/b", Size: 20},
			{Space: other, Digest: missing, Provider: "did:piri", URL: "http://piri/c", Size: 30},
		} {
			if err := r.PutLocation(ctx, loc); err != nil {
				t.Fatalf("PutLocation: %v", err)
			}
		}
		got, err := r.GetLocations(ctx, space, []multihash.Multihash{a, b, missing})
		if err != nil {
			t.Fatalf("GetLocations: %v", err)
		}
		if len(got) != 2 || got[string(a)].URL != "http://piri/a" || got[string(b)].Size != 20 ||
			!bytes.Equal(got[string(b)].Digest, b) || got[string(a)].Space != space {
			t.Fatalf("GetLocations = %+v", got)
		}
		if empty, err := r.GetLocations(ctx, space, nil); err != nil || len(empty) != 0 {
			t.Fatalf("GetLocations(nil) = %+v, %v", empty, err)
		}
		// A stored digest that is not a multihash fails the read.
		bad := []byte{0x0c}
		if err := r.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: bad, Provider: "did:piri", URL: "http://piri/x", Size: 1}); err != nil {
			t.Fatalf("PutLocation: %v", err)
		}
		if _, err := r.GetLocations(ctx, space, []multihash.Multihash{a, bad}); err == nil {
			t.Fatal("GetLocations over a non-multihash digest: want an error")
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

	t.Run("encryption params batch", func(t *testing.T) {
		space, other := testutil.RandomDID(t), testutil.RandomDID(t)
		a, b, missing := liveDigest(t, "params a"), liveDigest(t, "params b"), liveDigest(t, "params missing")
		for _, p := range []registry.BlobEncryptionParams{
			liveFEEParams(space, a), liveFEEParams(space, b), liveFEEParams(other, missing),
		} {
			if err := r.PutEncryptionParams(ctx, p); err != nil {
				t.Fatalf("PutEncryptionParams: %v", err)
			}
		}
		got, err := r.GetEncryptionParamsBatch(ctx, space, []multihash.Multihash{a, b, missing})
		if err != nil {
			t.Fatalf("GetEncryptionParamsBatch: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("GetEncryptionParamsBatch returned %d rows, want 2", len(got))
		}
		for _, d := range []multihash.Multihash{a, b} {
			if want := liveFEEParams(space, d); !reflect.DeepEqual(got[string(d)], want) {
				t.Fatalf("row %x = %+v, want %+v", d, got[string(d)], want)
			}
		}
		if empty, err := r.GetEncryptionParamsBatch(ctx, space, nil); err != nil || len(empty) != 0 {
			t.Fatalf("GetEncryptionParamsBatch(nil) = %+v, %v", empty, err)
		}
		// A stored digest that is not a multihash fails the read.
		bad := []byte{0x1c}
		if err := r.PutEncryptionParams(ctx, liveFEEParams(space, bad)); err != nil {
			t.Fatalf("PutEncryptionParams: %v", err)
		}
		if _, err := r.GetEncryptionParamsBatch(ctx, space, []multihash.Multihash{a, bad}); err == nil {
			t.Fatal("GetEncryptionParamsBatch over a non-multihash digest: want an error")
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
		space := testutil.RandomDID(t)
		park := registry.BlobPark{
			Space:         space,
			Digest:        digest,
			AddTask:       []byte{0x01, 0x02},
			AcceptTask:    []byte{0x03, 0x04},
			PutInvocation: []byte("sealed-inv"),
			Size:          42,
		}
		if recorded, err := r.PutPark(ctx, park); err != nil || !recorded {
			t.Fatalf("PutPark = %v, %v; want recorded", recorded, err)
		}
		got, err := r.GetPark(ctx, space, digest)
		if err != nil || string(got.PutInvocation) != "sealed-inv" || got.Size != 42 {
			t.Fatalf("GetPark = %+v, err %v", got, err)
		}
		// A second park for the key is refused and the first stands.
		park.Size = 43
		if recorded, err := r.PutPark(ctx, park); err != nil || recorded {
			t.Fatalf("PutPark (duplicate) = %v, %v; want refused", recorded, err)
		}
		if got, err := r.GetPark(ctx, space, digest); err != nil || got.Size != 42 {
			t.Fatalf("GetPark after the refused put = %+v, err %v; want the first park", got, err)
		}
		if parked, err := r.HasParkFor(ctx, park.AddTask); err != nil || !parked {
			t.Fatalf("HasParkFor(park's add task) = %v, %v", parked, err)
		}
		if parked, err := r.HasParkFor(ctx, []byte{0x09}); err != nil || parked {
			t.Fatalf("HasParkFor(another add task) = %v, %v", parked, err)
		}
		if err := r.DeletePark(ctx, space, digest); err != nil {
			t.Fatalf("DeletePark: %v", err)
		}
		if _, err := r.GetPark(ctx, space, digest); err != registry.ErrNotFound {
			t.Fatalf("GetPark after delete = %v, want ErrNotFound", err)
		}
	})

	t.Run("stream rows age out", func(t *testing.T) {
		space := testutil.RandomDID(t)
		addTask := liveCid(t, "stream-add").Bytes()
		if err := r.PutStream(ctx, registry.BlobStream{AddTask: addTask, Space: space, Bucket: "sb", Size: 4096}); err != nil {
			t.Fatalf("PutStream: %v", err)
		}
		rows, err := r.ListStaleStreams(ctx, time.Now().Add(-time.Minute), 10)
		if err != nil {
			t.Fatalf("ListStaleStreams: %v", err)
		}
		if len(rows) != 0 {
			t.Fatalf("a fresh row is stale: %+v", rows)
		}
		rows, err = r.ListStaleStreams(ctx, time.Now().Add(time.Minute), 10)
		if err != nil {
			t.Fatalf("ListStaleStreams: %v", err)
		}
		if len(rows) != 1 || !reflect.DeepEqual(rows[0].AddTask, addTask) || rows[0].Space != space ||
			rows[0].Bucket != "sb" || rows[0].Size != 4096 || rows[0].CreatedAt.IsZero() {
			t.Fatalf("stale rows = %+v", rows)
		}
		if err := r.DeleteStream(ctx, addTask); err != nil {
			t.Fatalf("DeleteStream: %v", err)
		}
		if err := r.DeleteStream(ctx, addTask); err != nil {
			t.Fatalf("DeleteStream (idempotent): %v", err)
		}
		rows, err = r.ListStaleStreams(ctx, time.Now().Add(time.Minute), 10)
		if err != nil || len(rows) != 0 {
			t.Fatalf("after delete: %+v, %v", rows, err)
		}
	})

	t.Run("a renewed stream lease is not stale", func(t *testing.T) {
		addTask := liveCid(t, "stream-lease").Bytes()
		if err := r.PutStream(ctx, registry.BlobStream{AddTask: addTask, Space: testutil.RandomDID(t), Bucket: "sb", Size: 1}); err != nil {
			t.Fatalf("PutStream: %v", err)
		}
		if _, err := pool.Exec(ctx, `UPDATE ingot.blob_streams SET touched_at = now() - interval '1 hour' WHERE add_task = $1`, addTask); err != nil {
			t.Fatalf("age row: %v", err)
		}
		rows, err := r.ListStaleStreams(ctx, time.Now().Add(-30*time.Minute), 10)
		if err != nil || len(rows) != 1 {
			t.Fatalf("an unrenewed row is not stale: %+v, %v", rows, err)
		}
		if err := r.TouchStreams(ctx, [][]byte{addTask, liveCid(t, "gone").Bytes()}); err != nil {
			t.Fatalf("TouchStreams: %v", err)
		}
		if err := r.TouchStreams(ctx, nil); err != nil {
			t.Fatalf("TouchStreams(nil): %v", err)
		}
		rows, err = r.ListStaleStreams(ctx, time.Now().Add(-30*time.Minute), 10)
		if err != nil || len(rows) != 0 {
			t.Fatalf("a renewed row is stale: %+v, %v", rows, err)
		}
		if err := r.DeleteStream(ctx, addTask); err != nil {
			t.Fatalf("DeleteStream: %v", err)
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
		if _, err := r.PutPart(ctx, registry.MultipartPart{UploadID: id, PartNumber: 2, ETagMD5: []byte{0x02}, Size: 2, BlobDigests: []multihash.Multihash{{0xd2}}}, nil); err != nil {
			t.Fatalf("PutPart 2: %v", err)
		}
		if _, err := r.PutPart(ctx, registry.MultipartPart{UploadID: id, PartNumber: 1, ETagMD5: []byte{0x01}, Size: 1, BlobDigests: []multihash.Multihash{{0xd1, 0xa}, {0xd1, 0xb}}},
			&registry.PartTree{Offset: 0, ChunkLog: 4, Nodes: []byte{0x0a}, Blocks: []byte{0x0b, 0x0c}, Head: []byte{0x0d}, Tail: []byte{0x0e}, Root: []byte{0x0f}}); err != nil {
			t.Fatalf("PutPart 1: %v", err)
		}
		parts, err := r.ListParts(ctx, id)
		if err != nil || len(parts) != 2 || parts[0].PartNumber != 1 || len(parts[0].BlobDigests) != 2 {
			t.Fatalf("ListParts = %+v, err %v", parts, err)
		}
		// The tree listing round-trips the tree rows by part number, absent
		// where none was recorded.
		trees, err := r.ListPartTrees(ctx, id)
		if err != nil || len(trees) != 1 || trees[1] == nil {
			t.Fatalf("ListPartTrees = %+v, err %v; want part 1's tree only", trees, err)
		}
		if tr := trees[1]; tr.ChunkLog != 4 || !bytes.Equal(tr.Blocks, []byte{0x0b, 0x0c}) || !bytes.Equal(tr.Head, []byte{0x0d}) || !bytes.Equal(tr.Tail, []byte{0x0e}) || !bytes.Equal(tr.Root, []byte{0x0f}) {
			t.Fatalf("part 1 tree = %+v", tr)
		}
		// A superseding part without a tree drops its predecessor's row; one
		// with a tree replaces it.
		if _, err := r.PutPart(ctx, registry.MultipartPart{UploadID: id, PartNumber: 1, ETagMD5: []byte{0x01}, Size: 1, BlobDigests: []multihash.Multihash{{0xd1}}}, nil); err != nil {
			t.Fatalf("PutPart 1 again: %v", err)
		}
		if again, _ := r.ListPartTrees(ctx, id); again[1] != nil {
			t.Fatalf("superseded part kept its tree: %+v", again[1])
		}
		if _, err := r.PutPart(ctx, registry.MultipartPart{UploadID: id, PartNumber: 2, ETagMD5: []byte{0x02}, Size: 2, BlobDigests: []multihash.Multihash{{0xd2}}},
			&registry.PartTree{Offset: 1, ChunkLog: 5, Nodes: []byte{0x1a}, Blocks: []byte{0x1b}}); err != nil {
			t.Fatalf("PutPart 2 with tree: %v", err)
		}
		if again, _ := r.ListPartTrees(ctx, id); again[2] == nil || again[2].Offset != 1 || again[2].ChunkLog != 5 || again[2].Head != nil {
			t.Fatalf("part 2 tree = %+v", again[2])
		}
		// Deleting the trees keeps the part records.
		if err := r.DeletePartTrees(ctx, id); err != nil {
			t.Fatalf("DeletePartTrees: %v", err)
		}
		if after, _ := r.ListPartTrees(ctx, id); len(after) != 0 {
			t.Fatalf("after DeletePartTrees = %+v, want no trees", after)
		}
		if after, _ := r.ListParts(ctx, id); len(after) != 2 {
			t.Fatalf("after DeletePartTrees = %+v, want two part records", after)
		}

		// single-winner latch
		won, err := r.LatchSession(ctx, id, registry.SessionOpen, registry.SessionCompleting)
		if err != nil || !won {
			t.Fatalf("Complete latch won=%v err=%v", won, err)
		}
		// A part cannot land once the session has left 'open', nor on a
		// session that does not exist.
		if _, err := r.PutPart(ctx, registry.MultipartPart{UploadID: id, PartNumber: 3, ETagMD5: []byte{0x03}, Size: 3, BlobDigests: []multihash.Multihash{{0xd3}}}, nil); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("PutPart on a completing session = %v, want ErrNotFound", err)
		}
		if _, err := r.PutPart(ctx, registry.MultipartPart{UploadID: "no-such-upload", PartNumber: 1, ETagMD5: []byte{0x01}, Size: 1, BlobDigests: []multihash.Multihash{{0xd1}}}, nil); !errors.Is(err, registry.ErrNotFound) {
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
		if _, err := r.PutPart(ctx, registry.MultipartPart{UploadID: "ls-1", PartNumber: 1, ETagMD5: []byte{1}, Size: 1, BlobDigests: []multihash.Multihash{shared}}, nil); err != nil {
			t.Fatalf("PutPart ls-1: %v", err)
		}
		if _, err := r.PutPart(ctx, registry.MultipartPart{UploadID: "ls-2", PartNumber: 1, ETagMD5: []byte{2}, Size: 1, BlobDigests: []multihash.Multihash{shared, {0xee, 0x02}}}, nil); err != nil {
			t.Fatalf("PutPart ls-2: %v", err)
		}
		if n, err := r.CountPartRefs(ctx, shared, "ls-1"); err != nil || n != 1 {
			t.Fatalf("CountPartRefs(shared, exclude ls-1) = %d, err %v (want 1)", n, err)
		}
		if n, err := r.CountPartRefs(ctx, []byte{0xee, 0x02}, "ls-2"); err != nil || n != 0 {
			t.Fatalf("CountPartRefs(unique, exclude owner) = %d, err %v (want 0)", n, err)
		}

		// CountLivePartRefs: parts of open sessions count.
		if n, err := r.CountLivePartRefs(ctx, lsSpace, shared); err != nil || n != 2 {
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
		if n, err := r.CountLivePartRefs(ctx, lsSpace, shared); err != nil || n != 1 {
			t.Fatalf("CountLivePartRefs(shared) after ls-1 completed = %d, err %v (want 1)", n, err)
		}
		if won, err := r.LatchSession(ctx, "ls-2", registry.SessionOpen, registry.SessionAborting); err != nil || !won {
			t.Fatalf("latch ls-2 to aborting won=%v err=%v", won, err)
		}
		if n, err := r.CountLivePartRefs(ctx, lsSpace, shared); err != nil || n != 0 {
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
		if err := r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seq, time.Now().Unix(), nil, -1,
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

	t.Run("retired segment keeps its row", func(t *testing.T) {
		seq, err := r.NextSegmentSeq(ctx)
		if err != nil {
			t.Fatalf("NextSegmentSeq: %v", err)
		}
		bucket := fmt.Sprintf("rt-%d", seq)
		sha := make([]byte, 32)
		sha[0] = 0x5e
		indexDigest := multihash.Multihash(digest)
		now := time.Now().Unix()
		if err := r.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq, bucket); err != nil {
			t.Fatalf("InsertSegmentOpen: %v", err)
		}
		if err := r.MarkSegmentSealed(ctx, blockstore.PlaneCatalog, seq, now, 10, sha,
			[]blockstore.OpRoot{{Bucket: bucket, Root: liveCid(t, "rt-root")}}); err != nil {
			t.Fatalf("MarkSegmentSealed: %v", err)
		}
		if err := r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seq, now, indexDigest, 7, nil); err != nil {
			t.Fatalf("MarkSegmentShipped: %v", err)
		}
		for range 2 { // idempotent
			if err := r.RetireSegment(ctx, blockstore.PlaneCatalog, seq, now); err != nil {
				t.Fatalf("RetireSegment: %v", err)
			}
		}

		live, err := r.ListSegments(ctx, blockstore.PlaneCatalog, bucket)
		if err != nil || len(live) != 0 {
			t.Fatalf("ListSegments = %+v (err %v), want none", live, err)
		}
		retired, err := r.ListRetiredSegments(ctx, blockstore.PlaneCatalog, bucket)
		if err != nil || len(retired) != 1 {
			t.Fatalf("ListRetiredSegments = %+v (err %v), want one row", retired, err)
		}
		got := retired[0]
		if got.Seq != seq || got.RetiredAt != now || got.ShippedAt != now || got.DeadBytes != 7 ||
			!bytes.Equal(got.SHA256, sha) || !bytes.Equal(got.IndexDigest, indexDigest) {
			t.Fatalf("retired row = %+v", got)
		}
		var opRoots int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM ingot.segment_op_roots WHERE seq = $1`,
			int64(seq)).Scan(&opRoots); err != nil || opRoots != 0 {
			t.Fatalf("op-root rows after retirement = %d (err %v), want 0", opRoots, err)
		}
		buckets, err := r.ListSegmentBuckets(ctx, blockstore.PlaneCatalog)
		if err != nil {
			t.Fatalf("ListSegmentBuckets: %v", err)
		}
		if slices.Contains(buckets, bucket) {
			t.Fatalf("ListSegmentBuckets lists %q, which has only retired segments", bucket)
		}

		if err := r.DeleteSegment(ctx, blockstore.PlaneCatalog, seq); err != nil {
			t.Fatalf("DeleteSegment: %v", err)
		}
		if retired, err := r.ListRetiredSegments(ctx, blockstore.PlaneCatalog, bucket); err != nil || len(retired) != 0 {
			t.Fatalf("ListRetiredSegments after delete = %+v (err %v)", retired, err)
		}
	})

	t.Run("catalog usage sums shipped segments", func(t *testing.T) {
		bucket := fmt.Sprintf("cu-%d", time.Now().UnixNano())
		space := seedBucket(t, bucket)
		// segment records a sealed segment of bucket b, ships it unless
		// dead is nil, and retires it if asked; it returns the seq.
		segment := func(b string, size int64, dead *int64, indexDigest multihash.Multihash, retire bool) uint64 {
			t.Helper()
			seq, err := r.NextSegmentSeq(ctx)
			if err != nil {
				t.Fatalf("NextSegmentSeq: %v", err)
			}
			if err := r.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq, b); err != nil {
				t.Fatalf("InsertSegmentOpen: %v", err)
			}
			if err := r.MarkSegmentSealed(ctx, blockstore.PlaneCatalog, seq, 1, size, nil, nil); err != nil {
				t.Fatalf("MarkSegmentSealed: %v", err)
			}
			if dead != nil {
				if err := r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seq, 2, indexDigest, *dead, nil); err != nil {
					t.Fatalf("MarkSegmentShipped: %v", err)
				}
			}
			if retire {
				if err := r.RetireSegment(ctx, blockstore.PlaneCatalog, seq, 3); err != nil {
					t.Fatalf("RetireSegment: %v", err)
				}
			}
			return seq
		}
		dead := func(n int64) *int64 { return &n }
		segment(bucket, 1000, dead(300), digest, false)
		unmeasured := segment(bucket, 500, dead(-1), digest, false)
		segment(bucket, 2000, dead(900), digest, true) // retired: its CAR is still on Forge
		segment(bucket, 59, dead(0), nil, false)       // shipped without registering anything
		segment(bucket, 700, nil, nil, false)          // not shipped
		// Rows of a bucket with no row of its own are nobody's usage.
		orphan := bucket + "-orphan"
		segment(orphan, 4000, dead(0), digest, false)

		usage, err := r.CatalogUsage(ctx)
		if err != nil {
			t.Fatalf("CatalogUsage: %v", err)
		}
		want := registry.CatalogUsage{Bucket: bucket, Space: space, Bytes: 3500, DeadOnArrivalBytes: 1200, UnmeasuredBytes: 500}
		i := slices.IndexFunc(usage, func(u registry.CatalogUsage) bool { return u.Bucket == bucket })
		if i < 0 || usage[i] != want {
			t.Fatalf("CatalogUsage = %+v, want an entry %+v", usage, want)
		}
		if slices.ContainsFunc(usage, func(u registry.CatalogUsage) bool { return u.Bucket == orphan }) {
			t.Fatalf("CatalogUsage = %+v, lists %q, which has no bucket row", usage, orphan)
		}
		if !slices.IsSortedFunc(usage, func(a, b registry.CatalogUsage) int { return strings.Compare(a.Bucket, b.Bucket) }) {
			t.Fatalf("CatalogUsage = %+v, not ordered by bucket", usage)
		}

		// An unmeasured segment's dead_bytes is NULL, read back as -1.
		segs, err := r.ListSegments(ctx, blockstore.PlaneCatalog, bucket)
		if err != nil {
			t.Fatalf("ListSegments: %v", err)
		}
		j := slices.IndexFunc(segs, func(m logstore.SegmentMeta) bool { return m.Seq == unmeasured })
		if j < 0 || segs[j].DeadBytes != -1 {
			t.Fatalf("ListSegments = %+v, want seq %d with DeadBytes -1", segs, unmeasured)
		}
		var isNull bool
		if err := pool.QueryRow(ctx, `SELECT dead_bytes IS NULL FROM ingot.segments WHERE seq = $1`,
			int64(unmeasured)).Scan(&isNull); err != nil || !isNull {
			t.Fatalf("unmeasured segment's dead_bytes IS NULL = %v (err %v), want true", isNull, err)
		}
	})
}

// TestCommitRootOnASingleConnection proves the
// enqueueing CAS never needs two pooled connections at once. Resolving a
// conflict or a missing bucket by reading through the pool, while the
// transaction already holds a connection, deadlocks outright on a
// single-connection pool and deadlocks any pool once enough concurrent
// commits exhaust it.
//
//	INGOT_TEST_DSN=postgres://... GOWORK=off go test ./registry/ -run SingleConnection -v
func TestCommitRootOnASingleConnection(t *testing.T) {
	dsn := os.Getenv("INGOT_TEST_DSN")
	if dsn == "" {
		t.Skip("set INGOT_TEST_DSN to run the live Postgres store test")
	}
	ctx := context.Background()

	// Migrations and the fixture run on an ordinary pool; only the calls under
	// test get the single connection, so a hang can only be them.
	setupPool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("setup pool: %v", err)
	}
	defer setupPool.Close()
	if err := migrations.Up(ctx, setupPool, zaptest.NewLogger(t)); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.MaxConns, cfg.MinConns = 1, 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	reg := registry.NewPostgres(pool)
	setup := registry.NewPostgres(setupPool)

	space := testutil.RandomDID(t)
	regs := []registry.UploadRegistration{{
		Bucket: "cas-single-conn", ObjectKey: "k", Space: space,
		Root: liveCid(t, "root"), Op: registry.UploadRegistrationAdd, Proofs: []byte("proofs"),
	}}

	// A deadlock hangs rather than fails, so every call below is bounded.
	bounded := func(name string, fn func(context.Context) error) error {
		c, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		err := fn(c)
		if c.Err() != nil {
			t.Fatalf("%s: needed a second pooled connection: %v", name, c.Err())
		}
		return err
	}

	// A bucket that does not exist: the conflict path has to say so without
	// reaching back to the pool.
	err = bounded("missing bucket", func(c context.Context) error {
		return reg.CommitRoot(c, "cas-single-conn", cid.Undef, liveCid(t, "next"), registry.RootCommit{Registrations: regs})
	})
	if !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("missing bucket: got %v, want ErrNotFound", err)
	}

	if err := setup.Create(ctx, "cas-single-conn", space, registry.CreateState{Tenant: testutil.RandomDID(t)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	// Deleting the bucket cascades to its registrations, which matters here:
	// this suite runs against a database that persists between runs.
	t.Cleanup(func() { _ = setup.Delete(context.Background(), "cas-single-conn") })

	// A root that does not match: the same path, reporting a conflict.
	err = bounded("stale root", func(c context.Context) error {
		return reg.CommitRoot(c, "cas-single-conn", liveCid(t, "stale"), liveCid(t, "next"), registry.RootCommit{Registrations: regs})
	})
	if !errors.Is(err, registry.ErrConflict) {
		t.Fatalf("stale root: got %v, want ErrConflict", err)
	}

	// And the happy path still commits both the root and its rows.
	if err := bounded("commit", func(c context.Context) error {
		return reg.CommitRoot(c, "cas-single-conn", cid.Undef, liveCid(t, "next"), registry.RootCommit{Registrations: regs})
	}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// Scoped to this run's space: the suite's database persists, so a global
	// count would depend on what ran before.
	queued, err := setup.ListUploadRegistrationsBySpace(ctx, space)
	if err != nil {
		t.Fatalf("list by space: %v", err)
	}
	if len(queued) != 1 {
		t.Fatalf("queued %d registrations for the space, want 1", len(queued))
	}
}

// TestDeletingABucketCascadesItsUploadRegistrations: a queued object-count
// change is only meaningful while its bucket exists, so the two share one
// fate. The database enforces it, which is what keeps bucket teardown free of
// a cleanup step to sequence, to fail on its own, or to leave behind.
//
//	INGOT_TEST_DSN=postgres://... GOWORK=off go test ./registry/ -run Cascades -v
func TestDeletingABucketCascadesItsUploadRegistrations(t *testing.T) {
	dsn := os.Getenv("INGOT_TEST_DSN")
	if dsn == "" {
		t.Skip("set INGOT_TEST_DSN to run the live Postgres store test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	if err := migrations.Up(ctx, pool, zaptest.NewLogger(t)); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	r := registry.NewPostgres(pool)

	space := testutil.RandomDID(t)
	if err := r.Create(ctx, "cascade-bucket", space, registry.CreateState{Tenant: testutil.RandomDID(t)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	t.Cleanup(func() { _ = r.Delete(context.Background(), "cascade-bucket") })

	if err := r.CommitRoot(ctx, "cascade-bucket", cid.Undef, liveCid(t, "root"),
		registry.RootCommit{Registrations: []registry.UploadRegistration{{
			Bucket: "cascade-bucket", ObjectKey: "k", Space: space,
			Root: liveCid(t, "mf"), Op: registry.UploadRegistrationAdd, Proofs: []byte("proofs"),
		}}}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// A dead-lettered row goes too: it is out of the sweep but still a row,
	// and the bearer proofs on it have no reason to outlive the bucket.
	queued, err := r.ListUploadRegistrationsBySpace(ctx, space)
	if err != nil {
		t.Fatalf("list by space: %v", err)
	}
	if len(queued) != 1 {
		t.Fatalf("queued %d, want 1", len(queued))
	}
	if err := r.DeadLetterUploadRegistrations(ctx, []int64{queued[0].Seq}, "test"); err != nil {
		t.Fatalf("dead-letter: %v", err)
	}

	if err := r.Delete(ctx, "cascade-bucket"); err != nil {
		t.Fatalf("delete bucket: %v", err)
	}

	left, err := r.ListUploadRegistrationsBySpace(ctx, space)
	if err != nil {
		t.Fatalf("list by space after delete: %v", err)
	}
	if len(left) != 0 {
		t.Fatalf("%d registrations outlived their bucket", len(left))
	}
}

// TestConcurrentCommitClaimsLive runs two commits claiming the same thousands
// of digests at once, many times over, one listing them in reverse. A commit
// takes a claim-key lock per digest, so the two take overlapping lock sets
// and must take them in one order whatever order their claims arrive in, or
// Postgres aborts one with a deadlock error. Each writer commits to a bucket
// of its own: the root swap serialises the writers of one bucket.
//
//	INGOT_TEST_DSN=postgres://... GOWORK=off go test ./registry/ -run ConcurrentCommitClaims -v
func TestConcurrentCommitClaimsLive(t *testing.T) {
	dsn := os.Getenv("INGOT_TEST_DSN")
	if dsn == "" {
		t.Skip("set INGOT_TEST_DSN to run the live Postgres store test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	if err := migrations.Up(ctx, pool, zaptest.NewLogger(t)); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	r := registry.NewPostgres(pool)
	space := testutil.RandomDID(t)

	buckets := []string{"concurrent-claims-w0-" + space.String()[len(space.String())-8:], "concurrent-claims-w1-" + space.String()[len(space.String())-8:]}
	for _, name := range buckets {
		if err := r.Create(ctx, name, space, registry.CreateState{Tenant: testutil.RandomDID(t)}); err != nil {
			t.Fatalf("create bucket %q: %v", name, err)
		}
	}
	t.Cleanup(func() {
		for _, name := range buckets {
			_ = r.Delete(context.Background(), name)
		}
	})

	const n = 2000
	digests := make([]multihash.Multihash, n)
	for i := range digests {
		d, err := multihash.Sum([]byte(fmt.Sprintf("concurrent-claims-%s-%d", space, i)), multihash.SHA2_256, -1)
		if err != nil {
			t.Fatalf("multihash: %v", err)
		}
		digests[i] = d
		if err := r.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: d, Provider: "did:key:p", URL: "http://p/blob", Size: 1}); err != nil {
			t.Fatalf("PutLocation: %v", err)
		}
	}
	claims := func(bucket, version string, reversed bool) []registry.BlobClaim {
		out := make([]registry.BlobClaim, n)
		for i, d := range digests {
			j := i
			if reversed {
				j = n - 1 - i
			}
			out[j] = registry.BlobClaim{Digest: d, Bucket: bucket, ObjectKey: "k", VersionID: version, Space: space}
		}
		return out
	}

	for round := 0; round < 10; round++ {
		errs := make(chan error, 2)
		for w := 0; w < 2; w++ {
			go func(bucket, version string, reversed bool) {
				st, err := r.Get(ctx, bucket)
				if err != nil {
					errs <- err
					return
				}
				errs <- r.CommitRoot(ctx, bucket, st.Root, liveCid(t, bucket+version), registry.RootCommit{Claims: claims(bucket, version, reversed)})
			}(buckets[w], fmt.Sprintf("r%d-w%d", round, w), w == 1)
		}
		for w := 0; w < 2; w++ {
			if err := <-errs; err != nil {
				t.Fatalf("round %d: commit: %v", round, err)
			}
		}
	}
	if c, err := r.CountClaims(ctx, space, digests[0]); err != nil || c != 20 {
		t.Fatalf("claims on the first digest = %d, %v; want 20", c, err)
	}
}

// TestGetReleaseWaitsForTheClaimKeyLockLive: the release lookup a part's
// park makes is serialised with the start of that release. A start in
// flight holds the claim-key lock until it commits, so the lookup waits and
// then reads the record started, rather than read it pending a moment
// before the start commits and let the part reuse a blob the release then
// removes.
//
//	INGOT_TEST_DSN=postgres://... GOWORK=off go test ./registry/ -run GetReleaseWaits -v
func TestGetReleaseWaitsForTheClaimKeyLockLive(t *testing.T) {
	dsn := os.Getenv("INGOT_TEST_DSN")
	if dsn == "" {
		t.Skip("set INGOT_TEST_DSN to run the live Postgres store test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	if err := migrations.Up(ctx, pool, zaptest.NewLogger(t)); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	r := registry.NewPostgres(pool)
	space := testutil.RandomDID(t)
	d := liveDigest(t, "get-release-waits")
	if err := r.EnqueueRelease(ctx, space, d, time.Now()); err != nil {
		t.Fatalf("EnqueueRelease: %v", err)
	}
	t.Cleanup(func() { _ = r.DeleteRelease(context.Background(), space, d) })

	// A start in flight: the claim-key lock held, the record marked started,
	// the transaction open.
	start, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer start.Rollback(ctx)
	if _, err := start.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1 || ':' || encode($2, 'hex'), 0))`,
		space.String(), []byte(d)); err != nil {
		t.Fatalf("lock claim key: %v", err)
	}
	if _, err := start.Exec(ctx,
		`UPDATE ingot.blob_release_intents SET started_at = now() WHERE space = $1 AND digest = $2`,
		space, d); err != nil {
		t.Fatalf("mark started: %v", err)
	}

	type lookup struct {
		pr  *registry.PendingRelease
		err error
	}
	done := make(chan lookup, 1)
	go func() {
		pr, err := r.GetRelease(ctx, space, d)
		done <- lookup{pr, err}
	}()
	select {
	case got := <-done:
		t.Fatalf("GetRelease returned %+v, %v while a start held the claim-key lock", got.pr, got.err)
	case <-time.After(300 * time.Millisecond):
	}
	if err := start.Commit(ctx); err != nil {
		t.Fatalf("commit start: %v", err)
	}
	select {
	case got := <-done:
		if got.err != nil || got.pr.StartedAt.IsZero() {
			t.Fatalf("GetRelease after the start committed = %+v, %v; want the record started", got.pr, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("GetRelease did not return once the start committed")
	}
}

// TestReleaseGraceRunsFromTheSwapLive: a drop's release is due the grace
// after the swap commits, not after its transaction began. A swap that
// waited on a claim-key lock (here held by another transaction) owes a
// reader that resolved the discarded version meanwhile the whole grace from
// the commit.
//
//	INGOT_TEST_DSN=postgres://... GOWORK=off go test ./registry/ -run GraceRunsFromTheSwap -v
func TestReleaseGraceRunsFromTheSwapLive(t *testing.T) {
	dsn := os.Getenv("INGOT_TEST_DSN")
	if dsn == "" {
		t.Skip("set INGOT_TEST_DSN to run the live Postgres store test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	if err := migrations.Up(ctx, pool, zaptest.NewLogger(t)); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	r := registry.NewPostgres(pool)
	space := testutil.RandomDID(t)
	bucket := "grace-swap-" + space.String()[len(space.String())-8:]
	d := liveDigest(t, "grace-runs-from-the-swap")
	if err := r.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: d, Provider: "did:key:p", URL: "http://p/blob", Size: 1}); err != nil {
		t.Fatalf("PutLocation: %v", err)
	}
	c := registry.BlobClaim{Digest: d, Bucket: bucket, ObjectKey: "k", VersionID: "v1", Space: space}
	mustClaim(t, r, c)
	t.Cleanup(func() {
		_ = r.DeleteRelease(context.Background(), space, d)
		_ = r.Delete(context.Background(), bucket)
	})

	// Another transaction holds the digest's claim-key lock while the drop's
	// commit begins and waits.
	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer holder.Rollback(ctx)
	if _, err := holder.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1 || ':' || encode($2, 'hex'), 0))`,
		space.String(), []byte(d)); err != nil {
		t.Fatalf("lock claim key: %v", err)
	}
	const grace, stall = time.Second, 300 * time.Millisecond
	st, err := r.Get(ctx, bucket)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	start := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- r.CommitRoot(ctx, bucket, st.Root, liveCid(t, "grace-swap-next"), registry.RootCommit{
			Drops:        []registry.ClaimDrop{{Digest: d, Bucket: bucket, ObjectKey: "k", VersionID: "v1", Space: space}},
			ReleaseGrace: grace,
		})
	}()
	time.Sleep(stall)
	if err := holder.Commit(ctx); err != nil {
		t.Fatalf("release the lock: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("CommitRoot: %v", err)
	}
	var pr *registry.PendingRelease
	for _, rec := range mustListReleases(t, r, space) {
		if string(rec.Digest) == string(d) {
			pr = &rec
		}
	}
	if pr == nil {
		t.Fatal("the drop recorded no release")
	}
	if earliest := start.Add(stall + grace); pr.NotBefore.Before(earliest) {
		t.Fatalf("release due %v after the commit began, want at least the lock wait plus the grace (%v): the grace ran from the transaction's start", pr.NotBefore.Sub(start), stall+grace)
	}
}

// TestBucketCORSConfigLive round-trips a bucket's own CORS document through
// the SQL store: the column scans back as the bytes stored, NULL reads as
// nil, a nil write restores NULL, and a missing bucket is ErrNotFound. The
// in-memory fake cannot validate the column order of the row scan or the
// nullable bytea binding. Skipped unless INGOT_TEST_DSN names a reachable
// database:
//
//	INGOT_TEST_DSN=postgres://... GOWORK=off go test ./registry/ -run BucketCORS -v
func TestBucketCORSConfigLive(t *testing.T) {
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
	if _, err := pool.Exec(ctx, `TRUNCATE ingot.buckets CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	r := registry.NewPostgres(pool)
	if err := r.Create(ctx, "b", testutil.RandomDID(t), registry.CreateState{Tenant: testutil.RandomDID(t)}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	st, err := r.Get(ctx, "b")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if st.CORSConfig != nil {
		t.Fatalf("CORSConfig on a new bucket = %q, want nil", st.CORSConfig)
	}

	// Binary, with a zero byte, to exercise the bytea round trip rather than
	// a text one.
	doc := []byte("<CORSConfiguration>\x00</CORSConfiguration>")
	if err := r.SetCORSConfig(ctx, "b", doc); err != nil {
		t.Fatalf("SetCORSConfig: %v", err)
	}
	if st, err = r.Get(ctx, "b"); err != nil {
		t.Fatalf("Get after set: %v", err)
	}
	if !bytes.Equal(st.CORSConfig, doc) {
		t.Fatalf("CORSConfig = %q, want %q", st.CORSConfig, doc)
	}

	if err := r.SetCORSConfig(ctx, "b", nil); err != nil {
		t.Fatalf("SetCORSConfig(nil): %v", err)
	}
	if st, err = r.Get(ctx, "b"); err != nil {
		t.Fatalf("Get after clear: %v", err)
	}
	if st.CORSConfig != nil {
		t.Fatalf("CORSConfig after clear = %q, want nil", st.CORSConfig)
	}

	if err := r.SetCORSConfig(ctx, "missing", doc); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("SetCORSConfig on a missing bucket = %v, want ErrNotFound", err)
	}
}
