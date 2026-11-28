package registry_test

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/queue/spaceshardqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/taskrunner"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
	"github.com/jackc/pgx/v5"
)

// liveDigest is the sha256 multihash of s: a digest the batch reads decode.
func liveDigest(t *testing.T, s string) multihash.Multihash {
	t.Helper()
	d, err := multihash.Sum([]byte(s), multihash.SHA2_256, -1)
	require.NoError(t, err, "multihash")
	return d
}

func liveCid(t *testing.T, s string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte(s), multihash.SHA2_256, -1)
	require.NoError(t, err, "multihash")
	return cid.NewCidV1(cid.DagCBOR, mh)
}

// TestPostgresStores exercises the SQL-backed store methods against a
// real Postgres (jsonb, bytea[], ON CONFLICT, the latch's RowsAffected
// semantics).
func TestPostgresStores(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)

	tasks := liveUploadTasks(t, pool)
	r := liveRegistry(pool, tasks)
	// seedBucket inserts a bucket row directly, bypassing Create, and returns
	// the space DID it generated. space and tenant have no default — Create
	// always supplies the DIDs Hilt returns — so the seed supplies them too,
	// with the tenant sentinel a pre-column row would carry.
	seedBucket := func(t *testing.T, name string) did.DID {
		t.Helper()
		space := testutil.RandomDID(t)
		_, err := pool.Exec(ctx, `INSERT INTO ingot.buckets (name, space, tenant) VALUES ($1, $2, $3)`,
			name, space.String(), registry.UnknownTenant.String())
		require.NoError(t, err, "seed bucket %q", name)
		return space
	}
	digest := []byte{0x12, 0x20, 0xab, 0xcd} // binary, to exercise bytea round-trips

	t.Run("bucket space round trips", func(t *testing.T) {
		space := seedBucket(t, "b")
		st, err := r.Get(ctx, "b")
		require.NoError(t, err, "Get")
		require.Equal(t, space, st.Space, "space")
		// The backfill sentinel reads back as the constant, no special case.
		require.Equal(t, registry.UnknownTenant, st.Tenant, "tenant")
	})

	t.Run("bucket tenant round trips", func(t *testing.T) {
		tenant := testutil.RandomDID(t)
		require.NoError(t, r.Create(ctx, "owned", testutil.RandomDID(t), registry.CreateState{Tenant: tenant}), "Create")
		st, err := r.Get(ctx, "owned")
		require.NoError(t, err, "Get")
		require.Equal(t, tenant, st.Tenant, "tenant")
	})

	t.Run("create rejects an undefined tenant", func(t *testing.T) {
		require.Error(t, r.Create(ctx, "tenantless", testutil.RandomDID(t), registry.CreateState{}), "Create without a tenant succeeded; the row would be unreadable")
		_, err := r.Get(ctx, "tenantless")
		require.ErrorIs(t, err, registry.ErrNotFound, "Get after rejected Create = %v, want ErrNotFound (no row written)", err)
	})

	t.Run("blob claims count to zero", func(t *testing.T) {
		// The blob_refs PK is (digest, bucket, object_key, version_id); space is
		// denormalized, so a given (bucket, key, version) belongs to one space
		// (a bucket has one space). A second space therefore implies a second
		// bucket — bucket "b2" below, not a re-keyed "b".
		space := testutil.RandomDID(t)
		space2 := testutil.RandomDID(t)
		add := func(bucket, key string, sp did.DID) {
			require.NoError(t, r.PutLocation(ctx, registry.BlobLocation{Space: sp, Digest: digest, Provider: "did:key:p", URL: "http://p/blob", Size: 1}), "PutLocation")
			mustClaim(t, r, registry.BlobClaim{Digest: digest, Bucket: bucket, ObjectKey: key, VersionID: registry.NullVersionID, Space: sp})
		}
		add("b", "k1", space)
		add("b", "k2", space)
		add("b", "k1", space) // ON CONFLICT DO NOTHING — does not inflate the count
		add("b2", "k1", space2)
		n, _ := r.CountReferences(ctx, space, digest, uuid.Nil)
		require.Equal(t, 2, n, "count space1")
		n, _ = r.CountReferences(ctx, space2, digest, uuid.Nil)
		require.Equal(t, 1, n, "count space2")
		drop(t, r, registry.ClaimDrop{Digest: digest, Bucket: "b", ObjectKey: "k1", VersionID: registry.NullVersionID, Space: space})
		drop(t, r, registry.ClaimDrop{Digest: digest, Bucket: "b", ObjectKey: "k2", VersionID: registry.NullVersionID, Space: space})
		n, _ = r.CountReferences(ctx, space, digest, uuid.Nil)
		require.Zero(t, n, "count after release")

		// A claim publishes the digest's upload intent, and the state stays
		// once the claims are gone: it is the durable mark of a committed blob.
		pubDigest := multihash.Multihash([]byte{0x12, 0x20, 0xc1, 0xa1})
		require.NoError(t, r.PutIntent(ctx, registry.UploadIntent{Space: space, Digest: pubDigest, Size: 1, State: registry.IntentAccepted, Bucket: "b"}), "PutIntent")
		require.NoError(t, r.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: pubDigest, Provider: "did:key:p", URL: "http://p/blob", Size: 1}), "PutLocation")
		mustClaim(t, r, registry.BlobClaim{Digest: pubDigest, Bucket: "b", ObjectKey: "kp", VersionID: registry.NullVersionID, Space: space})
		in, err := r.GetIntent(ctx, space, pubDigest)
		require.NoError(t, err, "intent after claim")
		require.Equal(t, registry.IntentPublished, in.State, "intent after claim")
		drop(t, r, registry.ClaimDrop{Digest: pubDigest, Bucket: "b", ObjectKey: "kp", VersionID: registry.NullVersionID, Space: space})
		in, err = r.GetIntent(ctx, space, pubDigest)
		require.NoError(t, err, "intent after the claim dropped")
		require.Equal(t, registry.IntentPublished, in.State, "intent after the claim dropped")
		// A claim on a digest with no intent row (a shipped segment) is fine.
		require.NoError(t, r.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: multihash.Multihash([]byte{0x12, 0x20, 0xc1, 0xa2}), Provider: "did:key:p", URL: "http://p/blob", Size: 1}), "PutLocation")
		mustClaim(t, r, registry.BlobClaim{Digest: multihash.Multihash([]byte{0x12, 0x20, 0xc1, 0xa2}), Bucket: "b", ObjectKey: "kn", VersionID: registry.NullVersionID, Space: space})
	})

	t.Run("claims and parts take pending references over", func(t *testing.T) {
		space := testutil.RandomDID(t)
		d := multihash.Multihash([]byte{0x12, 0x20, 0xb1, 0x01})
		ref := func(t *testing.T) uuid.UUID {
			t.Helper()
			add, expire := uuid.New(), uuid.New()
			st := registry.PendingRef(add, expire, space, d, time.Now().Add(time.Hour))
			_, err := pool.Exec(ctx, st.SQL, st.Args...)
			require.NoError(t, err, "pending ref")
			return add
		}
		count := func(t *testing.T, except uuid.UUID) int {
			t.Helper()
			n, err := r.CountReferences(ctx, space, d, except)
			require.NoError(t, err, "CountReferences")
			return n
		}
		first, second := ref(t), ref(t)
		require.Equal(t, [2]int{2, 1}, [2]int{count(t, uuid.Nil), count(t, first)}, "[references, references but the first]")
		// The claim takes the first reference over: still two references.
		mustClaim(t, r, registry.BlobClaim{Digest: d, Bucket: "pb", ObjectKey: "k", VersionID: "null#1", Space: space, Ref: first})
		n := count(t, uuid.Nil)
		require.Equal(t, 2, n, "references after the claim, want the claim and the second reference")
		// A reference already taken over, or dropped, refuses the commit,
		// and the root stays where it was.
		before, _ := r.Get(ctx, "pb")
		err := claim(t, r, registry.BlobClaim{Digest: d, Bucket: "pb", ObjectKey: "k2", VersionID: "null#2", Space: space, Ref: first})
		require.ErrorIs(t, err, registry.ErrReferenceExpired, "claim of a reference taken over")
		after, _ := r.Get(ctx, "pb")
		if before != nil {
			require.True(t, after.Root.Equals(before.Root), "a refused claim advanced the root: %v -> %v", before.Root, after.Root)
		}
		// A part takes the second reference over in its own write.
		seedBucket(t, "pp")
		require.NoError(t, r.CreateSession(ctx, registry.MultipartSession{UploadID: "refs-up", Bucket: "pp", ObjectKey: "o", Space: space}), "CreateSession")
		t.Cleanup(func() { _ = r.DeleteSession(context.Background(), "refs-up") })
		_, err = r.PutPart(ctx, registry.MultipartPart{UploadID: "refs-up", PartNumber: 1, ETagMD5: []byte{1}, Size: 1, BlobDigests: []multihash.Multihash{d}, Refs: []uuid.UUID{second}}, nil)
		require.NoError(t, err, "PutPart")
		require.Equal(t, 2, count(t, uuid.Nil), "references after the part, want the claim and the part")
		_, err = r.PutPart(ctx, registry.MultipartPart{UploadID: "refs-up", PartNumber: 2, ETagMD5: []byte{1}, Size: 1, BlobDigests: []multihash.Multihash{d}, Refs: []uuid.UUID{second}}, nil)
		require.ErrorIs(t, err, registry.ErrReferenceExpired, "part with a reference taken over")
	})

	t.Run("drop claim enqueues release atomically", func(t *testing.T) {
		space := testutil.RandomDID(t)
		add := func(key string) {
			require.NoError(t, r.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: digest, Provider: "did:key:p", URL: "http://p/blob", Size: 1}), "PutLocation")
			mustClaim(t, r, registry.BlobClaim{Digest: digest, Bucket: "rb", ObjectKey: key, VersionID: "null#1", Space: space})
		}
		add("k1")
		add("k2")

		// A zero grace (the commit helper's) makes the record due at once.
		drop(t, r, registry.ClaimDrop{Digest: digest, Bucket: "rb", ObjectKey: "k1", VersionID: "null#1", Space: space})
		require.False(t, releaseRecorded(t, pool, space, digest), "first drop recorded a release while a claim remains")
		drop(t, r, registry.ClaimDrop{Digest: digest, Bucket: "rb", ObjectKey: "k2", VersionID: "null#1", Space: space})
		require.True(t, releaseRecorded(t, pool, space, digest), "last drop recorded no release")

		// A later queueing of the same release keeps the later time, and a
		// batch of digests queues each once.
		queue := func(at time.Time, digests ...multihash.Multihash) {
			t.Helper()
			specs := make([]taskrunner.Spec, len(digests))
			for i, d := range digests {
				var err error
				specs[i], err = (spaceshardqueue.ReleaseTask{}).Spec(space, d, at)
				require.NoError(t, err)
			}
			_, err := tasks.SubmitAll(ctx, specs)
			require.NoError(t, err, "queue releases")
		}
		later := time.Now().Add(time.Hour).Truncate(time.Millisecond)
		queue(later, digest)
		d2 := multihash.Multihash([]byte{0xbb, 0x02})
		queue(registry.DueNow(), digest, d2, d2)
		queued := mustListReleases(t, pool, space)
		require.Len(t, queued, 2, "queued releases")
		for _, q := range queued {
			if string(q.Digest) == string(digest) {
				require.True(t, q.NotBefore.Equal(later), "release of %x due %v, want the later time %v", digest, q.NotBefore, later)
			}
		}
	})

	t.Run("intent lifecycle", func(t *testing.T) {
		space := testutil.RandomDID(t)
		require.NoError(t, r.PutIntent(ctx, registry.UploadIntent{Space: space, Digest: digest, Size: 9, State: registry.IntentUploading, Bucket: "b"}), "PutIntent")
		require.NoError(t, r.SetIntentState(ctx, space, digest, registry.IntentUploading), "SetIntentState (uploading, the constraint admits it)")
		require.NoError(t, r.SetIntentState(ctx, space, digest, registry.IntentParked), "SetIntentState")
		got, err := r.GetIntent(ctx, space, digest)
		require.NoError(t, err, "GetIntent")
		require.Equal(t, registry.IntentParked, got.State, "GetIntent = %+v", got)
		require.Equal(t, "b", got.Bucket, "GetIntent = %+v", got)
		parked, _ := r.ListIntentsByState(ctx, registry.IntentParked)
		require.Len(t, parked, 1, "parked")
		err = r.SetIntentState(ctx, space, []byte("missing"), registry.IntentParked)
		require.ErrorIs(t, err, registry.ErrNotFound, "SetIntentState missing")
		require.NoError(t, r.DeleteIntent(ctx, space, digest), "DeleteIntent")
		_, err = r.GetIntent(ctx, space, digest)
		require.ErrorIs(t, err, registry.ErrNotFound, "GetIntent after delete")
	})

	t.Run("location round trip", func(t *testing.T) {
		space := testutil.RandomDID(t)
		require.NoError(t, r.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: digest, Provider: "did:piri", URL: "http://piri/b", Size: 100}), "PutLocation")
		loc, err := r.GetLocation(ctx, space, digest)
		require.NoError(t, err, "GetLocation")
		require.Equal(t, "http://piri/b", loc.URL, "GetLocation = %+v", loc)
		require.Equal(t, int64(100), loc.Size, "GetLocation = %+v", loc)
		require.NoError(t, r.DeleteLocation(ctx, space, digest), "DeleteLocation")
		_, err = r.GetLocation(ctx, space, digest)
		require.ErrorIs(t, err, registry.ErrNotFound, "GetLocation after delete")
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
			require.NoError(t, r.PutLocation(ctx, loc), "PutLocation")
		}
		got, err := r.GetLocations(ctx, space, []multihash.Multihash{a, b, missing})
		require.NoError(t, err, "GetLocations")
		require.Len(t, got, 2, "GetLocations = %+v", got)
		require.Equal(t, "http://piri/a", got[string(a)].URL, "GetLocations = %+v", got)
		require.Equal(t, int64(20), got[string(b)].Size, "GetLocations = %+v", got)
		require.Equal(t, b, got[string(b)].Digest, "GetLocations = %+v", got)
		require.Equal(t, space, got[string(a)].Space, "GetLocations = %+v", got)
		empty, err := r.GetLocations(ctx, space, nil)
		require.NoError(t, err, "GetLocations(nil)")
		require.Empty(t, empty, "GetLocations(nil)")
		// A stored digest that is not a multihash fails the read.
		bad := []byte{0x0c}
		require.NoError(t, r.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: bad, Provider: "did:piri", URL: "http://piri/x", Size: 1}), "PutLocation")
		_, err = r.GetLocations(ctx, space, []multihash.Multihash{a, bad})
		require.Error(t, err, "GetLocations over a non-multihash digest")
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
		require.NoError(t, r.PutEncryptionParams(ctx, want), "PutEncryptionParams")
		got, err := r.GetEncryptionParams(ctx, space, encDigest)
		require.NoError(t, err, "GetEncryptionParams")
		require.Equal(t, want, *got, "GetEncryptionParams")
	})

	t.Run("encryption params batch", func(t *testing.T) {
		space, other := testutil.RandomDID(t), testutil.RandomDID(t)
		a, b, missing := liveDigest(t, "params a"), liveDigest(t, "params b"), liveDigest(t, "params missing")
		for _, p := range []registry.BlobEncryptionParams{
			liveFEEParams(space, a), liveFEEParams(space, b), liveFEEParams(other, missing),
		} {
			require.NoError(t, r.PutEncryptionParams(ctx, p), "PutEncryptionParams")
		}
		got, err := r.GetEncryptionParamsBatch(ctx, space, []multihash.Multihash{a, b, missing})
		require.NoError(t, err, "GetEncryptionParamsBatch")
		require.Len(t, got, 2, "GetEncryptionParamsBatch rows")
		for _, d := range []multihash.Multihash{a, b} {
			require.Equal(t, liveFEEParams(space, d), got[string(d)], "row %x", d)
		}
		empty, err := r.GetEncryptionParamsBatch(ctx, space, nil)
		require.NoError(t, err, "GetEncryptionParamsBatch(nil)")
		require.Empty(t, empty, "GetEncryptionParamsBatch(nil)")
		// A stored digest that is not a multihash fails the read.
		bad := []byte{0x1c}
		require.NoError(t, r.PutEncryptionParams(ctx, liveFEEParams(space, bad)), "PutEncryptionParams")
		_, err = r.GetEncryptionParamsBatch(ctx, space, []multihash.Multihash{a, bad})
		require.Error(t, err, "GetEncryptionParamsBatch over a non-multihash digest")
	})

	t.Run("encryption params delete removes the row", func(t *testing.T) {
		space := testutil.RandomDID(t)
		encDigest := []byte{0x05, 0x06}
		require.NoError(t, r.PutEncryptionParams(ctx, liveFEEParams(space, encDigest)), "PutEncryptionParams")
		require.NoError(t, r.DeleteEncryptionParams(ctx, space, encDigest), "DeleteEncryptionParams")
		_, err := r.GetEncryptionParams(ctx, space, encDigest)
		require.ErrorIs(t, err, registry.ErrNotFound, "GetEncryptionParams after delete")
	})

	t.Run("encryption params rewrap in place", func(t *testing.T) {
		// A rotation replaces only the wrapped CEK and its key version; the
		// parameters describing the unchanged ciphertext must survive.
		space := testutil.RandomDID(t)
		encDigest := []byte{0x0a, 0x0b}
		require.NoError(t, r.PutEncryptionParams(ctx, liveFEEParams(space, encDigest)), "PutEncryptionParams")
		require.NoError(t, r.RewrapEncryptionParams(ctx, space, encDigest, []byte{0xca, 0xfe, 0x00, 0x01}, "region-kek-v2"), "RewrapEncryptionParams")
		got, err := r.GetEncryptionParams(ctx, space, encDigest)
		require.NoError(t, err, "GetEncryptionParams")
		want := liveFEEParams(space, encDigest)
		want.RegionWrappedCEK = []byte{0xca, 0xfe, 0x00, 0x01}
		want.RegionKeyVersion = "region-kek-v2"
		require.Equal(t, want, *got, "after rewrap")
	})

	t.Run("encryption params rewrap of a missing row is ErrNotFound", func(t *testing.T) {
		space := testutil.RandomDID(t)
		err := r.RewrapEncryptionParams(ctx, space, []byte{0x0c}, []byte{0x01}, "region-kek-v2")
		require.ErrorIs(t, err, registry.ErrNotFound, "RewrapEncryptionParams(absent)")
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
			require.Error(t, r.PutEncryptionParams(ctx, partial), "PutEncryptionParams(%s) = nil, want a constraint error", name)
			_, err := r.GetEncryptionParams(ctx, space, d)
			require.ErrorIs(t, err, registry.ErrNotFound, "incomplete params (%s) leaked a row", name)
		}
		// A rewrap cannot blank out the key material either.
		encDigest := []byte{0x78}
		require.NoError(t, r.PutEncryptionParams(ctx, liveFEEParams(space, encDigest)), "PutEncryptionParams")
		require.Error(t, r.RewrapEncryptionParams(ctx, space, encDigest, nil, "region-kek-v2"), "RewrapEncryptionParams(nil CEK) = nil, want a constraint error")
		require.Error(t, r.RewrapEncryptionParams(ctx, space, encDigest, []byte{0x01}, ""), "RewrapEncryptionParams(empty version) = nil, want a constraint error")
	})

	t.Run("encryption params independent of location", func(t *testing.T) {
		// No foreign key and no cascade: the params outlive their location row,
		// so a caller removing a blob must delete from both tables.
		space := testutil.RandomDID(t)
		encDigest := []byte{0x08, 0x09}
		require.NoError(t, r.PutEncryptionParams(ctx, liveFEEParams(space, encDigest)), "PutEncryptionParams")
		require.NoError(t, r.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: encDigest, Provider: "did:piri", URL: "http://piri/enc", Size: 4096}), "PutLocation")
		require.NoError(t, r.DeleteLocation(ctx, space, encDigest), "DeleteLocation")
		_, err := r.GetEncryptionParams(ctx, space, encDigest)
		require.NoError(t, err, "DeleteLocation shredded the encryption params")
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
		recorded, err := r.PutPark(ctx, park)
		require.NoError(t, err, "PutPark")
		require.True(t, recorded, "PutPark: want recorded")
		got, err := r.GetPark(ctx, space, digest)
		require.NoError(t, err, "GetPark")
		require.Equal(t, "sealed-inv", string(got.PutInvocation), "GetPark = %+v", got)
		require.Equal(t, int64(42), got.Size, "GetPark = %+v", got)
		// A second park for the key is refused and the first stands.
		park.Size = 43
		recorded, err = r.PutPark(ctx, park)
		require.NoError(t, err, "PutPark (duplicate)")
		require.False(t, recorded, "PutPark (duplicate): want refused")
		got, err = r.GetPark(ctx, space, digest)
		require.NoError(t, err, "GetPark after the refused put")
		require.Equal(t, int64(42), got.Size, "GetPark after the refused put")
		require.NoError(t, r.DeletePark(ctx, space, digest), "DeletePark")
		_, err = r.GetPark(ctx, space, digest)
		require.ErrorIs(t, err, registry.ErrNotFound, "GetPark after delete")
	})

	t.Run("multipart session parts latch metadata", func(t *testing.T) {
		const id = "upl-1"
		meta := map[string]string{"x-amz-meta-foo": "bar"}
		bSpace := testutil.RandomDID(t)
		require.NoError(t, r.CreateSession(ctx, registry.MultipartSession{UploadID: id, Bucket: "b", ObjectKey: "k", Space: bSpace, ContentType: "text/plain", Metadata: meta}), "CreateSession")
		err := r.CreateSession(ctx, registry.MultipartSession{UploadID: id, Bucket: "b", ObjectKey: "k", Space: bSpace})
		require.ErrorIs(t, err, registry.ErrExists, "duplicate CreateSession")
		s, err := r.GetSession(ctx, id)
		require.NoError(t, err, "GetSession")
		// The metadata jsonb and the space round-trip.
		require.Equal(t, "text/plain", s.ContentType, "GetSession = %+v", s)
		require.Equal(t, "bar", s.Metadata["x-amz-meta-foo"], "GetSession = %+v", s)
		require.Equal(t, bSpace, s.Space, "GetSession = %+v", s)
		// The space is required: a session without one is refused.
		require.Error(t, r.CreateSession(ctx, registry.MultipartSession{UploadID: id + "-nospace", Bucket: "b", ObjectKey: "k"}), "CreateSession without a space was accepted")
		// The bucket's space rides on the session, for teardown once the
		// bucket row is gone.
		space := testutil.RandomDID(t)
		require.NoError(t, r.CreateSession(ctx, registry.MultipartSession{UploadID: id + "-space", Bucket: "b", ObjectKey: "k", Space: space}), "CreateSession (space)")
		got, err := r.GetSession(ctx, id+"-space")
		require.NoError(t, err, "GetSession (space)")
		require.Equal(t, space, got.Space, "GetSession space")
		_ = r.DeleteSession(ctx, id+"-space")

		// bytea[] round trip + ordering.
		_, err = r.PutPart(ctx, registry.MultipartPart{UploadID: id, PartNumber: 2, ETagMD5: []byte{0x02}, Size: 2, BlobDigests: []multihash.Multihash{{0xd2}}}, nil)
		require.NoError(t, err, "PutPart 2")
		_, err = r.PutPart(ctx, registry.MultipartPart{UploadID: id, PartNumber: 1, ETagMD5: []byte{0x01}, Size: 1, BlobDigests: []multihash.Multihash{{0xd1, 0xa}, {0xd1, 0xb}}},
			&registry.PartTree{Offset: 0, ChunkLog: 4, Nodes: []byte{0x0a}, Blocks: []byte{0x0b, 0x0c}, Head: []byte{0x0d}, Tail: []byte{0x0e}, Root: []byte{0x0f}})
		require.NoError(t, err, "PutPart 1")
		parts, err := r.ListParts(ctx, id)
		require.NoError(t, err, "ListParts")
		require.Len(t, parts, 2, "ListParts = %+v", parts)
		require.Equal(t, 1, parts[0].PartNumber, "ListParts = %+v", parts)
		require.Len(t, parts[0].BlobDigests, 2, "ListParts = %+v", parts)
		// The tree listing round-trips the tree rows by part number, absent
		// where none was recorded.
		trees, err := r.ListPartTrees(ctx, id)
		require.NoError(t, err, "ListPartTrees")
		require.Len(t, trees, 1, "ListPartTrees: want part 1's tree only")
		require.NotNil(t, trees[1], "ListPartTrees = %+v; want part 1's tree only", trees)
		tr := trees[1]
		require.Equal(t, uint8(4), tr.ChunkLog, "part 1 tree = %+v", tr)
		require.Equal(t, []byte{0x0b, 0x0c}, tr.Blocks, "part 1 tree = %+v", tr)
		require.Equal(t, []byte{0x0d}, tr.Head, "part 1 tree = %+v", tr)
		require.Equal(t, []byte{0x0e}, tr.Tail, "part 1 tree = %+v", tr)
		require.Equal(t, []byte{0x0f}, tr.Root, "part 1 tree = %+v", tr)
		// A superseding part without a tree drops its predecessor's row; one
		// with a tree replaces it.
		_, err = r.PutPart(ctx, registry.MultipartPart{UploadID: id, PartNumber: 1, ETagMD5: []byte{0x01}, Size: 1, BlobDigests: []multihash.Multihash{{0xd1}}}, nil)
		require.NoError(t, err, "PutPart 1 again")
		again, _ := r.ListPartTrees(ctx, id)
		require.Nil(t, again[1], "superseded part kept its tree")
		_, err = r.PutPart(ctx, registry.MultipartPart{UploadID: id, PartNumber: 2, ETagMD5: []byte{0x02}, Size: 2, BlobDigests: []multihash.Multihash{{0xd2}}},
			&registry.PartTree{Offset: 1, ChunkLog: 5, Nodes: []byte{0x1a}, Blocks: []byte{0x1b}})
		require.NoError(t, err, "PutPart 2 with tree")
		again, _ = r.ListPartTrees(ctx, id)
		require.NotNil(t, again[2], "part 2 tree")
		require.Equal(t, int64(1), again[2].Offset, "part 2 tree = %+v", again[2])
		require.Equal(t, uint8(5), again[2].ChunkLog, "part 2 tree = %+v", again[2])
		require.Nil(t, again[2].Head, "part 2 tree = %+v", again[2])
		// Deleting the trees keeps the part records.
		require.NoError(t, r.DeletePartTrees(ctx, id), "DeletePartTrees")
		after, _ := r.ListPartTrees(ctx, id)
		require.Empty(t, after, "after DeletePartTrees")
		parts, _ = r.ListParts(ctx, id)
		require.Len(t, parts, 2, "after DeletePartTrees")

		// A Complete records the session completed, once.
		completed := registry.CompletedSession(id, "etag", "v1")
		tag, err := pool.Exec(ctx, completed.SQL, completed.Args...)
		require.NoError(t, err, "complete the session")
		require.Equal(t, int64(1), tag.RowsAffected(), "complete the session")
		// A part cannot land once the session has left 'open', nor on a
		// session that does not exist.
		_, err = r.PutPart(ctx, registry.MultipartPart{UploadID: id, PartNumber: 3, ETagMD5: []byte{0x03}, Size: 3, BlobDigests: []multihash.Multihash{{0xd3}}}, nil)
		require.ErrorIs(t, err, registry.ErrNotFound, "PutPart on a completed session")
		_, err = r.PutPart(ctx, registry.MultipartPart{UploadID: "no-such-upload", PartNumber: 1, ETagMD5: []byte{0x01}, Size: 1, BlobDigests: []multihash.Multihash{{0xd1}}}, nil)
		require.ErrorIs(t, err, registry.ErrNotFound, "PutPart on a missing session")
		parts, _ = r.ListParts(ctx, id)
		require.Len(t, parts, 2, "parts after the refused writes")
		tag, err = pool.Exec(ctx, completed.SQL, completed.Args...)
		require.NoError(t, err, "complete the completed session again")
		require.Zero(t, tag.RowsAffected(), "complete the completed session again")

		// delete cascades parts
		require.NoError(t, r.DeleteSession(ctx, id), "DeleteSession")
		parts, _ = r.ListParts(ctx, id)
		require.Empty(t, parts, "parts after session delete (cascade)")
	})

	t.Run("multipart listing and part refs", func(t *testing.T) {
		lsSpace := testutil.RandomDID(t)
		mk := func(id, key string) {
			t.Helper()
			require.NoError(t, r.CreateSession(ctx, registry.MultipartSession{
				UploadID: id, Bucket: "b", ObjectKey: key, Space: lsSpace,
				ContentEncoding: "testenc", ChecksumAlgorithm: "CRC32", ChecksumType: "FULL_OBJECT",
			}), "CreateSession %s", id)
		}
		mk("ls-2", "zeta")
		mk("ls-1", "alpha")
		mk("ls-3", "alpha") // same key, created after ls-1

		// New session columns round-trip.
		s, err := r.GetSession(ctx, "ls-1")
		require.NoError(t, err, "GetSession new columns")
		require.Equal(t, "testenc", s.ContentEncoding, "GetSession new columns = %+v", s)
		require.Equal(t, "CRC32", s.ChecksumAlgorithm, "GetSession new columns = %+v", s)
		require.Equal(t, "FULL_OBJECT", s.ChecksumType, "GetSession new columns = %+v", s)
		require.False(t, s.CreatedAt.IsZero(), "GetSession new columns = %+v", s)

		// ListSessions: (object_key, created_at, upload_id) order.
		sessions, err := r.ListSessions(ctx, "b", lsSpace)
		require.NoError(t, err, "ListSessions")
		ids := make([]string, len(sessions))
		for i, x := range sessions {
			ids[i] = x.UploadID
		}
		require.Equal(t, []string{"ls-1", "ls-3", "ls-2"}, ids, "ListSessions order")
		// A bucket of the same name in another space lists none of them.
		other, err := r.ListSessions(ctx, "b", testutil.RandomDID(t))
		require.NoError(t, err, "ListSessions for another space")
		require.Empty(t, other, "ListSessions for another space")

		// CountPartRefs: bytea[] ANY-match across sessions, excluding one.
		shared := []byte{0xee, 0x01}
		_, err = r.PutPart(ctx, registry.MultipartPart{UploadID: "ls-1", PartNumber: 1, ETagMD5: []byte{1}, Size: 1, BlobDigests: []multihash.Multihash{shared}}, nil)
		require.NoError(t, err, "PutPart ls-1")
		_, err = r.PutPart(ctx, registry.MultipartPart{UploadID: "ls-2", PartNumber: 1, ETagMD5: []byte{2}, Size: 1, BlobDigests: []multihash.Multihash{shared, {0xee, 0x02}}}, nil)
		require.NoError(t, err, "PutPart ls-2")
		n, err := r.CountPartRefs(ctx, shared, "ls-1")
		require.NoError(t, err, "CountPartRefs(shared, exclude ls-1)")
		require.Equal(t, 1, n, "CountPartRefs(shared, exclude ls-1)")
		n, err = r.CountPartRefs(ctx, []byte{0xee, 0x02}, "ls-2")
		require.NoError(t, err, "CountPartRefs(unique, exclude owner)")
		require.Zero(t, n, "CountPartRefs(unique, exclude owner)")

		// CountReferences: parts of open sessions count.
		n, err = r.CountReferences(ctx, lsSpace, shared, uuid.Nil)
		require.NoError(t, err, "CountReferences(shared)")
		require.Equal(t, 2, n, "CountReferences(shared)")

		// A completed session's parts are no longer live, and the
		// completion stamps the state's change.
		completed := registry.CompletedSession("ls-1", "etag", "")
		_, err = pool.Exec(ctx, completed.SQL, completed.Args...)
		require.NoError(t, err, "complete ls-1")
		s, err = r.GetSession(ctx, "ls-1")
		require.NoError(t, err, "GetSession after the completion")
		require.False(t, s.StateChangedAt.Before(s.CreatedAt), "StateChangedAt after the completion = %v (created %v)", s.StateChangedAt, s.CreatedAt)
		require.False(t, s.StateChangedAt.IsZero(), "StateChangedAt after the completion = %v (created %v)", s.StateChangedAt, s.CreatedAt)
		n, err = r.CountReferences(ctx, lsSpace, shared, uuid.Nil)
		require.NoError(t, err, "CountReferences(shared) after ls-1 completed")
		require.Equal(t, 1, n, "CountReferences(shared) after ls-1 completed")

		for _, id := range []string{"ls-1", "ls-2", "ls-3"} {
			_ = r.DeleteSession(ctx, id)
		}
	})

	t.Run("revocation cursor upsert round trip", func(t *testing.T) {
		_, err := r.GetRevocationCursor(ctx)
		require.ErrorIs(t, err, registry.ErrNotFound, "GetRevocationCursor before put")
		first := registry.RevocationCursor{Token: "1733"}
		require.NoError(t, r.PutRevocationCursor(ctx, first), "PutRevocationCursor")
		got, err := r.GetRevocationCursor(ctx)
		require.NoError(t, err, "GetRevocationCursor")
		require.Equal(t, first, *got, "cursor")
		// The row is a single latch: a second put overwrites, never adds.
		second := registry.RevocationCursor{Token: "1734"}
		require.NoError(t, r.PutRevocationCursor(ctx, second), "PutRevocationCursor (upsert)")
		got, err = r.GetRevocationCursor(ctx)
		require.NoError(t, err, "GetRevocationCursor after upsert")
		require.Equal(t, second, *got, "cursor after upsert")
	})

	t.Run("forge_root advance guarded on root", func(t *testing.T) {
		committed := liveCid(t, "fg-committed")
		stale := liveCid(t, "fg-stale")
		seedBucket(t, "fg")
		require.NoError(t, r.CASRoot(ctx, "fg", cid.Undef, committed), "CASRoot")
		seq, err := r.NextSegmentSeq(ctx)
		require.NoError(t, err, "NextSegmentSeq")
		require.NoError(t, r.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq, "fg"), "InsertSegmentOpen")
		// Ship a segment whose op-roots include a stale root the bucket never
		// adopted; only the committed root may advance forge_root.
		require.NoError(t, r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seq, time.Now().Unix(), nil, -1,
			[]blockstore.OpRoot{{Bucket: "fg", Root: stale}, {Bucket: "fg", Root: committed}}), "MarkSegmentShipped")
		st, err := r.Get(ctx, "fg")
		require.NoError(t, err, "Get")
		require.True(t, st.ForgeRoot.Equals(committed), "forge_root = %v, want committed root (stale op-root must be skipped)", st.ForgeRoot)
	})

	t.Run("retired segment keeps its row", func(t *testing.T) {
		seq, err := r.NextSegmentSeq(ctx)
		require.NoError(t, err, "NextSegmentSeq")
		bucket := fmt.Sprintf("rt-%d", seq)
		sha := make([]byte, 32)
		sha[0] = 0x5e
		indexDigest := multihash.Multihash(digest)
		now := time.Now().Unix()
		require.NoError(t, r.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq, bucket), "InsertSegmentOpen")
		require.NoError(t, r.MarkSegmentSealed(ctx, blockstore.PlaneCatalog, seq, now, 10, sha,
			[]blockstore.OpRoot{{Bucket: bucket, Root: liveCid(t, "rt-root")}}), "MarkSegmentSealed")
		require.NoError(t, r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seq, now, indexDigest, 7, nil), "MarkSegmentShipped")
		for range 2 { // idempotent
			require.NoError(t, r.RetireSegment(ctx, blockstore.PlaneCatalog, seq, now), "RetireSegment")
		}

		live, err := r.ListSegments(ctx, blockstore.PlaneCatalog, bucket)
		require.NoError(t, err, "ListSegments = %+v (err %v)", live, err)
		require.Empty(t, live, "ListSegments = %+v (err %v)", live, err)
		retired, err := r.ListRetiredSegments(ctx, blockstore.PlaneCatalog, bucket)
		require.NoError(t, err, "ListRetiredSegments = %+v (err %v)", retired, err)
		require.Len(t, retired, 1, "ListRetiredSegments = %+v (err %v)", retired, err)
		got := retired[0]
		require.Equal(t, seq, got.Seq, "retired row = %+v", got)
		require.Equal(t, now, got.RetiredAt, "retired row = %+v", got)
		require.Equal(t, now, got.ShippedAt, "retired row = %+v", got)
		require.Equal(t, int64(7), got.DeadBytes, "retired row = %+v", got)
		require.Equal(t, sha, got.SHA256, "retired row = %+v", got)
		require.True(t, bytes.Equal(got.IndexDigest, indexDigest), "retired row = %+v", got)
		var opRoots int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ingot.segment_op_roots WHERE seq = $1`,
			int64(seq)).Scan(&opRoots), "count op-root rows")
		require.Zero(t, opRoots, "op-root rows after retirement")
		buckets, err := r.ListSegmentBuckets(ctx, blockstore.PlaneCatalog)
		require.NoError(t, err, "ListSegmentBuckets")
		require.False(t, slices.Contains(buckets, bucket), "ListSegmentBuckets lists %q, which has only retired segments", bucket)

		require.NoError(t, r.DeleteSegment(ctx, blockstore.PlaneCatalog, seq), "DeleteSegment")
		retired, err = r.ListRetiredSegments(ctx, blockstore.PlaneCatalog, bucket)
		require.NoError(t, err, "ListRetiredSegments after delete")
		require.Empty(t, retired, "ListRetiredSegments after delete")
	})

	t.Run("catalog usage sums shipped segments", func(t *testing.T) {
		bucket := fmt.Sprintf("cu-%d", time.Now().UnixNano())
		space := seedBucket(t, bucket)
		// segment records a sealed segment of bucket b, ships it unless
		// dead is nil, and retires it if asked; it returns the seq.
		segment := func(b string, size int64, dead *int64, indexDigest multihash.Multihash, retire bool) uint64 {
			t.Helper()
			seq, err := r.NextSegmentSeq(ctx)
			require.NoError(t, err, "NextSegmentSeq")
			require.NoError(t, r.InsertSegmentOpen(ctx, blockstore.PlaneCatalog, seq, b), "InsertSegmentOpen")
			require.NoError(t, r.MarkSegmentSealed(ctx, blockstore.PlaneCatalog, seq, 1, size, nil, nil), "MarkSegmentSealed")
			if dead != nil {
				require.NoError(t, r.MarkSegmentShipped(ctx, blockstore.PlaneCatalog, seq, 2, indexDigest, *dead, nil), "MarkSegmentShipped")
			}
			if retire {
				require.NoError(t, r.RetireSegment(ctx, blockstore.PlaneCatalog, seq, 3), "RetireSegment")
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
		require.NoError(t, err, "CatalogUsage")
		want := registry.CatalogUsage{Bucket: bucket, Space: space, Bytes: 3500, DeadOnArrivalBytes: 1200, UnmeasuredBytes: 500}
		i := slices.IndexFunc(usage, func(u registry.CatalogUsage) bool { return u.Bucket == bucket })
		require.GreaterOrEqual(t, i, 0, "CatalogUsage")
		require.Equal(t, want, usage[i], "CatalogUsage entry")
		require.False(t, slices.ContainsFunc(usage, func(u registry.CatalogUsage) bool { return u.Bucket == orphan }), "CatalogUsage = %+v, lists %q, which has no bucket row", usage, orphan)
		require.True(t, slices.IsSortedFunc(usage, func(a, b registry.CatalogUsage) int { return strings.Compare(a.Bucket, b.Bucket) }), "CatalogUsage = %+v, not ordered by bucket", usage)

		// An unmeasured segment's dead_bytes is NULL, read back as -1.
		segs, err := r.ListSegments(ctx, blockstore.PlaneCatalog, bucket)
		require.NoError(t, err, "ListSegments")
		j := slices.IndexFunc(segs, func(m logstore.SegmentMeta) bool { return m.Seq == unmeasured })
		require.GreaterOrEqual(t, j, 0, "ListSegments = %+v, want seq %d with DeadBytes -1", segs, unmeasured)
		require.Equal(t, int64(-1), segs[j].DeadBytes, "ListSegments = %+v, want seq %d with DeadBytes -1", segs, unmeasured)
		var isNull bool
		require.NoError(t, pool.QueryRow(ctx, `SELECT dead_bytes IS NULL FROM ingot.segments WHERE seq = $1`,
			int64(unmeasured)).Scan(&isNull), "read dead_bytes")
		require.True(t, isNull, "unmeasured segment's dead_bytes IS NULL")
	})
}

// TestCommitRootOnASingleConnection proves the
// enqueueing CAS never needs two pooled connections at once. Resolving a
// conflict or a missing bucket by reading through the pool, while the
// transaction already holds a connection, deadlocks outright on a
// single-connection pool and deadlocks any pool once enough concurrent
// commits exhaust it.
func TestCommitRootOnASingleConnection(t *testing.T) {
	ctx := context.Background()

	// Migrations and the fixture run on an ordinary pool; only the calls under
	// test get the single connection, so a hang can only be them.
	setupPool, dsn := pgtest.NewWithDSN(t)

	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err, "parse dsn")
	cfg.MaxConns, cfg.MinConns = 1, 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err, "pool")
	defer pool.Close()
	tasks := liveUploadTasks(t, setupPool)
	reg := liveRegistry(pool, tasks)
	setup := registry.NewPostgres(setupPool)

	space := testutil.RandomDID(t)
	counted := registry.RootCommit{ObjectsAdded: 1}

	// A deadlock hangs rather than fails, so every call below is bounded.
	bounded := func(name string, fn func(context.Context) error) error {
		c, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		err := fn(c)
		require.NoError(t, c.Err(), "%s: needed a second pooled connection", name)
		return err
	}

	// A bucket that does not exist: the conflict path has to say so without
	// reaching back to the pool.
	err = bounded("missing bucket", func(c context.Context) error {
		return reg.CommitRoot(c, "cas-single-conn", cid.Undef, liveCid(t, "next"), counted)
	})
	require.ErrorIs(t, err, registry.ErrNotFound, "missing bucket")

	require.NoError(t, setup.Create(ctx, "cas-single-conn", space, registry.CreateState{Tenant: testutil.RandomDID(t)}), "create bucket")
	t.Cleanup(func() { _ = setup.Delete(context.Background(), "cas-single-conn", space) })

	// A root that does not match: the same path, reporting a conflict.
	err = bounded("stale root", func(c context.Context) error {
		return reg.CommitRoot(c, "cas-single-conn", liveCid(t, "stale"), liveCid(t, "next"), counted)
	})
	require.ErrorIs(t, err, registry.ErrConflict, "stale root")

	// And the happy path still commits both the root and its rows.
	require.NoError(t, bounded("commit", func(c context.Context) error {
		return reg.CommitRoot(c, "cas-single-conn", cid.Undef, liveCid(t, "next"), counted)
	}), "commit")
	got := spaceObjects(t, setup, space)
	require.Len(t, got, 1, "counted %+v for the space", got)
	require.Equal(t, int64(1), got[0].Added, "counted %+v for the space", got)
}

// liveRegistry is a registry over pool whose CommitRoot and PutPart queue
// the tasks a commit's writes owe on tasks, carrying no authority.
func liveRegistry(pool *pgxpool.Pool, tasks *taskrunner.Runner) *registry.Postgres {
	return registry.WithOwedTasks(registry.NewPostgres(pool), func(ctx context.Context, tx pgx.Tx, w registry.CommitWrites) error {
		specs, err := spaceshardqueue.OwedSpecs(w, spaceshardqueue.ReleaseTask{})
		if err != nil {
			return err
		}
		if _, err := tasks.SubmitAllTx(ctx, tx, specs); err != nil {
			return err
		}
		return tasks.CancelTx(ctx, tx, spaceshardqueue.Name, w.Expiries...)
	})
}

// liveUploadTasks returns a migrated runner holding the space-shard queue,
// for a test registry to queue a commit's tasks on.
func liveUploadTasks(t *testing.T, pool *pgxpool.Pool) *taskrunner.Runner {
	t.Helper()
	r, err := taskrunner.New(pool, taskrunner.Options{Schema: "ingot", Queues: []taskrunner.QueueOptions{
		{Name: spaceshardqueue.Name},
	}})
	require.NoError(t, err, "task runner")
	// Submitting a task needs its handler registered, though these tests
	// never run one.
	noop := func(context.Context, *taskrunner.Exec, taskrunner.Task) ([]byte, error) { return nil, nil }
	require.NoError(t, r.Handle(spaceshardqueue.Name, spaceshardqueue.ReleaseAction, noop), "register release")
	require.NoError(t, r.Migrate(context.Background()), "task runner migrate")
	return r
}

// spaceObjects returns the object counts waiting to be reported for space.
func spaceObjects(t *testing.T, r *registry.Postgres, space did.DID) []registry.ObjectCount {
	t.Helper()
	due, err := r.DueObjects(context.Background(), 1000)
	require.NoError(t, err, "DueObjects")
	var out []registry.ObjectCount
	for _, c := range due {
		if c.Space == space {
			out = append(out, c)
		}
	}
	return out
}

// livePool returns a pool on a migrated database of the test's own.
func livePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := pgtest.New(t)
	return pool
}

// TestCommitRootCountsObjects: the objects a commit adds and takes away are
// counted in its own transaction, and a commit that fails counts none.
func TestCommitRootCountsObjects(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()
	r := liveRegistry(pool, liveUploadTasks(t, pool))
	space := testutil.RandomDID(t)
	require.NoError(t, r.Create(ctx, "count-bucket", space, registry.CreateState{Tenant: testutil.RandomDID(t)}), "create bucket")
	t.Cleanup(func() { _ = r.Delete(context.Background(), "count-bucket", space) })

	change := registry.RootCommit{ObjectsAdded: 2, ObjectsRemoved: 1}
	err := r.CommitRoot(ctx, "count-bucket", liveCid(t, "stale"), liveCid(t, "root"), change)
	require.ErrorIs(t, err, registry.ErrConflict, "stale commit")
	require.Empty(t, spaceObjects(t, r, space), "a failed commit counted")
	require.NoError(t, r.CommitRoot(ctx, "count-bucket", cid.Undef, liveCid(t, "root"), change), "commit")
	require.NoError(t, r.CommitRoot(ctx, "count-bucket", liveCid(t, "root"), liveCid(t, "next"), registry.RootCommit{ObjectsRemoved: 1}), "second commit")
	// The latest hour holds the bucket's totals: the commits share an hour
	// unless one fell either side of its turn.
	got := spaceObjects(t, r, space)
	require.NotEmpty(t, got, "counted %+v, want 2 added and 2 removed", got)
	require.Equal(t, int64(2), got[len(got)-1].Added, "counted %+v, want 2 added and 2 removed", got)
	require.Equal(t, int64(2), got[len(got)-1].Removed, "counted %+v, want 2 added and 2 removed", got)
}
