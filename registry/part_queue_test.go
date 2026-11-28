package registry_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/testutil"
)

// partQueueStore is what the multipart acceptance queue needs of a store.
type partQueueStore interface {
	registry.MultipartStore
	PutPart(ctx context.Context, p registry.MultipartPart, tree *registry.PartTree) ([]multihash.Multihash, error)
	registry.ParkStore
	registry.LocationStore
	registry.IntentStore
}

// TestPartQueue runs the acceptance-queue operations against Postgres.
func TestPartQueue(t *testing.T) {
	pool := pgtest.New(t)
	runPartQueueSuite(t, pool, liveRegistry(pool, liveUploadTasks(t, pool)))
}

func runPartQueueSuite(t *testing.T, pool *pgxpool.Pool, st partQueueStore) {
	ctx := t.Context()
	// exec commits an accept's writes, as its task's commit does.
	exec := func(sts ...registry.Statement) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		require.NoError(t, err, "begin")
		defer tx.Rollback(ctx)
		for _, st := range sts {
			_, err = tx.Exec(ctx, st.SQL, st.Args...)
			require.NoError(t, err, "exec %q", st.SQL)
		}
		require.NoError(t, tx.Commit(ctx), "commit")
	}
	space := testutil.RandomDID(t)
	// Upload IDs unique to this run, so a shared database's other rows do
	// not show up in the listings.
	id := "pq-" + space.String()
	for _, s := range []registry.MultipartSession{
		{UploadID: id, Bucket: "b", ObjectKey: "k", Space: space},
	} {
		require.NoError(t, st.CreateSession(ctx, s), "CreateSession")
		t.Cleanup(func() { _ = st.DeleteSession(ctx, s.UploadID) })
	}
	part := func(uploadID string, n int, digests ...multihash.Multihash) registry.MultipartPart {
		t.Helper()
		sizes := make([]int64, len(digests))
		for i := range sizes {
			sizes[i] = int64(i + 1)
		}
		p := registry.MultipartPart{UploadID: uploadID, PartNumber: n, ETagMD5: []byte{byte(n)}, Size: 1, BlobDigests: digests, SpanLengths: sizes}
		_, err := st.PutPart(ctx, p, nil)
		require.NoError(t, err, "PutPart %d", n)
		return p
	}
	states := func(uploadID string) map[int]string {
		t.Helper()
		parts, err := st.ListParts(ctx, uploadID)
		require.NoError(t, err, "ListParts")
		out := map[int]string{}
		for _, p := range parts {
			out[p.PartNumber] = p.State
		}
		return out
	}

	t.Run("a part is recorded parked with its sizes", func(t *testing.T) {
		part(id, 1, multihash.Multihash{0xd1, 0x0a}, multihash.Multihash{0xd1, 0x0b})
		parts, err := st.ListParts(ctx, id)
		require.NoError(t, err, "ListParts = %+v, err %v; want one parked part with sizes [1 2]", parts, err)
		require.Len(t, parts, 1, "ListParts = %+v, err %v; want one parked part with sizes [1 2]", parts, err)
		require.Equal(t, registry.PartParked, parts[0].State, "ListParts = %+v, err %v; want one parked part with sizes [1 2]", parts, err)
		require.True(t, slices.Equal(parts[0].SpanLengths, []int64{1, 2}), "ListParts = %+v, err %v; want one parked part with sizes [1 2]", parts, err)
		// Sizes must match the digests one for one, which a constraint
		// enforces.
		bad := registry.MultipartPart{UploadID: id, PartNumber: 9, ETagMD5: []byte{9}, Size: 1, BlobDigests: []multihash.Multihash{{0xd9}}, SpanLengths: []int64{1, 2}}
		_, err = st.PutPart(ctx, bad, nil)
		require.Error(t, err, "PutPart with mismatched sizes was accepted")
	})

	t.Run("a part recorded without span lengths reads back without them", func(t *testing.T) {
		// Rows written before span_lengths existed hold NULL there; the
		// reader derives the lengths per blob instead.
		legacy := id + "-legacy"
		require.NoError(t, st.CreateSession(ctx, registry.MultipartSession{UploadID: legacy, Bucket: "b", ObjectKey: "k", Space: space}), "CreateSession")
		t.Cleanup(func() { _ = st.DeleteSession(ctx, legacy) })
		p := registry.MultipartPart{UploadID: legacy, PartNumber: 7, ETagMD5: []byte{7}, Size: 1, BlobDigests: []multihash.Multihash{{0xd7}}}
		_, err := st.PutPart(ctx, p, nil)
		require.NoError(t, err, "PutPart without span lengths")
		parts, err := st.ListParts(ctx, legacy)
		require.NoError(t, err, "ListParts over a row without span lengths = %+v, %v; want part 7 without lengths", parts, err)
		require.Len(t, parts, 1, "ListParts over a row without span lengths = %+v, %v; want part 7 without lengths", parts, err)
		require.Nil(t, parts[0].SpanLengths, "ListParts over a row without span lengths = %+v, %v; want part 7 without lengths", parts, err)
		queued, err := st.ListParkedParts(ctx, legacy, 0, 100)
		require.NoError(t, err, "ListParkedParts over a row without span lengths = %v, %v", queued, err)
		require.NotEmpty(t, queued, "ListParkedParts over a row without span lengths = %v, %v", queued, err)
	})

	t.Run("the queue lists parked parts in order after a part number", func(t *testing.T) {
		part(id, 2, multihash.Multihash{0xd2})
		part(id, 3, multihash.Multihash{0xd3})
		part(id, 4, multihash.Multihash{0xd4})
		queued, err := st.ListParkedParts(ctx, id, 0, 2)
		require.NoError(t, err, "ListParkedParts(after 0, limit 2) = %+v, err %v; want parts 1 and 2", queued, err)
		require.Len(t, queued, 2, "ListParkedParts(after 0, limit 2) = %+v, err %v; want parts 1 and 2", queued, err)
		require.Equal(t, 1, queued[0].PartNumber, "ListParkedParts(after 0, limit 2) = %+v, err %v; want parts 1 and 2", queued, err)
		require.Equal(t, 2, queued[1].PartNumber, "ListParkedParts(after 0, limit 2) = %+v, err %v; want parts 1 and 2", queued, err)
		require.Len(t, queued[1].BlobDigests, 1, "queued part = %+v, want its digests and sizes", queued[1])
		require.Len(t, queued[1].SpanLengths, 1, "queued part = %+v, want its digests and sizes", queued[1])
		queued, err = st.ListParkedParts(ctx, id, 2, 10)
		require.NoError(t, err, "ListParkedParts(after 2) = %+v, err %v; want parts 3 and 4", queued, err)
		require.Len(t, queued, 2, "ListParkedParts(after 2) = %+v, err %v; want parts 3 and 4", queued, err)
		require.Equal(t, 3, queued[0].PartNumber, "ListParkedParts(after 2) = %+v, err %v; want parts 3 and 4", queued, err)
		require.Equal(t, 4, queued[1].PartNumber, "ListParkedParts(after 2) = %+v, err %v; want parts 3 and 4", queued, err)
	})

	t.Run("a park is swapped for its location only while it stands", func(t *testing.T) {
		digest := multihash.Multihash{0xf1, 0x01}
		loc := registry.BlobLocation{Provider: "did:web:piri", URL: "https://piri/blob", Size: 7}
		exec(registry.ConcludedBlob(space, digest, []byte{1}, loc)...)
		_, err := st.GetLocation(ctx, space, digest)
		require.ErrorIs(t, err, registry.ErrNotFound, "location written without a park")
		require.NoError(t, st.PutIntent(ctx, registry.UploadIntent{Space: space, Digest: digest, Size: 7, State: registry.IntentParked}), "PutIntent")
		park := registry.BlobPark{Space: space, Digest: digest, AddTask: []byte{1}, AcceptTask: []byte{2}, PutInvocation: []byte{3}, Size: 7}
		ok, err := st.PutPark(ctx, park)
		require.NoError(t, err, "PutPark")
		require.True(t, ok, "PutPark")
		// A conclude of another allocation of the same bytes leaves the
		// park, which is not the one it accepted.
		exec(registry.ConcludedBlob(space, digest, []byte{9}, loc)...)
		_, err = st.GetLocation(ctx, space, digest)
		require.ErrorIs(t, err, registry.ErrNotFound, "location written for another add task's conclude")
		in, err := st.GetIntent(ctx, space, digest)
		require.NoError(t, err, "GetIntent")
		require.Equal(t, registry.IntentParked, in.State, "intent should still be parked")
		exec(registry.ConcludedBlob(space, digest, []byte{1}, loc)...)
		_, err = st.GetPark(ctx, space, digest)
		require.ErrorIs(t, err, registry.ErrNotFound, "park after the swap")
		got, err := st.GetLocation(ctx, space, digest)
		require.NoError(t, err, "location after the swap")
		require.Equal(t, loc.URL, got.URL, "location after the swap")
		in, err = st.GetIntent(ctx, space, digest)
		require.NoError(t, err, "intent after the swap")
		require.Equal(t, registry.IntentAccepted, in.State, "intent after the swap")
		_ = st.DeleteLocation(ctx, space, digest)
	})

	t.Run("a park beside a location is dropped only while the location stands", func(t *testing.T) {
		digest := multihash.Multihash{0xf2, 0x01}
		park := registry.BlobPark{Space: space, Digest: digest, AddTask: []byte{1}, AcceptTask: []byte{2}, PutInvocation: []byte{3}, Size: 7}
		ok, err := st.PutPark(ctx, park)
		require.NoError(t, err, "PutPark = %v, %v", ok, err)
		require.True(t, ok, "PutPark = %v, %v", ok, err)
		exec(registry.LocatedBlob(space, digest)...)
		_, err = st.GetPark(ctx, space, digest)
		require.NoError(t, err, "park without a location dropped")
		require.NoError(t, st.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: digest, Provider: "did:web:piri", URL: "https://piri/blob", Size: 7}), "PutLocation")
		exec(registry.LocatedBlob(space, digest)...)
		_, err = st.GetPark(ctx, space, digest)
		require.ErrorIs(t, err, registry.ErrNotFound, "park beside a location survived")
		_ = st.DeleteLocation(ctx, space, digest)
	})

	t.Run("a blob's accept marks accepted the parts it completes", func(t *testing.T) {
		a, b := multihash.Multihash{0xf3, 0x0a}, multihash.Multihash{0xf3, 0x0b}
		part(id, 5, a, b)
		part(id, 6, b)
		require.NoError(t, st.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: a, Provider: "did:web:piri", URL: "https://piri/a", Size: 1}), "PutLocation")
		exec(registry.AcceptedParts(space, a)...)
		got := states(id)
		require.Equal(t, registry.PartParked, got[5], "parts with b unlocated = %v, want 5 and 6 parked", got)
		require.Equal(t, registry.PartParked, got[6], "parts with b unlocated = %v, want 5 and 6 parked", got)
		require.NoError(t, st.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: b, Provider: "did:web:piri", URL: "https://piri/b", Size: 2}), "PutLocation")
		exec(registry.AcceptedParts(space, b)...)
		got = states(id)
		require.Equal(t, registry.PartAccepted, got[5], "parts once every blob is located = %v, want 5 and 6 accepted", got)
		require.Equal(t, registry.PartAccepted, got[6], "parts once every blob is located = %v, want 5 and 6 accepted", got)
		_ = st.DeleteLocation(ctx, space, a)
		_ = st.DeleteLocation(ctx, space, b)
	})

	t.Run("the oldest parked part of a live upload is reported", func(t *testing.T) {
		parts, err := st.ListParts(ctx, id)
		require.NoError(t, err, "ListParts")
		var waiting time.Time
		for _, p := range parts {
			if p.State == registry.PartParked && (waiting.IsZero() || p.CreatedAt.Before(waiting)) {
				waiting = p.CreatedAt
			}
		}
		// A shared database may hold older ones, so only the bound is
		// checked.
		oldest, found, err := st.OldestParkedPart(ctx)
		require.NoError(t, err, "OldestParkedPart = %v, %v, %v; want one no later than %v", oldest, found, err, waiting)
		require.True(t, found, "OldestParkedPart = %v, %v, %v; want one no later than %v", oldest, found, err, waiting)
		require.False(t, oldest.After(waiting), "OldestParkedPart = %v, %v, %v; want one no later than %v", oldest, found, err, waiting)
	})
}
