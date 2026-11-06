package registry_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner/taskrunnertest"
	"github.com/fil-forge/libforge/testutil"
)

// partQueueStore is what the multipart acceptance queue needs of a store.
type partQueueStore interface {
	registry.MultipartStore
	registry.ParkStore
	registry.LocationStore
	registry.IntentStore
}

// TestPartQueue runs the acceptance-queue operations against Postgres.
func TestPartQueue(t *testing.T) {
	pool := pgtest.New(t)
	runPartQueueSuite(t, pool, registry.NewPostgres(pool, registry.WithTasks(liveUploadTasks(t, pool))))
}

func runPartQueueSuite(t *testing.T, pool *pgxpool.Pool, st partQueueStore) {
	ctx := t.Context()
	// exec commits an accept's writes, as its task's commit does.
	exec := func(sts ...registry.Statement) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer tx.Rollback(ctx)
		for _, st := range sts {
			if _, err := tx.Exec(ctx, st.SQL, st.Args...); err != nil {
				t.Fatalf("exec %q: %v", st.SQL, err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	space := testutil.RandomDID(t)
	// Upload IDs unique to this run, so a shared database's other rows do
	// not show up in the listings.
	id := "pq-" + space.String()
	for _, s := range []registry.MultipartSession{
		{UploadID: id, Bucket: "b", ObjectKey: "k", Space: space},
	} {
		if err := st.CreateSession(ctx, s); err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
		t.Cleanup(func() { _ = st.DeleteSession(ctx, s.UploadID) })
	}
	part := func(uploadID string, n int, digests ...multihash.Multihash) registry.MultipartPart {
		t.Helper()
		sizes := make([]int64, len(digests))
		for i := range sizes {
			sizes[i] = int64(i + 1)
		}
		p := registry.MultipartPart{UploadID: uploadID, PartNumber: n, ETagMD5: []byte{byte(n)}, Size: 1, BlobDigests: digests, SpanLengths: sizes}
		if _, err := st.PutPart(ctx, p, nil, registry.ReleaseTask{}); err != nil {
			t.Fatalf("PutPart %d: %v", n, err)
		}
		return p
	}
	states := func(uploadID string) map[int]string {
		t.Helper()
		parts, err := st.ListParts(ctx, uploadID)
		if err != nil {
			t.Fatalf("ListParts: %v", err)
		}
		out := map[int]string{}
		for _, p := range parts {
			out[p.PartNumber] = p.State
		}
		return out
	}

	t.Run("a part is recorded parked with its sizes, and queues the accept", func(t *testing.T) {
		part(id, 1, multihash.Multihash{0xd1, 0x0a}, multihash.Multihash{0xd1, 0x0b})
		parts, err := st.ListParts(ctx, id)
		if err != nil || len(parts) != 1 || parts[0].State != registry.PartParked || !slices.Equal(parts[0].SpanLengths, []int64{1, 2}) {
			t.Fatalf("ListParts = %+v, err %v; want one parked part with sizes [1 2]", parts, err)
		}
		tasks, err := taskrunnertest.Tasks(ctx, pool, "ingot", registry.AcceptQueue)
		if err != nil {
			t.Fatalf("accept tasks: %v", err)
		}
		if !slices.ContainsFunc(tasks, func(q taskrunnertest.QueuedTask) bool {
			return q.Subject == id && q.Action == registry.AcceptAction && q.State == "pending"
		}) {
			t.Fatalf("accept tasks = %+v; want one pending for %s", tasks, id)
		}
		// Sizes must match the digests one for one, which a constraint
		// enforces.
		bad := registry.MultipartPart{UploadID: id, PartNumber: 9, ETagMD5: []byte{9}, Size: 1, BlobDigests: []multihash.Multihash{{0xd9}}, SpanLengths: []int64{1, 2}}
		if _, err := st.PutPart(ctx, bad, nil, registry.ReleaseTask{}); err == nil {
			t.Fatal("PutPart with mismatched sizes was accepted")
		}
	})

	t.Run("a part recorded without span lengths reads back without them", func(t *testing.T) {
		// Rows written before span_lengths existed hold NULL there; the
		// reader derives the lengths per blob instead.
		legacy := id + "-legacy"
		if err := st.CreateSession(ctx, registry.MultipartSession{UploadID: legacy, Bucket: "b", ObjectKey: "k", Space: space}); err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
		t.Cleanup(func() { _ = st.DeleteSession(ctx, legacy) })
		p := registry.MultipartPart{UploadID: legacy, PartNumber: 7, ETagMD5: []byte{7}, Size: 1, BlobDigests: []multihash.Multihash{{0xd7}}}
		if _, err := st.PutPart(ctx, p, nil, registry.ReleaseTask{}); err != nil {
			t.Fatalf("PutPart without span lengths: %v", err)
		}
		parts, err := st.ListParts(ctx, legacy)
		if err != nil || len(parts) != 1 || parts[0].SpanLengths != nil {
			t.Fatalf("ListParts over a row without span lengths = %+v, %v; want part 7 without lengths", parts, err)
		}
		queued, err := st.ListParkedParts(ctx, legacy, 0, 100)
		if err != nil || len(queued) == 0 {
			t.Fatalf("ListParkedParts over a row without span lengths = %v, %v", queued, err)
		}
	})

	t.Run("the queue lists parked parts in order after a part number", func(t *testing.T) {
		part(id, 2, multihash.Multihash{0xd2})
		part(id, 3, multihash.Multihash{0xd3})
		part(id, 4, multihash.Multihash{0xd4})
		queued, err := st.ListParkedParts(ctx, id, 0, 2)
		if err != nil || len(queued) != 2 || queued[0].PartNumber != 1 || queued[1].PartNumber != 2 {
			t.Fatalf("ListParkedParts(after 0, limit 2) = %+v, err %v; want parts 1 and 2", queued, err)
		}
		if len(queued[1].BlobDigests) != 1 || len(queued[1].SpanLengths) != 1 {
			t.Fatalf("queued part = %+v, want its digests and sizes", queued[1])
		}
		queued, err = st.ListParkedParts(ctx, id, 2, 10)
		if err != nil || len(queued) != 2 || queued[0].PartNumber != 3 || queued[1].PartNumber != 4 {
			t.Fatalf("ListParkedParts(after 2) = %+v, err %v; want parts 3 and 4", queued, err)
		}
	})

	t.Run("a park is swapped for its location only while it stands", func(t *testing.T) {
		digest := multihash.Multihash{0xf1, 0x01}
		loc := registry.BlobLocation{Provider: "did:web:piri", URL: "https://piri/blob", Size: 7}
		exec(registry.ConcludedBlob(space, digest, []byte{1}, loc)...)
		if _, err := st.GetLocation(ctx, space, digest); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("location written without a park: %v", err)
		}
		if err := st.PutIntent(ctx, registry.UploadIntent{Space: space, Digest: digest, Size: 7, State: registry.IntentParked}); err != nil {
			t.Fatalf("PutIntent: %v", err)
		}
		park := registry.BlobPark{Space: space, Digest: digest, AddTask: []byte{1}, AcceptTask: []byte{2}, PutInvocation: []byte{3}, Size: 7}
		if ok, err := st.PutPark(ctx, park); err != nil || !ok {
			t.Fatalf("PutPark = %v, %v", ok, err)
		}
		// A conclude of another allocation of the same bytes leaves the
		// park, which is not the one it accepted.
		exec(registry.ConcludedBlob(space, digest, []byte{9}, loc)...)
		if _, err := st.GetLocation(ctx, space, digest); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("location written for another add task's conclude: %v", err)
		}
		if in, err := st.GetIntent(ctx, space, digest); err != nil || in.State != registry.IntentParked {
			t.Fatalf("intent = %+v, %v; want still parked", in, err)
		}
		exec(registry.ConcludedBlob(space, digest, []byte{1}, loc)...)
		if _, err := st.GetPark(ctx, space, digest); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("park after the swap: %v, want ErrNotFound", err)
		}
		if got, err := st.GetLocation(ctx, space, digest); err != nil || got.URL != loc.URL {
			t.Fatalf("location after the swap = %+v, %v", got, err)
		}
		if in, err := st.GetIntent(ctx, space, digest); err != nil || in.State != registry.IntentAccepted {
			t.Fatalf("intent after the swap = %+v, %v; want accepted", in, err)
		}
		_ = st.DeleteLocation(ctx, space, digest)
	})

	t.Run("a park beside a location is dropped only while the location stands", func(t *testing.T) {
		digest := multihash.Multihash{0xf2, 0x01}
		park := registry.BlobPark{Space: space, Digest: digest, AddTask: []byte{1}, AcceptTask: []byte{2}, PutInvocation: []byte{3}, Size: 7}
		if ok, err := st.PutPark(ctx, park); err != nil || !ok {
			t.Fatalf("PutPark = %v, %v", ok, err)
		}
		exec(registry.LocatedBlob(space, digest)...)
		if _, err := st.GetPark(ctx, space, digest); err != nil {
			t.Fatalf("park without a location dropped: %v", err)
		}
		if err := st.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: digest, Provider: "did:web:piri", URL: "https://piri/blob", Size: 7}); err != nil {
			t.Fatalf("PutLocation: %v", err)
		}
		exec(registry.LocatedBlob(space, digest)...)
		if _, err := st.GetPark(ctx, space, digest); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("park beside a location survived: %v", err)
		}
		_ = st.DeleteLocation(ctx, space, digest)
	})

	t.Run("a part is accepted once every blob is located and its row is unchanged", func(t *testing.T) {
		a, b := multihash.Multihash{0xf3, 0x0a}, multihash.Multihash{0xf3, 0x0b}
		first := part(id, 5, a, b)
		if err := st.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: a, Provider: "did:web:piri", URL: "https://piri/a", Size: 1}); err != nil {
			t.Fatalf("PutLocation: %v", err)
		}
		exec(registry.AcceptedPart(space, first))
		if got := states(id)[5]; got != registry.PartParked {
			t.Fatalf("part 5 with one blob located = %q, want parked", got)
		}
		if err := st.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: b, Provider: "did:web:piri", URL: "https://piri/b", Size: 2}); err != nil {
			t.Fatalf("PutLocation: %v", err)
		}
		// Replaced since it was read: the old row's digests no longer match.
		part(id, 5, a)
		exec(registry.AcceptedPart(space, first))
		if got := states(id)[5]; got != registry.PartParked {
			t.Fatalf("replaced part 5 = %q, want parked", got)
		}
		second := part(id, 5, a, b)
		exec(registry.AcceptedPart(space, second))
		if got := states(id)[5]; got != registry.PartAccepted {
			t.Fatalf("part 5 = %q, want accepted", got)
		}
		_ = st.DeleteLocation(ctx, space, a)
		_ = st.DeleteLocation(ctx, space, b)
	})

	t.Run("the oldest parked part of a live upload is reported", func(t *testing.T) {
		parts, err := st.ListParts(ctx, id)
		if err != nil {
			t.Fatalf("ListParts: %v", err)
		}
		var waiting time.Time
		for _, p := range parts {
			if p.State == registry.PartParked && (waiting.IsZero() || p.CreatedAt.Before(waiting)) {
				waiting = p.CreatedAt
			}
		}
		// A shared database may hold older ones, so only the bound is
		// checked.
		oldest, found, err := st.OldestParkedPart(ctx)
		if err != nil || !found || oldest.After(waiting) {
			t.Fatalf("OldestParkedPart = %v, %v, %v; want one no later than %v", oldest, found, err, waiting)
		}
	})
}
