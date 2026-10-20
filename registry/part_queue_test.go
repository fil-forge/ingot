package registry_test

import (
	"errors"
	"maps"
	"os"
	"slices"
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

// partQueueStore is what the multipart acceptance queue needs of a store.
type partQueueStore interface {
	registry.MultipartStore
	registry.ParkStore
	registry.LocationStore
}

// TestPartQueueLive runs the acceptance-queue operations against the in-memory
// store and, when INGOT_TEST_DSN is set, against Postgres.
func TestPartQueueLive(t *testing.T) {
	t.Run("inmem", func(t *testing.T) {
		runPartQueueSuite(t, inmem.NewMemStore())
	})
	t.Run("postgres", func(t *testing.T) {
		dsn := os.Getenv("INGOT_TEST_DSN")
		if dsn == "" {
			t.Skip("set INGOT_TEST_DSN to run the acceptance queue against Postgres")
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
		runPartQueueSuite(t, registry.NewPostgres(pool))
	})
}

func runPartQueueSuite(t *testing.T, st partQueueStore) {
	ctx := t.Context()
	space := testutil.RandomDID(t)
	// Upload IDs unique to this run, so a shared database's other rows do
	// not show up in the listings.
	id := "pq-" + space.String()
	other := id + "-aborting"
	for _, s := range []registry.MultipartSession{
		{UploadID: id, Bucket: "b", ObjectKey: "k", Space: space},
		{UploadID: other, Bucket: "b", ObjectKey: "k", Space: space},
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
		if err := st.PutPart(ctx, p, nil); err != nil {
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

	t.Run("a part is recorded spooled with its sizes", func(t *testing.T) {
		part(id, 1, multihash.Multihash{0xd1, 0x0a}, multihash.Multihash{0xd1, 0x0b})
		parts, err := st.ListParts(ctx, id)
		if err != nil || len(parts) != 1 || parts[0].State != registry.PartSpooled || !slices.Equal(parts[0].SpanLengths, []int64{1, 2}) {
			t.Fatalf("ListParts = %+v, err %v; want one spooled part with sizes [1 2]", parts, err)
		}
		// Sizes must match the digests one for one. The in-memory store
		// does not check this; the Postgres constraint does.
		if _, ok := st.(*registry.Postgres); ok {
			bad := registry.MultipartPart{UploadID: id, PartNumber: 9, ETagMD5: []byte{9}, Size: 1, BlobDigests: []multihash.Multihash{{0xd9}}, SpanLengths: []int64{1, 2}}
			if err := st.PutPart(ctx, bad, nil); err == nil {
				t.Fatal("PutPart with mismatched sizes was accepted")
			}
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
		p := registry.MultipartPart{UploadID: legacy, PartNumber: 7, ETagMD5: []byte{7}, Size: 1, BlobDigests: []multihash.Multihash{{0xd7}}, State: registry.PartParked}
		if err := st.PutPart(ctx, p, nil); err != nil {
			t.Fatalf("PutPart without span lengths: %v", err)
		}
		parts, err := st.ListParts(ctx, legacy)
		if err != nil {
			t.Fatalf("ListParts over a row without span lengths: %v", err)
		}
		found := false
		for _, got := range parts {
			if got.PartNumber == 7 {
				found = true
				if got.SpanLengths != nil {
					t.Fatalf("span lengths = %v, want nil", got.SpanLengths)
				}
			}
		}
		if !found {
			t.Fatal("part 7 not listed")
		}
		queued, err := st.ListParkedParts(ctx, legacy, 100)
		if err != nil || len(queued) == 0 {
			t.Fatalf("ListParkedParts over a row without span lengths = %v, %v", queued, err)
		}
		if _, err := st.MoveParts(ctx, []registry.MultipartPart{p}, registry.PartParked, registry.PartAccepted); err != nil {
			t.Fatalf("MoveParts: %v", err)
		}
	})

	t.Run("a move skips a replaced part", func(t *testing.T) {
		first := part(id, 2, multihash.Multihash{0xd2, 0x01})
		second := part(id, 2, multihash.Multihash{0xd2, 0x02})
		if n, err := st.MoveParts(ctx, []registry.MultipartPart{first}, registry.PartSpooled, registry.PartParked); err != nil || n != 0 {
			t.Fatalf("MoveParts with the replaced digests = %d, %v; want 0", n, err)
		}
		if n, err := st.MoveParts(ctx, []registry.MultipartPart{second}, registry.PartSpooled, registry.PartParked); err != nil || n != 1 {
			t.Fatalf("MoveParts = %d, %v; want 1", n, err)
		}
		// Only a part in the from state moves.
		if n, err := st.MoveParts(ctx, []registry.MultipartPart{second}, registry.PartSpooled, registry.PartParked); err != nil || n != 0 {
			t.Fatalf("second MoveParts = %d, %v; want 0", n, err)
		}
		if got := states(id)[2]; got != registry.PartParked {
			t.Fatalf("part 2 = %q, want parked", got)
		}
	})

	t.Run("the queue lists parked parts in order", func(t *testing.T) {
		p3 := part(id, 3, multihash.Multihash{0xd3})
		p4 := part(id, 4, multihash.Multihash{0xd4})
		if n, err := st.MoveParts(ctx, []registry.MultipartPart{p4, p3}, registry.PartSpooled, registry.PartParked); err != nil || n != 2 {
			t.Fatalf("MoveParts = %d, %v; want 2", n, err)
		}
		queued, err := st.ListParkedParts(ctx, id, 2)
		if err != nil || len(queued) != 2 || queued[0].PartNumber != 2 || queued[1].PartNumber != 3 {
			t.Fatalf("ListParkedParts(limit 2) = %+v, err %v; want parts 2 and 3", queued, err)
		}
		if len(queued[0].BlobDigests) != 1 || len(queued[0].SpanLengths) != 1 {
			t.Fatalf("queued part = %+v, want its digests and sizes", queued[0])
		}
		if _, err := st.MoveParts(ctx, queued, registry.PartParked, registry.PartAccepted); err != nil {
			t.Fatalf("MoveParts to accepted: %v", err)
		}
		want := map[int]string{1: registry.PartSpooled, 2: registry.PartAccepted, 3: registry.PartAccepted, 4: registry.PartParked}
		if got := states(id); !maps.Equal(got, want) {
			t.Fatalf("states = %v, want %v", got, want)
		}
	})

	t.Run("the restart sweep lists live uploads with parked parts", func(t *testing.T) {
		p := part(other, 1, multihash.Multihash{0xe1})
		if _, err := st.MoveParts(ctx, []registry.MultipartPart{p}, registry.PartSpooled, registry.PartParked); err != nil {
			t.Fatalf("MoveParts: %v", err)
		}
		if won, err := st.LatchSession(ctx, other, registry.SessionOpen, registry.SessionAborting); err != nil || !won {
			t.Fatalf("LatchSession = %v, %v", won, err)
		}
		ids, err := st.ListUploadsWithParkedParts(ctx)
		if err != nil {
			t.Fatalf("ListUploadsWithParkedParts: %v", err)
		}
		if !slices.Contains(ids, id) || slices.Contains(ids, other) {
			t.Fatalf("ListUploadsWithParkedParts = %v; want %s and not the aborting %s", ids, id, other)
		}
		if won, err := st.LatchSession(ctx, id, registry.SessionOpen, registry.SessionCompleting); err != nil || !won {
			t.Fatalf("LatchSession = %v, %v", won, err)
		}
		if ids, _ := st.ListUploadsWithParkedParts(ctx); !slices.Contains(ids, id) {
			t.Fatalf("ListUploadsWithParkedParts = %v; want the completing %s", ids, id)
		}
		// The completing upload's parked part 4 is waiting. A shared
		// database may hold older ones, so only the bounds are checked.
		parts, err := st.ListParts(ctx, id)
		if err != nil {
			t.Fatalf("ListParts: %v", err)
		}
		var waiting time.Time
		for _, p := range parts {
			if p.State == registry.PartParked {
				waiting = p.CreatedAt
			}
		}
		oldest, found, err := st.OldestParkedPart(ctx)
		if err != nil || !found || oldest.After(waiting) {
			t.Fatalf("OldestParkedPart = %v, %v, %v; want one no later than part 4's %v", oldest, found, err, waiting)
		}
	})

	t.Run("a park is swapped for its location only while it stands", func(t *testing.T) {
		digest := multihash.Multihash{0xf1, 0x01}
		loc := registry.BlobLocation{Space: space, Digest: digest, Provider: "did:web:piri", URL: "https://piri/blob", Size: 7}
		if swapped, err := st.ConcludePark(ctx, loc, []byte{1}); err != nil || swapped {
			t.Fatalf("ConcludePark without a park = %v, %v; want false", swapped, err)
		}
		if _, err := st.GetLocation(ctx, space, digest); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("location written without a park: %v", err)
		}
		park := registry.BlobPark{Space: space, Digest: digest, AddTask: []byte{1}, AcceptTask: []byte{2}, PutInvocation: []byte{3}, Size: 7}
		if ok, err := st.PutPark(ctx, park); err != nil || !ok {
			t.Fatalf("PutPark = %v, %v", ok, err)
		}
		// A conclude of another allocation of the same bytes leaves the
		// park, which is not the one it accepted.
		if swapped, err := st.ConcludePark(ctx, loc, []byte{9}); err != nil || swapped {
			t.Fatalf("ConcludePark of another add task = %v, %v; want false", swapped, err)
		}
		if _, err := st.GetLocation(ctx, space, digest); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("location written for another add task's conclude: %v", err)
		}
		if swapped, err := st.ConcludePark(ctx, loc, []byte{1}); err != nil || !swapped {
			t.Fatalf("ConcludePark = %v, %v; want true", swapped, err)
		}
		if _, err := st.GetPark(ctx, space, digest); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("park after the swap: %v, want ErrNotFound", err)
		}
		if got, err := st.GetLocation(ctx, space, digest); err != nil || got.URL != loc.URL {
			t.Fatalf("location after the swap = %+v, %v", got, err)
		}
		if swapped, err := st.ConcludePark(ctx, loc, []byte{1}); err != nil || swapped {
			t.Fatalf("second ConcludePark = %v, %v; want false", swapped, err)
		}
		_ = st.DeleteLocation(ctx, space, digest)
	})
}
