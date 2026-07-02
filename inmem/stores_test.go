package inmem

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/fil-forge/ingot/registry"
)

func TestBlobRefs_CountToZero(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	digest := []byte("digest-A")
	const space = "did:space:1"

	claim := func(bucket, key, version, sp string) registry.BlobClaim {
		return registry.BlobClaim{Digest: digest, Bucket: bucket, ObjectKey: key, VersionID: version, Space: sp}
	}

	// Two versions in the same space reference the same blob → count 2.
	mustAdd(t, m, claim("b", "k1", registry.NullVersionID, space))
	mustAdd(t, m, claim("b", "k2", registry.NullVersionID, space))
	if n := count(t, m, space, digest); n != 2 {
		t.Fatalf("count = %d, want 2", n)
	}

	// Idempotent add (same PK) does not inflate the count.
	mustAdd(t, m, claim("b", "k1", registry.NullVersionID, space))
	if n := count(t, m, space, digest); n != 2 {
		t.Fatalf("count after dup add = %d, want 2", n)
	}

	// A claim from a different space is counted under that space only.
	mustAdd(t, m, claim("b2", "k1", registry.NullVersionID, "did:space:2"))
	if n := count(t, m, space, digest); n != 2 {
		t.Fatalf("count for space 1 = %d, want 2 (space 2 must not leak in)", n)
	}
	if n := count(t, m, "did:space:2", digest); n != 1 {
		t.Fatalf("count for space 2 = %d, want 1", n)
	}

	// Releasing both space-1 versions drops its claim to zero (the remove gate).
	if err := m.DeleteBlobClaim(ctx, digest, "b", "k1", registry.NullVersionID); err != nil {
		t.Fatalf("DeleteBlobClaim: %v", err)
	}
	if n := count(t, m, space, digest); n != 1 {
		t.Fatalf("count after first delete = %d, want 1", n)
	}
	if err := m.DeleteBlobClaim(ctx, digest, "b", "k2", registry.NullVersionID); err != nil {
		t.Fatalf("DeleteBlobClaim: %v", err)
	}
	if n := count(t, m, space, digest); n != 0 {
		t.Fatalf("count after last delete = %d, want 0", n)
	}

	// Idempotent delete of an already-absent claim is not an error.
	if err := m.DeleteBlobClaim(ctx, digest, "b", "k1", registry.NullVersionID); err != nil {
		t.Fatalf("idempotent DeleteBlobClaim: %v", err)
	}
}

func TestMultipartLatch_SingleWinner_Sequential(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	const id = "upl-1"

	if err := m.CreateSession(ctx, registry.MultipartSession{UploadID: id, Bucket: "b", ObjectKey: "k"}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	// A second create for the same id collides.
	if err := m.CreateSession(ctx, registry.MultipartSession{UploadID: id, Bucket: "b", ObjectKey: "k"}); err != registry.ErrExists {
		t.Fatalf("duplicate CreateSession err = %v, want ErrExists", err)
	}

	// Complete wins the latch; a racing Abort observes the moved row and loses.
	won, err := m.LatchSession(ctx, id, registry.SessionOpen, registry.SessionCompleting)
	if err != nil || !won {
		t.Fatalf("Complete latch: won=%v err=%v, want won=true", won, err)
	}
	won, err = m.LatchSession(ctx, id, registry.SessionOpen, registry.SessionAborting)
	if err != nil || won {
		t.Fatalf("Abort latch after Complete: won=%v err=%v, want won=false", won, err)
	}

	s, err := m.GetSession(ctx, id)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if s.State != registry.SessionCompleting {
		t.Fatalf("session state = %q, want %q", s.State, registry.SessionCompleting)
	}

	// Latching a missing session is a clean loss, not an error.
	won, err = m.LatchSession(ctx, "nope", registry.SessionOpen, registry.SessionCompleting)
	if err != nil || won {
		t.Fatalf("latch missing: won=%v err=%v, want won=false err=nil", won, err)
	}
}

func TestMultipartLatch_SingleWinner_Concurrent(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	const id = "upl-race"
	if err := m.CreateSession(ctx, registry.MultipartSession{UploadID: id, Bucket: "b", ObjectKey: "k"}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	const racers = 32
	var winners int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			won, err := m.LatchSession(ctx, id, registry.SessionOpen, registry.SessionCompleting)
			if err != nil {
				t.Errorf("LatchSession: %v", err)
			}
			if won {
				atomic.AddInt64(&winners, 1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if winners != 1 {
		t.Fatalf("latch winners = %d, want exactly 1", winners)
	}
}

func TestIntents_StateMachine(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	digest := []byte("intent-digest")

	if err := m.PutIntent(ctx, registry.UploadIntent{
		Digest: digest, LocalPath: "/spool/x", Size: 42, State: registry.IntentSpooled, Bucket: "b",
	}); err != nil {
		t.Fatalf("PutIntent: %v", err)
	}

	got, err := m.GetIntent(ctx, digest)
	if err != nil {
		t.Fatalf("GetIntent: %v", err)
	}
	if got.State != registry.IntentSpooled || got.Size != 42 || got.LocalPath != "/spool/x" {
		t.Fatalf("GetIntent = %+v", got)
	}

	if err := m.SetIntentState(ctx, digest, registry.IntentParked); err != nil {
		t.Fatalf("SetIntentState: %v", err)
	}
	parked, err := m.ListIntentsByState(ctx, registry.IntentParked)
	if err != nil {
		t.Fatalf("ListIntentsByState: %v", err)
	}
	if len(parked) != 1 || string(parked[0].Digest) != string(digest) {
		t.Fatalf("parked intents = %+v, want one with our digest", parked)
	}
	if spooled, _ := m.ListIntentsByState(ctx, registry.IntentSpooled); len(spooled) != 0 {
		t.Fatalf("spooled intents = %+v, want none after state change", spooled)
	}

	// SetIntentState on a missing digest is an explicit miss.
	if err := m.SetIntentState(ctx, []byte("nope"), registry.IntentParked); err != registry.ErrNotFound {
		t.Fatalf("SetIntentState missing err = %v, want ErrNotFound", err)
	}

	if err := m.DeleteIntent(ctx, digest); err != nil {
		t.Fatalf("DeleteIntent: %v", err)
	}
	if _, err := m.GetIntent(ctx, digest); err != registry.ErrNotFound {
		t.Fatalf("GetIntent after delete err = %v, want ErrNotFound", err)
	}
}

func TestParts_OrderedAndCascade(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	const id = "upl-parts"
	if err := m.CreateSession(ctx, registry.MultipartSession{UploadID: id, Bucket: "b", ObjectKey: "k"}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	// Insert out of order; ListParts must return ascending part numbers.
	mustPutPart(t, m, registry.MultipartPart{UploadID: id, PartNumber: 2, ETagMD5: []byte("m2"), Size: 2, BlobDigests: [][]byte{[]byte("d2")}})
	mustPutPart(t, m, registry.MultipartPart{UploadID: id, PartNumber: 1, ETagMD5: []byte("m1"), Size: 1, BlobDigests: [][]byte{[]byte("d1a"), []byte("d1b")}})

	parts, err := m.ListParts(ctx, id)
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(parts) != 2 || parts[0].PartNumber != 1 || parts[1].PartNumber != 2 {
		t.Fatalf("parts order = %+v, want [1,2]", parts)
	}
	if len(parts[0].BlobDigests) != 2 {
		t.Fatalf("part 1 blob digests = %d, want 2", len(parts[0].BlobDigests))
	}

	// Returned data is a copy: mutating it must not corrupt the store.
	parts[0].BlobDigests[0][0] = 'X'
	again, _ := m.ListParts(ctx, id)
	if string(again[0].BlobDigests[0]) != "d1a" {
		t.Fatalf("store mutated through returned slice: %q", again[0].BlobDigests[0])
	}

	// PutPart against a missing session is rejected (FK).
	if err := m.PutPart(ctx, registry.MultipartPart{UploadID: "missing", PartNumber: 1, ETagMD5: []byte("m"), BlobDigests: [][]byte{[]byte("d")}}); err != registry.ErrNotFound {
		t.Fatalf("PutPart missing session err = %v, want ErrNotFound", err)
	}

	// Deleting the session cascades to its parts.
	if err := m.DeleteSession(ctx, id); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if after, _ := m.ListParts(ctx, id); len(after) != 0 {
		t.Fatalf("parts after session delete = %+v, want none (cascade)", after)
	}
}

func TestLocations_RoundTrip(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	const space = "did:space:loc"
	digest := []byte("loc-digest")

	if err := m.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: digest, Provider: "did:piri:1", URL: "http://piri/blob", Size: 100}); err != nil {
		t.Fatalf("PutLocation: %v", err)
	}
	loc, err := m.GetLocation(ctx, space, digest)
	if err != nil {
		t.Fatalf("GetLocation: %v", err)
	}
	if loc.URL != "http://piri/blob" || loc.Size != 100 || loc.Provider != "did:piri:1" {
		t.Fatalf("GetLocation = %+v", loc)
	}
	// An unencrypted blob carries no FEE wrap material.
	if loc.RegionWrappedCEK != nil || loc.BaseNonce != nil || loc.ProtectedHeader != nil ||
		loc.RegionKeyVersion != "" || loc.TenantRecipientKID != "" || loc.ChunkSize != 0 {
		t.Fatalf("unencrypted location carries wrap material: %+v", loc)
	}
	if _, err := m.GetLocation(ctx, "did:other", digest); err != registry.ErrNotFound {
		t.Fatalf("GetLocation wrong space err = %v, want ErrNotFound", err)
	}
	if err := m.DeleteLocation(ctx, space, digest); err != nil {
		t.Fatalf("DeleteLocation: %v", err)
	}
	if _, err := m.GetLocation(ctx, space, digest); err != registry.ErrNotFound {
		t.Fatalf("GetLocation after delete err = %v, want ErrNotFound", err)
	}
}

// TestLocations_FEEWrapMaterial round-trips the FEE wrap columns and verifies
// that (a) the stored copy does not alias the caller's byte slices, (b) a
// returned copy does not either, and (c) a re-wrap in place (a PutLocation with
// a new RegionWrappedCEK/RegionKeyVersion) updates only the wrap material.
func TestLocations_FEEWrapMaterial(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	const space = "did:space:enc"
	digest := []byte("enc-digest")

	enc := registry.BlobLocation{
		Space: space, Digest: digest, Provider: "did:piri:1", URL: "http://piri/enc", Size: 4096,
		RegionWrappedCEK:   []byte("wrapped-cek"),
		RegionKeyVersion:   "region-v1",
		TenantRecipientKID: "did:key:tenant#wrap",
		BaseNonce:          []byte("nonce07"),
		ChunkSize:          65536,
		ProtectedHeader:    []byte("cose-protected"),
	}
	if err := m.PutLocation(ctx, enc); err != nil {
		t.Fatalf("PutLocation: %v", err)
	}

	// Mutating the caller's slices after Put must not corrupt the store.
	enc.RegionWrappedCEK[0] = 'X'
	enc.BaseNonce[0] = 'X'
	enc.ProtectedHeader[0] = 'X'

	got, err := m.GetLocation(ctx, space, digest)
	if err != nil {
		t.Fatalf("GetLocation: %v", err)
	}
	if string(got.RegionWrappedCEK) != "wrapped-cek" || string(got.BaseNonce) != "nonce07" ||
		string(got.ProtectedHeader) != "cose-protected" {
		t.Fatalf("store aliased caller slices: %+v", got)
	}
	if got.RegionKeyVersion != "region-v1" || got.TenantRecipientKID != "did:key:tenant#wrap" || got.ChunkSize != 65536 {
		t.Fatalf("GetLocation wrap scalars = %+v", got)
	}

	// Mutating a returned copy must not corrupt the store either.
	got.RegionWrappedCEK[0] = 'Y'
	again, _ := m.GetLocation(ctx, space, digest)
	if string(again.RegionWrappedCEK) != "wrapped-cek" {
		t.Fatalf("store mutated through returned slice: %q", again.RegionWrappedCEK)
	}

	// Re-wrap in place: new CEK + key version, same location, other fields kept.
	rewrapped := *again
	rewrapped.RegionWrappedCEK = []byte("wrapped-cek-v2")
	rewrapped.RegionKeyVersion = "region-v2"
	if err := m.PutLocation(ctx, rewrapped); err != nil {
		t.Fatalf("PutLocation (re-wrap): %v", err)
	}
	after, err := m.GetLocation(ctx, space, digest)
	if err != nil {
		t.Fatalf("GetLocation after re-wrap: %v", err)
	}
	if string(after.RegionWrappedCEK) != "wrapped-cek-v2" || after.RegionKeyVersion != "region-v2" {
		t.Fatalf("re-wrap did not take: %+v", after)
	}
	if after.URL != "http://piri/enc" || string(after.BaseNonce) != "nonce07" || after.ChunkSize != 65536 {
		t.Fatalf("re-wrap disturbed non-wrap fields: %+v", after)
	}
}

// TestLocations_PartialFEE_Rejected verifies the all-or-nothing FEE invariant:
// a location with some but not all wrap material is rejected with ErrPartialFEE
// and never persisted, while a fully-absent (unencrypted) row is accepted.
func TestLocations_PartialFEE_Rejected(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	base := registry.BlobLocation{Space: "s", Digest: []byte("d"), Provider: "did:piri", URL: "u", Size: 10}

	partials := map[string]func(*registry.BlobLocation){
		"only wrapped CEK":  func(l *registry.BlobLocation) { l.RegionWrappedCEK = []byte("cek") },
		"only key version":  func(l *registry.BlobLocation) { l.RegionKeyVersion = "v1" },
		"only chunk size":   func(l *registry.BlobLocation) { l.ChunkSize = 4096 },
		"missing protected": func(l *registry.BlobLocation) {
			l.RegionWrappedCEK, l.RegionKeyVersion, l.TenantRecipientKID = []byte("cek"), "v1", "kid"
			l.BaseNonce, l.ChunkSize = []byte("nonce07"), 4096 // ProtectedHeader left empty
		},
	}
	for name, mutate := range partials {
		loc := base
		mutate(&loc)
		if err := m.PutLocation(ctx, loc); !errors.Is(err, registry.ErrPartialFEE) {
			t.Fatalf("PutLocation(%s) err = %v, want ErrPartialFEE", name, err)
		}
		// Nothing should have been stored.
		if _, err := m.GetLocation(ctx, "s", []byte("d")); !errors.Is(err, registry.ErrNotFound) {
			t.Fatalf("partial FEE (%s) leaked a row", name)
		}
	}

	// Fully absent (unencrypted) is accepted.
	if err := m.PutLocation(ctx, base); err != nil {
		t.Fatalf("PutLocation(unencrypted): %v", err)
	}
}

// helpers

func mustAdd(t *testing.T, m *MemStore, c registry.BlobClaim) {
	t.Helper()
	if err := m.AddBlobClaim(context.Background(), c); err != nil {
		t.Fatalf("AddBlobClaim: %v", err)
	}
}

func count(t *testing.T, m *MemStore, space string, digest []byte) int {
	t.Helper()
	n, err := m.CountClaims(context.Background(), space, digest)
	if err != nil {
		t.Fatalf("CountClaims: %v", err)
	}
	return n
}

func mustPutPart(t *testing.T, m *MemStore, p registry.MultipartPart) {
	t.Helper()
	if err := m.PutPart(context.Background(), p); err != nil {
		t.Fatalf("PutPart: %v", err)
	}
}
