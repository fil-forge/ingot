package inmem

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/did"
)

func TestBlobRefs_CountToZero(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	digest := []byte("digest-A")
	space := testutil.RandomDID(t)
	space2 := testutil.RandomDID(t)

	claim := func(bucket, key, version string, sp did.DID) registry.BlobClaim {
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
	mustAdd(t, m, claim("b2", "k1", registry.NullVersionID, space2))
	if n := count(t, m, space, digest); n != 2 {
		t.Fatalf("count for space 1 = %d, want 2 (space 2 must not leak in)", n)
	}
	if n := count(t, m, space2, digest); n != 1 {
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
	space := testutil.RandomDID(t)
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
	if _, err := m.GetLocation(ctx, testutil.RandomDID(t), digest); err != registry.ErrNotFound {
		t.Fatalf("GetLocation wrong space err = %v, want ErrNotFound", err)
	}
	if err := m.DeleteLocation(ctx, space, digest); err != nil {
		t.Fatalf("DeleteLocation: %v", err)
	}
	if _, err := m.GetLocation(ctx, space, digest); err != registry.ErrNotFound {
		t.Fatalf("GetLocation after delete err = %v, want ErrNotFound", err)
	}
}

// feeParams returns a complete parameter set for space/digest, the shape a FEE
// envelope's row takes.
func feeParams(space did.DID, digest []byte) registry.BlobEncryptionParams {
	return registry.BlobEncryptionParams{
		Space:            space,
		Digest:           digest,
		RegionWrappedCEK: []byte("wrapped-cek-bytes"),
		RegionKeyVersion: "region-kek-v1",
		HeaderLen:        212,
		BaseNonce:        []byte("nonce07"),
		ChunkSize:        65536,
		AAD:              []byte("cose-enc-structure"),
	}
}

func TestEncryptionParams_RoundTrip(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	space := testutil.RandomDID(t)
	digest := []byte("enc-digest")
	want := feeParams(space, digest)

	if err := m.PutEncryptionParams(ctx, want); err != nil {
		t.Fatalf("PutEncryptionParams: %v", err)
	}
	got, err := m.GetEncryptionParams(ctx, space, digest)
	if err != nil {
		t.Fatalf("GetEncryptionParams: %v", err)
	}
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("GetEncryptionParams = %+v, want %+v", *got, want)
	}
}

// A blob with no row is a plaintext blob, which is how the read path learns not
// to decrypt.
func TestEncryptionParams_MissingIsNotFound(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()

	_, err := m.GetEncryptionParams(ctx, testutil.RandomDID(t), []byte("absent"))
	if !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("GetEncryptionParams err = %v, want ErrNotFound", err)
	}
}

func TestEncryptionParams_DeleteRemovesRow(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	space := testutil.RandomDID(t)
	digest := []byte("enc-digest")
	if err := m.PutEncryptionParams(ctx, feeParams(space, digest)); err != nil {
		t.Fatalf("PutEncryptionParams: %v", err)
	}

	if err := m.DeleteEncryptionParams(ctx, space, digest); err != nil {
		t.Fatalf("DeleteEncryptionParams: %v", err)
	}
	if _, err := m.GetEncryptionParams(ctx, space, digest); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("GetEncryptionParams after delete err = %v, want ErrNotFound", err)
	}
}

func TestEncryptionParams_DeleteIsIdempotent(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()

	if err := m.DeleteEncryptionParams(ctx, testutil.RandomDID(t), []byte("absent")); err != nil {
		t.Fatalf("DeleteEncryptionParams(absent): %v", err)
	}
}

// The store must not alias the caller's byte slices, nor let a caller reach
// back into it through a returned copy.
func TestEncryptionParams_NoSliceAliasing(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	space := testutil.RandomDID(t)
	digest := []byte("enc-digest")
	params := feeParams(space, digest)

	if err := m.PutEncryptionParams(ctx, params); err != nil {
		t.Fatalf("PutEncryptionParams: %v", err)
	}
	params.RegionWrappedCEK[0] = 'X'
	params.BaseNonce[0] = 'X'
	params.AAD[0] = 'X'

	got, err := m.GetEncryptionParams(ctx, space, digest)
	if err != nil {
		t.Fatalf("GetEncryptionParams: %v", err)
	}
	got.RegionWrappedCEK[0] = 'Y'
	got.BaseNonce[0] = 'Y'

	again, err := m.GetEncryptionParams(ctx, space, digest)
	if err != nil {
		t.Fatalf("GetEncryptionParams (again): %v", err)
	}
	if !reflect.DeepEqual(*again, feeParams(space, digest)) {
		t.Fatalf("stored params were aliased: %+v", *again)
	}
}

// A rotation re-wrap replaces only the wrapped CEK and its key version; every
// other parameter describes the unchanged ciphertext and must survive.
func TestEncryptionParams_RewrapInPlace(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	space := testutil.RandomDID(t)
	digest := []byte("enc-digest")
	if err := m.PutEncryptionParams(ctx, feeParams(space, digest)); err != nil {
		t.Fatalf("PutEncryptionParams: %v", err)
	}

	newCEK := []byte("re-wrapped-cek-bytes")
	if err := m.RewrapEncryptionParams(ctx, space, digest, newCEK, "region-kek-v2"); err != nil {
		t.Fatalf("RewrapEncryptionParams: %v", err)
	}
	newCEK[0] = 'X' // the store must not alias the caller's slice

	got, err := m.GetEncryptionParams(ctx, space, digest)
	if err != nil {
		t.Fatalf("GetEncryptionParams: %v", err)
	}
	want := feeParams(space, digest)
	want.RegionWrappedCEK = []byte("re-wrapped-cek-bytes")
	want.RegionKeyVersion = "region-kek-v2"
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("after rewrap = %+v, want %+v", *got, want)
	}
}

func TestEncryptionParams_RewrapMissingIsNotFound(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()

	err := m.RewrapEncryptionParams(ctx, testutil.RandomDID(t), []byte("absent"), []byte("cek"), "v1")
	if !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("RewrapEncryptionParams err = %v, want ErrNotFound", err)
	}
}

// The two tables have independent lifecycles and no cascade between them:
// deleting a location leaves the encryption parameters in place, which is why a
// caller removing a blob must delete both.
func TestEncryptionParams_IndependentOfLocation(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	space := testutil.RandomDID(t)
	digest := []byte("enc-digest")
	if err := m.PutLocation(ctx, registry.BlobLocation{Space: space, Digest: digest, Provider: "did:piri:1", URL: "http://piri/enc", Size: 4096}); err != nil {
		t.Fatalf("PutLocation: %v", err)
	}
	if err := m.PutEncryptionParams(ctx, feeParams(space, digest)); err != nil {
		t.Fatalf("PutEncryptionParams: %v", err)
	}

	if err := m.DeleteLocation(ctx, space, digest); err != nil {
		t.Fatalf("DeleteLocation: %v", err)
	}

	if _, err := m.GetEncryptionParams(ctx, space, digest); err != nil {
		t.Fatalf("DeleteLocation shredded the encryption params: %v", err)
	}
}

func TestRevocationCursor_UpsertRoundTrip(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()

	if _, err := m.GetRevocationCursor(ctx); err != registry.ErrNotFound {
		t.Fatalf("GetRevocationCursor before put err = %v, want ErrNotFound", err)
	}
	first := registry.RevocationCursor{RecordedAt: time.Now().UTC(), Revoke: testCid(t, "revoked-1")}
	if err := m.PutRevocationCursor(ctx, first); err != nil {
		t.Fatalf("PutRevocationCursor: %v", err)
	}
	got, err := m.GetRevocationCursor(ctx)
	if err != nil {
		t.Fatalf("GetRevocationCursor: %v", err)
	}
	if !got.RecordedAt.Equal(first.RecordedAt) || !got.Revoke.Equals(first.Revoke) {
		t.Fatalf("cursor = %+v, want %+v", got, first)
	}
	// The cursor is a single latch: a second put overwrites, never adds.
	second := registry.RevocationCursor{RecordedAt: first.RecordedAt.Add(time.Hour), Revoke: testCid(t, "revoked-2")}
	if err := m.PutRevocationCursor(ctx, second); err != nil {
		t.Fatalf("PutRevocationCursor (upsert): %v", err)
	}
	got, err = m.GetRevocationCursor(ctx)
	if err != nil {
		t.Fatalf("GetRevocationCursor after upsert: %v", err)
	}
	if !got.RecordedAt.Equal(second.RecordedAt) || !got.Revoke.Equals(second.Revoke) {
		t.Fatalf("cursor after upsert = %+v, want %+v", got, second)
	}
}

// helpers

func mustAdd(t *testing.T, m *MemStore, c registry.BlobClaim) {
	t.Helper()
	if err := m.AddBlobClaim(context.Background(), c); err != nil {
		t.Fatalf("AddBlobClaim: %v", err)
	}
}

func count(t *testing.T, m *MemStore, space did.DID, digest []byte) int {
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
