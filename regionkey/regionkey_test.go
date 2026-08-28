package regionkey_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/fil-forge/ingot/regionkey"
	"github.com/fil-forge/libforge/testutil"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// randKey returns n cryptographically random bytes.
func randKey(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err, "rand.Read")
	return b
}

// digestOf returns the sha256 multihash of data, the digest form Ingot keys
// blobs by.
func digestOf(t *testing.T, data []byte) multihash.Multihash {
	t.Helper()
	mh, err := multihash.Sum(data, multihash.SHA2_256, -1)
	require.NoError(t, err, "multihash.Sum")
	return mh
}

// testBinding returns a binding over a fresh space and the digest of blob.
func testBinding(t *testing.T, blob string) regionkey.BindingContext {
	t.Helper()
	return regionkey.BindingContext{Space: testutil.RandomDID(t), Digest: digestOf(t, []byte(blob))}
}

// newProvider builds an InProcessProvider holding a single key under the
// given version.
func newProvider(t *testing.T, version regionkey.KeyVersion, kek []byte) *regionkey.InProcessProvider {
	t.Helper()
	p, err := regionkey.NewInProcessProvider(version, kek)
	require.NoError(t, err, "NewInProcessProvider")
	return p
}

// AC: "I can wrap a CEK through the key-provider interface (with a binding
// parameter) and unwrap it back to the original CEK."
func TestWrapUnwrapRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, "region#1", randKey(t, regionkey.KEKLen))
	binding := testBinding(t, "blob-1")
	cek := randKey(t, 32)

	wrapped, err := p.Wrap(ctx, binding, cek)
	require.NoError(t, err, "Wrap")
	require.Len(t, wrapped.Ciphertext, len(cek)+28, "GCM output is nonce (12) + ciphertext + tag (16)")
	require.NotEqual(t, cek, wrapped.Ciphertext, "wrapped bytes must not equal the plaintext CEK")

	got, err := p.Unwrap(ctx, binding, wrapped)
	require.NoError(t, err, "Unwrap")
	require.Equal(t, cek, got, "round-trip must recover the original CEK")
}

// AC: "When I attempt to unwrap with a provider holding a different KEK for
// that binding, unwrap returns an error." Both providers advertise the same
// version, so the version lookup succeeds and GCM authentication is what
// rejects the wrong key.
func TestUnwrapWithDifferentKEK(t *testing.T) {
	ctx := context.Background()
	binding := testBinding(t, "blob-1")
	cek := randKey(t, 32)

	wrapper := newProvider(t, "region#1", randKey(t, regionkey.KEKLen))
	otherKEK := newProvider(t, "region#1", randKey(t, regionkey.KEKLen))

	wrapped, err := wrapper.Wrap(ctx, binding, cek)
	require.NoError(t, err, "Wrap")

	got, err := otherKEK.Unwrap(ctx, binding, wrapped)
	require.Error(t, err, "unwrap with a different KEK must fail")
	require.ErrorIs(t, err, regionkey.ErrAuthentication, "wrong same-version KEK surfaces as an authentication failure")
	require.Nil(t, got)
}

// The RFC's transplant property: the wrap is context-bound to (space, blob
// digest), so wrap material moved to a different blob's row — same KEK, same
// version — fails authentication rather than yielding the CEK.
func TestUnwrapWrongBindingFails(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, "region#1", randKey(t, regionkey.KEKLen))
	cek := randKey(t, 32)

	binding := testBinding(t, "blob-1")
	wrapped, err := p.Wrap(ctx, binding, cek)
	require.NoError(t, err, "Wrap")

	otherDigest := binding
	otherDigest.Digest = digestOf(t, []byte("blob-2"))
	_, err = p.Unwrap(ctx, otherDigest, wrapped)
	require.ErrorIs(t, err, regionkey.ErrAuthentication, "a different digest must fail authentication")

	otherSpace := binding
	otherSpace.Space = testutil.RandomDID(t)
	_, err = p.Unwrap(ctx, otherSpace, wrapped)
	require.ErrorIs(t, err, regionkey.ErrAuthentication, "a different space must fail authentication")
}

// Tampered wrap material fails authentication.
func TestUnwrapTamperedCiphertextFails(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, "region#1", randKey(t, regionkey.KEKLen))
	binding := testBinding(t, "blob-1")

	wrapped, err := p.Wrap(ctx, binding, randKey(t, 32))
	require.NoError(t, err, "Wrap")
	wrapped.Ciphertext[len(wrapped.Ciphertext)-1] ^= 0x01

	_, err = p.Unwrap(ctx, binding, wrapped)
	require.ErrorIs(t, err, regionkey.ErrAuthentication)

	// A wrap too short to even carry a nonce is reported the same way.
	_, err = p.Unwrap(ctx, binding, regionkey.WrappedKey{Version: "region#1", Ciphertext: []byte{0x01}})
	require.ErrorIs(t, err, regionkey.ErrAuthentication)
}

// AC: "When I attempt to unwrap ... a different KEK ..." — the sibling case
// where the second provider does not even hold the wrap's version (e.g. data
// wrapped by a different region). This is reported as ErrUnknownVersion,
// which is distinct from the authentication failure above.
func TestUnwrapUnknownVersion(t *testing.T) {
	ctx := context.Background()
	binding := testBinding(t, "blob-1")
	cek := randKey(t, 32)

	wrapper := newProvider(t, "region#1", randKey(t, regionkey.KEKLen))
	newRegion := newProvider(t, "region#2", randKey(t, regionkey.KEKLen))

	wrapped, err := wrapper.Wrap(ctx, binding, cek)
	require.NoError(t, err, "Wrap")

	_, err = newRegion.Unwrap(ctx, binding, wrapped)
	require.ErrorIs(t, err, regionkey.ErrUnknownVersion, "a version the source does not hold must report ErrUnknownVersion")
}

// AC: "I can read the key version stored alongside the wrapped bytes and
// confirm it matches the KEK version used to wrap."
func TestWrappedKeyRecordsVersion(t *testing.T) {
	ctx := context.Background()
	const version regionkey.KeyVersion = "region#7"
	p := newProvider(t, version, randKey(t, regionkey.KEKLen))

	wrapped, err := p.Wrap(ctx, testBinding(t, "blob-1"), randKey(t, 32))
	require.NoError(t, err, "Wrap")
	require.Equal(t, version, wrapped.Version, "the wrap must record the KEK version that produced it")
}

// Rotation-readiness: an archived (non-current) key version still unwraps,
// and the current version is what new wraps record. This exercises the
// versioning groundwork the RFC requires from day one.
func TestVersionSelectionAcrossRotation(t *testing.T) {
	ctx := context.Background()
	binding := testBinding(t, "blob-1")
	cek := randKey(t, 32)

	// Wrap under the original key, as an existing DB row would have been.
	p := newProvider(t, "region#1", randKey(t, regionkey.KEKLen))
	wrappedOld, err := p.Wrap(ctx, binding, cek)
	require.NoError(t, err, "Wrap under old key")

	// Rotate: region#2 becomes current, region#1 is archived, not destroyed.
	require.NoError(t, p.Rotate("region#2", randKey(t, regionkey.KEKLen)), "Rotate")

	// New wraps use the current version.
	wrappedNew, err := p.Wrap(ctx, binding, cek)
	require.NoError(t, err, "Wrap after rotation")
	require.Equal(t, regionkey.KeyVersion("region#2"), wrappedNew.Version)

	// The archived version still unwraps existing data.
	gotOld, err := p.Unwrap(ctx, binding, wrappedOld)
	require.NoError(t, err, "Unwrap of archived-version data")
	require.Equal(t, cek, gotOld)

	// And so does the freshly wrapped data.
	gotNew, err := p.Unwrap(ctx, binding, wrappedNew)
	require.NoError(t, err, "Unwrap of current-version data")
	require.Equal(t, cek, gotNew)
}

// AC: "I can swap in a different key-provider implementation ... without
// changing any wrap/unwrap call sites." roundTrip is written against the
// regionkey.Provider interface only; it drives both the real
// InProcessProvider and an unrelated mock without change.
func TestProviderIsSwappable(t *testing.T) {
	ctx := context.Background()
	binding := testBinding(t, "blob-1")
	cek := randKey(t, 32)

	roundTrip := func(t *testing.T, p regionkey.Provider) []byte {
		t.Helper()
		wrapped, err := p.Wrap(ctx, binding, cek)
		require.NoError(t, err, "Wrap")
		got, err := p.Unwrap(ctx, binding, wrapped)
		require.NoError(t, err, "Unwrap")
		return got
	}

	inProcess := newProvider(t, "region#1", randKey(t, regionkey.KEKLen))
	require.Equal(t, cek, roundTrip(t, inProcess), "in-process provider round-trips")

	// The same call site drives a completely different implementation.
	require.Equal(t, cek, roundTrip(t, echoProvider{}), "mock provider drives the same call site")
}

// v1 holds a single region-wide KEK: every binding wraps under the same key
// version, even though each wrap is bound to its own binding.
func TestBindingsShareSingleKeyVersion(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, "region#1", randKey(t, regionkey.KEKLen))
	cek := randKey(t, 32)

	bindingA := testBinding(t, "blob-a")
	bindingB := testBinding(t, "blob-b")

	w1, err := p.Wrap(ctx, bindingA, cek)
	require.NoError(t, err)
	w2, err := p.Wrap(ctx, bindingB, cek)
	require.NoError(t, err)
	require.Equal(t, w1.Version, w2.Version, "all bindings share the single region key version in v1")

	// Each wrap unwraps under its own binding only.
	got, err := p.Unwrap(ctx, bindingA, w1)
	require.NoError(t, err)
	require.Equal(t, cek, got)
	_, err = p.Unwrap(ctx, bindingA, w2)
	require.ErrorIs(t, err, regionkey.ErrAuthentication, "binding B's wrap must not unwrap under binding A")
}

// NewInProcessProvider and Rotate validate their inputs: a KEK of the wrong
// length, an empty version, or a duplicate version is rejected.
func TestNewInProcessProviderValidates(t *testing.T) {
	_, err := regionkey.NewInProcessProvider("", randKey(t, regionkey.KEKLen))
	require.Error(t, err, "empty version must be rejected")
	_, err = regionkey.NewInProcessProvider("region#1", randKey(t, regionkey.KEKLen-1))
	require.Error(t, err, "short KEK must be rejected")

	p, err := regionkey.NewInProcessProvider("region#1", randKey(t, regionkey.KEKLen))
	require.NoError(t, err)
	require.Error(t, p.Rotate("", randKey(t, regionkey.KEKLen)), "Rotate must reject an empty version")
	require.Error(t, p.Rotate("region#2", randKey(t, regionkey.KEKLen-1)), "Rotate must reject a short KEK")
	require.Error(t, p.Rotate("region#1", randKey(t, regionkey.KEKLen)), "Rotate must reject a duplicate version")
	require.NoError(t, p.Rotate("region#2", randKey(t, regionkey.KEKLen)))
}

// --- test doubles ---

// echoProvider is a minimal alternative Provider implementation. It performs
// no real cryptography; it exists only to prove a call site written against
// regionkey.Provider drives any implementation unchanged.
type echoProvider struct{}

func (echoProvider) Wrap(ctx context.Context, binding regionkey.BindingContext, cek []byte) (regionkey.WrappedKey, error) {
	return regionkey.WrappedKey{Version: "echo", Ciphertext: bytes.Clone(cek)}, nil
}

func (echoProvider) Unwrap(ctx context.Context, binding regionkey.BindingContext, wrapped regionkey.WrappedKey) ([]byte, error) {
	return bytes.Clone(wrapped.Ciphertext), nil
}
