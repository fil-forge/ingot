package regionkey_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/fil-forge/ingot/fee/aeskw"
	"github.com/fil-forge/ingot/regionkey"
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

// newProvider builds a SoftwareProvider over a single-key StaticKEKSource with
// the given version and KEK, registering Close for cleanup.
func newProvider(t *testing.T, version regionkey.KeyVersion, kek []byte) *regionkey.SoftwareProvider {
	t.Helper()
	src, err := regionkey.NewStaticKEKSource(version, kek)
	require.NoError(t, err, "NewStaticKEKSource")
	t.Cleanup(func() { _ = src.Close() })
	return regionkey.NewSoftwareProvider(src)
}

// AC: "I can wrap a CEK through the key-provider interface (with a scope
// parameter) and unwrap it back to the original CEK."
func TestWrapUnwrapRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, "region#1", randKey(t, regionkey.KEKLen))
	scope := regionkey.Scope{Tenant: "did:web:example.com:tenant:acme", Bucket: "photos"}
	cek := randKey(t, 32)

	wrapped, err := p.Wrap(ctx, scope, cek)
	require.NoError(t, err, "Wrap")
	require.Len(t, wrapped.Ciphertext, len(cek)+8, "A256KW output is 8 bytes longer than the CEK")
	require.NotEqual(t, cek, wrapped.Ciphertext, "wrapped bytes must not equal the plaintext CEK")

	got, err := p.Unwrap(ctx, scope, wrapped)
	require.NoError(t, err, "Unwrap")
	require.Equal(t, cek, got, "round-trip must recover the original CEK")
}

// AC: "When I attempt to unwrap with a provider holding a different KEK for that
// scope, unwrap returns an error." Both providers advertise the same version,
// so the version lookup succeeds and the A256KW integrity check is what rejects
// the wrong key.
func TestUnwrapWithDifferentKEK(t *testing.T) {
	ctx := context.Background()
	scope := regionkey.Scope{}
	cek := randKey(t, 32)

	wrapper := newProvider(t, "region#1", randKey(t, regionkey.KEKLen))
	otherKEK := newProvider(t, "region#1", randKey(t, regionkey.KEKLen))

	wrapped, err := wrapper.Wrap(ctx, scope, cek)
	require.NoError(t, err, "Wrap")

	got, err := otherKEK.Unwrap(ctx, scope, wrapped)
	require.Error(t, err, "unwrap with a different KEK must fail")
	require.ErrorIs(t, err, aeskw.ErrIntegrity, "wrong same-version KEK surfaces as an integrity failure")
	require.Nil(t, got)
}

// AC: "When I attempt to unwrap ... a different KEK ..." — the sibling case
// where the second provider does not even hold the wrap's version (e.g. data
// wrapped by a different region). This is reported as ErrUnknownVersion, which
// is distinct from the integrity failure above.
func TestUnwrapUnknownVersion(t *testing.T) {
	ctx := context.Background()
	scope := regionkey.Scope{}
	cek := randKey(t, 32)

	wrapper := newProvider(t, "region#1", randKey(t, regionkey.KEKLen))
	newRegion := newProvider(t, "region#2", randKey(t, regionkey.KEKLen))

	wrapped, err := wrapper.Wrap(ctx, scope, cek)
	require.NoError(t, err, "Wrap")

	_, err = newRegion.Unwrap(ctx, scope, wrapped)
	require.ErrorIs(t, err, regionkey.ErrUnknownVersion, "a version the source does not hold must report ErrUnknownVersion")
}

// AC: "I can read the key version stored alongside the wrapped bytes and confirm
// it matches the KEK version used to wrap."
func TestWrappedKeyRecordsVersion(t *testing.T) {
	ctx := context.Background()
	const version regionkey.KeyVersion = "region#7"
	p := newProvider(t, version, randKey(t, regionkey.KEKLen))

	wrapped, err := p.Wrap(ctx, regionkey.Scope{}, randKey(t, 32))
	require.NoError(t, err, "Wrap")
	require.Equal(t, version, wrapped.Version, "the wrap must record the KEK version that produced it")
}

// Rotation-readiness: an archived (non-current) key version still unwraps, and
// the current version is what new wraps record. This exercises the versioning
// groundwork the RFC requires from day one, using a multi-version source.
func TestVersionSelectionAcrossRotation(t *testing.T) {
	ctx := context.Background()
	scope := regionkey.Scope{}
	cek := randKey(t, 32)

	oldKEK := randKey(t, regionkey.KEKLen)
	newKEK := randKey(t, regionkey.KEKLen)

	// Wrap under the old key, as an existing DB row would have been.
	oldProvider := newProvider(t, "region#1", oldKEK)
	wrappedOld, err := oldProvider.Wrap(ctx, scope, cek)
	require.NoError(t, err, "Wrap under old key")

	// After a rotation the source's current version is region#2 but it retains
	// region#1 (archive-don't-destroy).
	src := &mapKEKSource{
		current: "region#2",
		keys: map[regionkey.KeyVersion][]byte{
			"region#1": oldKEK,
			"region#2": newKEK,
		},
	}
	p := regionkey.NewSoftwareProvider(src)

	// New wraps use the current version.
	wrappedNew, err := p.Wrap(ctx, scope, cek)
	require.NoError(t, err, "Wrap after rotation")
	require.Equal(t, regionkey.KeyVersion("region#2"), wrappedNew.Version)

	// The archived version still unwraps existing data.
	gotOld, err := p.Unwrap(ctx, scope, wrappedOld)
	require.NoError(t, err, "Unwrap of archived-version data")
	require.Equal(t, cek, gotOld)

	// And so does the freshly wrapped data.
	gotNew, err := p.Unwrap(ctx, scope, wrappedNew)
	require.NoError(t, err, "Unwrap of current-version data")
	require.Equal(t, cek, gotNew)
}

// AC: "The raw KEK is held in process memory only for the duration of a single
// wrap/unwrap call, in a locked buffer, and is zeroed immediately afterward on
// both success and error paths." The inspectable source captures every KEK the
// provider imports; after each call the buffer must be zeroed (Destroy ran).
func TestKEKZeroedAfterOperation(t *testing.T) {
	ctx := context.Background()
	scope := regionkey.Scope{}

	t.Run("success path", func(t *testing.T) {
		inner, err := regionkey.NewStaticKEKSource("region#1", randKey(t, regionkey.KEKLen))
		require.NoError(t, err)
		t.Cleanup(func() { _ = inner.Close() })
		spy := &inspectingKEKSource{inner: inner}
		p := regionkey.NewSoftwareProvider(spy)

		wrapped, err := p.Wrap(ctx, scope, randKey(t, 32))
		require.NoError(t, err, "Wrap")
		_, err = p.Unwrap(ctx, scope, wrapped)
		require.NoError(t, err, "Unwrap")

		spy.assertAllZeroed(t)
	})

	t.Run("error path", func(t *testing.T) {
		inner, err := regionkey.NewStaticKEKSource("region#1", randKey(t, regionkey.KEKLen))
		require.NoError(t, err)
		t.Cleanup(func() { _ = inner.Close() })
		spy := &inspectingKEKSource{inner: inner}
		p := regionkey.NewSoftwareProvider(spy)

		// A 20-byte CEK is not block-aligned, so aeskw.Wrap fails after the KEK
		// has been imported — the defer must still zero it.
		_, err = p.Wrap(ctx, scope, make([]byte, 20))
		require.Error(t, err, "Wrap with an invalid CEK must fail")

		spy.assertAllZeroed(t)
	})
}

// AC: "I can swap in a different key-provider implementation ... without
// changing any wrap/unwrap call sites." roundTrip is written against the
// regionkey.Provider interface only; it drives both the real SoftwareProvider and
// an unrelated mock without change.
func TestProviderIsSwappable(t *testing.T) {
	ctx := context.Background()
	scope := regionkey.Scope{Bucket: "b"}
	cek := randKey(t, 32)

	roundTrip := func(t *testing.T, p regionkey.Provider) []byte {
		t.Helper()
		wrapped, err := p.Wrap(ctx, scope, cek)
		require.NoError(t, err, "Wrap")
		got, err := p.Unwrap(ctx, scope, wrapped)
		require.NoError(t, err, "Unwrap")
		return got
	}

	software := newProvider(t, "region#1", randKey(t, regionkey.KEKLen))
	require.Equal(t, cek, roundTrip(t, software), "software provider round-trips")

	// The same call site drives a completely different implementation.
	require.Equal(t, cek, roundTrip(t, echoProvider{}), "mock provider drives the same call site")
}

// AC: "The scope parameter is threaded through end to end even though v1 always
// resolves it to a single region-wide key." A wrap under one scope unwraps
// under a different scope, because the single-key source ignores the scope.
func TestScopeThreadedButResolvesToSingleKey(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, "region#1", randKey(t, regionkey.KEKLen))
	cek := randKey(t, 32)

	wrapScope := regionkey.Scope{Tenant: "did:web:example.com:tenant:a", Bucket: "alpha"}
	readScope := regionkey.Scope{Tenant: "did:web:example.com:tenant:z", Bucket: "omega"}

	wrapped, err := p.Wrap(ctx, wrapScope, cek)
	require.NoError(t, err, "Wrap under one scope")

	got, err := p.Unwrap(ctx, readScope, wrapped)
	require.NoError(t, err, "Unwrap under a different scope")
	require.Equal(t, cek, got, "v1 resolves every scope to the single region key")

	// Two different scopes both wrap under the same single key/version.
	w1, err := p.Wrap(ctx, wrapScope, cek)
	require.NoError(t, err)
	w2, err := p.Wrap(ctx, readScope, cek)
	require.NoError(t, err)
	require.Equal(t, w1.Version, w2.Version, "all scopes share the single region key version in v1")
}

func TestWrapRejectsEmptyVersionFromSource(t *testing.T) {
	ctx := context.Background()
	src := &mapKEKSource{current: "", keys: map[regionkey.KeyVersion][]byte{"": randKey(t, regionkey.KEKLen)}}
	p := regionkey.NewSoftwareProvider(src)

	_, err := p.Wrap(ctx, regionkey.Scope{}, randKey(t, 32))
	require.Error(t, err, "a source advertising an empty version must be rejected")
}

// A StaticKEKSource is nil-safe to Close and, once closed, refuses to hand out
// its (now-wiped) key rather than silently wrapping under zeroes.
func TestStaticKEKSourceCloseSafety(t *testing.T) {
	ctx := context.Background()

	// Zero-value source Closes without panicking.
	require.NotPanics(t, func() { _ = (&regionkey.StaticKEKSource{}).Close() })

	src, err := regionkey.NewStaticKEKSource("region#1", randKey(t, regionkey.KEKLen))
	require.NoError(t, err)

	require.NoError(t, src.Close())
	require.NotPanics(t, func() { _ = src.Close() }, "Close must be idempotent")

	_, _, err = src.CurrentKEK(ctx, regionkey.Scope{})
	require.Error(t, err, "using a closed source must error, not hand out a wiped key")
	_, err = src.KEKAt(ctx, regionkey.Scope{}, "region#1")
	require.Error(t, err, "using a closed source must error")
}

// A misbehaving KEKSource that returns a nil KEK with no error must surface an
// error, not nil-panic the provider's deferred Destroy.
func TestProviderRejectsNilKEKFromSource(t *testing.T) {
	ctx := context.Background()
	p := regionkey.NewSoftwareProvider(nilKEKSource{})

	require.NotPanics(t, func() {
		_, err := p.Wrap(ctx, regionkey.Scope{}, randKey(t, 32))
		require.Error(t, err, "Wrap must reject a nil KEK from the source")
	})
	require.NotPanics(t, func() {
		_, err := p.Unwrap(ctx, regionkey.Scope{}, regionkey.WrappedKey{Version: "region#1", Ciphertext: make([]byte, 40)})
		require.Error(t, err, "Unwrap must reject a nil KEK from the source")
	})
}

// --- test doubles ---

// mapKEKSource is a KEKSource holding multiple key versions in memory, used to
// exercise version selection and rotation. It builds locked KEKs via the
// exported regionkey.NewKEK, exactly as an out-of-package (e.g. Vault-backed)
// source would.
type mapKEKSource struct {
	current regionkey.KeyVersion
	keys    map[regionkey.KeyVersion][]byte
}

func (s *mapKEKSource) CurrentKEK(ctx context.Context, scope regionkey.Scope) (regionkey.KeyVersion, *regionkey.KEK, error) {
	k, err := s.KEKAt(ctx, scope, s.current)
	if err != nil {
		return "", nil, err
	}
	return s.current, k, nil
}

func (s *mapKEKSource) KEKAt(ctx context.Context, scope regionkey.Scope, version regionkey.KeyVersion) (*regionkey.KEK, error) {
	raw, ok := s.keys[version]
	if !ok {
		return nil, regionkey.ErrUnknownVersion
	}
	return regionkey.NewKEK(raw)
}

// inspectingKEKSource wraps another source and retains every KEK it hands out
// so a test can assert the provider zeroed them after use.
type inspectingKEKSource struct {
	inner  regionkey.KEKSource
	handed []*regionkey.KEK
}

func (s *inspectingKEKSource) CurrentKEK(ctx context.Context, scope regionkey.Scope) (regionkey.KeyVersion, *regionkey.KEK, error) {
	v, k, err := s.inner.CurrentKEK(ctx, scope)
	if k != nil {
		s.handed = append(s.handed, k)
	}
	return v, k, err
}

func (s *inspectingKEKSource) KEKAt(ctx context.Context, scope regionkey.Scope, version regionkey.KeyVersion) (*regionkey.KEK, error) {
	k, err := s.inner.KEKAt(ctx, scope, version)
	if k != nil {
		s.handed = append(s.handed, k)
	}
	return k, err
}

func (s *inspectingKEKSource) assertAllZeroed(t *testing.T) {
	t.Helper()
	require.NotEmpty(t, s.handed, "expected the provider to import at least one KEK")
	for i, k := range s.handed {
		require.Equalf(t, make([]byte, regionkey.KEKLen), k.Bytes(), "KEK %d must be zeroed after the operation", i)
	}
}

// echoProvider is a minimal alternative Provider implementation. It performs no
// real cryptography; it exists only to prove a call site written against
// regionkey.Provider drives any implementation unchanged.
type echoProvider struct{}

func (echoProvider) Wrap(ctx context.Context, scope regionkey.Scope, cek []byte) (regionkey.WrappedKey, error) {
	return regionkey.WrappedKey{Version: "echo", Ciphertext: bytes.Clone(cek)}, nil
}

func (echoProvider) Unwrap(ctx context.Context, scope regionkey.Scope, wrapped regionkey.WrappedKey) ([]byte, error) {
	return bytes.Clone(wrapped.Ciphertext), nil
}

// nilKEKSource is a deliberately misbehaving KEKSource: it reports no error but
// returns a nil *KEK, the contract violation the provider's nil guards defend
// against.
type nilKEKSource struct{}

func (nilKEKSource) CurrentKEK(ctx context.Context, scope regionkey.Scope) (regionkey.KeyVersion, *regionkey.KEK, error) {
	return "region#1", nil, nil
}

func (nilKEKSource) KEKAt(ctx context.Context, scope regionkey.Scope, version regionkey.KeyVersion) (*regionkey.KEK, error) {
	return nil, nil
}
