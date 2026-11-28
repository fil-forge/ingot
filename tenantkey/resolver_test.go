package tenantkey

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/internal/reqscope"
)

var testTenant = did.MustParse("did:plc:ewvi7nxzyoun6zhxrhs64oiz")

// wrapDoc builds a did:plc-shaped document for testTenant whose "#wrap"
// method is mutated by edit.
func wrapDoc(t *testing.T, edit func(vm *did.VerificationMethod)) (did.Document, *ecdh.PrivateKey) {
	t.Helper()
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	require.NoError(t, err)
	doc := did.NewDocument(testTenant)
	hilt := did.VerificationMethod{
		ID: doc.Fragment("hilt"), Controller: testTenant,
		Type:     did.MultikeyVerificationMethodType,
		Material: did.GenericMap{did.MultikeyPublicKeyMultibaseProp: "zQ3shokFTS3brHcDQrn82RUDfCZESWL1ZdCEJwekUDPQiYBme"},
	}
	wrap := did.VerificationMethod{
		ID: doc.Fragment(WrapFragment), Controller: testTenant,
		Type:     did.MultikeyVerificationMethodType,
		Material: did.GenericMap{did.MultikeyPublicKeyMultibaseProp: EncodePublicKey(priv.PublicKey())},
	}
	if edit != nil {
		edit(&wrap)
	}
	for _, vm := range []did.VerificationMethod{hilt, wrap} {
		require.NoError(t, doc.VerificationMethods.Add(vm))
	}
	return doc, priv
}

func fixedDocs(doc did.Document) did.Resolver {
	return did.ResolverFunc(func(_ context.Context, d did.DID) (did.Document, error) {
		if d != testTenant {
			return did.Document{}, errors.New("unknown DID")
		}
		return doc, nil
	})
}

func TestResolver_WrapKey(t *testing.T) {
	doc, priv := wrapDoc(t, nil)
	kid, pub, err := NewResolver(fixedDocs(doc)).WrapKey(context.Background(), testTenant)
	require.NoError(t, err, "WrapKey")
	require.True(t, pub.Equal(priv.PublicKey()), "resolved key differs from the published one")
	want := EncodePublicKey(priv.PublicKey())
	require.Equal(t, want, kid, "kid")
}

func TestResolver_RelativeMethodID(t *testing.T) {
	doc, priv := wrapDoc(t, func(vm *did.VerificationMethod) {
		u, err := did.ParseURL("#" + WrapFragment)
		require.NoError(t, err)
		vm.ID = u
	})
	_, pub, err := NewResolver(fixedDocs(doc)).WrapKey(context.Background(), testTenant)
	require.NoError(t, err, "WrapKey with relative id")
	require.True(t, pub.Equal(priv.PublicKey()), "resolved key differs from the published one")
}

func TestResolver_Failures(t *testing.T) {
	past := did.DateTimeStamp(time.Now().Add(-time.Hour))
	cases := []struct {
		name string
		edit func(vm *did.VerificationMethod)
		want error
	}{
		{"revoked", func(vm *did.VerificationMethod) { vm.Revoked = &past }, ErrNoWrapKey},
		{"expired", func(vm *did.VerificationMethod) { vm.Expires = &past }, ErrNoWrapKey},
		{"wrong type", func(vm *did.VerificationMethod) { vm.Type = did.JsonWebKeyVerificationMethodType }, ErrNotX25519},
		{"no material", func(vm *did.VerificationMethod) { vm.Material = did.GenericMap{} }, ErrNotX25519},
		{"ed25519 key", func(vm *did.VerificationMethod) {
			vm.Material[did.MultikeyPublicKeyMultibaseProp] = "z6MkjFRxLLGdBqQSLkZbVnuwUFiomK8eGBkPtim9ETvP7vec"
		}, ErrNotX25519},
		{"renamed fragment", func(vm *did.VerificationMethod) { vm.ID = did.NewDocument(testTenant).Fragment("wrap-1") }, ErrNoWrapKey},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc, _ := wrapDoc(t, tc.edit)
			_, _, err := NewResolver(fixedDocs(doc)).WrapKey(context.Background(), testTenant)
			require.ErrorIs(t, err, tc.want, "err")
		})
	}

	t.Run("resolver error", func(t *testing.T) {
		boom := errors.New("directory down")
		docs := did.ResolverFunc(func(context.Context, did.DID) (did.Document, error) { return did.Document{}, boom })
		_, _, err := NewResolver(docs).WrapKey(context.Background(), testTenant)
		require.ErrorIs(t, err, boom, "err")
	})
}

func TestRequestSource(t *testing.T) {
	doc, priv := wrapDoc(t, nil)
	src := NewRequestSource(NewResolver(fixedDocs(doc)))

	_, _, err := src.WrapKey(context.Background())
	require.ErrorIs(t, err, ErrNoTenant, "no tenant on ctx")
	ctx := context.WithValue(context.Background(), reqscope.TenantKey(), testTenant)
	kid, pub, err := src.WrapKey(ctx)
	require.NoError(t, err, "WrapKey")
	require.True(t, pub.Equal(priv.PublicKey()), "unexpected key or kid")
	require.Equal(t, EncodePublicKey(pub), kid, "unexpected key or kid")
}

func TestStatic(t *testing.T) {
	priv, _ := ecdh.X25519().GenerateKey(rand.Reader)
	s := NewStatic(priv.PublicKey())
	kid, pub, err := s.WrapKey(context.Background())
	require.NoError(t, err, "Static.WrapKey = (%s, %v, %v)", kid, pub, err)
	require.True(t, pub.Equal(priv.PublicKey()), "Static.WrapKey = (%s, %v, %v)", kid, pub, err)
	require.Equal(t, EncodePublicKey(pub), kid, "Static.WrapKey = (%s, %v, %v)", kid, pub, err)
}
