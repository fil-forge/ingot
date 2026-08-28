package tenantkey

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/fil-forge/ucantone/did"

	"github.com/fil-forge/ingot/internal/reqscope"
)

var testTenant = did.MustParse("did:plc:ewvi7nxzyoun6zhxrhs64oiz")

// wrapDoc builds a did:plc-shaped document for testTenant whose "#wrap"
// method is mutated by edit.
func wrapDoc(t *testing.T, edit func(vm *did.VerificationMethod)) (did.Document, *ecdh.PrivateKey) {
	t.Helper()
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
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
		if err := doc.VerificationMethods.Add(vm); err != nil {
			t.Fatal(err)
		}
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
	if err != nil {
		t.Fatalf("WrapKey: %v", err)
	}
	if !pub.Equal(priv.PublicKey()) {
		t.Fatal("resolved key differs from the published one")
	}
	if want := EncodePublicKey(priv.PublicKey()); kid != want {
		t.Fatalf("kid = %s, want the fingerprint %s", kid, want)
	}
}

func TestResolver_RelativeMethodID(t *testing.T) {
	doc, priv := wrapDoc(t, func(vm *did.VerificationMethod) {
		u, err := did.ParseURL("#" + WrapFragment)
		if err != nil {
			t.Fatal(err)
		}
		vm.ID = u
	})
	_, pub, err := NewResolver(fixedDocs(doc)).WrapKey(context.Background(), testTenant)
	if err != nil {
		t.Fatalf("WrapKey with relative id: %v", err)
	}
	if !pub.Equal(priv.PublicKey()) {
		t.Fatal("resolved key differs from the published one")
	}
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
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}

	t.Run("resolver error", func(t *testing.T) {
		boom := errors.New("directory down")
		docs := did.ResolverFunc(func(context.Context, did.DID) (did.Document, error) { return did.Document{}, boom })
		if _, _, err := NewResolver(docs).WrapKey(context.Background(), testTenant); !errors.Is(err, boom) {
			t.Fatalf("err = %v, want the resolver's", err)
		}
	})
}

func TestRequestSource(t *testing.T) {
	doc, priv := wrapDoc(t, nil)
	src := NewRequestSource(NewResolver(fixedDocs(doc)))

	if _, _, err := src.WrapKey(context.Background()); !errors.Is(err, ErrNoTenant) {
		t.Fatalf("no tenant on ctx: err = %v, want ErrNoTenant", err)
	}
	ctx := context.WithValue(context.Background(), reqscope.TenantKey(), testTenant)
	kid, pub, err := src.WrapKey(ctx)
	if err != nil {
		t.Fatalf("WrapKey: %v", err)
	}
	if !pub.Equal(priv.PublicKey()) || kid != EncodePublicKey(pub) {
		t.Fatal("unexpected key or kid")
	}
}

func TestStatic(t *testing.T) {
	priv, _ := ecdh.X25519().GenerateKey(rand.Reader)
	s := NewStatic(priv.PublicKey())
	kid, pub, err := s.WrapKey(context.Background())
	if err != nil || !pub.Equal(priv.PublicKey()) || kid != EncodePublicKey(pub) {
		t.Fatalf("Static.WrapKey = (%s, %v, %v)", kid, pub, err)
	}
}
