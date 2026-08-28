package ingot

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/fil-forge/libforge/testutil"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/ingot/regionkey"
)

// regionKeyCfg wraps a RegionKeyConfig into the minimal Config the provider
// constructor reads.
func regionKeyCfg(rk config.RegionKeyConfig) config.Config {
	return config.Config{RegionKey: rk}
}

// TestProvideRegionKeyProvider_InProcess: the "inprocess" provider constructs
// from a configured KEK and round-trips a wrap, proving the config path feeds
// regionkey correctly end to end.
func TestProvideRegionKeyProvider_InProcess(t *testing.T) {
	kek := make([]byte, regionkey.KEKLen)
	kek[0] = 0xAB
	p, err := provideRegionKeyProvider(regionKeyCfg(config.RegionKeyConfig{
		Provider:  "inprocess",
		InProcess: config.InProcessConfig{KEK: base64.StdEncoding.EncodeToString(kek), Version: "v7"},
	}), zap.NewNop())
	if err != nil {
		t.Fatalf("provideRegionKeyProvider: %v", err)
	}

	digest, err := multihash.Sum([]byte("blob-1"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	binding := regionkey.BindingContext{Space: testutil.RandomDID(t), Digest: digest}
	cek := []byte("0123456789abcdef0123456789abcdef")

	wrapped, err := p.Wrap(context.Background(), binding, cek)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	if wrapped.Version != "v7" {
		t.Fatalf("Version = %q, want the configured v7", wrapped.Version)
	}
	got, err := p.Unwrap(context.Background(), binding, wrapped)
	if err != nil {
		t.Fatalf("Unwrap: %v", err)
	}
	if string(got) != string(cek) {
		t.Fatalf("round-trip mismatch")
	}
}

// An empty KEK generates one at startup (development): the provider works,
// the wraps just die with the process.
func TestProvideRegionKeyProvider_InProcessGeneratedKEK(t *testing.T) {
	p, err := provideRegionKeyProvider(regionKeyCfg(config.RegionKeyConfig{Provider: "inprocess"}), zap.NewNop())
	if err != nil {
		t.Fatalf("provideRegionKeyProvider: %v", err)
	}
	wrapped, err := p.Wrap(context.Background(), regionkey.BindingContext{}, []byte("a 32-byte content encryption k."))
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	if wrapped.Version != "v1" {
		t.Fatalf("Version = %q, want the default v1", wrapped.Version)
	}
}

// TestProvideRegionKeyProvider_OpenBao: with a key configured the provider
// constructs (no connection is made until the first wrap); without one it
// fails.
func TestProvideRegionKeyProvider_OpenBao(t *testing.T) {
	if _, err := provideRegionKeyProvider(regionKeyCfg(config.RegionKeyConfig{
		Provider: "openbao",
		OpenBao:  config.OpenBaoConfig{Address: "http://127.0.0.1:8200", Key: "region-kek"},
	}), zap.NewNop()); err != nil {
		t.Fatalf("provideRegionKeyProvider: %v", err)
	}
	if _, err := provideRegionKeyProvider(regionKeyCfg(config.RegionKeyConfig{Provider: "openbao"}), zap.NewNop()); err == nil {
		t.Fatal("expected an error with no transit key configured")
	}
}

func TestProvideRegionKeyProvider_Selection(t *testing.T) {
	if _, err := provideRegionKeyProvider(regionKeyCfg(config.RegionKeyConfig{}), zap.NewNop()); err == nil {
		t.Fatal("expected an error for an unconfigured provider")
	}
	if _, err := provideRegionKeyProvider(regionKeyCfg(config.RegionKeyConfig{Provider: "hsm"}), zap.NewNop()); err == nil {
		t.Fatal("expected an error for an unknown provider")
	}
	if _, err := provideRegionKeyProvider(regionKeyCfg(config.RegionKeyConfig{
		Provider:  "inprocess",
		InProcess: config.InProcessConfig{KEK: "not-base64!!"},
	}), zap.NewNop()); err == nil {
		t.Fatal("expected an error for a malformed KEK")
	}
}
