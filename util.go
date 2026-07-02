package ingot

import (
	"fmt"
	"os"

	"github.com/fil-forge/ucantone/multikey"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
)

// LoadOrCreateSigner loads the ed25519 space signer persisted at path, or
// generates and persists a new one on first run. It returns a [ucan.Issuer] —
// the *space*, the did:key ingot is the root UCAN authority over — built by
// pairing the persisted key with its own key DID via [multikey.KeyIssuer].
func LoadOrCreateSigner(path string) (ucan.Issuer, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		s, err := ed25519.Generate()
		if err != nil {
			return nil, fmt.Errorf("ingot: generate signer: %w", err)
		}
		formatted := multikey.FormatSigner(s)
		if err := os.WriteFile(path, []byte(formatted), 0o600); err != nil {
			return nil, fmt.Errorf("ingot: persist signer: %w", err)
		}
		return multikey.KeyIssuer(s), nil
	}
	if err != nil {
		return nil, fmt.Errorf("ingot: read signer: %w", err)
	}
	s, err := ed25519.Parse(string(data))
	if err != nil {
		return nil, fmt.Errorf("ingot: parse signer: %w", err)
	}
	return multikey.KeyIssuer(s), nil
}
