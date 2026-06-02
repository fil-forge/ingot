package ingot

import (
	"fmt"
	"os"

	"github.com/fil-forge/ucantone/principal"
	"github.com/fil-forge/ucantone/principal/ed25519"
)

// LoadOrCreateSigner loads the ed25519 space signer persisted at path, or
// generates and persists a new one on first run. The signer is the *space* —
// the did:key ingot is the root UCAN authority over.
func LoadOrCreateSigner(path string) (principal.Signer, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		s, err := ed25519.Generate()
		if err != nil {
			return nil, fmt.Errorf("ingot: generate signer: %w", err)
		}
		formatted := ed25519.Format(s)
		if err := os.WriteFile(path, []byte(formatted), 0o600); err != nil {
			return nil, fmt.Errorf("ingot: persist signer: %w", err)
		}
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("ingot: read signer: %w", err)
	}
	s, err := ed25519.Parse(string(data))
	if err != nil {
		return nil, fmt.Errorf("ingot: parse signer: %w", err)
	}
	return s, nil
}
