package ingot

import (
	"bytes"
	"fmt"
	"os"

	"github.com/fil-forge/ucantone/multikey"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
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

// LoadProofsContainer resolves a proofs config value (e.g. Config.HiltProofs):
// if it names an existing file, the file's contents are decoded as a UCAN
// container; otherwise the value itself is decoded as a string-encoded
// container. Either way the encoding is self-described by the container's
// leading codec byte (raw / base64 / base64url, optionally gzipped).
func LoadProofsContainer(value string) (*container.Container, error) {
	data := []byte(value)
	if _, err := os.Stat(value); err == nil {
		data, err = os.ReadFile(value)
		if err != nil {
			return nil, fmt.Errorf("ingot: read proofs file %s: %w", value, err)
		}
	}
	// Textual encodings tolerate surrounding whitespace (e.g. a trailing
	// newline in a file); a raw CBOR container may legitimately start or end
	// with whitespace-valued bytes, so trim only for the base64 codecs.
	if trimmed := bytes.TrimSpace(data); len(trimmed) > 0 {
		switch trimmed[0] {
		case container.Base64, container.Base64url, container.Base64Gzip, container.Base64urlGzip:
			data = trimmed
		}
	}
	ct, err := container.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("ingot: decode proofs container: %w", err)
	}
	return ct, nil
}
