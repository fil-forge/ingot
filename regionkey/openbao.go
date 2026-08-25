package regionkey

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/openbao/openbao/api/v2"
)

// OpenBaoProvider is the production region [Provider] of the regional
// security RFC: the wrap is an AES-256-GCM transit operation performed inside
// the region's OpenBao, against a key created with type aes256-gcm96 and
// derived=true, so the region KEK is non-exportable and never enters Ingot's
// process. The binding context rides transit's derivation context: each
// (space, blob digest) gets its own derived subkey, and wrap material
// transplanted between rows fails authentication — the same semantics
// [InProcessProvider] mirrors for tests.
//
// The provider owns only the encrypt/decrypt calls. Everything else about the
// key is deployment and operator business: creating it (derived=true,
// exportable left false), rotating it, advancing min_decryption_version, and
// the batched rewrap campaigns that move stored ciphertexts between versions
// — Ingot's token needs encrypt, decrypt, and rewrap only.
type OpenBaoProvider struct {
	logical *api.Logical
	mount   string
	key     string
}

var _ Provider = (*OpenBaoProvider)(nil)

// DefaultTransitMount is the transit engine's conventional mount path, used
// when NewOpenBaoProvider is given an empty mount.
const DefaultTransitMount = "transit"

// NewOpenBaoProvider returns a provider wrapping CEKs with the transit key
// named key on the given mount (empty mount means [DefaultTransitMount]).
// The caller wires the client — address (unix socket or TCP), token, TLS —
// so custody of the connection stays with the deployment.
func NewOpenBaoProvider(client *api.Client, mount, key string) (*OpenBaoProvider, error) {
	if client == nil {
		return nil, errors.New("regionkey: OpenBao client must not be nil")
	}
	if key == "" {
		return nil, errors.New("regionkey: transit key name must not be empty")
	}
	if mount == "" {
		mount = DefaultTransitMount
	}
	return &OpenBaoProvider{logical: client.Logical(), mount: mount, key: key}, nil
}

// Wrap implements [Provider.Wrap]: a transit encrypt of cek under the key's
// current version, with the binding as the derivation context. The returned
// ciphertext is transit's "vault:vN:…" string, which embeds the version it
// was wrapped under; WrappedKey.Version records the same version ("vN") for
// the provider-agnostic bookkeeping column.
func (p *OpenBaoProvider) Wrap(ctx context.Context, binding BindingContext, cek []byte) (WrappedKey, error) {
	secret, err := p.logical.WriteWithContext(ctx, p.mount+"/encrypt/"+p.key, map[string]interface{}{
		"plaintext": base64.StdEncoding.EncodeToString(cek),
		"context":   base64.StdEncoding.EncodeToString(bindingBytes(binding)),
	})
	if err != nil {
		return WrappedKey{}, fmt.Errorf("regionkey: transit encrypt: %w", err)
	}
	ciphertext, ok := secret.Data["ciphertext"].(string)
	if !ok || ciphertext == "" {
		return WrappedKey{}, errors.New("regionkey: transit encrypt returned no ciphertext")
	}
	version, err := keyVersionOf(secret.Data["key_version"])
	if err != nil {
		return WrappedKey{}, fmt.Errorf("regionkey: transit encrypt: %w", err)
	}
	return WrappedKey{Version: version, Ciphertext: []byte(ciphertext)}, nil
}

// Unwrap implements [Provider.Unwrap]: a transit decrypt with the binding as
// the derivation context. Transit selects the KEK version from the
// ciphertext's own "vault:vN:" prefix, so wrapped.Version is not sent; it is
// the DB row's bookkeeping copy of the same fact. A wrong binding and
// tampered bytes fail authentication inside OpenBao and surface as
// [ErrAuthentication]; a version the key no longer serves (behind
// min_decryption_version, or from some other key entirely) surfaces as
// [ErrUnknownVersion].
func (p *OpenBaoProvider) Unwrap(ctx context.Context, binding BindingContext, wrapped WrappedKey) ([]byte, error) {
	secret, err := p.logical.WriteWithContext(ctx, p.mount+"/decrypt/"+p.key, map[string]interface{}{
		"ciphertext": string(wrapped.Ciphertext),
		"context":    base64.StdEncoding.EncodeToString(bindingBytes(binding)),
	})
	if err != nil {
		return nil, fmt.Errorf("regionkey: transit decrypt: %w", mapTransitDecryptError(err))
	}
	encoded, ok := secret.Data["plaintext"].(string)
	if !ok || encoded == "" {
		return nil, errors.New("regionkey: transit decrypt returned no plaintext")
	}
	cek, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("regionkey: transit decrypt plaintext: %w", err)
	}
	return cek, nil
}

// keyVersionOf renders transit's key_version response field (a json.Number)
// as the "vN" tag matching the ciphertext prefix.
func keyVersionOf(v interface{}) (KeyVersion, error) {
	n, ok := v.(json.Number)
	if !ok {
		return "", fmt.Errorf("regionkey: unexpected key_version %v (%T)", v, v)
	}
	return KeyVersion("v" + n.String()), nil
}

// mapTransitDecryptError classifies a transit decrypt failure onto the
// package's sentinel errors. Transit reports every client-side decrypt
// problem as HTTP 400 with a message, so the split is by message: version
// policy refusals (rotated past min_decryption_version, or a version the key
// never had) become [ErrUnknownVersion]; every other 400 — invalid or
// tampered ciphertext, wrong derivation context — is an authentication
// failure. The message matching is pinned by unit tests so an upstream
// wording change is caught rather than silently reclassified. Non-400 errors
// (network, permission) pass through untouched.
func mapTransitDecryptError(err error) error {
	var re *api.ResponseError
	if !errors.As(err, &re) || re.StatusCode != 400 {
		return err
	}
	for _, msg := range re.Errors {
		if strings.Contains(msg, "disallowed by policy") ||
			strings.Contains(msg, "too old") ||
			strings.Contains(msg, "latest key version") {
			return fmt.Errorf("%w: %w", ErrUnknownVersion, err)
		}
	}
	return fmt.Errorf("%w: %w", ErrAuthentication, err)
}
