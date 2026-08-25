package regionkey_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openbao/openbao/api/v2"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/regionkey"
)

// fakeTransit is an httptest stand-in for OpenBao's transit endpoints. It pins
// the provider's request shape (paths, base64 field encoding) and lets the
// error-mapping tests serve exact upstream error bodies.
type fakeTransit struct {
	t *testing.T
	// lastPath/lastBody record the most recent request for assertions.
	lastPath string
	lastBody map[string]interface{}
	// respond is invoked to write the response for the current request.
	respond func(w http.ResponseWriter)
}

func (f *fakeTransit) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.lastPath = r.URL.Path
	f.lastBody = map[string]interface{}{}
	require.NoError(f.t, json.NewDecoder(r.Body).Decode(&f.lastBody))
	f.respond(w)
}

// newBaoClient builds an api.Client pointed at the fake server.
func newBaoClient(t *testing.T, srv *httptest.Server) *api.Client {
	t.Helper()
	cfg := api.DefaultConfig()
	cfg.Address = srv.URL
	client, err := api.NewClient(cfg)
	require.NoError(t, err)
	client.SetToken("test-token")
	return client
}

func respondJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

func TestOpenBaoWrapRequestAndResponse(t *testing.T) {
	fake := &fakeTransit{t: t, respond: func(w http.ResponseWriter) {
		respondJSON(w, 200, `{"data":{"ciphertext":"vault:v3:abcdef","key_version":3}}`)
	}}
	srv := httptest.NewServer(fake)
	defer srv.Close()

	p, err := regionkey.NewOpenBaoProvider(newBaoClient(t, srv), "region-transit", "region-kek")
	require.NoError(t, err)

	binding := testBinding(t, "blob-1")
	cek := randKey(t, 32)
	wrapped, err := p.Wrap(context.Background(), binding, cek)
	require.NoError(t, err, "Wrap")

	require.Equal(t, "/v1/region-transit/encrypt/region-kek", fake.lastPath)
	require.Equal(t, base64.StdEncoding.EncodeToString(cek), fake.lastBody["plaintext"],
		"plaintext must be the base64 CEK")
	wantCtx := base64.StdEncoding.EncodeToString([]byte(binding.Space.String() + "\x00" + string(binding.Digest)))
	require.Equal(t, wantCtx, fake.lastBody["context"], "context must be the base64 binding encoding")

	require.Equal(t, regionkey.KeyVersion("v3"), wrapped.Version, "Version comes from key_version")
	require.Equal(t, []byte("vault:v3:abcdef"), wrapped.Ciphertext)
}

func TestOpenBaoUnwrapRequestAndResponse(t *testing.T) {
	cek := randKey(t, 32)
	fake := &fakeTransit{t: t, respond: func(w http.ResponseWriter) {
		respondJSON(w, 200, `{"data":{"plaintext":"`+base64.StdEncoding.EncodeToString(cek)+`"}}`)
	}}
	srv := httptest.NewServer(fake)
	defer srv.Close()

	// Empty mount falls back to the conventional "transit".
	p, err := regionkey.NewOpenBaoProvider(newBaoClient(t, srv), "", "region-kek")
	require.NoError(t, err)

	binding := testBinding(t, "blob-1")
	got, err := p.Unwrap(context.Background(), binding,
		regionkey.WrappedKey{Version: "v3", Ciphertext: []byte("vault:v3:abcdef")})
	require.NoError(t, err, "Unwrap")

	require.Equal(t, "/v1/transit/decrypt/region-kek", fake.lastPath)
	require.Equal(t, "vault:v3:abcdef", fake.lastBody["ciphertext"], "ciphertext is sent as the transit string")
	require.Equal(t, cek, got)
}

// The provider classifies transit decrypt failures by HTTP status and message;
// these cases pin the mapping to the upstream wordings so a change upstream
// fails here instead of silently reclassifying.
func TestOpenBaoUnwrapErrorMapping(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		body     string
		sentinel error
	}{
		{"tampered or wrong-binding ciphertext", 400,
			`{"errors":["invalid ciphertext: could not decrypt"]}`, regionkey.ErrAuthentication},
		{"version behind min_decryption_version", 400,
			`{"errors":["ciphertext or signature version is disallowed by policy (too old)"]}`, regionkey.ErrUnknownVersion},
		{"version beyond the key's latest", 400,
			`{"errors":["ciphertext version is larger than the latest key version"]}`, regionkey.ErrUnknownVersion},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeTransit{t: t, respond: func(w http.ResponseWriter) {
				respondJSON(w, tc.status, tc.body)
			}}
			srv := httptest.NewServer(fake)
			defer srv.Close()

			p, err := regionkey.NewOpenBaoProvider(newBaoClient(t, srv), "", "region-kek")
			require.NoError(t, err)
			_, err = p.Unwrap(context.Background(), testBinding(t, "blob-1"),
				regionkey.WrappedKey{Version: "v1", Ciphertext: []byte("vault:v1:junk")})
			require.ErrorIs(t, err, tc.sentinel)
		})
	}

	t.Run("non-400 passes through unclassified", func(t *testing.T) {
		fake := &fakeTransit{t: t, respond: func(w http.ResponseWriter) {
			respondJSON(w, 403, `{"errors":["permission denied"]}`)
		}}
		srv := httptest.NewServer(fake)
		defer srv.Close()

		p, err := regionkey.NewOpenBaoProvider(newBaoClient(t, srv), "", "region-kek")
		require.NoError(t, err)
		_, err = p.Unwrap(context.Background(), testBinding(t, "blob-1"),
			regionkey.WrappedKey{Version: "v1", Ciphertext: []byte("vault:v1:junk")})
		require.Error(t, err)
		require.NotErrorIs(t, err, regionkey.ErrAuthentication, "a permission failure is not an authentication verdict")
		require.NotErrorIs(t, err, regionkey.ErrUnknownVersion)
	})
}

func TestNewOpenBaoProviderValidates(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	client := newBaoClient(t, srv)

	_, err := regionkey.NewOpenBaoProvider(nil, "transit", "region-kek")
	require.Error(t, err, "nil client must be rejected")
	_, err = regionkey.NewOpenBaoProvider(client, "transit", "")
	require.Error(t, err, "empty key name must be rejected")
	_, err = regionkey.NewOpenBaoProvider(client, "", "region-kek")
	require.NoError(t, err)
}
