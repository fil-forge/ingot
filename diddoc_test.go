package ingot

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fil-forge/libforge/identity"
	"github.com/fil-forge/ucantone/did/web"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
)

// TestDIDDocumentHandler serves a did:web agent's document at the well-known
// path and checks the shape peers resolve: the document id is the did:web and
// its single verification method is the key, referenced as "#key-0". A
// catch-all "/:bucket/*" route mounted afterwards (as versitygw's S3 route
// table is) must not shadow it.
func TestDIDDocumentHandler(t *testing.T) {
	id, err := identity.New("", "did:web:ingot.test")
	require.NoError(t, err, "identity.New")
	doc, err := id.DIDDocument()
	require.NoError(t, err, "DIDDocument")

	handler, err := didDocumentHandler(doc)
	require.NoError(t, err, "didDocumentHandler")

	app := fiber.New()
	app.Get(web.WellKnownDIDPath, handler)
	app.Get("/:bucket/*", func(c fiber.Ctx) error {
		return c.Status(http.StatusTeapot).SendString("S3 route table")
	})

	res, err := app.Test(httptest.NewRequest(http.MethodGet, web.WellKnownDIDPath, nil))
	require.NoError(t, err, "GET %s", web.WellKnownDIDPath)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode, "status")
	ct := res.Header.Get("Content-Type")
	require.Contains(t, ct, "application/json", "Content-Type")
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err, "read body")

	var got struct {
		ID                 string `json:"id"`
		VerificationMethod []struct {
			ID                 string `json:"id"`
			Type               string `json:"type"`
			Controller         string `json:"controller"`
			PublicKeyMultibase string `json:"publicKeyMultibase"`
		} `json:"verificationMethod"`
		CapabilityInvocation []string `json:"capabilityInvocation"`
	}
	require.NoError(t, json.Unmarshal(body, &got), "decode document %s", body)
	require.Equal(t, "did:web:ingot.test", got.ID, "id")
	require.Len(t, got.VerificationMethod, 1, "verificationMethod count")
	vm := got.VerificationMethod[0]
	require.Equal(t, "did:web:ingot.test#key-0", vm.ID, "verificationMethod id = %q, want did:web:ingot.test#key-0", vm.ID)
	require.Equal(t, "Multikey", vm.Type, "verificationMethod = %+v, want a Multikey with publicKeyMultibase", vm)
	require.NotEmpty(t, vm.PublicKeyMultibase, "verificationMethod = %+v, want a Multikey with publicKeyMultibase", vm)
	require.Equal(t, "did:web:ingot.test", vm.Controller, "verificationMethod controller")
	require.Len(t, got.CapabilityInvocation, 1, "capabilityInvocation")
	require.Equal(t, vm.ID, got.CapabilityInvocation[0], "capabilityInvocation")

	cc := res.Header.Get("Cache-Control")
	require.Equal(t, "max-age=300", cc, "Cache-Control")
	lm := res.Header.Get("Last-Modified")
	require.NotEmpty(t, lm, "Last-Modified is not set")
	etag := res.Header.Get("ETag")
	require.NotEmpty(t, etag, "ETag is not set")

	// A client that already holds the document gets a 304.
	req := httptest.NewRequest(http.MethodGet, web.WellKnownDIDPath, nil)
	req.Header.Set("If-None-Match", etag)
	res, err = app.Test(req)
	require.NoError(t, err, "conditional GET %s", web.WellKnownDIDPath)
	defer res.Body.Close()
	require.Equal(t, http.StatusNotModified, res.StatusCode, "conditional status")
	body, _ = io.ReadAll(res.Body)
	require.Empty(t, body, "conditional body")
}
