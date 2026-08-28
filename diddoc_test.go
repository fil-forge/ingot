package ingot

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fil-forge/libforge/identity"
	"github.com/fil-forge/ucantone/did/web"
	"github.com/gofiber/fiber/v3"
)

// TestDIDDocumentHandler serves a did:web agent's document at the well-known
// path and checks the shape peers resolve: the document id is the did:web and
// its single verification method is the key, referenced as "#key-0". A
// catch-all "/:bucket/*" route mounted afterwards (as versitygw's S3 route
// table is) must not shadow it.
func TestDIDDocumentHandler(t *testing.T) {
	id, err := identity.New("", "did:web:ingot.test")
	if err != nil {
		t.Fatalf("identity.New: %v", err)
	}
	doc, err := id.DIDDocument()
	if err != nil {
		t.Fatalf("DIDDocument: %v", err)
	}

	app := fiber.New()
	app.Get(web.WellKnownDIDPath, didDocumentHandler(doc))
	app.Get("/:bucket/*", func(c fiber.Ctx) error {
		return c.Status(http.StatusTeapot).SendString("S3 route table")
	})

	res, err := app.Test(httptest.NewRequest(http.MethodGet, web.WellKnownDIDPath, nil))
	if err != nil {
		t.Fatalf("GET %s: %v", web.WellKnownDIDPath, err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

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
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode document %s: %v", body, err)
	}
	if got.ID != "did:web:ingot.test" {
		t.Fatalf("id = %q, want did:web:ingot.test", got.ID)
	}
	if len(got.VerificationMethod) != 1 {
		t.Fatalf("verificationMethod count = %d, want 1: %s", len(got.VerificationMethod), body)
	}
	vm := got.VerificationMethod[0]
	if vm.ID != "did:web:ingot.test#key-0" {
		t.Fatalf("verificationMethod id = %q, want did:web:ingot.test#key-0", vm.ID)
	}
	if vm.Type != "Multikey" || vm.PublicKeyMultibase == "" {
		t.Fatalf("verificationMethod = %+v, want a Multikey with publicKeyMultibase", vm)
	}
	if vm.Controller != "did:web:ingot.test" {
		t.Fatalf("verificationMethod controller = %q, want did:web:ingot.test", vm.Controller)
	}
	if len(got.CapabilityInvocation) != 1 || got.CapabilityInvocation[0] != vm.ID {
		t.Fatalf("capabilityInvocation = %v, want [%s]", got.CapabilityInvocation, vm.ID)
	}
}
