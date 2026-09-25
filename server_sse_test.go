package ingot

import (
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
)

// TestSSEResponseHeader pins that a response carrying an ETag header also
// reports x-amz-server-side-encryption: aws:kms, and that one without an
// ETag (a listing, an error, the DID document) is left alone.
func TestSSEResponseHeader(t *testing.T) {
	app := fiber.New()
	app.Use(sseResponseHeader)
	app.Get("/etag", func(c fiber.Ctx) error {
		c.Set(fiber.HeaderETag, `"abc-1"`)
		return c.SendString("body")
	})
	app.Get("/plain", func(c fiber.Ctx) error { return c.SendString("body") })

	res, err := app.Test(httptest.NewRequest("GET", "/etag", nil))
	require.NoError(t, err)
	require.Equal(t, "aws:kms", res.Header.Get("x-amz-server-side-encryption"))

	res, err = app.Test(httptest.NewRequest("GET", "/plain", nil))
	require.NoError(t, err)
	require.Empty(t, res.Header.Get("x-amz-server-side-encryption"))
}
