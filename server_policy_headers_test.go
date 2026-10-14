package ingot

import (
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
)

// TestPolicyHeaders_LeavesOtherResponsesAlone checks that a successful GET
// carrying a policy query key but answered by something other than the
// bucket policy backend, such as an object read, keeps its own content type.
func TestPolicyHeaders_LeavesOtherResponsesAlone(t *testing.T) {
	app := fiber.New()
	app.Use(policyHeaders)
	app.Get("/:bucket/*", func(c fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, "text/plain")
		return c.SendString("object body")
	})

	res, err := app.Test(httptest.NewRequest("GET", "/photos/report.txt?policy", nil))
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	require.Equal(t, "text/plain", res.Header.Get("Content-Type"))
	require.Empty(t, res.Header.Get("ETag"))
}
