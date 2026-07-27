package ingot

import (
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"

	"github.com/fil-forge/ingot/internal/cors"
)

// newCORSTestApp mounts corsMiddleware exactly as buildS3API does
// (app.Use on "/") in front of a stand-in route.
func newCORSTestApp(t *testing.T, patterns []string) *fiber.App {
	t.Helper()
	m, err := cors.NewMatcher(patterns)
	if err != nil {
		t.Fatalf("NewMatcher: %v", err)
	}
	app := fiber.New()
	app.Use("/", corsMiddleware(m))
	app.All("/*", func(c fiber.Ctx) error { return c.SendString("backend") })
	return app
}

func TestCORSMiddleware(t *testing.T) {
	patterns := []string{
		"https://app.example",
		"https://staging.example",
		"https://*.dev.example",
	}

	t.Run("allowed origin gets reflected headers", func(t *testing.T) {
		app := newCORSTestApp(t, patterns)
		req := httptest.NewRequest("GET", "/bucket/key", nil)
		req.Header.Set("Origin", "https://feature-1.dev.example")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Test: %v", err)
		}
		if resp.StatusCode != fiber.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "https://feature-1.dev.example" {
			t.Errorf("Allow-Origin = %q, want the request origin echoed", got)
		}
		if got := resp.Header.Get("Vary"); got == "" {
			t.Error("Vary header missing on origin-reflected response")
		}
		if got := resp.Header.Get("Access-Control-Expose-Headers"); got != corsExposeHeaders {
			t.Errorf("Expose-Headers = %q, want %q", got, corsExposeHeaders)
		}
		if got := resp.Header.Get("Access-Control-Allow-Credentials"); got != "" {
			t.Errorf("Allow-Credentials = %q, must never be set", got)
		}
	})

	t.Run("disallowed origin passes through without CORS headers", func(t *testing.T) {
		app := newCORSTestApp(t, patterns)
		req := httptest.NewRequest("GET", "/bucket/key", nil)
		req.Header.Set("Origin", "https://evil.example")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Test: %v", err)
		}
		if resp.StatusCode != fiber.StatusOK {
			t.Fatalf("status = %d, want 200 (request still served)", resp.StatusCode)
		}
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("Allow-Origin = %q, want unset for a disallowed origin", got)
		}
	})

	t.Run("no origin header is untouched", func(t *testing.T) {
		app := newCORSTestApp(t, patterns)
		resp, err := app.Test(httptest.NewRequest("GET", "/bucket/key", nil))
		if err != nil {
			t.Fatalf("Test: %v", err)
		}
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("Allow-Origin = %q, want unset without an Origin header", got)
		}
	})

	t.Run("preflight is answered 204 and short-circuits", func(t *testing.T) {
		app := newCORSTestApp(t, patterns)
		req := httptest.NewRequest("OPTIONS", "/bucket/key", nil)
		req.Header.Set("Origin", "https://app.example")
		req.Header.Set("Access-Control-Request-Method", "PUT")
		req.Header.Set("Access-Control-Request-Headers", "authorization, content-type")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Test: %v", err)
		}
		if resp.StatusCode != fiber.StatusNoContent {
			t.Fatalf("status = %d, want 204", resp.StatusCode)
		}
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "https://app.example" {
			t.Errorf("Allow-Origin = %q, want the request origin echoed", got)
		}
		if got := resp.Header.Get("Access-Control-Allow-Methods"); got != "PUT" {
			t.Errorf("Allow-Methods = %q, want the requested method mirrored", got)
		}
		if got := resp.Header.Get("Access-Control-Allow-Headers"); got != "authorization, content-type" {
			t.Errorf("Allow-Headers = %q, want the requested headers mirrored", got)
		}
	})

	t.Run("preflight from a disallowed origin reaches the router", func(t *testing.T) {
		app := newCORSTestApp(t, patterns)
		req := httptest.NewRequest("OPTIONS", "/bucket/key", nil)
		req.Header.Set("Origin", "https://evil.example")
		req.Header.Set("Access-Control-Request-Method", "PUT")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Test: %v", err)
		}
		// Not short-circuited: the stand-in route answers, with no CORS
		// headers — the browser blocks it.
		if resp.StatusCode != fiber.StatusOK {
			t.Fatalf("status = %d, want 200 from the fall-through route", resp.StatusCode)
		}
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("Allow-Origin = %q, want unset", got)
		}
	})

	t.Run("plain OPTIONS without request-method falls through", func(t *testing.T) {
		app := newCORSTestApp(t, patterns)
		req := httptest.NewRequest("OPTIONS", "/bucket/key", nil)
		req.Header.Set("Origin", "https://app.example")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Test: %v", err)
		}
		if resp.StatusCode != fiber.StatusOK {
			t.Fatalf("status = %d, want 200 from the fall-through route", resp.StatusCode)
		}
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "https://app.example" {
			t.Errorf("Allow-Origin = %q, want the request origin echoed", got)
		}
	})
}
