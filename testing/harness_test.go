package testing_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	mstesting "github.com/fil-forge/ingot/testing"
)

func TestHarnessLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	h, err := mstesting.StartHarness(ctx, mstesting.WithLogger(zaptest.NewLogger(t)))
	if err != nil {
		t.Fatalf("StartHarness: %v", err)
	}
	t.Cleanup(func() {
		if err := h.Stop(t.Context()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	if !strings.HasPrefix(h.Endpoint, "http://127.0.0.1:") {
		t.Fatalf("unexpected endpoint %q", h.Endpoint)
	}

	// /health is wired in buildS3API; hit it to confirm the listener
	// is actually serving HTTP, not just accepting TCP.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.Endpoint+"/health", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/health status = %d, want 200", resp.StatusCode)
	}

	cfg := h.Config()
	if cfg.Endpoint != h.Endpoint || cfg.AccessKey != h.AccessKey || cfg.SecretKey != h.SecretKey {
		t.Fatalf("Config() mismatch: %+v vs %+v", cfg, h)
	}
}
