package ingot

import (
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3log"
	"github.com/gofiber/fiber/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// logAuditError drives one request through a fiber app whose handler feeds
// err to the audit logger, returning the entries zap recorded.
func logAuditError(t *testing.T, err error) []observer.LoggedEntry {
	t.Helper()
	core, recorded := observer.New(zapcore.ErrorLevel)
	audit := &errorAuditLogger{logger: zap.New(core)}

	app := fiber.New()
	app.Get("/bucket/key", func(c fiber.Ctx) error {
		audit.Log(c, err, nil, s3log.LogMeta{Action: "GetObject"})
		return nil
	})
	if _, terr := app.Test(httptest.NewRequest("GET", "/bucket/key", nil)); terr != nil {
		t.Fatalf("app.Test: %v", terr)
	}
	return recorded.All()
}

func TestErrorAuditLogger_LogsUnexpectedError(t *testing.T) {
	entries := logAuditError(t, errors.New("hilt: authorize: connection refused"))
	if len(entries) != 1 {
		t.Fatalf("expected 1 log entry, got %d: %v", len(entries), entries)
	}
	if got := entries[0].ContextMap()["error"]; got != "hilt: authorize: connection refused" {
		t.Fatalf("expected the error on the entry, got %v", got)
	}
}

// The S3 error table is an expected outcome the response already carries, so
// it must not reach the log.
func TestErrorAuditLogger_SkipsS3Errors(t *testing.T) {
	entries := logAuditError(t, s3err.GetAPIError(s3err.ErrNoSuchKey))
	if len(entries) != 0 {
		t.Fatalf("expected no log entries, got %v", entries)
	}
}

func TestErrorAuditLogger_SkipsSuccess(t *testing.T) {
	entries := logAuditError(t, nil)
	if len(entries) != 0 {
		t.Fatalf("expected no log entries, got %v", entries)
	}
}
