package ingot

import (
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3log"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// logAuditError drives one request through a fiber app whose handler feeds
// err to the audit logger, returning the entries zap recorded.
func logAuditError(t *testing.T, err error) []observer.LoggedEntry {
	t.Helper()
	core, recorded := observer.New(zapcore.ErrorLevel)
	audit := &auditLogger{logger: zap.New(core)}

	app := fiber.New()
	app.Get("/bucket/key", func(c fiber.Ctx) error {
		audit.Log(c, err, nil, s3log.LogMeta{Action: "GetObject"})
		return nil
	})
	_, terr := app.Test(httptest.NewRequest("GET", "/bucket/key", nil))
	require.NoError(t, terr, "app.Test")
	return recorded.All()
}

func TestAuditLogger_LogsUnexpectedError(t *testing.T) {
	entries := logAuditError(t, errors.New("hilt: authorize: connection refused"))
	require.Len(t, entries, 1)
	require.Equal(t, "hilt: authorize: connection refused", entries[0].ContextMap()["error"], "the error on the entry")
}

// The S3 error table is an expected outcome the response already carries, so
// it must not reach the log.
func TestAuditLogger_SkipsS3Errors(t *testing.T) {
	entries := logAuditError(t, s3err.GetAPIError(s3err.ErrNoSuchKey))
	require.Empty(t, entries)
}

func TestAuditLogger_SkipsSuccess(t *testing.T) {
	entries := logAuditError(t, nil)
	require.Empty(t, entries)
}
