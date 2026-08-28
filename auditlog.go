package ingot

import (
	"github.com/fil-forge/versitygw/s3api/utils"
	"github.com/fil-forge/versitygw/s3err"
	"github.com/fil-forge/versitygw/s3log"
	"github.com/gofiber/fiber/v3"
	"go.uber.org/zap"
)

// errorAuditLogger is a versitygw audit-log sink that reports failed S3
// requests through zap. Without it, a request that fails for a reason S3 has
// no error code for — a broken hilt handshake, a network read that never
// returns — is announced only by versitygw's debuglogger, which writes a bare
// "[INTERNAL ERROR]" line to stderr with no timestamp, level, or request ID
// to line up against the logs of the service that actually failed.
//
// Only those errors are logged. An [s3err.S3Error] (NoSuchKey and the rest of
// the S3 error table) is an expected outcome the S3 response already carries.
type errorAuditLogger struct {
	logger *zap.Logger
}

var _ s3log.AuditLogger = (*errorAuditLogger)(nil)

func (l *errorAuditLogger) Log(ctx fiber.Ctx, err error, _ []byte, meta s3log.LogMeta) {
	if err == nil {
		return
	}
	if _, ok := err.(s3err.S3Error); ok {
		return
	}
	l.logger.Error("S3 request failed",
		zap.String("action", meta.Action),
		zap.String("method", ctx.Method()),
		zap.String("path", ctx.Path()),
		zap.String("request_id", utils.RequestID(ctx)),
		zap.Error(err),
	)
}

// HangUp and Shutdown exist for sinks holding a file handle or an HTTP
// connection; zap's lifecycle belongs to the host.
func (l *errorAuditLogger) HangUp() error { return nil }

func (l *errorAuditLogger) Shutdown() error { return nil }
