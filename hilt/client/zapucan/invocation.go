package zapucan

import (
	"github.com/fil-forge/ucantone/ucan"
	"go.uber.org/zap"
)

func WithInvocation(logger *zap.Logger, inv ucan.Invocation) *zap.Logger {
	fields := []zap.Field{
		zap.Stringer("issuer", inv.Issuer()),
		zap.Stringer("subject", inv.Subject()),
		zap.Stringer("command", inv.Command()),
		zap.Object("arguments", RawMap(inv.ArgumentsBytes())),
	}
	if inv.Audience().Defined() {
		fields = append(fields, zap.Stringer("audience", inv.Audience()))
	}
	if len(inv.MetadataBytes()) > 0 {
		fields = append(fields, zap.Object("metadata", RawMap(inv.MetadataBytes())))
	}
	if len(inv.Proofs()) > 0 {
		fields = append(fields, zap.Stringers("proofs", inv.Proofs()))
	}
	return logger.With(zap.Dict("invocation", fields...))
}
