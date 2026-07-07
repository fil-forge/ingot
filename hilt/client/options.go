package client

import (
	"net/http"

	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/ucan"
	"go.uber.org/zap"
)

type Option func(*clientConfig)

type clientConfig struct {
	httpClient *http.Client
	logger     *zap.Logger
}

func WithHTTPClient(httpClient *http.Client) Option {
	return func(cfg *clientConfig) {
		cfg.httpClient = httpClient
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(cfg *clientConfig) {
		if logger != nil {
			cfg.logger = logger
		}
	}
}

type MethodOption func(*methodConfig)

type methodConfig struct {
	issuer ucan.Issuer
	proofs ucanlib.ProofStore
}

func WithIssuer(iss ucan.Issuer) MethodOption {
	return func(cfg *methodConfig) {
		if iss != nil {
			cfg.issuer = iss
		}
	}
}

func WithProofs(proofs ucanlib.ProofStore) MethodOption {
	return func(cfg *methodConfig) {
		if proofs != nil {
			cfg.proofs = proofs
		}
	}
}
