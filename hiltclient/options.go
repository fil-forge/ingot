package hiltclient

import (
	"net/http"

	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"go.uber.org/zap"
)

type Option func(*clientConfig)

type clientConfig struct {
	httpClient *http.Client
	logger     *zap.Logger
	product    did.DID
}

func WithHTTPClient(httpClient *http.Client) Option {
	return func(cfg *clientConfig) {
		cfg.httpClient = httpClient
	}
}

// WithProduct sets the default product/plan DID used when registering customers
// (see [UploadClient.RegisterCustomer]).
func WithProduct(product did.DID) Option {
	return func(cfg *clientConfig) {
		cfg.product = product
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
