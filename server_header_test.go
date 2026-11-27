package ingot

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/fil-forge/libforge/identity"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/ingot/iam"
)

// TestBuildS3API_ServerHeader checks that responses name Ingot in the Server
// header, including the ones answered ahead of Ingot's own middleware: the
// health endpoint and the DID document.
func TestBuildS3API_ServerHeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id, err := identity.New("", "did:web:ingot.test")
	require.NoError(t, err)

	svc := iam.New(neverAuthorizer{}, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), iam.NewTenantCache())
	t.Cleanup(func() { require.NoError(t, svc.Shutdown()) })

	cfg := config.ServerConfig{Region: "us-east-1", MaxConnections: 16, MaxRequests: 16}
	// No backend: none of these requests reaches a backend handler.
	api, err := buildS3API(ctx, nil, cfg, svc, id, zap.NewNop())
	require.NoError(t, err)
	addr := serveS3API(t, api)

	tests := []struct {
		name          string
		path          string
		authorization string
	}{
		{name: "health", path: "/health"},
		{name: "DID document", path: "/.well-known/did.json"},
		{name: "rejected request", path: "/bucket/key", authorization: "not a signature"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+tt.path, nil)
			require.NoError(t, err)
			if tt.authorization != "" {
				req.Header.Set("Authorization", tt.authorization)
			}
			res, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			require.NoError(t, res.Body.Close())
			require.Equal(t, []string{"Ingot"}, res.Header.Values("Server"))
		})
	}
}
