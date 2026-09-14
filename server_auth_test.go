package ingot

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	hiltclient "github.com/fil-forge/hilt/pkg/client"
	s3 "github.com/fil-forge/libforge/commands/s3"
	s3bkt "github.com/fil-forge/libforge/commands/s3/bucket"
	s3req "github.com/fil-forge/libforge/commands/s3/request"
	"github.com/fil-forge/libforge/identity"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/versitygw/auth"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/ingot/iam"
	ingottesting "github.com/fil-forge/ingot/testing"
)

// recordingIAM wraps the real iam.Service and records every access key
// versitygw's auth dispatch hands it, so a test can tell the IAM path from the
// root short-circuit (which never consults IAM).
type recordingIAM struct {
	*iam.Service
	seen []string
}

func (r *recordingIAM) GetUserAccountForRequest(ctx fiber.Ctx, access string) (auth.Account, error) {
	r.seen = append(r.seen, access)
	return r.Service.GetUserAccountForRequest(ctx, access)
}

// TestBuildS3API_NoRootShortCircuit pins the no-root wiring end to end: a
// request signed with a plain (non-did:key) access key — the shape a root
// credential would have — goes through versitygw's auth dispatch into
// iam.Service, which rejects it as InvalidAccessKeyId. Were the gateway's
// root account enabled, or were it to match an empty root config, the key
// would be resolved before IAM and the request would fail on the signature
// instead.
func TestBuildS3API_NoRootShortCircuit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id, err := identity.New("", "did:web:ingot.test")
	require.NoError(t, err)

	svc := iam.New(neverAuthorizer{}, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), iam.NewTenantCache())
	t.Cleanup(func() { require.NoError(t, svc.Shutdown()) })
	rec := &recordingIAM{Service: svc}

	cfg := config.ServerConfig{Region: "us-east-1", MaxConnections: 16, MaxRequests: 16}
	// No backend: authentication fails before any handler runs.
	api, err := buildS3API(ctx, nil, cfg, rec, id, zap.NewNop())
	require.NoError(t, err)

	addr := freeAddr(t)
	serveErr := make(chan error, 1)
	go func() { serveErr <- api.ServeMultiPort([]string{addr}) }()
	t.Cleanup(func() {
		require.NoError(t, api.ShutDown())
		select {
		case err := <-serveErr:
			// The serve loop reports its own listener closing after ShutDown.
			if err != nil && !strings.Contains(err.Error(), "closed") {
				t.Fatalf("s3api serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("s3api did not stop")
		}
	})
	waitListening(t, addr)

	_, err = ingottesting.ListKeys(ctx, ingottesting.Config{
		Endpoint:  "http://" + addr,
		AccessKey: "ingot",
		SecretKey: "ingotsecret",
		Region:    cfg.Region,
	}, "bucket")
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "InvalidAccessKeyId", apiErr.ErrorCode())

	require.Equal(t, []string{"ingot"}, rec.seen, "the plain key must reach IAM, not a root short-circuit")
}

// neverAuthorizer fails the test if hilt is consulted: a malformed access key
// is rejected by iam.Service's did:key parse first.
type neverAuthorizer struct{}

func (neverAuthorizer) AuthorizeRequest(context.Context, s3.Request, ...hiltclient.MethodOption) (*s3req.AuthorizeOK, ucan.Container, error) {
	return nil, nil, errors.New("hilt must not be consulted for a malformed access key")
}

func (neverAuthorizer) BucketInfo(context.Context, string, did.DID, ...hiltclient.MethodOption) (*s3bkt.InfoOK, ucan.Container, error) {
	return nil, nil, errors.New("hilt must not be consulted for a malformed access key")
}

// freeAddr reserves and releases a loopback port for the server under test.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// waitListening blocks until the server accepts connections at addr.
func waitListening(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			require.NoError(t, conn.Close())
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("s3api never listened on %s", addr)
}
