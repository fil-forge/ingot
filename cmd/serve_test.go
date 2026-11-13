package cmd

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	jsg "github.com/alanshaw/dag-json-gen"
	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/libforge/identity"
	"github.com/fil-forge/swarf/pkg/api"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/fil-forge/ucantone/ucan/delegation"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"

	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/registry"
)

// TestApp_GraphValidates ensures the fx graph is complete — every collaborator
// ingot.ServerModule consumes is provided. fx resolves the lifecycle invoke
// eagerly at fx.New, so app.Err() surfaces any missing provider without
// binding a listener or running migrations. Guards cmd/serve.go's provider
// list from drifting away from what ServerModule needs.
//
// buildApp dials and pings Postgres as it builds the graph, so it gets a
// database of its own. The DB-free graph validation lives in the root
// package's TestModuleValidate_* (fx.ValidateApp over ingot.Module).
func TestApp_GraphValidates(t *testing.T) {
	_, dsn := pgtest.NewWithDSN(t)

	app, err := buildApp(t.Context(), appConfig(t, dsn), zaptest.NewLogger(t))
	if err != nil {
		t.Fatalf("fx graph does not validate: %v", err)
	}
	if app == nil {
		t.Fatal("buildApp returned a nil app")
	}
}

// TestApp_StopSavesRevocationCursor starts the app with the revocation
// consumer reading a stub firehose, stops it as soon as the consumer has
// applied a record, and checks that the record's cursor was saved. The
// consumer saves the head before it streams, and at stop whatever its
// periodic save has not, which lands only if the pool outlives the consumer:
// this pins buildApp closing the pool after the consumer has stopped.
//
// The stop beats the consumer's first periodic save by a wide margin, so a
// pool that closes first fails the test; a slow enough runner would let the
// periodic save land first and hide that, but never fail a correct build.
func TestApp_StopSavesRevocationCursor(t *testing.T) {
	pool, dsn := pgtest.NewWithDSN(t)

	mh, err := multihash.Sum([]byte("revoked"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	rec := api.FirehoseRevocation{
		Revoke:     cid.NewCidV1(cid.DagCBOR, mh),
		Cause:      cid.NewCidV1(cid.DagCBOR, mh),
		RecordedAt: jsg.DagJsonTime(time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)),
	}
	var payload bytes.Buffer
	if err := rec.MarshalDagJSON(&payload); err != nil {
		t.Fatalf("encode record: %v", err)
	}
	// The stub answers the head, then streams the record after it and holds
	// the stream open as an idle feed.
	const head, recCursor = "41", "42"
	firehose := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/revocations/head":
			_, _ = fmt.Fprintln(w, head)
		case "/revocations/after/" + head:
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprintf(w, "id: %s\nevent: revocation\ndata: %s\n\n", recCursor, payload.String())
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(func() {
		firehose.CloseClientConnections()
		firehose.Close()
	})

	cfg := appConfig(t, dsn)
	cfg.RevocationServiceURL = firehose.URL
	cfg.RevocationServiceDID = "did:web:revocation.example"
	obs, logs := observer.New(zap.InfoLevel)
	logger := zaptest.NewLogger(t, zaptest.WrapOptions(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		return zapcore.NewTee(c, obs)
	})))
	app, err := buildApp(t.Context(), cfg, logger)
	if err != nil {
		t.Fatalf("build app: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if err := app.Start(ctx); err != nil {
		t.Fatalf("start app: %v", err)
	}
	for logs.FilterMessage("revocation: record processed").Len() == 0 {
		if ctx.Err() != nil {
			_ = app.Stop(context.Background())
			t.Fatal("the consumer applied no record")
		}
		time.Sleep(time.Millisecond)
	}
	if err := app.Stop(ctx); err != nil {
		t.Fatalf("stop app: %v", err)
	}
	// Nothing that stops before the pool, the consumer included, may find it
	// closed.
	for _, e := range logs.All() {
		if msg, _ := e.ContextMap()["error"].(string); strings.Contains(msg, "closed pool") {
			t.Errorf("%q logged against the closed pool: %s", e.Message, msg)
		}
	}

	cur, err := registry.NewPostgres(pool).GetRevocationCursor(ctx)
	if err != nil {
		t.Fatalf("read the cursor saved at stop: %v", err)
	}
	if cur.Token != recCursor {
		t.Fatalf("saved cursor is %q, want the applied record's %q", cur.Token, recCursor)
	}
}

// appConfig returns a daemon config for an app on dsn. The other services it
// names are unreachable addresses, which building and starting the app never
// calls.
func appConfig(t *testing.T, dsn string) *config.Config {
	t.Helper()
	signer, err := ed25519.Generate()
	if err != nil {
		t.Fatalf("generate agent key: %v", err)
	}
	pemBytes, err := identity.EncodeSignerToPEM(signer)
	if err != nil {
		t.Fatalf("encode agent key: %v", err)
	}
	keyFile := filepath.Join(t.TempDir(), "agent.pem")
	if err := os.WriteFile(keyFile, pemBytes, 0o600); err != nil {
		t.Fatalf("write agent key: %v", err)
	}

	return &config.Config{
		Addr:             "127.0.0.1:0",
		DataDir:          t.TempDir(),
		PostgresDSN:      dsn,
		Identity:         config.IdentityConfig{KeyFile: keyFile},
		UploadServiceURL: "http://127.0.0.1:8000",
		UploadServiceDID: "did:web:upload.example",
		AuthServiceURL:   "http://127.0.0.1:7000",
		AuthServiceDID:   "did:web:auth.example",
		// A hilt-backed client refuses to build without proof chains, and the
		// provider runs eagerly here; one delegation satisfies it.
		AuthServiceProofs: encodedProofs(t, mintDelegation(t)),
		// The region key provider is a required dependency; inprocess needs
		// no external service (the constructor generates a throwaway KEK).
		RegionKey: config.RegionKeyConfig{Provider: "inprocess"},
		// The tenant key source is required too; it makes no network calls at
		// construction, so an unreachable directory is fine here.
		TenantKey: config.TenantKeyConfig{PLCDirectoryURL: "http://127.0.0.1:2582"},
	}
}

// mintDelegation returns one space→agent /blob/add delegation.
func mintDelegation(t *testing.T) ucan.Delegation {
	t.Helper()
	space, err := ed25519.GenerateIssuer()
	if err != nil {
		t.Fatalf("generate space: %v", err)
	}
	agent, err := ed25519.GenerateIssuer()
	if err != nil {
		t.Fatalf("generate agent: %v", err)
	}
	d, err := blobcmds.Add.Delegate(space, agent.DID(), space.DID(), delegation.WithNoExpiration())
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	return d
}

// encodedProofs renders the delegations as a base64 container, the inline
// form an auth_service_proofs config value accepts.
func encodedProofs(t *testing.T, dlgs ...ucan.Delegation) string {
	t.Helper()
	encoded, err := container.Encode(container.Base64, container.New(container.WithDelegations(dlgs...)))
	if err != nil {
		t.Fatalf("encode proofs container: %v", err)
	}
	return string(encoded)
}
