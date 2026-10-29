package cmd

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multiformats/go-multihash"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"

	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/ingot/registry"
)

// TestApp_GraphValidates ensures the fx graph is complete — every collaborator
// ingot.ServerModule consumes is provided. fx resolves the lifecycle invoke
// eagerly at fx.New, so app.Err() surfaces any missing provider without
// binding a listener or running migrations. Guards cmd/serve.go's provider
// list from drifting away from what ServerModule needs.
//
// buildApp dials and pings Postgres as it builds the graph, so this
// runs only when INGOT_TEST_DSN names a reachable database (same gate as
// registry's live test). The always-on, DB-free graph validation lives in the
// root package's TestModuleValidate_* (fx.ValidateApp over ingot.Module).
func TestApp_GraphValidates(t *testing.T) {
	dsn := os.Getenv("INGOT_TEST_DSN")
	if dsn == "" {
		t.Skip("set INGOT_TEST_DSN to run the buildApp graph test (buildApp pings Postgres)")
	}

	app, err := buildApp(t.Context(), appConfig(t, dsn), zaptest.NewLogger(t))
	if err != nil {
		t.Fatalf("fx graph does not validate: %v", err)
	}
	if app == nil {
		t.Fatal("buildApp returned a nil app")
	}
}

// TestApp_LiveStopSavesRevocationCursor starts the app with the revocation
// consumer reading a stub firehose, stops it as soon as the consumer has
// applied a record, and checks that the record's cursor was saved. The
// consumer saves at stop whatever its periodic save has not, which lands
// only if the pool outlives the consumer: this pins buildApp closing the
// pool after everything that uses it.
//
// The stop beats the consumer's first periodic save by a wide margin, so a
// pool that closes first fails the test; a slow enough runner would let the
// periodic save land first and hide that, but never fail a correct build.
// The app runs in a scratch database: its background sweeps would otherwise
// act on rows other packages' live tests write to the shared one.
func TestApp_LiveStopSavesRevocationCursor(t *testing.T) {
	dsn := os.Getenv("INGOT_TEST_DSN")
	if dsn == "" {
		t.Skip("set INGOT_TEST_DSN to run the app shutdown test")
	}
	dsn = scratchDatabase(t, dsn, "ingot_cmd_shutdown")

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
	// The stub sends the record, then holds the stream open as an idle feed.
	firehose := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintf(w, "event: revocation\ndata: %s\n\n", payload.String())
		w.(http.Flusher).Flush()
		<-r.Context().Done()
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

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	cur, err := registry.NewPostgres(pool).GetRevocationCursor(ctx)
	if err != nil {
		t.Fatalf("read the cursor saved at stop: %v", err)
	}
	if !cur.Revoke.Equals(rec.Revoke) {
		t.Fatalf("saved cursor is at %s, want the applied record %s", cur.Revoke, rec.Revoke)
	}
}

// scratchDatabase creates a database named name on the server dsn names,
// dropping it again at cleanup, and returns dsn pointed at it. dsn must be a
// postgres:// URL.
func scratchDatabase(t *testing.T, dsn, name string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Skip("INGOT_TEST_DSN is not a postgres:// URL; this test needs one to name a scratch database")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(admin.Close)
	if _, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
		t.Fatalf("drop stale scratch database: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create scratch database: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	u.Path = "/" + name
	return u.String()
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
