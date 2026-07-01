package cmd

import (
	"testing"

	"go.uber.org/zap/zaptest"
)

// TestStandaloneApp_GraphValidates ensures the standalone (no-Forge) fx graph
// is complete — every collaborator ingot.ServerModule consumes is provided,
// including the ones added by the data-plane inversion (registry.IntentStore
// and uploader.BodyUploader). fx resolves the lifecycle invoke eagerly at
// fx.New, so app.Err() surfaces any missing provider here without binding a
// listener or running migrations. Guards cmd/serve.go's provider list from
// drifting away from what ServerModule needs.
func TestStandaloneApp_GraphValidates(t *testing.T) {
	cfg := &DaemonConfig{}
	cfg.Addr = "127.0.0.1:0"
	cfg.DataDir = t.TempDir()
	cfg.RootAccess = "key"
	cfg.RootSecret = "secret"

	app, err := standaloneApp(cfg, zaptest.NewLogger(t))
	if err != nil {
		t.Fatalf("standalone fx graph does not validate: %v", err)
	}
	if app == nil {
		t.Fatal("standaloneApp returned a nil app")
	}
}
