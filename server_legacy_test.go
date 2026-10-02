package ingot

import (
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestWarnLegacySpool(t *testing.T) {
	dir := t.TempDir()
	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	warnLegacySpool(logger, dir)
	if logs.Len() != 0 {
		t.Fatalf("warned without a spool directory: %v", logs.All())
	}

	if err := os.Mkdir(filepath.Join(dir, "spool"), 0o755); err != nil {
		t.Fatal(err)
	}
	warnLegacySpool(logger, dir)
	if logs.Len() != 1 {
		t.Fatalf("warnings = %d, want 1", logs.Len())
	}
	if _, err := os.Stat(filepath.Join(dir, "spool")); err != nil {
		t.Fatalf("the directory must be left in place: %v", err)
	}
}
