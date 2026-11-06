package ingot

import (
	"path/filepath"
	"testing"

	"github.com/fil-forge/libforge/identity"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/s3frontend"
)

// TestTaskRunnerHasTheBackendsQueues: the daemon's runner configures every
// queue the backend registers handlers on, so startup does not fail at
// registration, and is named after the agent's DID for placement.
func TestTaskRunnerHasTheBackendsQueues(t *testing.T) {
	id, err := identity.New("", "did:web:ingot.test")
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	r, err := provideTaskRunner(nil, id, zap.NewNop())
	if err != nil {
		t.Fatalf("provideTaskRunner: %v", err)
	}
	cache, err := blockstore.NewBlobCache(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("NewBlobCache: %v", err)
	}
	b := s3frontend.New(s3frontend.Deps{Cache: cache})
	// Its gauges are on the global meter provider, which a later test may
	// install a reader behind.
	t.Cleanup(func() { _ = b.CloseMetrics() })
	if err := b.RegisterUploadTasks(r); err != nil {
		t.Fatalf("RegisterUploadTasks: %v", err)
	}
	if err := b.RegisterShardTasks(r); err != nil {
		t.Fatalf("RegisterShardTasks: %v", err)
	}
	if err := b.RegisterReleaseTasks(r); err != nil {
		t.Fatalf("RegisterReleaseTasks: %v", err)
	}
	if err := b.RegisterAcceptTasks(r); err != nil {
		t.Fatalf("RegisterAcceptTasks: %v", err)
	}
	if got := r.Node(); got != "did:web:ingot.test" {
		t.Fatalf("runner node = %q, want the agent's DID", got)
	}
}
