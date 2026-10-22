package s3frontend

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/tokenstore"
)

// recordingShips records the proof store each capture's ctx carried.
type recordingShips struct {
	mu     sync.Mutex
	stores []ucanlib.ProofStore
}

func (r *recordingShips) CaptureShipAuthority(ctx context.Context, _ did.DID) {
	store, _ := reqscope.ProofStore(ctx)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stores = append(r.stores, store)
}

// TestDeleteCapturesShipAuthority checks the wiring from Deps.Ships to the
// commit: a DeleteObject, which uploads no body, still hands its request's
// proof store to the ship uploader.
func TestDeleteCapturesShipAuthority(t *testing.T) {
	ctx := context.Background()
	mem := inmem.NewMemStore()
	log, err := logstore.Open(ctx, logstore.Config{
		Dir:     filepath.Join(t.TempDir(), "segments"),
		Meta:    mem,
		Catalog: logstore.PlaneConfig{Ship: false},
		Logger:  zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatalf("logstore: %v", err)
	}
	t.Cleanup(func() { _ = log.Close(ctx) })
	ships := &recordingShips{}
	b, _, _ := newRefTestBackendOver(t, mem, log, 0, func(d *Deps) { d.Ships = ships })
	putObj(t, b, "k", []byte("body"))

	store := ucanlib.ProofStore(tokenstore.NewMemStore())
	bucket, key := "bk", "k"
	if _, err := b.DeleteObject(context.WithValue(ctx, reqscope.ProofStoreKey(), store),
		&s3.DeleteObjectInput{Bucket: &bucket, Key: &key}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	ships.mu.Lock()
	defer ships.mu.Unlock()
	if n := len(ships.stores); n != 2 {
		t.Fatalf("captured %d times, want 2 (the put's commit and the delete's)", n)
	}
	if ships.stores[1] != store {
		t.Fatal("the delete's commit didn't hand over its request's proof store")
	}
}
