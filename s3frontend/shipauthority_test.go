package s3frontend

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

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
	mem := newTestStore(t)
	log, err := logstore.Open(ctx, logstore.Config{
		Dir:     filepath.Join(t.TempDir(), "segments"),
		Meta:    mem,
		Catalog: logstore.PlaneConfig{Ship: false},
		Logger:  zaptest.NewLogger(t),
	})
	require.NoError(t, err, "logstore")
	t.Cleanup(func() { _ = log.Close(ctx) })
	ships := &recordingShips{}
	b, _, _ := newRefTestBackendOver(t, mem, log, 0, func(d *Deps) { d.Ships = ships })
	putObj(t, b, "k", []byte("body"))

	store := ucanlib.ProofStore(tokenstore.NewMemStore())
	bucket, key := "bk", "k"
	_, err = b.DeleteObject(context.WithValue(ctx, reqscope.ProofStoreKey(), store),
		&s3.DeleteObjectInput{Bucket: &bucket, Key: &key})
	require.NoError(t, err, "DeleteObject")

	ships.mu.Lock()
	defer ships.mu.Unlock()
	require.Len(t, ships.stores, 2, "captures: want the put's commit and the delete's")
	require.Same(t, store, ships.stores[1], "the delete's commit didn't hand over its request's proof store")
}
