package registry_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/fil-forge/libforge/testutil"

	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/registry"
)

// Of many concurrent latches of one session, exactly one wins.
func TestMultipartLatchHasOneWinner(t *testing.T) {
	ctx := t.Context()
	r := registry.NewPostgres(pgtest.New(t))
	space := testutil.RandomDID(t)
	if err := r.Create(ctx, "b", space, registry.CreateState{Tenant: testutil.RandomDID(t)}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	const id = "upl-race"
	if err := r.CreateSession(ctx, registry.MultipartSession{UploadID: id, Bucket: "b", Space: space, ObjectKey: "k"}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	const racers = 32
	var winners atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			won, err := r.LatchSession(ctx, id, registry.SessionOpen, registry.SessionCompleting)
			if err != nil {
				t.Errorf("LatchSession: %v", err)
			}
			if won {
				winners.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if n := winners.Load(); n != 1 {
		t.Fatalf("latch winners = %d, want exactly 1", n)
	}
}
