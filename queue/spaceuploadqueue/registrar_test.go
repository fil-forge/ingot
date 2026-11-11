package spaceuploadqueue

import (
	"context"
	"errors"
	"sync"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/ipfs/go-cid"

	"github.com/fil-forge/ingot/uploader"
)

// recordingRegistrar captures the roots registered and retracted, so a test
// can assert exactly which changes went out.
type recordingRegistrar struct {
	mu        sync.Mutex
	added     []cid.Cid
	retracted []cid.Cid
	// batches records the size of each round trip.
	batches []int
	// spent makes PrepareAuthority fail, standing in for an expired chain the
	// space cannot renew.
	spent bool
}

func (r *recordingRegistrar) roundTrips() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.batches...)
}

func (r *recordingRegistrar) CaptureAuthority(_ context.Context, _ did.DID, _ ucan.Command) ([]byte, error) {
	return container.Encode(container.RawGzip, container.New())
}

// PrepareAuthority keeps whatever the row holds unless spent is set, which
// stands in for a chain that has outlived hilt's delegation with no live
// authority to renew from.
func (r *recordingRegistrar) PrepareAuthority(_ context.Context, space did.DID, _ ucan.Command, stored []byte) ([]byte, bool, error) {
	r.mu.Lock()
	spent := r.spent
	r.mu.Unlock()
	if spent {
		return nil, false, errors.New("no proof store for space " + space.String())
	}
	return stored, false, nil
}

// spendAuthority makes every later PrepareAuthority fail, as it does for a
// space whose stored chain has expired and which has seen no recent write.
func (r *recordingRegistrar) spendAuthority() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spent = true
}

func (r *recordingRegistrar) RegisterUploads(_ context.Context, changes []uploader.QueuedUpload) ([]error, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, len(changes))
	for _, ch := range changes {
		r.added = append(r.added, ch.Root)
	}
	return make([]error, len(changes)), nil
}

func (r *recordingRegistrar) RetractUploads(_ context.Context, changes []uploader.QueuedUpload) ([]error, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, len(changes))
	for _, ch := range changes {
		r.retracted = append(r.retracted, ch.Root)
	}
	return make([]error, len(changes)), nil
}

func (r *recordingRegistrar) snapshot() (added, retracted []cid.Cid) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]cid.Cid(nil), r.added...), append([]cid.Cid(nil), r.retracted...)
}

// renewingRegistrar starts with spent authority and recovers once renew is
// called, standing in for a space that is written again while its queued
// changes are waiting.
type renewingRegistrar struct {
	recordingRegistrar
	renewals int
}

func (r *renewingRegistrar) PrepareAuthority(ctx context.Context, space did.DID, cmd ucan.Command, stored []byte) ([]byte, bool, error) {
	r.mu.Lock()
	spent := r.spent
	if !spent {
		r.renewals++
	}
	r.mu.Unlock()
	if spent {
		return nil, false, errors.New("no proof store for space " + space.String())
	}
	// Renewed: a different chain from the one the row holds.
	fresh, err := container.Encode(container.RawGzip, container.New())
	if err != nil {
		return nil, false, err
	}
	return fresh, true, nil
}

// failingRegistrar refuses the ops named in fail, so a test can watch a queued
// row survive a refusal and land on a later sweep.
type failingRegistrar struct {
	recordingRegistrar
	mu   sync.Mutex
	fail map[string]bool
}

func (r *failingRegistrar) refuse(op string, yes bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail == nil {
		r.fail = map[string]bool{}
	}
	r.fail[op] = yes
}

func (r *failingRegistrar) refuses(op string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fail[op]
}

func (r *failingRegistrar) RegisterUploads(ctx context.Context, changes []uploader.QueuedUpload) ([]error, error) {
	if r.refuses("add") {
		return refusals(len(changes)), nil
	}
	return r.recordingRegistrar.RegisterUploads(ctx, changes)
}

func (r *failingRegistrar) RetractUploads(ctx context.Context, changes []uploader.QueuedUpload) ([]error, error) {
	if r.refuses("remove") {
		return refusals(len(changes)), nil
	}
	return r.recordingRegistrar.RetractUploads(ctx, changes)
}

// refusals reports every change in a batch as refused, the way the upload
// service answers a batch it accepted but could not act on.
func refusals(n int) []error {
	out := make([]error, n)
	for i := range out {
		out[i] = errors.New("upload service unavailable")
	}
	return out
}
