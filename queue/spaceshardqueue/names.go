package spaceshardqueue

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
)

// Name is queue-space-shard, the task queue of the tasks on one blob in one
// space: a write's add of it and the expiry of the reference the add took,
// its accept and its release. Its subject is the space and the digest
// (Subject), so at most one task on a blob in a space runs at a time, and a
// space's subjects share a prefix.
const Name = "queue-space-shard"

// The queue-space-shard actions. An add takes a write's pending reference to
// the blob, under the blob's subject, so it never overlaps a release; an
// expire drops the reference at its deadline if nothing took it over. An
// accept concludes a parked blob on the upload service and marks accepted
// the parts it completes. A release removes a blob nothing in the space
// claims any more from the space on the network, and its local rows and copy.
const (
	AddAction     = "add"
	ExpireAction  = "expire"
	AcceptAction  = "accept"
	ReleaseAction = "release"
)

// Subject is the subject of the tasks on digest in space.
func Subject(space did.DID, digest multihash.Multihash) string {
	return space.String() + "/" + hex.EncodeToString(digest)
}

// SpaceSubjects is the subject pattern matching every queue-space-shard task
// in space, for taskrunner.Runner.Expedite.
func SpaceSubjects(space did.DID) string {
	return space.String() + "/*"
}

// ParseSubject returns the space and digest a queue-space-shard subject
// names.
func ParseSubject(subject string) (did.DID, multihash.Multihash, error) {
	i := strings.LastIndexByte(subject, '/')
	if i < 0 {
		return did.Undef, nil, fmt.Errorf("spaceshardqueue: subject %q has no digest", subject)
	}
	space, err := did.Parse(subject[:i])
	if err != nil {
		return did.Undef, nil, fmt.Errorf("spaceshardqueue: subject space: %w", err)
	}
	digest, err := hex.DecodeString(subject[i+1:])
	if err != nil {
		return did.Undef, nil, fmt.Errorf("spaceshardqueue: subject digest: %w", err)
	}
	return space, digest, nil
}

// ReleaseTask is a release's arguments: the authority it removes the blob
// with, captured from the request that left it unclaimed, since the release
// runs after that request has gone. Either may be empty, and the release then
// borrows authority the space captured since.
type ReleaseTask struct {
	// RemoveProofs is an encoded UCAN container holding the chain for
	// /blob/remove on the space.
	RemoveProofs []byte
	// AbortProofs is the same for /blob/abort, for a blob still parked.
	AbortProofs []byte
}

// Spec returns the task that releases digest in space at notBefore. It is a
// replacement, so of two releases of one blob pending, the later is kept: a
// reader of the version that dropped the claim keeps its grace.
func (args ReleaseTask) Spec(space did.DID, digest multihash.Multihash, notBefore time.Time) (taskrunner.Spec, error) {
	var buf bytes.Buffer
	if err := args.MarshalCBOR(&buf); err != nil {
		return taskrunner.Spec{}, fmt.Errorf("spaceshardqueue: encode release: %w", err)
	}
	return taskrunner.Spec{
		Queue:     Name,
		Subject:   Subject(space, digest),
		Action:    ReleaseAction,
		Args:      buf.Bytes(),
		NotBefore: &notBefore,
		Replace:   true,
	}, nil
}

// releaseTaskFrom decodes a release task's arguments.
func releaseTaskFrom(t taskrunner.Task) (ReleaseTask, error) {
	var args ReleaseTask
	if len(t.Args) == 0 {
		return args, nil
	}
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return ReleaseTask{}, fmt.Errorf("spaceshardqueue: decode release: %w", err)
	}
	return args, nil
}

// AcceptSpec returns the accept of digest in space an upload's accept
// queues: a replacement due now, so a blob has at most one pending, however
// many parts or uploads name it.
func AcceptSpec(space did.DID, digest multihash.Multihash) taskrunner.Spec {
	now := registry.DueNow()
	return taskrunner.Spec{
		Queue:     Name,
		Subject:   Subject(space, digest),
		Action:    AcceptAction,
		NotBefore: &now,
		Replace:   true,
	}
}

// OwedSpecs returns the tasks w queues: its spawns, then the releases it
// owes, carrying release's authority.
func OwedSpecs(w registry.CommitWrites, release ReleaseTask) ([]taskrunner.Spec, error) {
	specs := append([]taskrunner.Spec(nil), w.Spawns...)
	for _, r := range w.Releases {
		spec, err := release.Spec(r.Space, r.Digest, r.At)
		if err != nil {
			return nil, err
		}
		specs = append(specs, spec)
	}
	return specs, nil
}

// ApplyWrites adds w to the commit of the task ex runs (registry
// CommitWrites.Apply), with the releases w owes, carrying release's
// authority, and the cancels of the expiries of the pending references its
// claims take over.
func ApplyWrites(ctx context.Context, ex *taskrunner.Exec, w registry.CommitWrites, release ReleaseTask) error {
	specs, err := OwedSpecs(w, release)
	if err != nil {
		return err
	}
	w.Spawns = specs
	if err := w.Apply(ctx, ex); err != nil {
		return err
	}
	return ex.Cancel(ctx, Name, w.Expiries...)
}
