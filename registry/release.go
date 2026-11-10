package registry

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/taskrunner"
)

// ReleaseQueue is the task queue a space's blobs are released on, and its
// multipart part blobs accepted on (BlobAcceptAction). Its subject is the
// space and the digest (ReleaseSubject), so at most one task on a blob in a
// space runs at a time, and a space's subjects share a prefix.
const ReleaseQueue = "queue-space-shard"

// ReleaseAction removes a blob nothing in the space claims any more from the
// space on the network, and its local rows and copy.
const ReleaseAction = "release"

// The queue-space-shard actions a write's use of a blob goes through. An add
// takes the write's pending reference to the blob, under the blob's subject,
// so it never overlaps a release; an expire drops the reference at its
// deadline if nothing took it over.
const (
	AddAction    = "add"
	ExpireAction = "expire"
)

// ReleaseSubject is the subject of the release of digest in space.
func ReleaseSubject(space did.DID, digest multihash.Multihash) string {
	return space.String() + "/" + hex.EncodeToString(digest)
}

// SpaceReleases is the subject pattern matching every task on the release
// queue in space, its releases and its blobs' accepts, for
// taskrunner.Runner.Expedite.
func SpaceReleases(space did.DID) string {
	return space.String() + "/*"
}

// ParseReleaseSubject returns the space and digest a release subject names.
func ParseReleaseSubject(subject string) (did.DID, multihash.Multihash, error) {
	i := strings.LastIndexByte(subject, '/')
	if i < 0 {
		return did.Undef, nil, fmt.Errorf("registry: release subject %q has no digest", subject)
	}
	space, err := did.Parse(subject[:i])
	if err != nil {
		return did.Undef, nil, fmt.Errorf("registry: release subject space: %w", err)
	}
	digest, err := hex.DecodeString(subject[i+1:])
	if err != nil {
		return did.Undef, nil, fmt.Errorf("registry: release subject digest: %w", err)
	}
	return space, digest, nil
}

// DueNow is the NotBefore of a release due at once: a second back, so a
// database clock a little behind this host's still finds it due, and a caller
// running it inline straight away does not find it not yet due.
func DueNow() time.Time {
	return time.Now().Add(-time.Second)
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
		return taskrunner.Spec{}, fmt.Errorf("registry: encode release: %w", err)
	}
	return taskrunner.Spec{
		Queue:     ReleaseQueue,
		Subject:   ReleaseSubject(space, digest),
		Action:    ReleaseAction,
		Args:      buf.Bytes(),
		NotBefore: &notBefore,
		Replace:   true,
	}, nil
}

// ReleaseTaskFrom decodes a release task's arguments.
func ReleaseTaskFrom(t taskrunner.Task) (ReleaseTask, error) {
	var args ReleaseTask
	if len(t.Args) == 0 {
		return args, nil
	}
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return ReleaseTask{}, fmt.Errorf("registry: decode release: %w", err)
	}
	return args, nil
}

// Statement is one write for a task to commit with its outcome
// (taskrunner.Exec.Write).
type Statement struct {
	SQL  string
	Args []any
}

// ReleasedRows are the writes that forget a blob in space once its release
// has taken it off the network: its park and location rows. A release commits
// them with its outcome, so nothing between the network step and the commit
// can fail and leave a blob gone from the provider still located here.
func ReleasedRows(space did.DID, digest multihash.Multihash) []Statement {
	return []Statement{
		{`DELETE FROM ingot.blob_parks WHERE space = $1 AND digest = $2`, []any{space, []byte(digest)}},
		{`DELETE FROM ingot.blob_locations WHERE space = $1 AND digest = $2`, []any{space, []byte(digest)}},
	}
}

// ReleasedIntent is the write that forgets the space's upload intent for a
// released blob, the last trace of how far it got.
func ReleasedIntent(space did.DID, digest multihash.Multihash) Statement {
	return Statement{`DELETE FROM ingot.upload_intents WHERE space = $1 AND digest = $2`, []any{space, []byte(digest)}}
}

// PendingRef is the write that records a pending reference to digest in
// space, taken by the queue-space-shard.add add and dropped at deadline by
// the queue-space-shard.expire expire unless a claim or a part row takes it
// over first.
func PendingRef(add, expire taskrunner.TaskID, space did.DID, digest multihash.Multihash, deadline time.Time) Statement {
	return Statement{
		`INSERT INTO ingot.blob_pending_refs (add_task, expire_task, space, digest, deadline) VALUES ($1, $2, $3, $4, $5)`,
		[]any{add, expire, space, []byte(digest), deadline},
	}
}

// DroppedPendingRef is the write that drops the pending reference whose
// expiry is expire.
func DroppedPendingRef(expire taskrunner.TaskID) Statement {
	return Statement{`DELETE FROM ingot.blob_pending_refs WHERE expire_task = $1`, []any{expire}}
}
