package registry

import (
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/taskrunner"
)

// DueNow is the NotBefore of a release due at once: a second back, so a
// database clock a little behind this host's still finds it due, and a caller
// running it inline straight away does not find it not yet due.
func DueNow() time.Time {
	return time.Now().Add(-time.Second)
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
