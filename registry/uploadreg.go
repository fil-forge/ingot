package registry

import (
	"context"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
)

// UploadRegistrationOp is what a queued row asks the upload service to do with
// a version's content entry.
type UploadRegistrationOp string

const (
	// UploadRegistrationAdd records a committed version as a content entry, so
	// the space counts it.
	UploadRegistrationAdd UploadRegistrationOp = "add"
	// UploadRegistrationRemove drops a retired version's entry, so the space
	// stops counting it. It does not touch the version's blobs — those are
	// released per digest by the reference index.
	UploadRegistrationRemove UploadRegistrationOp = "remove"
)

// UploadRegistration is one queued change to the upload service's
// content-entry list for a space: the outbox row the write path enqueues in
// the same transaction as the bucket-root CAS, and the registration sweeper
// drains.
//
// Seq is the commit order. The sweeper replays a key's changes in it, because
// out of order an add can land after the remove that retires it and leave the
// root counted for good.
type UploadRegistration struct {
	Seq       int64
	Bucket    string
	ObjectKey string
	Space     did.DID
	Root      cid.Cid
	Op        UploadRegistrationOp
	Attempts  int
	NextAt    time.Time
	// Proofs is the encoded UCAN container holding the delegation chain that
	// authorizes the invocation, captured from the request that committed the
	// version. The sweeper runs with no request to borrow authority from, and
	// the space may never be written again, so the row carries its own. It is
	// bearer authority for one space and one command; the row is deleted as
	// soon as the registration lands.
	Proofs []byte

	// DeadLetteredAt is set once the row has been taken out of the sweep, having
	// run out of usable authority with no way to renew it. A dead-lettered row
	// is never claimed and never holds its key's later rows back — one change
	// that can no longer be made leaves the count short by one, where leaving
	// it in the queue would freeze the whole key.
	DeadLetteredAt   *time.Time
	DeadLetterReason string
}

// UploadRegistrationStore is the outbox the registration sweeper drains
// (upload_registrations). Rows are enqueued by Registry.CASRootEnqueue, in the
// commit's own transaction; nothing else writes them.
type UploadRegistrationStore interface {
	// ListDueUploadRegistrations returns rows whose next_at has passed, in
	// commit order, at most limit.
	ListDueUploadRegistrations(ctx context.Context, now time.Time, limit int) ([]UploadRegistration, error)
	// DeleteUploadRegistrations drops the rows the upload service has taken,
	// in one statement. Idempotent, and a seq that is already gone is not an
	// error: a sweep that died between sending a change and recording it
	// replays the change rather than losing it.
	DeleteUploadRegistrations(ctx context.Context, seqs []int64) error
	// RefreshUploadRegistrationProofs replaces a row's stored authority with a
	// chain the sweeper has just renewed, so the renewal survives a restart
	// and later sweeps need not repeat it.
	RefreshUploadRegistrationProofs(ctx context.Context, seq int64, proofs []byte) error
	// DeadLetterUploadRegistrations takes rows out of the sweep for good, recording
	// why. Dead-lettered rows are neither claimed nor counted as blocking their
	// key.
	DeadLetterUploadRegistrations(ctx context.Context, seqs []int64, reason string) error
	// ListDeadLetteredUploadRegistrations returns the dead-lettered rows, oldest
	// first, for inspection or a manual replay.
	ListDeadLetteredUploadRegistrations(ctx context.Context, limit int) ([]UploadRegistration, error)
	// RescheduleUploadRegistrations records a failed attempt against each row
	// and holds them all until nextAt, in one statement. The sweeper groups
	// rows by the delay they have earned, so one call settles every row owed
	// the same wait. A key's later rows stay behind the rescheduled one, since
	// replaying a key out of order is what the queue exists to prevent.
	RescheduleUploadRegistrations(ctx context.Context, seqs []int64, nextAt time.Time) error
	// ListUploadRegistrationsBySpace returns a space's queued rows regardless
	// of next_at, for inspecting what a space still owes.
	ListUploadRegistrationsBySpace(ctx context.Context, space did.DID) ([]UploadRegistration, error)
	// DeleteUploadRegistrationsBySpace drops every row for a space, queued or
	// dead-lettered, and reports how many went. Bucket teardown calls it: the
	// space is about to be deleted, so its object count stops meaning
	// anything, and rows left behind would be retried against a space that no
	// longer exists, keep their bearer proofs on disk, and — if the bucket
	// name were reused — sit in front of the new bucket's registrations for
	// the same key.
	DeleteUploadRegistrationsBySpace(ctx context.Context, space did.DID) (int64, error)
}
