package registry

import (
	"bytes"
	"context"
	"fmt"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5"

	"github.com/fil-forge/ingot/taskrunner"
)

// UploadRegistrationOp is what a queued registration asks the upload service
// to do with a version's content entry. It is the registration task's action.
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

// UploadQueue is the task queue upload registrations run on. Its subject is
// the space, so a space's changes are sent in commit order: out of order, an
// add can land after the remove that retires it and leave the root counted for
// good.
const UploadQueue = "queue-space-upload"

// UploadRegistration is one change to the upload service's content-entry list
// for a space. A commit queues it as a task in its own transaction
// (RootCommit.Registrations), so the change exists exactly when the version it
// describes committed.
type UploadRegistration struct {
	// Bucket and ObjectKey name the object the change belongs to, for logs.
	Bucket    string
	ObjectKey string
	// Space is the space whose count changes, and the task's subject.
	Space did.DID
	// Root is the version's manifest, the content entry's root.
	Root cid.Cid
	// Op says whether the entry is added or removed.
	Op UploadRegistrationOp
	// Proofs is the encoded UCAN container holding the delegation chain that
	// authorizes the invocation, captured from the request that committed the
	// version. The task runs with no request to borrow authority from, and
	// the space may never be written again, so it carries its own. It is
	// bearer authority for one space and one command.
	Proofs []byte
}

// UploadTask is a queued registration's arguments: everything but the space,
// which is the task's subject, and the op, which is its action.
type UploadTask struct {
	Bucket    string
	ObjectKey string
	Root      []byte
	Proofs    []byte
}

// Spec returns the task that carries reg.
func (reg UploadRegistration) Spec() (taskrunner.Spec, error) {
	var buf bytes.Buffer
	args := UploadTask{Bucket: reg.Bucket, ObjectKey: reg.ObjectKey, Root: reg.Root.Bytes(), Proofs: reg.Proofs}
	if err := args.MarshalCBOR(&buf); err != nil {
		return taskrunner.Spec{}, fmt.Errorf("registry: encode upload registration: %w", err)
	}
	return taskrunner.Spec{Queue: UploadQueue, Subject: reg.Space.String(), Action: string(reg.Op), Args: buf.Bytes()}, nil
}

// UploadRegistrationFromTask decodes the registration a task carries.
func UploadRegistrationFromTask(t taskrunner.Task) (UploadRegistration, error) {
	var args UploadTask
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return UploadRegistration{}, fmt.Errorf("registry: decode upload registration: %w", err)
	}
	space, err := did.Parse(t.Subject)
	if err != nil {
		return UploadRegistration{}, fmt.Errorf("registry: upload registration space: %w", err)
	}
	_, root, err := cid.CidFromBytes(args.Root)
	if err != nil {
		return UploadRegistration{}, fmt.Errorf("registry: upload registration root: %w", err)
	}
	return UploadRegistration{
		Bucket:    args.Bucket,
		ObjectKey: args.ObjectKey,
		Space:     space,
		Root:      root,
		Op:        UploadRegistrationOp(t.Action),
		Proofs:    args.Proofs,
	}, nil
}

// TaskSubmitter adds tasks inside a caller's transaction. *taskrunner.Runner
// satisfies it.
type TaskSubmitter interface {
	SubmitTx(ctx context.Context, tx pgx.Tx, s taskrunner.Spec) (taskrunner.TaskID, error)
}

// AdoptUploadRegistrations moves the rows left in the upload-registration
// outbox, which registrations went through before they were tasks, onto the
// task queue in one transaction, oldest first. Rows already taken out of the
// old queue for good are left where they are, as the record of a change that
// was never made. It returns how many it moved.
func (r *Postgres) AdoptUploadRegistrations(ctx context.Context, ts TaskSubmitter) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("registry: adopt upload registrations: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	rows, err := tx.Query(ctx, `SELECT seq, bucket, object_key, space, root, op, proofs
		FROM ingot.upload_registrations WHERE dead_lettered_at IS NULL ORDER BY seq FOR UPDATE`)
	if err != nil {
		return 0, fmt.Errorf("registry: adopt upload registrations: %w", err)
	}
	var regs []UploadRegistration
	var seqs []int64
	for rows.Next() {
		var seq int64
		var space, op string
		var root []byte
		var reg UploadRegistration
		if err := rows.Scan(&seq, &reg.Bucket, &reg.ObjectKey, &space, &root, &op, &reg.Proofs); err != nil {
			rows.Close()
			return 0, fmt.Errorf("registry: adopt upload registrations: %w", err)
		}
		if reg.Space, err = did.Parse(space); err != nil {
			rows.Close()
			return 0, fmt.Errorf("registry: adopt upload registration %d: space: %w", seq, err)
		}
		if _, reg.Root, err = cid.CidFromBytes(root); err != nil {
			rows.Close()
			return 0, fmt.Errorf("registry: adopt upload registration %d: root: %w", seq, err)
		}
		reg.Op = UploadRegistrationOp(op)
		regs = append(regs, reg)
		seqs = append(seqs, seq)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("registry: adopt upload registrations: %w", err)
	}
	for _, reg := range regs {
		spec, err := reg.Spec()
		if err != nil {
			return 0, err
		}
		if _, err := ts.SubmitTx(ctx, tx, spec); err != nil {
			return 0, fmt.Errorf("registry: adopt upload registrations: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM ingot.upload_registrations WHERE seq = ANY($1)`, seqs); err != nil {
		return 0, fmt.Errorf("registry: adopt upload registrations: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("registry: adopt upload registrations: %w", err)
	}
	return len(regs), nil
}
