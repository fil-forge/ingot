package s3frontend

import (
	"context"

	"github.com/fil-forge/ucantone/did"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot/queue/bucketqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
	uploadcmds "github.com/fil-forge/libforge/commands/upload"
)

// commit runs a queue-bucket.commit of changes to st's bucket and waits for
// it, with the request's authority for the tasks the commit spawns: the
// releases of the claims it drops and the upload registrations it queues.
func (b *Backend) commit(ctx context.Context, st *registry.State, changes ...bucketqueue.KeyChange) (bucketqueue.Outcome, error) {
	out, err := b.commits.Commit(ctx, st, b.commitAuthority(ctx, st.Space), changes...)
	if err != nil {
		return bucketqueue.Outcome{}, mapCommitError(err, "commit")
	}
	return out, nil
}

// commitOne is commit of one change, returning its result or the S3 error
// it failed with.
func (b *Backend) commitOne(ctx context.Context, st *registry.State, change bucketqueue.KeyChange, op string) (bucketqueue.ChangeResult, registry.VersioningState, error) {
	out, err := b.commit(ctx, st, change)
	if err != nil {
		return bucketqueue.ChangeResult{}, "", err
	}
	return oneResult(out, op)
}

// commitOneChild is commitOne run as a child of the task ex runs: a
// Complete commits its object with its own writes.
func (b *Backend) commitOneChild(ctx context.Context, ex *taskrunner.Exec, st *registry.State, change bucketqueue.KeyChange, op string) (bucketqueue.ChangeResult, registry.VersioningState, error) {
	out, err := b.commits.CommitChild(ctx, ex, st, b.commitAuthority(ctx, st.Space), change)
	if err != nil {
		return bucketqueue.ChangeResult{}, "", mapCommitError(err, "commit")
	}
	return oneResult(out, op)
}

// oneResult is a one-change commit's result, or the S3 error the change
// failed with.
func oneResult(out bucketqueue.Outcome, op string) (bucketqueue.ChangeResult, registry.VersioningState, error) {
	res := out.Results[0]
	if res.Err != nil {
		return res, out.Versioning, mapCommitError(res.Err, op)
	}
	return res, out.Versioning, nil
}

// commitAuthority captures the request's authority for the tasks a commit
// spawns. A command the request holds no chain for is left empty.
func (b *Backend) commitAuthority(ctx context.Context, space did.DID) bucketqueue.Authority {
	auth := bucketqueue.Authority{Release: b.releaseAuthority(ctx, space)}
	var err error
	if auth.RegisterAdd, err = b.proofs.Capture(ctx, space, uploadcmds.Add.Command); err != nil {
		b.logger.Debug("commit: no /upload/add authority to queue registrations with", zap.Stringer("space", space), zap.Error(err))
	}
	if auth.RegisterRemove, err = b.proofs.Capture(ctx, space, uploadcmds.Remove.Command); err != nil {
		b.logger.Debug("commit: no /upload/remove authority to queue retractions with", zap.Stringer("space", space), zap.Error(err))
	}
	return auth
}
