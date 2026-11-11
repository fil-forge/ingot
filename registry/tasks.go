package registry

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/fil-forge/ingot/taskrunner"
)

// TaskSubmitter adds and cancels tasks inside a caller's transaction.
// *taskrunner.Runner satisfies it.
type TaskSubmitter interface {
	SubmitTx(ctx context.Context, tx pgx.Tx, s taskrunner.Spec) (taskrunner.TaskID, error)
	SubmitAllTx(ctx context.Context, tx pgx.Tx, specs []taskrunner.Spec) ([]taskrunner.TaskID, error)
	CancelTx(ctx context.Context, tx pgx.Tx, queueName string, ids ...taskrunner.TaskID) error
}
