package taskrunnertest_test

import (
	"context"
	"strings"
	"testing"

	"github.com/fil-forge/ingot/taskrunner"
	"github.com/fil-forge/ingot/taskrunner/taskrunnertest"
)

// counterCase is a tree whose child adds one to a counter and whose parent
// then reports the counter, read either from before the tree ran or, wrongly,
// after its own child's write.
func counterCase(readOwnWrite bool) taskrunnertest.Case {
	var counters string
	return taskrunnertest.Case{
		Queues: []taskrunner.QueueOptions{{Name: "queue-parent"}, {Name: "queue-child"}},
		Setup: func(ctx context.Context, env taskrunnertest.Env, r *taskrunner.Runner) error {
			counters = env.Table("counters")
			if _, err := env.DB.Exec(ctx, `CREATE TABLE `+counters+` (n int NOT NULL); INSERT INTO `+counters+` VALUES (0)`); err != nil {
				return err
			}
			if err := r.Handle("queue-child", "add", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
				return nil, ex.Write(ctx, `UPDATE `+counters+` SET n = n + 1`)
			}); err != nil {
				return err
			}
			return r.Handle("queue-parent", "report", func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
				var before string
				if !readOwnWrite {
					if err := ex.DB().QueryRow(ctx, `SELECT n::text FROM `+counters).Scan(&before); err != nil {
						return nil, err
					}
				}
				if _, err := ex.Run(ctx, taskrunner.Spec{Queue: "queue-child", Subject: "c", Action: "add"}); err != nil {
					return nil, err
				}
				if readOwnWrite {
					err := ex.DB().QueryRow(ctx, `SELECT n::text FROM `+counters).Scan(&before)
					return []byte(before), err
				}
				return []byte(before), nil
			})
		},
		Spec: taskrunner.Spec{Queue: "queue-parent", Subject: "p", Action: "report"},
		State: func(ctx context.Context, env taskrunnertest.Env) (any, error) {
			var n int
			err := env.DB.QueryRow(ctx, `SELECT n FROM `+counters).Scan(&n)
			return n, err
		},
	}
}

func TestCheckOwnWritesPassesAHandlerThatReadsFirst(t *testing.T) {
	taskrunnertest.CheckOwnWrites(t, counterCase(false))
}

func TestCompareOwnWritesCatchesAHandlerThatReadsItsOwnWrite(t *testing.T) {
	err := taskrunnertest.CompareOwnWrites(context.Background(),
		func() taskrunnertest.Env { return taskrunnertest.NewEnv(t) }, counterCase(true))
	if err == nil || !strings.Contains(err.Error(), "reads its own tree's writes") {
		t.Fatalf("CompareOwnWrites error %v, want a reported difference", err)
	}
}
