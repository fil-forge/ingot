package taskrunner

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

// drained is what a drain did.
type drained struct {
	// ran is set when the target task reached an outcome, which res and err
	// hold.
	ran bool
	res []byte
	err error
	// full is set when the drain stopped at DrainLimit with tasks still due.
	full bool
	// stop is set when a tree ended without an outcome: it gave way to an
	// older one (*dieError), ctx ended, or its commit kept failing
	// (errNotCommitted). The drain stops there.
	stop error
}

// yieldError stops a drain at a sync task whose caller is waiting to run it.
type yieldError struct{ id TaskID }

func (e *yieldError) Error() string { return fmt.Sprintf("left task %s to its waiting caller", e.id) }

// drain runs the subject's due tasks under h, oldest first, up to the task
// with seq upTo. It stops early after running target, and before a sync task
// other than target whose caller is waiting, which it leaves to the caller.
func (r *Runner) drain(ctx context.Context, q *queue, h *hold, upTo int64, target TaskID) drained {
	for n := 0; n < r.opts.DrainLimit; n++ {
		if h.lost.Load() {
			return drained{stop: errLeaseLost}
		}
		tasks, err := r.claim(ctx, q, h.subject, upTo, target)
		if errors.Is(err, pgx.ErrNoRows) {
			return drained{}
		}
		if err != nil {
			return drained{stop: err}
		}
		res, terr, stop := r.runRoot(ctx, h, tasks)
		if stop != nil {
			return drained{stop: stop}
		}
		if tasks[0].ID == target {
			return drained{ran: true, res: res, err: terr}
		}
	}
	return drained{full: true}
}

// pollLoop hands due, free subjects to workers, up to the queue's worker
// count, every PollInterval, whenever a task is submitted to the queue in
// this process, and whenever a worker frees up with more due (see work).
func (r *Runner) pollLoop(ctx context.Context, q *queue) {
	defer r.wg.Done()
	t := time.NewTicker(r.opts.PollInterval)
	defer t.Stop()
	for {
		r.dispatch(ctx, q)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-q.poke:
		}
	}
}

func (r *Runner) dispatch(ctx context.Context, q *queue) {
	r.mu.Lock()
	free := q.workers - len(q.inflight)
	busy := make([]string, 0, len(q.inflight))
	for s := range q.inflight {
		busy = append(busy, s)
	}
	r.mu.Unlock()
	if free <= 0 {
		return
	}
	// A subject whose oldest due task has a waiting Run caller is the
	// caller's to take. The due-task index (dueIndex) keeps this to the due
	// rows.
	rows, err := r.db.Query(ctx, `SELECT d.subject FROM (
			SELECT DISTINCT ON (subject) subject, seq, waiting_until, retry_at FROM `+q.tasks+`
			WHERE state = 'pending' AND (not_before IS NULL OR not_before <= now())
			AND NOT (subject = ANY($1))
			ORDER BY subject, seq
		) d
		WHERE (d.waiting_until IS NULL OR d.waiting_until <= now())
		AND (d.retry_at IS NULL OR d.retry_at <= now())
		AND NOT EXISTS (SELECT 1 FROM `+q.leases+` l WHERE l.subject = d.subject AND l.expires >= now())
		ORDER BY d.seq LIMIT $2`, busy, free)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("poll for due tasks", zapQueue(q.name), zapErr(err))
		}
		return
	}
	subjects, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("poll for due tasks", zapQueue(q.name), zapErr(err))
		}
		return
	}
	r.mu.Lock()
	q.saturated = len(subjects) == free
	r.mu.Unlock()
	for _, s := range subjects {
		r.mu.Lock()
		q.inflight[s] = true
		r.mu.Unlock()
		r.wg.Add(1)
		go r.work(ctx, q, s)
	}
}

// work takes a subject's lease and drains it. It polls again once it frees
// its worker if the subject has more due, or the last poll had more due
// subjects than free workers, so a backlog drains as fast as the workers run
// it rather than a pollful per PollInterval.
func (r *Runner) work(ctx context.Context, q *queue, subject string) {
	defer r.wg.Done()
	more := false
	defer func() {
		r.mu.Lock()
		delete(q.inflight, subject)
		more = more || q.saturated
		r.mu.Unlock()
		if more {
			r.pokeQueue(q)
		}
	}()
	h, _, err := r.tryLease(ctx, q, subject, treeAge{created: time.Now()})
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("take lease", zapSubject(q.name, subject), zapErr(err))
		}
		return
	}
	if h == nil {
		return
	}
	d := r.drain(ctx, q, h, math.MaxInt64, TaskID{})
	r.endHold(ctx, h, d.stop)
	var die *dieError
	var yield *yieldError
	switch stop := d.stop; {
	case stop == nil:
		more = d.full
	case errors.As(stop, &yield), errors.Is(stop, errLeaseLost), errors.Is(stop, errNotPending):
	case errors.As(stop, &die):
		// Keep the subject out of the next poll until the older tree that
		// held it up lets go.
		r.awaitFree(ctx, die.key, r.opts.PollInterval)
		more = true
	case errors.Is(stop, errNotCommitted), ctx.Err() != nil:
	default:
		r.log.Warn("run tasks", zapSubject(q.name, subject), zapErr(stop))
	}
}

func zapErr(err error) zap.Field       { return zap.Error(err) }
func zapQueue(q string) zap.Field      { return zap.String("queue", q) }
func zapInt(k string, v int) zap.Field { return zap.Int(k, v) }
func zapSubject(q, s string) zap.Field {
	return zap.Dict("subject", zap.String("queue", q), zap.String("subject", s))
}
func zapTask(t Task) zap.Field {
	return zap.Dict("task", zap.String("queue", t.Queue), zap.String("action", t.Action),
		zap.String("subject", t.Subject), zap.Stringer("id", t.ID))
}
