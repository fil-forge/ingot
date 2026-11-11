# taskrunner

Durable task queues in Postgres. At most one task per (queue, subject) runs at a time, and a task's database writes, the tasks it spawns and its completion commit together in one transaction once its handlers have run. Tasks survive a crash: a task is recorded before it runs, and one interrupted part way is run again once its lease expires.

The package imports nothing else from Ingot. The design is in the [task runner RFC](https://github.com/fil-one/RFC/pull/40).

## Setting up

Queues are listed in order. A task may run sync children only in queues later in the list. Each queue gets a tasks table and a leases table in the schema given.

```go
r, err := taskrunner.New(pool, taskrunner.Options{
	Schema: "ingot",
	Queues: []taskrunner.QueueOptions{
		{Name: "queue-bucket", Workers: 4},
		{Name: "queue-space-shard", Workers: 8},
		{Name: "queue-shard", Workers: 4},
	},
	Logger: logger,
})
if err != nil {
	return err
}
// Register every handler before Start.
if err := r.Handle("queue-shard", "evict", evict); err != nil {
	return err
}
if err := r.Migrate(ctx); err != nil {
	return err
}
if err := r.Start(ctx); err != nil {
	return err
}
defer r.Stop(ctx)
```

`Workers` is how many of a queue's async tasks this process runs at once; zero runs none and leaves them to other processes. A sync task runs in its caller's goroutine, but only once the tasks due ahead of it on its subject have run, and those need workers on some instance. `Migrate` creates or updates every queue's tables, and several processes may call it at once at startup.

## Handlers

A handler gets the task and an `*Exec`, which records the task's writes and spawns and runs its sync children. Arguments and results are bytes the runner never reads; Ingot encodes them with cbor-gen.

```go
func evict(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
	var path string
	err := ex.DB().QueryRow(ctx, `SELECT path FROM ingot.cached_blobs WHERE digest = $1`, task.Subject).Scan(&path)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // already evicted
	}
	if err != nil {
		return nil, err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	return nil, ex.Write(ctx, `DELETE FROM ingot.cached_blobs WHERE digest = $1`, task.Subject)
}
```

Four rules follow from how the runner works:

- **Writes go through `ex.Write`, and are not visible until the commit.** Read through `ex.DB()`, and decide from what was there before the task wrote anything. Never re-read a row your own task, or one of its children, has written.
- **Handlers must be idempotent.** Effects outside Postgres (a file removed, a network call) happen before the commit, so a crash between the two runs them again.
- **Return any error before the first effect outside Postgres.** After that, fail only for something a retry cannot fix.
- **A sync child must be in a later queue,** and no task may spawn its own queue and action.

### Conditional writes

A write that only makes sense if an earlier read still holds is conditional. If it affects no rows, the commit fails and the whole task runs again from scratch, up to `Options.CommitAttempts` times (3 by default). After that the task stays pending and is resumed when its lease expires.

```go
err := ex.WriteConditional(ctx,
	`UPDATE ingot.shard_refs SET owned = true WHERE space = $1 AND digest = $2 AND NOT owned`,
	space, digest)
```

## Submitting tasks

### Async

`Submit` records a task and returns at once. A worker runs it when it is due and its subject is free.

```go
id, err := r.Submit(ctx, taskrunner.Spec{
	Queue:   "queue-space-upload",
	Subject: space,
	Action:  "add",
	Args:    args,
})
```

`SubmitTx` records it inside a transaction of your own, so it commits or rolls back with your writes. This is how code that does not run as a task yet can hand work to the runner.

```go
tx, err := pool.Begin(ctx)
// ... your own writes on tx ...
if _, err := r.SubmitTx(ctx, tx, spec); err != nil {
	return err
}
return tx.Commit(ctx)
```

### Sync

`Run` records a task and runs it in the calling goroutine once it is its subject's oldest due task and the subject's lease is free. It runs no other task: those due ahead of it are left to the queue's workers, on this instance or another, so the task's `Expires` bounds the caller's whole wait. While a task is due ahead of it the caller waits for a worker in this process to release the subject, or for the task to be cancelled here, and looks again by itself once every `PollInterval`, for a task that another instance runs or cancels. It returns once the task has an outcome: its result, the handler's error, or `ErrExpired`. A commit that fails is retried until it lands or the task's `Expires` passes, so `ErrExpired` means nothing was written.

```go
expires := time.Now().Add(30 * time.Second)
res, err := r.Run(ctx, taskrunner.Spec{Queue: "queue-bucket", Subject: bucket, Action: "commit", Args: args, Expires: &expires})
if errors.Is(err, taskrunner.ErrExpired) {
	// Nothing was written: safe to tell the client to retry.
} else if err != nil {
	// The handler's own error, or ctx ended.
}
```

A sync task cannot have a `NotBefore`, and a batching action cannot run sync. `Expires` defaults to `ctx`'s deadline. If `ctx` ends first, `Run` returns the context's error and the task stays recorded for a worker to finish, unless its `Expires` passes first. A lease take in flight when `ctx` ends finishes first, for up to the lease TTL, so that it leaves no lease behind. Give a sync task an `Expires`, or a `ctx` with a deadline, whenever a caller that has given up must not see the task take effect later.

### Waiting

`Wait` waits for any task by queue and ID.

```go
res, err := r.Wait(ctx, "queue-space-upload", id)
var failed *taskrunner.HandlerError
if errors.As(err, &failed) {
	log.Printf("task failed: %s", failed.Message)
} else if errors.Is(err, taskrunner.ErrNotFound) {
	// Never recorded, or dropped by a replacement.
} else if err != nil {
	// ErrExpired, ErrCancelled, or ctx ended.
}
```

## Submitting from a handler

`ex.Submit` adds an async task that commits with the submitting task, so it exists only if the submitter succeeds. `ex.SubmitAll` adds many in one statement per queue, as `SubmitAllTx` does, for a handler that queues work for each of many subjects. `ex.Run` runs a sync child inline. The child's writes join its parent's, its subject stays leased until the parent commits, and if it fails, the parent fails too, even if the parent's handler carries on.

```go
func commit(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
	for _, digest := range sourceShards {
		_, err := ex.Run(ctx, taskrunner.Spec{
			Queue:   "queue-space-shard",
			Subject: space + "/" + digest,
			Action:  "add",
		})
		if err != nil {
			return nil, err
		}
	}
	if _, err := ex.Submit(ctx, taskrunner.Spec{Queue: "queue-space-upload", Subject: space, Action: "add", Args: root}); err != nil {
		return nil, err
	}
	return nil, ex.Write(ctx, `UPDATE ingot.buckets SET root = $2 WHERE name = $1`, task.Subject, newRoot)
}
```

Two tasks whose sync children take the same subjects in different orders cannot deadlock: the younger task releases its leases and starts again, so a sync child's handler may run more than once.

## Delays and expiry

`NotBefore` delays an async task. A delayed task never holds up the due tasks behind it on its subject. `Expires` is the latest time a task may start; one found after it is recorded as expired without running.

```go
grace := time.Now().Add(60 * time.Second)
deadline := time.Now().Add(24 * time.Hour)
_, err := ex.Submit(ctx, taskrunner.Spec{
	Queue:     "queue-space-shard",
	Subject:   key,
	Action:    "release",
	NotBefore: &grace,
	Expires:   &deadline,
})
```

## Replacing a pending task

A task submitted with `Replace` set needs a `NotBefore`. If a replacement with the same subject and action is pending and has not started, the one with the later `NotBefore` is kept and the other is dropped. This keeps a single pending task whose time can only move later, such as a cached file's eviction pushed back each time the file is read.

```go
ttl := time.Now().Add(24 * time.Hour)
_, err := ex.Submit(ctx, taskrunner.Spec{
	Queue:     "queue-shard",
	Subject:   digest,
	Action:    "evict",
	NotBefore: &ttl,
	Replace:   true,
})
```

A task without `Replace` is never a replacement and leaves pending ones alone. A replacement that has run, whether it is running or failed and waiting to retry, can no longer be replaced, so a new one is added beside it: its attempt may have acted, so it keeps its place and finishes what it started.

## Submitting many tasks at once

`SubmitAllTx` adds many tasks of one queue inside your transaction in one statement, for a write that queues work for each of thousands of subjects, and returns their IDs in order. Of two replacements in the batch for the same subject and action, the one with the later `NotBefore` is kept, as it would be if they were submitted one by one, and both get its ID.

```go
ids, err := r.SubmitAllTx(ctx, tx, specs)
```

`SubmitAll` does the same outside a transaction of yours, for tasks of any queues, one statement per queue, committed together straight away.

## Cancelling

A pending task that has not started can be cancelled by queue and ID, and many at once in one statement. Cancelling a task that has started, finished or been cancelled does nothing, so a task that can be cancelled must still check its own preconditions when it runs.

```go
// From a handler: the cancel commits with the task's other writes.
err := ex.Cancel(ctx, "queue-space-shard", expireID)

// From your own transaction.
err = r.CancelTx(ctx, tx, "queue-space-shard", expireID)

// On its own, or many at once.
err = r.Cancel(ctx, "queue-space-shard", expireIDs...)
```

A caller that knows a task's subject and action but not its ID, and must leave the work alone or wait for it, asks the queue what is left. `Pending` returns the pending tasks of the action on each of the given subjects that has any, delayed, due or running, oldest first, and changes nothing.

```go
// Which of these blobs still have a release to come?
releasing, err := r.Pending(ctx, "queue-space-shard", subjects, "release")
if len(releasing[subject]) > 0 {
	// Leave this blob to its release.
}
```

## Running delayed work now

`Expedite` makes a subject's pending tasks of an action due now, whatever their `NotBefore`, and returns them all by subject, for the caller to wait on, or to wait behind with a sync task on each subject. A subject ending in `*` stands for every subject that starts with what comes before it, which is why a submitted subject may not end in `*`. A bucket delete uses that to run its space's pending releases before the space goes. No task it returns can be replaced any more, due already or not, so nothing pushes it back again. A task waiting to retry is brought forward too, whether or not it holds its subject: the caller asked for now. A placed task brought forward still runs only on its own instance. `ExpediteTasks` does the same for given task IDs, so a caller cleaning up after itself brings forward only its own tasks and leaves others' of the same subject and action at their times.

```go
due, err := r.Expedite(ctx, "queue-space-shard", space+"/*", "release")
for _, ids := range due {
	for _, id := range ids {
		if _, err := r.Wait(ctx, "queue-space-shard", id); err != nil {
			return err
		}
	}
}
```

Subjects compare byte by byte, so a prefix reads only the tasks it matches.

## Batching

A batching handler takes the subject's oldest due task and the due tasks straight after it with the same action, up to a limit, and stops at the first task with a different action so the subject's order holds. Batching actions run async only.

It returns one outcome per task, and the batch's writes commit together. A task whose outcome carries an error is retried or failed on its own, under its own attempts, while its batchmates succeed. Returning an error instead fails or retries the whole batch. An outcome with an error can also carry `Args`, which replace the task's arguments for its next attempt, so something the attempt learned, such as renewed authority, is not lost if the process restarts before the retry.

```go
err := r.HandleBatch("queue-space-upload", "add", 100,
	func(ctx context.Context, ex *taskrunner.Exec, tasks []taskrunner.Task) ([]taskrunner.Outcome, error) {
		roots := make([][]byte, len(tasks))
		for i, t := range tasks {
			roots[i] = t.Args
		}
		refused, err := registerRoots(ctx, tasks[0].Subject, roots)
		if err != nil {
			return nil, err // nothing was answered: retry the batch
		}
		out := make([]taskrunner.Outcome, len(tasks))
		for i := range tasks {
			out[i].Err = refused[i] // nil for each root the service took
		}
		return out, nil
	})
```

## Retries

An action's async tasks are retried when it is registered with a policy. The delay starts at `First`, grows by `Multiplier` after each failure up to `Max`, and the task fails once it has run `Attempts` times (zero means no limit). Wrap an error with `Permanent` to fail at once. The handler sees how many earlier runs failed in `task.Attempt`.

```go
err := r.Handle("queue-space-shard", "release", release, taskrunner.WithRetry(taskrunner.RetryPolicy{
	First:      time.Second,
	Max:        5 * time.Minute,
	Multiplier: 2,
	Attempts:   10,
}))

func release(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
	if proofsExpired(task.Args) {
		return nil, taskrunner.Permanent(errors.New("proofs expired"))
	}
	// ...
}
```

By default the due tasks behind a task waiting to retry run during its backoff. Set `HoldsSubject` when the order on the subject must survive a failure, such as a registration followed by its retraction; the subject then waits for the retry, sync tasks on it included.

```go
err := r.Handle("queue-space-upload", "remove", remove, taskrunner.WithRetry(taskrunner.RetryPolicy{
	First:        time.Second,
	Max:          time.Minute,
	Multiplier:   2,
	HoldsSubject: true,
}))
```

A task waiting to retry counts as not started, so it can still be cancelled: its failed attempt returned before any effect outside Postgres.

Sync tasks are never retried: the error goes to the caller, which retries the request if it wants to.

## Failed tasks

A task that fails for good, with a permanent error or out of attempts, stays in its queue with its last error. It no longer holds up its subject, and it is kept until it is resubmitted or its queue's `FailedRetention` passes, so the work it stood for can be inspected and replayed. `ListFailed` lists a queue's failed tasks, for one subject or all of them, a page at a time: pass the last task's ID of one page to get the next. `Resubmit` replaces one with a new pending task on the same subject and action, which runs after the tasks already queued there. It can take new arguments, such as fresh authority, and starts its attempts afresh. A task that ran sync is listed but not resubmitted (`ErrSyncTask`): its caller has had the failure, and its handler may depend on what only the caller held.

```go
after := uuid.Nil
for {
	failed, err := r.ListFailed(ctx, "queue-space-upload", space, after, 100)
	if err != nil || len(failed) == 0 {
		break
	}
	for _, f := range failed {
		log.Printf("%s.%s failed: %s", f.Task.Queue, f.Task.Action, f.Error)
		_, err = r.Resubmit(ctx, f.Task.Queue, f.Task.ID, withFreshProofs(f.Task.Args))
	}
	after = failed[len(failed)-1].Task.ID
}
```

A handler can do the same through `ex.ListFailed` and `ex.Resubmit`, so the replay commits with its other writes. A task whose action has no registered handler fails the same way, so tasks left behind by a handler removed in a deploy can be found and replayed.

## Leases

A worker or sync caller holds its subject's lease while it runs tasks there. The runner renews every lease it holds three times per `Options.LeaseTTL`, 30 seconds by default. A lease left unrenewed past its TTL, because its process crashed, stalled or lost its database connection, is taken over by the next consumer to ask, which runs the interrupted task again. The old holder's commit then fails, so it writes nothing, and it starts no further sync child once renewal has found the lease gone.

A worker makes at most `Options.DrainLimit` handler runs on one subject, 100 by default and a batch counting as one, before it releases the lease. Each queue is checked for due subjects every `Options.PollInterval`, 1 second by default, and at once when this process submits a task to it or a worker frees up with more due.

Sync callers waiting on one subject in this process form a line in their tasks' order. Only the caller at the front asks for the lease, and a release wakes that caller alone, so the database work per task stays flat however many callers wait. The front caller takes the lease and claims its task in one statement, and the task's commit releases the lease, so a turn on a busy subject costs two commits besides the handler's own work. While another task is due ahead of the caller's, the same statement takes nothing and only reads, and the caller backs off, waking the workers when that task is theirs to run.

## Retention

Finished tasks are deleted once they are older than their queue's `Retention`, 24 hours by default: those that succeeded, expired, were cancelled or were resubmitted. Failed tasks are kept until they are resubmitted, unless the queue sets a `FailedRetention`. Deletion runs every `Options.RetentionInterval`, 1 minute by default.

```go
taskrunner.QueueOptions{Name: "queue-space-upload", Workers: 4, Retention: 6 * time.Hour}
```

Waiting on a task deleted past its retention returns `ErrNotFound`.

## Placement

A task that touches one instance's local state, such as a cached file, is placed on that instance with `Node`. Only the runner whose `Options.Node` matches runs it. While it is the oldest due task on its subject, other instances run none of the tasks behind it either, so the subject's order holds. A placed task still waiting on its `NotBefore` holds up nothing, like any delayed task. A sync task or child placed on another instance is refused with `ErrOtherNode`.

```go
r, err := taskrunner.New(pool, taskrunner.Options{Schema: "ingot", Queues: queues, Node: hostname})

_, err = ex.Submit(ctx, taskrunner.Spec{
	Queue:   "queue-shard",
	Subject: digest,
	Action:  "cache",
	Node:    &hostname,
})
```

## Metrics

Pass a `MeterProvider` and the runner reports through OpenTelemetry, under the instrumentation scope `github.com/fil-forge/ingot/taskrunner`:

| Metric | Unit | Attributes | What it shows |
|---|---|---|---|
| `taskrunner.queue.ready` | `{task}` | `queue`, `action` | Pending tasks that are due |
| `taskrunner.queue.delayed` | `{task}` | `queue`, `action` | Pending tasks waiting on their `NotBefore`, or on a retry that holds their subject |
| `taskrunner.queue.failed` | `{task}` | `queue`, `action` | Tasks that failed for good and await inspection or resubmission. Actions with no registered handler report too |
| `taskrunner.queue.oldest_ready_age` | `s` | `queue`, `action` | How long the oldest due task has been due, 0 when none is. Growth means the workers are not keeping up, or a subject is stuck |
| `taskrunner.task.duration` | `s` | `queue`, `action`, `outcome` | Time from start to outcome, with `outcome` one of `succeeded`, `failed`, `retried` or `expired`. Sync children are recorded too |
| `taskrunner.lease.wait` | `s` | `queue` | Time a sync task or child waited for its subject's lease |
| `taskrunner.lease.takeovers` | `{lease}` | `queue` | Leases taken over after they expired, which means a consumer stopped renewing |
| `taskrunner.workers.busy` | `{worker}` | `queue` | Workers running a queue's tasks in this process |
| `taskrunner.workers.limit` | `{worker}` | `queue` | The most workers this process runs for a queue |

```go
r, err := taskrunner.New(pool, taskrunner.Options{Schema: "ingot", Queues: queues, MeterProvider: otel.GetMeterProvider()})
```

The queue gauges are read from Postgres each time the exporter collects, and every registered action reports, at zero when it has no tasks. Without a provider the runner reports to the global OpenTelemetry provider, which is a no-op until one is installed.

## Testing handlers

`taskrunnertest.NewEnv` creates a fresh schema in the database named by `INGOT_TEST_DSN` and drops it when the test ends, so tests never touch other data. Without the variable, the test fails; `make test` starts a Postgres and sets it.

`taskrunnertest.CheckOwnWrites` catches a handler that reads its own task's writes. It runs a task twice, once with writes held until the commit, as in production, and once with each write applied as it is made, and fails the test if the results or the state left behind differ.

```go
func TestCommitReadsBeforeWriting(t *testing.T) {
	taskrunnertest.CheckOwnWrites(t, taskrunnertest.Case{
		Queues: []taskrunner.QueueOptions{{Name: "queue-bucket"}, {Name: "queue-space-shard"}},
		Setup: func(ctx context.Context, env taskrunnertest.Env, r *taskrunner.Runner) error {
			if _, err := env.DB.Exec(ctx, `CREATE TABLE `+env.Table("refs")+` (...)`); err != nil {
				return err
			}
			if err := r.Handle("queue-space-shard", "add", add); err != nil {
				return err
			}
			return r.Handle("queue-bucket", "commit", commit)
		},
		Spec: taskrunner.Spec{Queue: "queue-bucket", Subject: "b", Action: "commit"},
		State: func(ctx context.Context, env taskrunnertest.Env) (any, error) {
			return readRefs(ctx, env)
		},
	})
}
```
