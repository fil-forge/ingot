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

`Workers` is how many of a queue's async tasks this process runs at once. Sync tasks run in their caller's goroutine and need no workers.

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

A write that only makes sense if an earlier read still holds is conditional. If it affects no rows, the commit fails and the whole task runs again from scratch, up to `Options.CommitAttempts` times. After that the task stays pending and is resumed when its lease expires.

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

`Run` records a task and runs it in the calling goroutine once its subject's lease is free, first running any older due tasks on the same subject. It returns once the task has an outcome: its result, the handler's error, or `ErrExpired`. A commit that fails is retried until it lands or the task's `Expires` passes, so `ErrExpired` means nothing was written.

```go
expires := time.Now().Add(30 * time.Second)
res, err := r.Run(ctx, taskrunner.Spec{Queue: "queue-bucket", Subject: bucket, Action: "commit", Args: args, Expires: &expires})
if errors.Is(err, taskrunner.ErrExpired) {
	// Nothing was written: safe to tell the client to retry.
} else if err != nil {
	// The handler's own error, or ctx ended.
}
```

`Expires` defaults to `ctx`'s deadline. If `ctx` ends first, `Run` returns the context's error and the task stays recorded for a worker to finish, unless its `Expires` passes first. Give a sync task an `Expires`, or a `ctx` with a deadline, whenever a caller that has given up must not see the task take effect later.

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

`ex.Submit` adds an async task that commits with the submitting task, so it exists only if the submitter succeeds. `ex.Run` runs a sync child inline. The child's writes join its parent's, its subject stays leased until the parent commits, and if it fails, the parent fails too, even if the parent's handler carries on.

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

A task without `Replace` is never a replacement and leaves pending ones alone. A replacement that has started can no longer be replaced, so a new one is added beside it.

## Cancelling

A pending task that has not started can be cancelled by queue and ID. Cancelling a task that has started, finished or been cancelled does nothing, so a task that can be cancelled must still check its own preconditions when it runs.

```go
// From a handler: the cancel commits with the task's other writes.
err := ex.Cancel(ctx, "queue-space-shard", expireID)

// From your own transaction.
err = r.CancelTx(ctx, tx, "queue-space-shard", expireID)

// On its own.
err = r.Cancel(ctx, "queue-space-shard", expireID)
```

## Batching

A batching handler takes the subject's oldest due task and the due tasks straight after it with the same action, up to a limit, and stops at the first task with a different action so the subject's order holds. It returns one result per task, and the batch commits or fails together. Batching actions run async only.

```go
err := r.HandleBatch("queue-space-upload", "add", 100,
	func(ctx context.Context, ex *taskrunner.Exec, tasks []taskrunner.Task) ([][]byte, error) {
		roots := make([][]byte, len(tasks))
		for i, t := range tasks {
			roots[i] = t.Args
		}
		if err := registerRoots(ctx, tasks[0].Subject, roots); err != nil {
			return nil, err
		}
		return make([][]byte, len(tasks)), nil
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

## Testing handlers

`taskrunnertest.NewEnv` creates a fresh schema in the database named by `INGOT_TEST_DSN` and drops it when the test ends, so tests never touch other data. Without the variable, the test is skipped.

`taskrunnertest.CheckOwnWrites` catches a handler that reads its own task's writes. It runs a task twice, once with writes held until the commit, as in production, and once with each write applied as it is made, and fails the test if the results or the state left behind differ.

```go
func TestCommitReadsBeforeWriting_Live(t *testing.T) {
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
