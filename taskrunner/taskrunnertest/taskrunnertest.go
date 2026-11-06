// Package taskrunnertest helps test task handlers against a real Postgres.
package taskrunnertest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fil-forge/ingot/taskrunner"
)

// DSNEnv names the environment variable holding the test database's DSN.
// Tests that need it fail when it is unset.
const DSNEnv = "INGOT_TEST_DSN"

// Env is a fresh schema in the test database, dropped when the test ends.
type Env struct {
	// DB is a pool on the test database.
	DB *pgxpool.Pool
	// Schema is the schema's name, unique to this Env.
	Schema string
}

// Table returns the quoted name of a table in the Env's schema.
func (e Env) Table(name string) string {
	return pgx.Identifier{e.Schema, name}.Sanitize()
}

// NewEnv creates an Env, failing t when DSNEnv is unset.
func NewEnv(t testing.TB) Env {
	t.Helper()
	dsn := os.Getenv(DSNEnv)
	if dsn == "" {
		t.Fatalf("%s is unset: set it to a Postgres the tests may create schemas in", DSNEnv)
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	var b [6]byte
	_, _ = rand.Read(b[:])
	schema := "taskrunner_test_" + hex.EncodeToString(b[:])
	if _, err := pool.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		pool.Close()
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
		pool.Close()
	})
	return Env{DB: pool, Schema: schema}
}

// Case is a task tree to run both ways in CheckOwnWrites.
type Case struct {
	// Queues are the runner's queues. Workers may be zero: the tree runs
	// inline.
	Queues []taskrunner.QueueOptions
	// Setup creates the handlers' tables in env and registers the handlers.
	Setup func(ctx context.Context, env Env, r *taskrunner.Runner) error
	// Spec is the root task, run sync.
	Spec taskrunner.Spec
	// Expires, when set, gives each run's task an Expires this long after the
	// run starts, so a task whose commit never lands ends the same way in
	// both runs.
	Expires time.Duration
	// State reads what the tree left behind, for comparing the two runs.
	State func(ctx context.Context, env Env) (any, error)
}

// CheckOwnWrites runs c twice in fresh schemas: once with the tree's writes
// held until it commits, as in production, and once with each applied as it
// is made. A handler that reads a row its own tree wrote sees it only the
// second time, so a difference in the result, the error or the state left
// behind fails t.
func CheckOwnWrites(t *testing.T, c Case) {
	t.Helper()
	if err := CompareOwnWrites(context.Background(), func() Env { return NewEnv(t) }, c); err != nil {
		t.Error(err)
	}
}

// outcome is one run of a Case.
type outcome struct {
	Result []byte
	Err    string
	State  any
}

// CompareOwnWrites is CheckOwnWrites returning the difference as an error.
func CompareOwnWrites(ctx context.Context, newEnv func() Env, c Case) error {
	held, err := runCase(ctx, newEnv(), c, false)
	if err != nil {
		return fmt.Errorf("run with writes held: %w", err)
	}
	applied, err := runCase(ctx, newEnv(), c, true)
	if err != nil {
		return fmt.Errorf("run with writes applied: %w", err)
	}
	if !reflect.DeepEqual(held, applied) {
		return fmt.Errorf("a handler reads its own tree's writes:\n  writes held:    %+v\n  writes applied: %+v", held, applied)
	}
	return nil
}

func runCase(ctx context.Context, env Env, c Case, immediate bool) (outcome, error) {
	r, err := taskrunner.New(env.DB, taskrunner.Options{
		Schema:                 env.Schema,
		Queues:                 c.Queues,
		PollInterval:           50 * time.Millisecond,
		ApplyWritesImmediately: immediate,
	})
	if err != nil {
		return outcome{}, err
	}
	if err := c.Setup(ctx, env, r); err != nil {
		return outcome{}, fmt.Errorf("setup: %w", err)
	}
	if err := r.Migrate(ctx); err != nil {
		return outcome{}, err
	}
	if err := r.Start(ctx); err != nil {
		return outcome{}, err
	}
	defer func() { _ = r.Stop(ctx) }()

	spec := c.Spec
	if c.Expires > 0 {
		e := time.Now().Add(c.Expires)
		spec.Expires = &e
	}
	var o outcome
	o.Result, err = r.Run(ctx, spec)
	if err != nil {
		o.Err = err.Error()
	}
	if o.State, err = c.State(ctx, env); err != nil {
		return outcome{}, fmt.Errorf("read state: %w", err)
	}
	return o, nil
}
