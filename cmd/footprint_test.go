package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/queue/spaceshardqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/libforge/taskrunner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFootprintDisk: each entry at the top of the data directory reports its
// regular files and their bytes, an empty directory included.
func TestFootprintDisk(t *testing.T) {
	dir := t.TempDir()
	for name, size := range map[string]int{
		"segments/bucket-a/1.seg": 10,
		"segments/bucket-b/2.seg": 5,
		"cache/ab/blob":           20,
		"tokens.cbor":             3,
	} {
		path := filepath.Join(dir, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, make([]byte, size), 0o644))
	}
	require.NoError(t, os.Mkdir(filepath.Join(dir, "spool"), 0o755))
	require.NoError(t, os.Symlink("tokens.cbor", filepath.Join(dir, "link")))

	got, err := diskUsage(dir)
	require.NoError(t, err)
	want := diskFootprint{
		Entries: []diskEntry{
			{Name: "cache", diskFigures: diskFigures{Files: 1, Bytes: 20}},
			{Name: "link"},
			{Name: "segments", diskFigures: diskFigures{Files: 2, Bytes: 15}},
			{Name: "spool"},
			{Name: "tokens.cbor", diskFigures: diskFigures{Files: 1, Bytes: 3}},
		},
		Total: diskFigures{Files: 4, Bytes: 38},
	}
	require.Equal(t, want, got, "diskUsage")

	_, err = diskUsage(filepath.Join(dir, "missing"))
	require.Error(t, err, "diskUsage of a missing data directory succeeded")
}

// TestFootprintPostgres: the report lists the ingot schema's tables, the
// task queues' among them, with each total the sum of its parts, and counts
// the release tasks by how they ended or are waiting.
func TestFootprintPostgres(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	r, err := taskrunner.New(pool, taskrunner.Options{
		Schema:       ingotSchema,
		Queues:       []taskrunner.QueueOptions{{Name: spaceshardqueue.Name, Workers: 2}},
		PollInterval: 10 * time.Millisecond,
		Logger:       zaptest.NewLogger(t),
	})
	require.NoError(t, err)
	require.NoError(t, r.Migrate(ctx))

	// Before any release: no timings.
	before, err := footprint(ctx, pool, t.TempDir())
	require.NoError(t, err)
	require.Zero(t, before.Releases, "releases before any ran")

	err = r.Handle(spaceshardqueue.Name, spaceshardqueue.ReleaseAction,
		func(ctx context.Context, ex *taskrunner.Exec, task taskrunner.Task) ([]byte, error) {
			switch task.Subject {
			case "stale":
				return nil, nil
			case "broken":
				return nil, taskrunner.Permanent(errors.New("broken"))
			}
			return []byte(spaceshardqueue.ReleasedResult), nil
		})
	require.NoError(t, err)
	// A release due at once, as a commit with no grace queues it, waits
	// before the runner starts, so it runs that long behind.
	const wait = 200 * time.Millisecond
	dueNow := registry.DueNow()
	late, err := r.Submit(ctx, taskrunner.Spec{Queue: spaceshardqueue.Name, Subject: "released-late", Action: spaceshardqueue.ReleaseAction, NotBefore: &dueNow})
	require.NoError(t, err)
	time.Sleep(wait)
	require.NoError(t, r.Start(ctx))
	_, err = r.Wait(ctx, spaceshardqueue.Name, late)
	require.NoError(t, err)
	// Two more wait out a grace, so their time from queued to finished is at
	// least that, and they run hardly behind.
	const grace = 300 * time.Millisecond
	for _, subject := range []string{"released-1", "released-2", "stale", "broken"} {
		spec := taskrunner.Spec{Queue: spaceshardqueue.Name, Subject: subject, Action: spaceshardqueue.ReleaseAction}
		if strings.HasPrefix(subject, "released") {
			due := time.Now().Add(grace)
			spec.NotBefore = &due
		}
		id, err := r.Submit(ctx, spec)
		require.NoError(t, err)
		_, err = r.Wait(ctx, spaceshardqueue.Name, id)
		if subject != "broken" {
			require.NoError(t, err, "release %s", subject)
		}
	}
	require.NoError(t, r.Stop(ctx))
	// With the runner stopped, one release due at once has been due for the
	// wait, and one waits out its grace.
	later, dueAgain := time.Now().Add(time.Hour), registry.DueNow()
	for subject, notBefore := range map[string]*time.Time{"due": &dueAgain, "waiting": &later} {
		_, err = r.Submit(ctx, taskrunner.Spec{Queue: spaceshardqueue.Name, Subject: subject, Action: spaceshardqueue.ReleaseAction, NotBefore: notBefore})
		require.NoError(t, err)
	}
	time.Sleep(wait)

	report, err := footprint(ctx, pool, t.TempDir())
	require.NoError(t, err)

	rel := report.Releases
	assert.Equal(t, int64(3), rel.Released, "released")
	assert.Equal(t, int64(2), rel.Pending, "pending")
	assert.Equal(t, int64(1), rel.Due, "due")
	assert.Equal(t, int64(1), rel.Failed, "failed")
	ordered := func(s *releaseSeconds) bool {
		return s != nil && s.P50 >= 0 && s.P50 <= s.P90 && s.P90 <= s.P99 && s.P99 <= s.Max
	}
	// Allow for the database's clock and the test's to differ a little.
	const slack = 50 * time.Millisecond
	took, lag := rel.Seconds, rel.LagSeconds
	assert.True(t, ordered(took) && took.P50 >= (grace-slack).Seconds(),
		"release seconds = %+v, want ordered quantiles of at least the %v grace", took, grace)
	// The late release ran the wait behind, and no lag outruns the time
	// from queued to finished. The others ran hardly behind.
	assert.True(t, ordered(lag) && took != nil && lag.Max >= (wait-slack).Seconds() && lag.Max <= took.Max+slack.Seconds() && lag.P50 <= (grace-slack).Seconds(),
		"release lag = %+v, want ordered quantiles up to the %v wait, none above %+v", lag, wait, took)
	// The report's own queries take some of the headroom. It must stay under
	// the second registry.DueNow sets a release back, or measuring from that
	// time instead of from when it was queued would pass too.
	const headroom = 500 * time.Millisecond
	assert.GreaterOrEqual(t, rel.OldestDueSeconds, (wait - slack).Seconds(), "oldest due seconds")
	assert.LessOrEqual(t, rel.OldestDueSeconds, (wait + headroom).Seconds(), "oldest due seconds")

	pg := report.Postgres
	var names []string
	var sum int64
	for _, tb := range pg.Tables {
		names = append(names, tb.Name)
		sum += tb.TotalBytes
		assert.Equal(t, tb.TableBytes+tb.IndexBytes, tb.TotalBytes, "table %s: total %d bytes", tb.Name, tb.TotalBytes)
	}
	for _, name := range []string{"buckets", "queue_space_shard_tasks", "queue_space_shard_leases"} {
		assert.True(t, slices.Contains(names, name), "tables %v lack %s", names, name)
	}
	assert.Equal(t, sum, pg.SchemaBytes, "schema bytes")
	assert.Positive(t, pg.SchemaBytes, "schema bytes")
	assert.GreaterOrEqual(t, pg.DatabaseBytes, pg.SchemaBytes, "database bytes = %d, want at least the schema's %d", pg.DatabaseBytes, pg.SchemaBytes)
	assert.Greater(t, pg.WALBytes, before.Postgres.WALBytes, "WAL position %d did not move on from %d", pg.WALBytes, before.Postgres.WALBytes)
}

// TestFootprintJSON: the report carries the field names the command's help
// documents.
func TestFootprintJSON(t *testing.T) {
	b, err := json.Marshal(footprintReport{
		Postgres: postgresFootprint{Tables: []tableFootprint{{}}},
		Disk:     diskFootprint{Entries: []diskEntry{{}}},
		Releases: releaseFootprint{Seconds: &releaseSeconds{}, LagSeconds: &releaseSeconds{}},
	})
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(b, &got))
	want := map[string][]string{
		"":                     {"time", "postgres", "disk", "releases"},
		"postgres":             {"database_bytes", "wal_bytes", "schema_bytes", "tables"},
		"postgres.tables":      {"name", "total_bytes", "table_bytes", "index_bytes", "live_rows", "dead_rows"},
		"disk":                 {"entries", "total"},
		"disk.entries":         {"name", "files", "bytes"},
		"disk.total":           {"files", "bytes"},
		"releases":             {"released", "seconds", "lag_seconds", "pending", "due", "oldest_due_seconds", "failed"},
		"releases.seconds":     {"p50", "p90", "p99", "max"},
		"releases.lag_seconds": {"p50", "p90", "p99", "max"},
	}
	for path, keys := range want {
		obj := lookup(t, got, path)
		for _, k := range keys {
			_, ok := obj[k]
			assert.True(t, ok, "%s has no %q: %v", path, k, obj)
		}
		assert.Len(t, obj, len(keys), "%s has %d fields", path, len(obj))
	}
}

// lookup follows a dotted path through decoded JSON, taking the first
// element of an array.
func lookup(t *testing.T, v map[string]any, path string) map[string]any {
	t.Helper()
	if path == "" {
		return v
	}
	cur := any(v)
	for _, k := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		require.True(t, ok, "%s: %v is not an object", path, cur)
		cur = m[k]
		if a, ok := cur.([]any); ok && len(a) > 0 {
			cur = a[0]
		}
	}
	m, ok := cur.(map[string]any)
	require.True(t, ok, "%s: %v is not an object", path, cur)
	return m
}

// TestFootprintCommand: the command reads data_dir and postgres_dsn from the
// daemon's config and prints the report, and refuses a config without them.
func TestFootprintCommand(t *testing.T) {
	pool, dsn := pgtest.NewWithDSN(t)
	r, err := taskrunner.New(pool, taskrunner.Options{
		Schema: ingotSchema,
		Queues: []taskrunner.QueueOptions{{Name: spaceshardqueue.Name}},
		Logger: zaptest.NewLogger(t),
	})
	require.NoError(t, err)
	require.NoError(t, r.Migrate(context.Background()))
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "segments"), 0o755))
	execute := func(config string) (string, error) {
		path := filepath.Join(dir, "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(config), 0o600))
		root := newRootCmd()
		var out bytes.Buffer
		root.SetOut(&out)
		root.SetErr(&out)
		root.SetArgs([]string{"footprint", "--config", path})
		err := root.Execute()
		return out.String(), err
	}

	out, err := execute("data_dir: " + dataDir + "\npostgres_dsn: " + dsn + "\n")
	require.NoError(t, err, "footprint: %v\n%s", err, out)
	var report footprintReport
	require.NoError(t, json.Unmarshal([]byte(out), &report), "footprint printed %q", out)
	assert.NotEmpty(t, report.Postgres.Tables, "footprint")
	if assert.Len(t, report.Disk.Entries, 1, "footprint") {
		assert.Equal(t, "segments", report.Disk.Entries[0].Name, "footprint")
	}

	_, err = execute("postgres_dsn: " + dsn + "\n")
	assert.ErrorContains(t, err, "needs data_dir", "footprint without data_dir")
}

// TestDevCommandsListed: the measurement commands are left out of the help
// unless INGOT_DEV_COMMANDS asks for them, and run either way.
func TestDevCommandsListed(t *testing.T) {
	help := func() string {
		t.Helper()
		root := newRootCmd()
		var out bytes.Buffer
		root.SetOut(&out)
		root.SetArgs([]string{"--help"})
		require.NoError(t, root.Execute())
		return out.String()
	}
	listed := func(out, name string) bool {
		for _, line := range strings.Split(out, "\n") {
			if f := strings.Fields(line); len(f) > 0 && f[0] == name {
				return true
			}
		}
		return false
	}

	for _, value := range []string{"", "0", "false", "no"} {
		t.Setenv(devCommandsEnv, value)
		out := help()
		for _, name := range []string{"catalog", "footprint"} {
			assert.False(t, listed(out, name), "%s=%q: help lists %s:\n%s", devCommandsEnv, value, name, out)
		}
		assert.True(t, listed(out, "serve"), "%s=%q: help lacks serve:\n%s", devCommandsEnv, value, out)
	}

	t.Setenv(devCommandsEnv, "1")
	out := help()
	for _, name := range []string{"catalog", "footprint"} {
		assert.True(t, listed(out, name), "%s=1: help lacks %s:\n%s", devCommandsEnv, name, out)
	}

	t.Setenv(devCommandsEnv, "")
	for _, args := range [][]string{{"footprint", "--help"}, {"catalog", "usage", "--help"}} {
		root := newRootCmd()
		var out bytes.Buffer
		root.SetOut(&out)
		root.SetArgs(args)
		assert.NoError(t, root.Execute(), "ingot %v while unlisted", args)
		assert.Contains(t, out.String(), "as JSON", "ingot %v while unlisted", args)
	}
}
