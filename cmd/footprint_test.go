package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/fil-forge/ingot/internal/pgtest"
	"github.com/fil-forge/ingot/queue/spaceshardqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/taskrunner"
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
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "spool"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("tokens.cbor", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}

	got, err := diskUsage(dir)
	if err != nil {
		t.Fatal(err)
	}
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
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("diskUsage = %+v, want %+v", got, want)
	}

	if _, err := diskUsage(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("diskUsage of a missing data directory succeeded")
	}
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
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	// Before any release: no timings.
	before, err := footprint(ctx, pool, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if before.Releases != (releaseFootprint{}) {
		t.Fatalf("releases before any ran = %+v, want none", before.Releases)
	}

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
	if err != nil {
		t.Fatal(err)
	}
	// A release due at once, as a commit with no grace queues it, waits
	// before the runner starts, so it runs that long behind.
	const wait = 200 * time.Millisecond
	dueNow := registry.DueNow()
	late, err := r.Submit(ctx, taskrunner.Spec{Queue: spaceshardqueue.Name, Subject: "released-late", Action: spaceshardqueue.ReleaseAction, NotBefore: &dueNow})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(wait)
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Wait(ctx, spaceshardqueue.Name, late); err != nil {
		t.Fatal(err)
	}
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
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.Wait(ctx, spaceshardqueue.Name, id); err != nil && subject != "broken" {
			t.Fatalf("release %s: %v", subject, err)
		}
	}
	if err := r.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	// With the runner stopped, one release due at once has been due for the
	// wait, and one waits out its grace.
	later, dueAgain := time.Now().Add(time.Hour), registry.DueNow()
	for subject, notBefore := range map[string]*time.Time{"due": &dueAgain, "waiting": &later} {
		if _, err := r.Submit(ctx, taskrunner.Spec{Queue: spaceshardqueue.Name, Subject: subject, Action: spaceshardqueue.ReleaseAction, NotBefore: notBefore}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(wait)

	report, err := footprint(ctx, pool, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	rel := report.Releases
	if rel.Released != 3 || rel.Pending != 2 || rel.Due != 1 || rel.Failed != 1 {
		t.Errorf("releases = %+v, want 3 released, 2 pending, 1 due, 1 failed", rel)
	}
	ordered := func(s *releaseSeconds) bool {
		return s != nil && s.P50 >= 0 && s.P50 <= s.P90 && s.P90 <= s.P99 && s.P99 <= s.Max
	}
	// Allow for the database's clock and the test's to differ a little.
	const slack = 50 * time.Millisecond
	took, lag := rel.Seconds, rel.LagSeconds
	if !ordered(took) || took.P50 < (grace-slack).Seconds() {
		t.Errorf("release seconds = %+v, want ordered quantiles of at least the %v grace", took, grace)
	}
	// The late release ran the wait behind, and no lag outruns the time
	// from queued to finished. The others ran hardly behind.
	if !ordered(lag) || took == nil || lag.Max < (wait-slack).Seconds() || lag.Max > took.Max+slack.Seconds() || lag.P50 > (grace-slack).Seconds() {
		t.Errorf("release lag = %+v, want ordered quantiles up to the %v wait, none above %+v", lag, wait, took)
	}
	if rel.OldestDueSeconds < (wait-slack).Seconds() || rel.OldestDueSeconds > (wait+500*time.Millisecond).Seconds() {
		t.Errorf("oldest due = %v seconds, want about the %v wait", rel.OldestDueSeconds, wait)
	}

	pg := report.Postgres
	var names []string
	var sum int64
	for _, tb := range pg.Tables {
		names = append(names, tb.Name)
		sum += tb.TotalBytes
		if tb.TotalBytes != tb.TableBytes+tb.IndexBytes {
			t.Errorf("table %s: total %d bytes, want table %d + indexes %d", tb.Name, tb.TotalBytes, tb.TableBytes, tb.IndexBytes)
		}
	}
	for _, name := range []string{"buckets", "queue_space_shard_tasks", "queue_space_shard_leases"} {
		if !slices.Contains(names, name) {
			t.Errorf("tables %v lack %s", names, name)
		}
	}
	if pg.SchemaBytes != sum || pg.SchemaBytes <= 0 {
		t.Errorf("schema bytes = %d, want the tables' %d", pg.SchemaBytes, sum)
	}
	if pg.DatabaseBytes < pg.SchemaBytes {
		t.Errorf("database bytes = %d, want at least the schema's %d", pg.DatabaseBytes, pg.SchemaBytes)
	}
	if pg.WALBytes <= before.Postgres.WALBytes {
		t.Errorf("WAL position %d did not move on from %d", pg.WALBytes, before.Postgres.WALBytes)
	}
}

// TestFootprintJSON: the report carries the field names the command's help
// documents.
func TestFootprintJSON(t *testing.T) {
	b, err := json.Marshal(footprintReport{
		Postgres: postgresFootprint{Tables: []tableFootprint{{}}},
		Disk:     diskFootprint{Entries: []diskEntry{{}}},
		Releases: releaseFootprint{Seconds: &releaseSeconds{}, LagSeconds: &releaseSeconds{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
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
			if _, ok := obj[k]; !ok {
				t.Errorf("%s has no %q: %v", path, k, obj)
			}
		}
		if len(obj) != len(keys) {
			t.Errorf("%s has %d fields, want %v", path, len(obj), keys)
		}
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
		if !ok {
			t.Fatalf("%s: %v is not an object", path, cur)
		}
		cur = m[k]
		if a, ok := cur.([]any); ok && len(a) > 0 {
			cur = a[0]
		}
	}
	m, ok := cur.(map[string]any)
	if !ok {
		t.Fatalf("%s: %v is not an object", path, cur)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(filepath.Join(dataDir, "segments"), 0o755); err != nil {
		t.Fatal(err)
	}
	execute := func(config string) (string, error) {
		path := filepath.Join(dir, "config.yaml")
		if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
			t.Fatal(err)
		}
		root := newRootCmd()
		var out bytes.Buffer
		root.SetOut(&out)
		root.SetErr(&out)
		root.SetArgs([]string{"footprint", "--config", path})
		err := root.Execute()
		return out.String(), err
	}

	out, err := execute("data_dir: " + dataDir + "\npostgres_dsn: " + dsn + "\n")
	if err != nil {
		t.Fatalf("footprint: %v\n%s", err, out)
	}
	var report footprintReport
	if err := json.Unmarshal([]byte(out), &report); err != nil {
		t.Fatalf("footprint printed %q: %v", out, err)
	}
	if len(report.Postgres.Tables) == 0 || len(report.Disk.Entries) != 1 || report.Disk.Entries[0].Name != "segments" {
		t.Errorf("footprint = %+v, want the schema's tables and the segments directory", report)
	}

	if _, err := execute("postgres_dsn: " + dsn + "\n"); err == nil || !strings.Contains(err.Error(), "needs data_dir") {
		t.Errorf("footprint without data_dir: %v, want a refusal", err)
	}
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
		if err := root.Execute(); err != nil {
			t.Fatal(err)
		}
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
			if listed(out, name) {
				t.Errorf("%s=%q: help lists %s:\n%s", devCommandsEnv, value, name, out)
			}
		}
		if !listed(out, "serve") {
			t.Errorf("%s=%q: help lacks serve:\n%s", devCommandsEnv, value, out)
		}
	}

	t.Setenv(devCommandsEnv, "1")
	out := help()
	for _, name := range []string{"catalog", "footprint"} {
		if !listed(out, name) {
			t.Errorf("%s=1: help lacks %s:\n%s", devCommandsEnv, name, out)
		}
	}

	t.Setenv(devCommandsEnv, "")
	for _, args := range [][]string{{"footprint", "--help"}, {"catalog", "usage", "--help"}} {
		root := newRootCmd()
		var out bytes.Buffer
		root.SetOut(&out)
		root.SetArgs(args)
		if err := root.Execute(); err != nil || !strings.Contains(out.String(), "as JSON") {
			t.Errorf("ingot %v while unlisted: %v\n%s", args, err, out.String())
		}
	}
}
