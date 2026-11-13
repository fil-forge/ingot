package preload

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func testConfig(t *testing.T, buckets int, until uint64) Config {
	t.Helper()
	cfg := Config{
		KeyPrefix:          "packs/",
		Seed:               "test",
		Until:              until,
		Workers:            3,
		RealMinSize:        1 << 20,
		RealMaxSize:        2 << 20,
		RealWorkers:        2,
		Encryption:         "none",
		StatePath:          filepath.Join(t.TempDir(), "state.json"),
		CheckpointInterval: time.Hour,
		ReportInterval:     time.Hour,
		MaxAttempts:        3,
		backoff:            func(int) time.Duration { return time.Millisecond },
	}
	for i := range buckets {
		cfg.Buckets = append(cfg.Buckets, BucketName("preload-", i))
	}
	return cfg
}

// checkBucket fails t unless bucket holds exactly bulk objects 0 to bulk-1
// and the real objects st lists, of the sizes st records.
func checkBucket(t *testing.T, f *fakeS3, cfg Config, bucket string, bulk uint64, st *BucketState) {
	t.Helper()
	objs := f.objects(bucket)
	for n := range bulk {
		key := BulkKey(cfg.KeyPrefix, cfg.Seed, bucket, n)
		size, ok := objs[key]
		if !ok {
			t.Fatalf("%s: bulk object %d (%s) missing", bucket, n, key)
		}
		if size != 0 {
			t.Fatalf("%s: bulk object %d has %d bytes", bucket, n, size)
		}
		delete(objs, key)
	}
	for j, r := range st.Real {
		if r.Index != uint64(j) {
			t.Fatalf("%s: real object %d recorded with index %d", bucket, j, r.Index)
		}
		body := RealBody(cfg.Seed, bucket, r.Index, cfg.RealMinSize, cfg.RealMaxSize)
		sum, err := body.SHA256()
		if err != nil {
			t.Fatal(err)
		}
		if want := RealKey(cfg.KeyPrefix, sum); r.Key != want {
			t.Fatalf("%s: real object %d recorded as %s, want %s", bucket, j, r.Key, want)
		}
		if size, ok := objs[r.Key]; !ok || size != r.Size || size != body.Size() {
			t.Fatalf("%s: real object %d: stored %d bytes (present %v), recorded %d, body %d", bucket, j, size, ok, r.Size, body.Size())
		}
		if r.Size < cfg.RealMinSize || r.Size > cfg.RealMaxSize {
			t.Fatalf("%s: real object %d is %d bytes, outside %d to %d", bucket, j, r.Size, cfg.RealMinSize, cfg.RealMaxSize)
		}
		delete(objs, r.Key)
	}
	if len(objs) != 0 {
		t.Fatalf("%s: %d unexpected objects", bucket, len(objs))
	}
}

func readTestState(t *testing.T, cfg Config) *State {
	t.Helper()
	st, err := ReadState(cfg.StatePath, cfg.Seed, cfg.KeyPrefix)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestLoadWritesEveryObject(t *testing.T) {
	f := newFakeS3()
	cfg := testConfig(t, 3, 50)
	cfg.RealEvery = 20
	r, err := Load(t.Context(), f, cfg)
	if err != nil {
		t.Fatal(err)
	}
	st := readTestState(t, cfg)
	for _, b := range cfg.Buckets {
		bs := st.Buckets[b]
		if !bs.Ready || bs.Bulk != 50 || len(bs.Real) != 2 {
			t.Fatalf("%s: state %+v, want ready with 50 bulk and 2 real", b, bs)
		}
		checkBucket(t, f, cfg, b, 50, bs)
		if sse := f.buckets[b].sse; sse != "none" {
			t.Fatalf("%s: encryption %q, want none", b, sse)
		}
		if br := r.Buckets[b]; br.Bulk != 50 || br.Written != 52 || br.Real != 2 {
			t.Fatalf("%s: report %+v", b, br)
		}
	}
	if !r.Final || r.Total.Written != 156 || r.Total.Existed != 0 || r.Total.Errors != 0 || r.Total.Real != 6 {
		t.Fatalf("final report totals %+v", r.Total)
	}
	if r.Latency[OpPutBulk].Total.Count != 150 || r.Latency[OpPutReal].Total.Count != 6 {
		t.Fatalf("latency counts %+v", r.Latency)
	}
}

// A second load with a higher target writes only what the first did not.
func TestLoadResumesPastATestPoint(t *testing.T) {
	f := newFakeS3()
	cfg := testConfig(t, 2, 30)
	cfg.RealEvery = 20
	if _, err := Load(t.Context(), f, cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Until = 60
	r, err := Load(t.Context(), f, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if f.creates != 2 {
		t.Fatalf("%d bucket creates, want 2: a ready bucket is not set up again", f.creates)
	}
	st := readTestState(t, cfg)
	for _, b := range cfg.Buckets {
		if bs := st.Buckets[b]; bs.Bulk != 60 || len(bs.Real) != 3 {
			t.Fatalf("%s: state %+v, want 60 bulk and 3 real", b, bs)
		}
		checkBucket(t, f, cfg, b, 60, st.Buckets[b])
	}
	// 30 bulk and 2 real per bucket.
	if r.Total.Written != 64 || r.Total.Existed != 0 {
		t.Fatalf("second load totals %+v", r.Total)
	}
}

// A load interrupted part way records a watermark it can resume from, and
// the resumed load finds the objects written past it taken.
func TestLoadResumesAfterInterrupt(t *testing.T) {
	f := newFakeS3()
	cfg := testConfig(t, 2, 200)
	cfg.RealEvery = 50
	ctx, cancel := context.WithCancel(t.Context())
	f.after = func(stored int) {
		if stored == 150 {
			cancel()
		}
	}
	_, err := Load(ctx, f, cfg)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted load returned %v, want context.Canceled", err)
	}
	st := readTestState(t, cfg)
	// Every object the state does not account for was written past the
	// watermark, or is a real object written but not recorded: the resumed
	// load must find exactly these taken. A slow PUT holds the watermark
	// while its bucket's other workers carry on, so there can be many.
	var unrecorded uint64
	for _, b := range cfg.Buckets {
		bs := st.Buckets[b]
		if bs.Bulk >= 200 {
			t.Fatalf("%s: watermark %d after an interrupt", b, bs.Bulk)
		}
		objs := f.objects(b)
		for n := range bs.Bulk {
			if _, ok := objs[BulkKey(cfg.KeyPrefix, cfg.Seed, b, n)]; !ok {
				t.Fatalf("%s: bulk object %d below the watermark %d missing", b, n, bs.Bulk)
			}
		}
		unrecorded += uint64(len(objs)) - bs.Bulk - uint64(len(bs.Real))
	}

	f.after = nil
	r, err := Load(t.Context(), f, cfg)
	if err != nil {
		t.Fatal(err)
	}
	st = readTestState(t, cfg)
	for _, b := range cfg.Buckets {
		checkBucket(t, f, cfg, b, 200, st.Buckets[b])
	}
	if r.Total.Existed != unrecorded {
		t.Fatalf("resumed load found %d keys taken, want the %d written but not recorded", r.Total.Existed, unrecorded)
	}
}

// An interrupted load whose final save fails says so, so the command does
// not report the progress as saved.
func TestLoadReportsAFailedFinalSave(t *testing.T) {
	f := newFakeS3()
	cfg := testConfig(t, 1, 100)
	ctx, cancel := context.WithCancel(t.Context())
	f.after = func(stored int) {
		if stored == 20 {
			// The state file's directory goes, so the final write fails.
			os.RemoveAll(filepath.Dir(cfg.StatePath))
			cancel()
		}
	}
	_, err := Load(ctx, f, cfg)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrStateNotSaved) {
		t.Fatalf("load returned %v, want the interrupt and ErrStateNotSaved", err)
	}
}

func TestLoadRetriesPassingFailures(t *testing.T) {
	f := newFakeS3()
	cfg := testConfig(t, 1, 10)
	victim := BulkKey(cfg.KeyPrefix, cfg.Seed, cfg.Buckets[0], 3)
	var mu sync.Mutex
	failures := 0
	f.fail = func(_ context.Context, _, key string) error {
		mu.Lock()
		defer mu.Unlock()
		if key == victim && failures < 2 {
			failures++
			return &statusError{503}
		}
		return nil
	}
	r, err := Load(t.Context(), f, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Total.Errors != 2 || r.Total.Written != 10 {
		t.Fatalf("totals %+v, want 2 errors and 10 written", r.Total)
	}
	if !strings.Contains(r.LastError, victim) {
		t.Fatalf("last error %q does not name %s", r.LastError, victim)
	}
}

func TestLoadTimesOutHungAttempts(t *testing.T) {
	f := newFakeS3()
	cfg := testConfig(t, 1, 10)
	cfg.AttemptTimeout = 50 * time.Millisecond
	victim := BulkKey(cfg.KeyPrefix, cfg.Seed, cfg.Buckets[0], 3)
	var once sync.Once
	f.fail = func(ctx context.Context, _, key string) error {
		hang := false
		if key == victim {
			once.Do(func() { hang = true })
		}
		if hang {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	r, err := Load(t.Context(), f, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Total.Errors != 1 || r.Total.Written != 10 || r.Buckets[cfg.Buckets[0]].Bulk != 10 {
		t.Fatalf("report %+v, want the hung attempt retried", r.Total)
	}
}

func TestLoadGivesUpAfterMaxAttempts(t *testing.T) {
	f := newFakeS3()
	cfg := testConfig(t, 1, 10)
	victim := BulkKey(cfg.KeyPrefix, cfg.Seed, cfg.Buckets[0], 3)
	f.fail = func(_ context.Context, _, key string) error {
		if key == victim {
			return errors.New("connection reset")
		}
		return nil
	}
	_, err := Load(t.Context(), f, cfg)
	if err == nil || !strings.Contains(err.Error(), "giving up after 3 attempts") {
		t.Fatalf("load returned %v, want it to give up", err)
	}
	if st := readTestState(t, cfg); st.Buckets[cfg.Buckets[0]].Bulk > 3 {
		t.Fatalf("watermark %d passed the failed object 3", st.Buckets[cfg.Buckets[0]].Bulk)
	}
}

func TestLoadStopsOnRefusal(t *testing.T) {
	f := newFakeS3()
	cfg := testConfig(t, 2, 10)
	f.fail = func(context.Context, string, string) error { return &statusError{403} }
	_, err := Load(t.Context(), f, cfg)
	var se *statusError
	if !errors.As(err, &se) || se.code != 403 {
		t.Fatalf("load returned %v, want the 403", err)
	}
}

func TestLoadCountsTakenKeysAsWritten(t *testing.T) {
	f := newFakeS3()
	cfg := testConfig(t, 1, 10)
	b := cfg.Buckets[0]
	f.buckets[b] = &fakeBucket{objects: map[string]int64{
		BulkKey(cfg.KeyPrefix, cfg.Seed, b, 2): 0,
		BulkKey(cfg.KeyPrefix, cfg.Seed, b, 7): 0,
	}}
	r, err := Load(t.Context(), f, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Total.Written != 8 || r.Total.Existed != 2 || r.Buckets[b].Bulk != 10 {
		t.Fatalf("report %+v", r)
	}
	checkBucket(t, f, cfg, b, 10, readTestState(t, cfg).Buckets[b])
}

// A bucket set up with one encryption is not quietly loaded under another.
func TestLoadRefusesAnotherEncryption(t *testing.T) {
	f := newFakeS3()
	cfg := testConfig(t, 1, 5)
	cfg.Encryption = ""
	if _, err := Load(t.Context(), f, cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Encryption = "none"
	cfg.Until = 10
	if _, err := Load(t.Context(), f, cfg); err == nil || !strings.Contains(err.Error(), "encryption") {
		t.Fatalf("load with another encryption returned %v", err)
	}
	if n := len(f.objects(cfg.Buckets[0])); n != 5 {
		t.Fatalf("%d objects after the refused load, want 5", n)
	}
}

func TestLoadRefusesAnotherSeedsState(t *testing.T) {
	f := newFakeS3()
	cfg := testConfig(t, 1, 5)
	if _, err := Load(t.Context(), f, cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Seed = "other"
	if _, err := Load(t.Context(), f, cfg); err == nil || !strings.Contains(err.Error(), "seed") {
		t.Fatalf("load with another seed returned %v", err)
	}
}

func TestLoadRejectsBadConfig(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"no buckets":      func(c *Config) { c.Buckets = nil },
		"no workers":      func(c *Config) { c.Workers = 0 },
		"bad bucket name": func(c *Config) { c.Buckets = []string{"Preload_0"} },
		"inverted sizes":  func(c *Config) { c.RealEvery = 5; c.RealMaxSize = c.RealMinSize - 1 },
		"no state file":   func(c *Config) { c.StatePath = "" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig(t, 1, 5)
			mutate(&cfg)
			if _, err := Load(t.Context(), newFakeS3(), cfg); err == nil {
				t.Fatal("load accepted the config")
			}
		})
	}
}
