package preload

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
		require.True(t, ok, "%s: bulk object %d (%s) missing", bucket, n, key)
		require.Zero(t, size, "%s: bulk object %d has %d bytes", bucket, n, size)
		delete(objs, key)
	}
	for j, r := range st.Real {
		require.Equal(t, uint64(j), r.Index, "%s: real object %d recorded with index %d", bucket, j, r.Index)
		body := RealBody(cfg.Seed, bucket, r.Index, cfg.RealMinSize, cfg.RealMaxSize)
		sum, err := body.SHA256()
		require.NoError(t, err)
		want := RealKey(cfg.KeyPrefix, sum)
		require.Equal(t, want, r.Key, "%s: real object %d recorded as %s", bucket, j, r.Key)
		size, ok := objs[r.Key]
		require.True(t, ok, "%s: real object %d missing", bucket, j)
		require.Equal(t, r.Size, size, "%s: real object %d: stored bytes against recorded", bucket, j)
		require.Equal(t, body.Size(), size, "%s: real object %d: stored bytes against body", bucket, j)
		require.GreaterOrEqual(t, r.Size, cfg.RealMinSize, "%s: real object %d is %d bytes, outside %d to %d", bucket, j, r.Size, cfg.RealMinSize, cfg.RealMaxSize)
		require.LessOrEqual(t, r.Size, cfg.RealMaxSize, "%s: real object %d is %d bytes, outside %d to %d", bucket, j, r.Size, cfg.RealMinSize, cfg.RealMaxSize)
		delete(objs, r.Key)
	}
	require.Empty(t, objs, "%s: %d unexpected objects", bucket, len(objs))
}

func readTestState(t *testing.T, cfg Config) *State {
	t.Helper()
	st, err := ReadState(cfg.StatePath, cfg.Seed, cfg.KeyPrefix)
	require.NoError(t, err)
	return st
}

func TestLoadWritesEveryObject(t *testing.T) {
	f := newFakeS3()
	cfg := testConfig(t, 3, 50)
	cfg.RealEvery = 20
	r, err := Load(t.Context(), f, cfg)
	require.NoError(t, err)
	st := readTestState(t, cfg)
	for _, b := range cfg.Buckets {
		bs := st.Buckets[b]
		require.True(t, bs.Ready, "%s: state %+v, want ready with 50 bulk and 2 real", b, bs)
		require.Equal(t, uint64(50), bs.Bulk, "%s: state %+v, want ready with 50 bulk and 2 real", b, bs)
		require.Len(t, bs.Real, 2, "%s: state %+v, want ready with 50 bulk and 2 real", b, bs)
		checkBucket(t, f, cfg, b, 50, bs)
		sse := f.buckets[b].sse
		require.Equal(t, "none", sse, "%s: encryption %q", b, sse)
		br := r.Buckets[b]
		require.Equal(t, uint64(50), br.Bulk, "%s: report %+v", b, br)
		require.Equal(t, uint64(52), br.Written, "%s: report %+v", b, br)
		require.Equal(t, uint64(2), br.Real, "%s: report %+v", b, br)
	}
	require.True(t, r.Final, "final report totals %+v", r.Total)
	require.Equal(t, uint64(156), r.Total.Written, "final report totals %+v", r.Total)
	require.Zero(t, r.Total.Existed, "final report totals %+v", r.Total)
	require.Zero(t, r.Total.Errors, "final report totals %+v", r.Total)
	require.Equal(t, uint64(6), r.Total.Real, "final report totals %+v", r.Total)
	require.Equal(t, uint64(150), r.Latency[OpPutBulk].Total.Count, "latency counts %+v", r.Latency)
	require.Equal(t, uint64(6), r.Latency[OpPutReal].Total.Count, "latency counts %+v", r.Latency)
}

// A second load with a higher target writes only what the first did not.
func TestLoadResumesPastATestPoint(t *testing.T) {
	f := newFakeS3()
	cfg := testConfig(t, 2, 30)
	cfg.RealEvery = 20
	_, err := Load(t.Context(), f, cfg)
	require.NoError(t, err)
	cfg.Until = 60
	r, err := Load(t.Context(), f, cfg)
	require.NoError(t, err)
	require.Equal(t, 2, f.creates, "%d bucket creates, want 2: a ready bucket is not set up again", f.creates)
	st := readTestState(t, cfg)
	for _, b := range cfg.Buckets {
		bs := st.Buckets[b]
		require.Equal(t, uint64(60), bs.Bulk, "%s: state %+v, want 60 bulk and 3 real", b, bs)
		require.Len(t, bs.Real, 3, "%s: state %+v, want 60 bulk and 3 real", b, bs)
		checkBucket(t, f, cfg, b, 60, st.Buckets[b])
	}
	// 30 bulk and 2 real per bucket.
	require.Equal(t, uint64(64), r.Total.Written, "second load totals %+v", r.Total)
	require.Zero(t, r.Total.Existed, "second load totals %+v", r.Total)
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
	require.ErrorIs(t, err, context.Canceled, "interrupted load")
	st := readTestState(t, cfg)
	// Every object the state does not account for was written past the
	// watermark, or is a real object written but not recorded: the resumed
	// load must find exactly these taken. A slow PUT holds the watermark
	// while its bucket's other workers carry on, so there can be many.
	var unrecorded uint64
	for _, b := range cfg.Buckets {
		bs := st.Buckets[b]
		require.Less(t, bs.Bulk, uint64(200), "%s: watermark %d after an interrupt", b, bs.Bulk)
		objs := f.objects(b)
		for n := range bs.Bulk {
			_, ok := objs[BulkKey(cfg.KeyPrefix, cfg.Seed, b, n)]
			require.True(t, ok, "%s: bulk object %d below the watermark %d missing", b, n, bs.Bulk)
		}
		unrecorded += uint64(len(objs)) - bs.Bulk - uint64(len(bs.Real))
	}

	f.after = nil
	r, err := Load(t.Context(), f, cfg)
	require.NoError(t, err)
	st = readTestState(t, cfg)
	for _, b := range cfg.Buckets {
		checkBucket(t, f, cfg, b, 200, st.Buckets[b])
	}
	require.Equal(t, unrecorded, r.Total.Existed, "resumed load found %d keys taken, want the %d written but not recorded", r.Total.Existed, unrecorded)
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
	require.ErrorIs(t, err, context.Canceled, "load returned %v, want the interrupt and ErrStateNotSaved", err)
	require.ErrorIs(t, err, ErrStateNotSaved, "load returned %v, want the interrupt and ErrStateNotSaved", err)
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
	require.NoError(t, err)
	require.Equal(t, uint64(2), r.Total.Errors, "totals %+v, want 2 errors and 10 written", r.Total)
	require.Equal(t, uint64(10), r.Total.Written, "totals %+v, want 2 errors and 10 written", r.Total)
	require.Contains(t, r.LastError, victim, "last error %q does not name %s", r.LastError, victim)
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
	require.NoError(t, err)
	require.Equal(t, uint64(1), r.Total.Errors, "report %+v, want the hung attempt retried", r.Total)
	require.Equal(t, uint64(10), r.Total.Written, "report %+v, want the hung attempt retried", r.Total)
	require.Equal(t, uint64(10), r.Buckets[cfg.Buckets[0]].Bulk, "report %+v, want the hung attempt retried", r.Total)
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
	require.ErrorContains(t, err, "giving up after 3 attempts", "load must give up")
	st := readTestState(t, cfg)
	require.LessOrEqual(t, st.Buckets[cfg.Buckets[0]].Bulk, uint64(3), "watermark passed the failed object 3")
}

func TestLoadStopsOnRefusal(t *testing.T) {
	f := newFakeS3()
	cfg := testConfig(t, 2, 10)
	f.fail = func(context.Context, string, string) error { return &statusError{403} }
	_, err := Load(t.Context(), f, cfg)
	var se *statusError
	require.ErrorAs(t, err, &se, "load returned %v, want the 403", err)
	require.Equal(t, 403, se.code, "load")
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
	require.NoError(t, err)
	require.Equal(t, uint64(8), r.Total.Written, "report %+v", r)
	require.Equal(t, uint64(2), r.Total.Existed, "report %+v", r)
	require.Equal(t, uint64(10), r.Buckets[b].Bulk, "report %+v", r)
	checkBucket(t, f, cfg, b, 10, readTestState(t, cfg).Buckets[b])
}

// A bucket set up with one encryption is not quietly loaded under another.
func TestLoadRefusesAnotherEncryption(t *testing.T) {
	f := newFakeS3()
	cfg := testConfig(t, 1, 5)
	cfg.Encryption = ""
	_, err := Load(t.Context(), f, cfg)
	require.NoError(t, err)
	cfg.Encryption = "none"
	cfg.Until = 10
	_, err = Load(t.Context(), f, cfg)
	require.ErrorContains(t, err, "encryption", "load with another encryption")
	n := len(f.objects(cfg.Buckets[0]))
	require.Equal(t, 5, n, "%d objects after the refused load", n)
}

func TestLoadRefusesAnotherSeedsState(t *testing.T) {
	f := newFakeS3()
	cfg := testConfig(t, 1, 5)
	_, err := Load(t.Context(), f, cfg)
	require.NoError(t, err)
	cfg.Seed = "other"
	_, err = Load(t.Context(), f, cfg)
	require.ErrorContains(t, err, "seed", "load with another seed")
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
			_, err := Load(t.Context(), newFakeS3(), cfg)
			require.Error(t, err, "load accepted the config")
		})
	}
}
