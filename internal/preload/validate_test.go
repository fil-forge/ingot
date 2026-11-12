package preload

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// loadForValidation loads buckets of 50 bulk and 2 real objects each and
// returns the load's config.
func loadForValidation(t *testing.T, f *fakeS3, buckets int) Config {
	t.Helper()
	cfg := testConfig(t, buckets, 50)
	cfg.RealEvery = 20
	if _, err := Load(t.Context(), f, cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func validateConfig(load Config) ValidateConfig {
	return ValidateConfig{
		Buckets:        load.Buckets,
		KeyPrefix:      load.KeyPrefix,
		Seed:           load.Seed,
		StatePath:      load.StatePath,
		Label:          "test",
		Sample:         "sample",
		Samples:        5,
		RangeSize:      64 << 10,
		ListBuckets:    2,
		Workers:        4,
		RequestTimeout: time.Minute,
	}
}

func TestValidateChecksALoadedCatalog(t *testing.T) {
	f := newFakeS3()
	load := loadForValidation(t, f, 3)
	var progress strings.Builder
	cfg := validateConfig(load)
	cfg.Progress = &progress
	r, err := Validate(t.Context(), f, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Failures != 0 {
		t.Fatalf("%d failures: %v", r.Failures, r.FailureSample)
	}
	// 52 objects a bucket at seven a page is eight pages.
	for op, want := range map[string]uint64{OpGetRange: 6, OpHead: 15, OpListPage: 16, OpPut: 15, OpDelete: 15} {
		if got := r.Latency[op].Count; got != want {
			t.Errorf("%s: %d timed, want %d", op, got, want)
		}
	}
	if len(r.Lists) != 2 || r.Lists[0].Bucket != load.Buckets[0] || r.Lists[0].Objects != 52 || r.Lists[1].Pages != 8 {
		t.Fatalf("lists %+v", r.Lists)
	}
	if p := r.Point; p.Buckets != 3 || p.MinBulk != 50 || p.MaxBulk != 50 || p.Real != 6 {
		t.Fatalf("test point %+v", p)
	}
	if !slices.Equal(r.Steps, ValidateSteps) {
		t.Fatalf("steps %v", r.Steps)
	}
	if lines := strings.Count(progress.String(), "\n"); lines != len(ValidateSteps) {
		t.Fatalf("progress %q", progress.String())
	}
	// The objects the validation wrote are gone, and nothing else changed.
	st := readTestState(t, load)
	for _, b := range load.Buckets {
		checkBucket(t, f, load, b, 50, st.Buckets[b])
	}
}

// recordingS3 records which objects and ranges a validation reads.
type recordingS3 struct {
	*fakeS3
	mu    sync.Mutex
	reads map[string]bool
}

func (r *recordingS3) note(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads[s] = true
}

func (r *recordingS3) GetObject(ctx context.Context, in *s3.GetObjectInput, o ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	r.note("get " + *in.Key + " " + *in.Range)
	return r.fakeS3.GetObject(ctx, in, o...)
}

func (r *recordingS3) HeadObject(ctx context.Context, in *s3.HeadObjectInput, o ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	r.note("head " + *in.Key)
	return r.fakeS3.HeadObject(ctx, in, o...)
}

func TestValidateReadsTheSameSampleAgain(t *testing.T) {
	f := newFakeS3()
	load := loadForValidation(t, f, 2)
	reads := func(sample string) map[string]bool {
		rec := &recordingS3{fakeS3: f, reads: map[string]bool{}}
		cfg := validateConfig(load)
		cfg.Sample = sample
		cfg.Steps = []string{StepGet, StepHead}
		r, err := Validate(t.Context(), rec, cfg)
		if err != nil || r.Failures != 0 {
			t.Fatalf("validate: %v, report %+v", err, r)
		}
		return rec.reads
	}
	cold, warm := reads("a"), reads("a")
	if len(cold) != 14 || !maps.Equal(cold, warm) {
		t.Fatalf("two runs with one sample read %d and %d requests, the same: %v", len(cold), len(warm), maps.Equal(cold, warm))
	}
	if maps.Equal(cold, reads("b")) {
		t.Fatal("another sample read the same objects and ranges")
	}
}

// keepingDeletes acknowledges deletes without applying them.
type keepingDeletes struct{ *fakeS3 }

func (keepingDeletes) DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}

func TestValidateFindsWrongAnswers(t *testing.T) {
	f := newFakeS3()
	load := loadForValidation(t, f, 1)
	b := load.Buckets[0]
	st := readTestState(t, load)
	// A real object whose bytes changed, a bulk object that is gone, and one
	// that is no longer empty.
	f.buckets[b].bodies[st.Buckets[b].Real[0].Key][100]++
	delete(f.buckets[b].objects, BulkKey(load.KeyPrefix, load.Seed, b, 7))
	f.buckets[b].objects[BulkKey(load.KeyPrefix, load.Seed, b, 8)] = 3

	cfg := validateConfig(load)
	cfg.Samples = 50 // every bulk object
	cfg.RangeSize = 1 << 30
	r, err := Validate(t.Context(), keepingDeletes{f}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	all := strings.Join(r.FailureSample, "\n")
	for _, want := range []string{
		"not the ones written",
		fmt.Sprintf("head %s/%s: status 404", b, BulkKey(load.KeyPrefix, load.Seed, b, 7)),
		"3 bytes, want an empty object",
		"51 objects, the load recorded 52",
		"after its delete: still there",
	} {
		if !strings.Contains(all, want) {
			t.Errorf("no failure says %q in:\n%s", want, all)
		}
	}
	// One read, two heads, one listing, and every written object kept.
	if want := uint64(4 + cfg.Samples); r.Failures != want {
		t.Fatalf("%d failures, want %d:\n%s", r.Failures, want, all)
	}
}

func TestValidateDeletesWhatItWroteWhenPutsFail(t *testing.T) {
	f := newFakeS3()
	load := loadForValidation(t, f, 2)
	before := map[string]map[string]int64{}
	for _, b := range load.Buckets {
		before[b] = f.objects(b)
	}
	var puts int
	f.fail = func(_ context.Context, _, _ string) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		if puts++; puts%2 == 0 {
			return &statusError{503}
		}
		return nil
	}
	cfg := validateConfig(load)
	cfg.Steps = []string{StepWrite}
	r, err := Validate(t.Context(), f, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Failures != 5 || r.Latency[OpPut].Count != 5 || r.Latency[OpDelete].Count != 10 {
		t.Fatalf("failures %d, puts %d, deletes %d", r.Failures, r.Latency[OpPut].Count, r.Latency[OpDelete].Count)
	}
	for _, b := range load.Buckets {
		if !maps.Equal(f.objects(b), before[b]) {
			t.Fatalf("%s changed", b)
		}
	}
}

func TestValidateStopsWhenCanceled(t *testing.T) {
	f := newFakeS3()
	load := loadForValidation(t, f, 1)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	r, err := Validate(ctx, f, validateConfig(load))
	if err == nil || r == nil || r.Failures != 0 {
		t.Fatalf("validate under a canceled context: %v, report %+v", err, r)
	}
}

func TestValidateRefuses(t *testing.T) {
	f := newFakeS3()
	load := loadForValidation(t, f, 1)
	for name, change := range map[string]func(*ValidateConfig){
		"a bucket the load did not set up": func(c *ValidateConfig) { c.Buckets = append(c.Buckets, "preload-999") },
		"an unknown step":                  func(c *ValidateConfig) { c.Steps = []string{"get", "scan"} },
		"no label":                         func(c *ValidateConfig) { c.Label = "" },
		"another seed":                     func(c *ValidateConfig) { c.Seed = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validateConfig(load)
			change(&cfg)
			if _, err := Validate(t.Context(), f, cfg); err == nil {
				t.Fatal("validate ran")
			}
		})
	}
}
