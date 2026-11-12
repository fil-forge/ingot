package preload

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	if len(r.Lists) != 2 || r.Lists[0].Bucket != load.Buckets[0] || r.Lists[0].Objects != 52 || r.Lists[1].Pages != 8 ||
		r.Lists[0].Missing != 0 || r.Lists[0].Extra != 0 {
		t.Fatalf("lists %+v", r.Lists)
	}
	if p := r.Point; p.Buckets != 3 || p.MinBulk != 50 || p.MaxBulk != 50 || p.Real != 6 {
		t.Fatalf("test point %+v", p)
	}
	if !slices.Equal(r.Steps, ValidateSteps) || r.Interrupted || len(r.WriteRun) != 16 {
		t.Fatalf("steps %v, interrupted %v, write run %q", r.Steps, r.Interrupted, r.WriteRun)
	}
	// A line per step, and one as the write step starts.
	if lines := strings.Count(progress.String(), "\n"); lines != len(ValidateSteps)+1 || !strings.Contains(progress.String(), "run "+r.WriteRun) {
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
		fmt.Sprintf("list %s: %s is missing", b, BulkKey(load.KeyPrefix, load.Seed, b, 7)),
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

// ctxS3 answers a request whose context has ended with that context's
// error, as the SDK does, and counts the PUTs and deletes it is sent.
type ctxS3 struct {
	*fakeS3
	puts, deletes *atomic.Int64
}

func newCtxS3(f *fakeS3) ctxS3 {
	return ctxS3{f, new(atomic.Int64), new(atomic.Int64)}
}

func (c ctxS3) PutObject(ctx context.Context, in *s3.PutObjectInput, o ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	c.puts.Add(1)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.fakeS3.PutObject(ctx, in, o...)
}

func (c ctxS3) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, o ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	c.deletes.Add(1)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.fakeS3.DeleteObject(ctx, in, o...)
}

func TestValidateInterruptedWriteLeavesNothing(t *testing.T) {
	f := newFakeS3()
	load := loadForValidation(t, f, 2)
	before := map[string]map[string]int64{}
	for _, b := range load.Buckets {
		before[b] = f.objects(b)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	base := f.stored
	f.after = func(stored int) {
		if stored == base+7 {
			cancel()
		}
	}
	cfg := validateConfig(load)
	cfg.Steps = []string{StepHead, StepWrite}
	cfg.Workers = 1
	var progress strings.Builder
	cfg.Progress = &progress
	client := newCtxS3(f)
	r, err := Validate(ctx, client, cfg)
	if err == nil || !r.Interrupted || !slices.Equal(r.Steps, []string{StepHead}) || r.WriteRun == "" || r.Failures != 0 {
		t.Fatalf("interrupted write: %v, report %+v", err, r)
	}
	// No PUT goes out after the interrupt, and each one sent gets a
	// delete.
	if puts, deletes := client.puts.Load(), client.deletes.Load(); puts != 7 || deletes != 7 {
		t.Fatalf("%d PUTs and %d deletes, want 7 of each", puts, deletes)
	}
	out := progress.String()
	for _, want := range []string{"run " + r.WriteRun, "interrupt again to stop at once", "deleting the 7 objects whose PUT was sent"} {
		if !strings.Contains(out, want) {
			t.Fatalf("progress %q lacks %q", out, want)
		}
	}
	for _, b := range load.Buckets {
		if !maps.Equal(f.objects(b), before[b]) {
			t.Fatalf("%s: %d objects left, want %d", b, len(f.objects(b)), len(before[b]))
		}
	}
}

// cancelingHead ends the run's context part way through a HEAD, after the
// step has handed out its last request, then answers it.
type cancelingHead struct {
	*fakeS3
	cancel context.CancelFunc
}

func (c cancelingHead) HeadObject(ctx context.Context, in *s3.HeadObjectInput, o ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	time.Sleep(50 * time.Millisecond)
	c.cancel()
	return &s3.HeadObjectOutput{ContentLength: new(int64)}, nil
}

// An interrupt during the last requests of the last step still marks the run
// interrupted: their answers are not counted, so the run is not complete.
func TestValidateCountsAnInterruptDuringTheLastRequests(t *testing.T) {
	f := newFakeS3()
	load := loadForValidation(t, f, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cfg := validateConfig(load)
	cfg.Steps = []string{StepWrite}
	cfg.Samples = 1
	cfg.Workers = 1
	r, err := Validate(ctx, cancelingHead{f, cancel}, cfg)
	if err == nil || !r.Interrupted || len(r.Steps) != 0 {
		t.Fatalf("validate: %v, report %+v", err, r)
	}
}

func TestValidateListNeedsEveryRecordedObject(t *testing.T) {
	f := newFakeS3()
	load := loadForValidation(t, f, 1)
	b := load.Buckets[0]
	for n := range uint64(3) {
		delete(f.buckets[b].objects, BulkKey(load.KeyPrefix, load.Seed, b, n))
		f.buckets[b].objects[ValidateKey(load.KeyPrefix, load.Seed, b, "left", n)] = 0
	}
	cfg := validateConfig(load)
	cfg.Steps = []string{StepList}
	r, err := Validate(t.Context(), f, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Lists) != 1 || r.Lists[0].Objects != 52 || r.Lists[0].Missing != 3 || r.Lists[0].Extra != 3 || r.Failures != 3 {
		t.Fatalf("lists %+v, failures %v", r.Lists, r.FailureSample)
	}
}

// stuckListing ignores the continuation token, so every page is the first.
type stuckListing struct{ *fakeS3 }

func (s stuckListing) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, o ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	first := *in
	first.ContinuationToken = nil
	return s.fakeS3.ListObjectsV2(ctx, &first, o...)
}

func TestValidateStopsOnAListingThatDoesNotMoveOn(t *testing.T) {
	f := newFakeS3()
	load := loadForValidation(t, f, 1)
	cfg := validateConfig(load)
	cfg.Steps = []string{StepList}
	r, err := Validate(t.Context(), stuckListing{f}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Failures != 1 || !strings.Contains(r.FailureSample[0], "out of order") || r.Latency[OpListPage].Count != 2 {
		t.Fatalf("failures %v, pages %d", r.FailureSample, r.Latency[OpListPage].Count)
	}
}

// misplacedKey moves the first page's first key to its end, and continues
// after it, as a gateway whose next page starts after the last key returned
// would. Every page after that is the first page again.
type misplacedKey struct{ *fakeS3 }

func (m misplacedKey) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, o ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	first := *in
	first.ContinuationToken = nil
	out, err := m.fakeS3.ListObjectsV2(ctx, &first, o...)
	if err != nil {
		return nil, err
	}
	out.Contents = append(out.Contents[1:], out.Contents[0])
	out.NextContinuationToken = out.Contents[len(out.Contents)-1].Key
	return out, nil
}

func TestValidateStopsAtAKeyOutOfOrder(t *testing.T) {
	f := newFakeS3()
	load := loadForValidation(t, f, 1)
	cfg := validateConfig(load)
	cfg.Steps = []string{StepList}
	r, err := Validate(t.Context(), misplacedKey{f}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Failures != 1 || !strings.Contains(r.FailureSample[0], "out of order") || r.Latency[OpListPage].Count != 1 {
		t.Fatalf("failures %v, pages %d", r.FailureSample, r.Latency[OpListPage].Count)
	}
}

// lateCommit is a gateway that commits a PUT even when the client has given
// up on it, a little after the client went away, as a server that doesn't
// cancel a handler when its client disconnects may. The PUT during which
// cancel ends the run is the one it holds.
type lateCommit struct {
	*fakeS3
	cancel context.CancelFunc
	once   sync.Once
	late   sync.WaitGroup
}

func (l *lateCommit) PutObject(ctx context.Context, in *s3.PutObjectInput, o ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	held := false
	l.once.Do(func() { held = true })
	if !held {
		return l.fakeS3.PutObject(ctx, in, o...)
	}
	l.cancel()
	time.Sleep(20 * time.Millisecond)
	if err := ctx.Err(); err != nil {
		l.late.Go(func() {
			time.Sleep(50 * time.Millisecond)
			l.fakeS3.PutObject(context.Background(), in, o...)
		})
		return nil, err
	}
	return l.fakeS3.PutObject(ctx, in, o...)
}

// A PUT in flight at an interrupt runs to its answer before its delete is
// sent, so a gateway can't commit it after the delete.
func TestValidateFinishesAPutInFlightBeforeItsDelete(t *testing.T) {
	f := newFakeS3()
	load := loadForValidation(t, f, 1)
	before := f.objects(load.Buckets[0])
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	client := &lateCommit{fakeS3: f, cancel: cancel}
	cfg := validateConfig(load)
	cfg.Steps = []string{StepWrite}
	cfg.Workers = 1
	if _, err := Validate(ctx, client, cfg); err == nil {
		t.Fatal("validate was not interrupted")
	}
	client.late.Wait()
	if after := f.objects(load.Buckets[0]); !maps.Equal(after, before) {
		t.Fatalf("%d objects, want %d", len(after), len(before))
	}
}

// noLength answers a HEAD without a Content-Length.
type noLength struct{ *fakeS3 }

func (noLength) HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{}, nil
}

func TestValidateNeedsAContentLength(t *testing.T) {
	f := newFakeS3()
	load := loadForValidation(t, f, 1)
	cfg := validateConfig(load)
	cfg.Steps = []string{StepHead}
	r, err := Validate(t.Context(), noLength{f}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Failures != uint64(cfg.Samples) || !strings.Contains(r.FailureSample[0], "no Content-Length") {
		t.Fatalf("failures %v", r.FailureSample)
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
