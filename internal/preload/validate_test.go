package preload

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loadForValidation loads buckets of 50 bulk and 2 real objects each and
// returns the load's config.
func loadForValidation(t *testing.T, f *fakeS3, buckets int) Config {
	t.Helper()
	cfg := testConfig(t, buckets, 50)
	cfg.RealEvery = 20
	_, err := Load(t.Context(), f, cfg)
	require.NoError(t, err)
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
		WriteReal:      4,
		RealMinSize:    load.RealMinSize,
		RealMaxSize:    load.RealMaxSize,
	}
}

func TestValidateChecksALoadedCatalog(t *testing.T) {
	f := newFakeS3()
	load := loadForValidation(t, f, 3)
	var progress strings.Builder
	cfg := validateConfig(load)
	cfg.Progress = &progress
	r, err := Validate(t.Context(), f, cfg)
	require.NoError(t, err)
	require.Zero(t, r.Failures, "%d failures: %v", r.Failures, r.FailureSample)
	// 52 objects a bucket at seven a page is eight pages.
	for op, want := range map[string]uint64{OpGetRange: 6, OpHead: 15, OpListPage: 16, OpPut: 15, OpDelete: 15, OpPutReal: 4, OpDeleteReal: 4} {
		got := r.Latency[op].Count
		assert.Equal(t, want, got, "%s: %d timed", op, got)
	}
	require.Len(t, r.Lists, 2, "lists %+v", r.Lists)
	require.Equal(t, load.Buckets[0], r.Lists[0].Bucket, "lists %+v", r.Lists)
	require.Equal(t, uint64(52), r.Lists[0].Objects, "lists %+v", r.Lists)
	require.Equal(t, 8, r.Lists[1].Pages, "lists %+v", r.Lists)
	require.Zero(t, r.Lists[0].Missing, "lists %+v", r.Lists)
	require.Zero(t, r.Lists[0].Extra, "lists %+v", r.Lists)
	require.True(t, r.Lists[0].Complete, "lists %+v", r.Lists)
	p := r.Point
	require.Equal(t, 3, p.Buckets, "test point %+v", p)
	require.Equal(t, uint64(50), p.MinBulk, "test point %+v", p)
	require.Equal(t, uint64(50), p.MaxBulk, "test point %+v", p)
	require.Equal(t, uint64(6), p.Real, "test point %+v", p)
	require.True(t, slices.Equal(r.Steps, ValidateSteps), "steps %v, interrupted %v, write run %q", r.Steps, r.Interrupted, r.WriteRun)
	require.False(t, r.Interrupted, "steps %v, interrupted %v, write run %q", r.Steps, r.Interrupted, r.WriteRun)
	require.Len(t, r.WriteRun, 16, "steps %v, interrupted %v, write run %q", r.Steps, r.Interrupted, r.WriteRun)
	// A line per step, and one as the write step starts.
	require.Equal(t, len(ValidateSteps)+1, strings.Count(progress.String(), "\n"), "progress lines: %q", progress.String())
	require.Contains(t, progress.String(), "run "+r.WriteRun, "progress")
	// The objects the validation wrote are gone, and nothing else changed.
	st := readTestState(t, load)
	for _, b := range load.Buckets {
		checkBucket(t, f, load, b, 50, st.Buckets[b])
	}
}

// realPuts records the real objects a validation writes.
type realPuts struct {
	*fakeS3
	mu   sync.Mutex
	puts []realPut
}

type realPut struct {
	bucket, key, sum string
	size             int64
}

func (r *realPuts) PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if n := aws.ToInt64(in.ContentLength); n > 0 {
		r.mu.Lock()
		sum, _ := base64.StdEncoding.DecodeString(aws.ToString(in.ChecksumSHA256))
		r.puts = append(r.puts, realPut{aws.ToString(in.Bucket), aws.ToString(in.Key), hex.EncodeToString(sum), n})
		r.mu.Unlock()
	}
	return r.fakeS3.PutObject(ctx, in, opts...)
}

// TestValidateWritesRealObjects: the write step writes its real objects over
// the buckets in turn, each named by its body's digest as the load names its
// own, sized as the load sizes them, and new on every run.
func TestValidateWritesRealObjects(t *testing.T) {
	f := newFakeS3()
	load := loadForValidation(t, f, 3)
	st := readTestState(t, load)
	cfg := validateConfig(load)
	cfg.Steps = []string{StepWrite}
	var progress strings.Builder
	cfg.Progress = &progress
	keyRE := regexp.MustCompile("^" + regexp.QuoteMeta(load.KeyPrefix) + "[0-9a-f]{64}$")
	runs := map[string]bool{}
	for range 2 {
		rec := &realPuts{fakeS3: f}
		r, err := Validate(t.Context(), rec, cfg)
		require.NoError(t, err, "validate: %v, failures %v", err, r.FailureSample)
		require.Zero(t, r.Failures, "validate: %v, failures %v", err, r.FailureSample)
		slices.SortFunc(rec.puts, func(a, b realPut) int { return strings.Compare(a.bucket, b.bucket) })
		var buckets []string
		for _, p := range rec.puts {
			buckets = append(buckets, p.bucket)
			// The fake checks the body against its checksum, so a key that
			// ends in the checksum names the body's digest.
			assert.Regexp(t, keyRE, p.key, "real object %s/%s", p.bucket, p.key)
			assert.True(t, strings.HasSuffix(p.key, p.sum), "real object %s/%s does not end in its checksum %s", p.bucket, p.key, p.sum)
			assert.GreaterOrEqual(t, p.size, load.RealMinSize, "real object %s/%s size", p.bucket, p.key)
			assert.LessOrEqual(t, p.size, load.RealMaxSize, "real object %s/%s size", p.bucket, p.key)
			for _, o := range st.Buckets[p.bucket].Real {
				assert.NotEqual(t, p.key, o.Key, "wrote the load's object %s/%s", p.bucket, p.key)
			}
			assert.False(t, runs[p.key], "wrote %s/%s on an earlier run too", p.bucket, p.key)
			runs[p.key] = true
		}
		b := load.Buckets
		want := []string{b[0], b[0], b[1], b[2]}
		assert.True(t, slices.Equal(buckets, want), "real objects went to %v, want %v", buckets, want)
		assert.Contains(t, progress.String(), OpPutReal+" 4", "progress")
		assert.Contains(t, progress.String(), OpDeleteReal+" 4", "progress")
	}
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
		require.NoError(t, err, "validate: %v, report %+v", err, r)
		require.Zero(t, r.Failures, "validate: %v, report %+v", err, r)
		return rec.reads
	}
	cold, warm := reads("a"), reads("a")
	require.Len(t, cold, 14, "requests one sample read")
	require.True(t, maps.Equal(cold, warm), "two runs with one sample read different requests")
	require.False(t, maps.Equal(cold, reads("b")), "another sample read the same objects and ranges")
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
	require.NoError(t, err)
	all := strings.Join(r.FailureSample, "\n")
	for _, want := range []string{
		"not the ones written",
		fmt.Sprintf("head %s/%s: status 404", b, BulkKey(load.KeyPrefix, load.Seed, b, 7)),
		"3 bytes, want an empty object",
		fmt.Sprintf("list %s: %s is missing", b, BulkKey(load.KeyPrefix, load.Seed, b, 7)),
		"after its delete: still there",
	} {
		assert.Contains(t, all, want, "no failure says %q in:\n%s", want, all)
	}
	// One read, two heads, one listing, and every written object kept.
	want := uint64(4 + cfg.Samples + cfg.WriteReal)
	require.Equal(t, want, r.Failures, "%d failures", r.Failures)
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
	require.NoError(t, err)
	// Half of the 14 PUTs fail, and all 14 objects get a delete.
	timed := r.Latency[OpPut].Count + r.Latency[OpPutReal].Count
	require.Equal(t, uint64(7), r.Failures, "failures")
	require.Equal(t, uint64(7), timed, "timed puts")
	require.Equal(t, uint64(10), r.Latency[OpDelete].Count, "bulk deletes")
	require.Equal(t, uint64(4), r.Latency[OpDeleteReal].Count, "real deletes")
	for _, b := range load.Buckets {
		require.True(t, maps.Equal(f.objects(b), before[b]), "%s changed", b)
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

// TestValidateInterruptedWriteLeavesNothing interrupts the write step among
// its zero-byte PUTs and after its first real one: two buckets of five
// zero-byte objects come first, then the real ones.
func TestValidateInterruptedWriteLeavesNothing(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cancelAt int
		real     uint64
	}{
		{"zero-byte", 7, 0},
		{"real", 11, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
				if stored == base+tc.cancelAt {
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
			require.Error(t, err, "interrupted write: %v, report %+v", err, r)
			require.True(t, r.Interrupted, "interrupted write: %v, report %+v", err, r)
			require.True(t, slices.Equal(r.Steps, []string{StepHead}), "interrupted write: %v, report %+v", err, r)
			require.NotEmpty(t, r.WriteRun, "interrupted write: %v, report %+v", err, r)
			require.Zero(t, r.Failures, "interrupted write: %v, report %+v", err, r)
			// No PUT goes out after the interrupt, and each one sent gets a
			// delete.
			want := int64(tc.cancelAt)
			puts, deletes := client.puts.Load(), client.deletes.Load()
			require.Equal(t, want, puts, "%d PUTs and %d deletes", puts, deletes)
			require.Equal(t, want, deletes, "%d PUTs and %d deletes", puts, deletes)
			got := r.Latency[OpDeleteReal].Count
			require.Equal(t, tc.real, got, "%d real objects deleted", got)
			out := progress.String()
			for _, want := range []string{"run " + r.WriteRun, "interrupt again to stop at once", fmt.Sprintf("deleting the %d objects whose PUT was sent", tc.cancelAt)} {
				require.Contains(t, out, want, "progress %q lacks %q", out, want)
			}
			for _, b := range load.Buckets {
				require.True(t, maps.Equal(f.objects(b), before[b]), "%s: objects left differ from those before", b)
			}
		})
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
	require.Error(t, err, "validate: %v, report %+v", err, r)
	require.True(t, r.Interrupted, "validate: %v, report %+v", err, r)
	require.Empty(t, r.Steps, "validate: %v, report %+v", err, r)
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
	require.NoError(t, err)
	require.Len(t, r.Lists, 1, "lists %+v, failures %v", r.Lists, r.FailureSample)
	require.Equal(t, uint64(52), r.Lists[0].Objects, "lists %+v, failures %v", r.Lists, r.FailureSample)
	require.Equal(t, uint64(3), r.Lists[0].Missing, "lists %+v, failures %v", r.Lists, r.FailureSample)
	require.Equal(t, uint64(3), r.Lists[0].Extra, "lists %+v, failures %v", r.Lists, r.FailureSample)
	require.Equal(t, uint64(3), r.Failures, "lists %+v, failures %v", r.Lists, r.FailureSample)
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
	require.NoError(t, err)
	require.Equal(t, uint64(1), r.Failures, "failures %v", r.FailureSample)
	require.Contains(t, r.FailureSample[0], "out of order")
	require.Equal(t, uint64(2), r.Latency[OpListPage].Count, "list pages")
}

// misplacedKey moves the first page's first key to its end, and continues
// after it, as a gateway whose next page starts after the last key returned
// would. Every page after that is the first page again, until it gives up
// after a few, so a listing that doesn't stop fails rather than hangs.
type misplacedKey struct {
	*fakeS3
	pages *atomic.Int64
}

func (m misplacedKey) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, o ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.pages.Add(1) > 10 {
		return nil, errors.New("misplacedKey: the listing went on for 10 pages")
	}
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
	r, err := Validate(t.Context(), misplacedKey{f, new(atomic.Int64)}, cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(1), r.Failures, "failures %v", r.FailureSample)
	require.Contains(t, r.FailureSample[0], "out of order")
	require.Equal(t, uint64(1), r.Latency[OpListPage].Count, "list pages")
	// The key that came late isn't reported missing.
	require.Len(t, r.Lists, 1, "lists %+v", r.Lists)
	require.False(t, r.Lists[0].Complete, "lists %+v", r.Lists)
	require.Zero(t, r.Lists[0].Missing, "lists %+v", r.Lists)
}

// failingLaterPages fails every listing page after the first.
type failingLaterPages struct{ *fakeS3 }

func (f failingLaterPages) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, o ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if in.ContinuationToken != nil {
		return nil, &statusError{503}
	}
	return f.fakeS3.ListObjectsV2(ctx, in, o...)
}

// A listing that fails part way still reports the recorded keys missing
// from what it listed before it failed.
func TestValidateReportsMissingKeysBeforeAFailedPage(t *testing.T) {
	f := newFakeS3()
	load := loadForValidation(t, f, 1)
	b := load.Buckets[0]
	keys := slices.Sorted(maps.Keys(f.objects(b)))
	delete(f.buckets[b].objects, keys[0])
	cfg := validateConfig(load)
	cfg.Steps = []string{StepList}
	r, err := Validate(t.Context(), failingLaterPages{f}, cfg)
	require.NoError(t, err)
	all := strings.Join(r.FailureSample, "\n")
	require.Equal(t, uint64(2), r.Failures, "failures %d:\n%s", r.Failures, all)
	require.Contains(t, all, keys[0]+" is missing", "failures %d:\n%s", r.Failures, all)
	require.Contains(t, all, "status 503", "failures %d:\n%s", r.Failures, all)
	require.Len(t, r.Lists, 1, "lists %+v", r.Lists)
	require.False(t, r.Lists[0].Complete, "lists %+v", r.Lists)
	require.Equal(t, uint64(1), r.Lists[0].Missing, "lists %+v", r.Lists)
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
	_, err := Validate(ctx, client, cfg)
	require.Error(t, err, "validate was not interrupted")
	client.late.Wait()
	after := f.objects(load.Buckets[0])
	require.True(t, maps.Equal(after, before), "%d objects, want %d", len(after), len(before))
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
	require.NoError(t, err)
	require.Equal(t, uint64(cfg.Samples), r.Failures, "failures %v", r.FailureSample)
	require.Contains(t, r.FailureSample[0], "no Content-Length", "failures %v", r.FailureSample)
}

func TestValidateStopsWhenCanceled(t *testing.T) {
	f := newFakeS3()
	load := loadForValidation(t, f, 1)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	r, err := Validate(ctx, f, validateConfig(load))
	require.Error(t, err, "validate under a canceled context: %v, report %+v", err, r)
	require.NotNil(t, r, "validate under a canceled context: %v, report %+v", err, r)
	require.Zero(t, r.Failures, "validate under a canceled context: %v, report %+v", err, r)
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
			_, err := Validate(t.Context(), f, cfg)
			require.Error(t, err, "validate ran")
		})
	}
}
