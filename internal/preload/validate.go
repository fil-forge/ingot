package preload

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ValidateS3 is the part of the S3 API a validation uses. *s3.Client
// implements it.
type ValidateS3 interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// The steps a validation can run, in the order it runs them.
const (
	// StepGet reads a range of every real object.
	StepGet = "get"
	// StepHead reads the metadata of a sample of bulk objects.
	StepHead = "head"
	// StepList lists whole buckets.
	StepList = "list"
	// StepWrite writes new zero-byte objects and a few real ones
	// create-only, as the pack server would, then deletes them.
	StepWrite = "write"
)

// ValidateSteps lists every step, in the order a validation runs them.
var ValidateSteps = []string{StepGet, StepHead, StepList, StepWrite}

// Operation names, as a validation report keys its latencies by them. The
// write step's real objects are timed apart from its zero-byte ones, their
// PUTs under the load's OpPutReal.
const (
	OpGetRange   = "get_range"
	OpHead       = "head"
	OpListPage   = "list_page"
	OpPut        = "put"
	OpDelete     = "delete"
	OpDeleteReal = "delete_real"
)

// maxFailureSample bounds the failures a report spells out.
const maxFailureSample = 100

// ValidateConfig is what a validation reads and writes, and how.
type ValidateConfig struct {
	// Buckets are the buckets to validate. Each must be one the load set up.
	Buckets []string
	// KeyPrefix and Seed are the load's.
	KeyPrefix string
	Seed      string
	// StatePath is the load's state file. A validation reads it and never
	// writes it.
	StatePath string

	// Label names the run in its report.
	Label string
	// Sample determines which bulk objects are read and which range of each
	// real object: two runs with the same Sample read the same ones, so the
	// second of them reads what the first left cached.
	Sample string
	// Steps are the steps to run; nil runs every one.
	Steps []string

	// Samples is the number of bulk objects read, and of new zero-byte
	// objects written and deleted, in each bucket.
	Samples int
	// WriteReal is the number of new real objects written and deleted, over
	// the buckets in turn from the first, each from RealMinSize to
	// RealMaxSize bytes as the load sizes its own.
	WriteReal                int
	RealMinSize, RealMaxSize int64
	// RangeSize is the length of each ranged read of a real object.
	RangeSize int64
	// ListBuckets is the number of buckets listed in full, from the first.
	ListBuckets int
	// Workers is the number of requests in flight.
	Workers int
	// RequestTimeout bounds each request; 0 leaves it unbounded.
	RequestTimeout time.Duration

	// Progress, when set, receives a line of text as each step ends.
	Progress io.Writer
}

func (c *ValidateConfig) validate() error {
	switch {
	case len(c.Buckets) == 0:
		return errors.New("no buckets")
	case c.StatePath == "":
		return errors.New("no state file")
	case c.Label == "":
		return errors.New("no label")
	case c.Samples < 1:
		return errors.New("samples must be at least 1")
	case c.WriteReal < 0:
		return errors.New("real objects written must not be negative")
	case c.WriteReal > 0 && (c.RealMinSize < 1 || c.RealMaxSize < c.RealMinSize):
		return fmt.Errorf("real object sizes %d to %d are not a range of positive sizes", c.RealMinSize, c.RealMaxSize)
	case c.RangeSize < 1:
		return errors.New("range size must be at least 1 byte")
	case c.ListBuckets < 0:
		return errors.New("list buckets must not be negative")
	case c.Workers < 1:
		return errors.New("workers must be at least 1")
	case c.RequestTimeout < 0:
		return errors.New("request timeout must not be negative")
	}
	for _, s := range c.Steps {
		if !slices.Contains(ValidateSteps, s) {
			return fmt.Errorf("unknown step %q: steps are %s", s, strings.Join(ValidateSteps, ", "))
		}
	}
	return nil
}

func (c *ValidateConfig) runs(step string) bool {
	return c.Steps == nil || slices.Contains(c.Steps, step)
}

// ValidateReport is what a validation found.
type ValidateReport struct {
	Time   time.Time `json:"time"`
	Label  string    `json:"label"`
	Sample string    `json:"sample"`
	// Steps are the steps that ran to the end. Interrupted marks a run that
	// stopped before the rest of those asked for.
	Steps       []string `json:"steps"`
	Interrupted bool     `json:"interrupted,omitempty"`
	ElapsedS    float64  `json:"elapsed_s"`
	// WriteRun is different on every run, so the objects the write step
	// writes are too: zero-byte object i of a bucket is ValidateKey(prefix,
	// seed, bucket, WriteRun, i), and a real object's body depends on it.
	WriteRun string `json:"write_run,omitempty"`
	// Point is the catalog the validation ran against, as the load's state
	// file records it.
	Point TestPoint `json:"test_point"`
	// Latency is keyed by operation (OpGetRange, OpHead, ...). It times
	// requests that succeeded, including those whose answer was wrong.
	Latency map[string]LatencySummary `json:"latency"`
	Lists   []ListResult              `json:"lists,omitempty"`
	// Failures counts requests that failed or answered wrongly, and
	// FailureSample spells out the first of them.
	Failures      uint64   `json:"failures"`
	FailureSample []string `json:"failure_sample,omitempty"`
}

// TestPoint describes the buckets a validation ran against.
type TestPoint struct {
	Buckets int    `json:"buckets"`
	MinBulk uint64 `json:"min_bulk"`
	MaxBulk uint64 `json:"max_bulk"`
	Real    uint64 `json:"real"`
}

// ListResult is one bucket's full listing.
type ListResult struct {
	Bucket  string `json:"bucket"`
	Objects uint64 `json:"objects"`
	// Missing counts the objects the load recorded that the listing lacked,
	// each also a failure. Extra counts listed objects the load did not
	// record, which a load stopped part way, or a validation stopped part
	// way, can leave.
	Missing uint64 `json:"missing"`
	Extra   uint64 `json:"extra"`
	// Complete is set when the listing ran to its end; a failure says why
	// one stopped.
	Complete  bool    `json:"complete"`
	Pages     int     `json:"pages"`
	DurationS float64 `json:"duration_s"`
}

// Validate times reads, listings, writes and deletes against the catalog a
// load built, and checks each answer: a bulk object is there and empty, a
// range of a real object holds the bytes the load wrote, a listing is in
// order and holds every object the load recorded, new objects, zero-byte and
// real, can be written create-only, and once deleted they are gone. It deletes only the objects it
// wrote itself, and deletes them even when ctx ends.
//
// A request that fails or answers wrongly is counted in the report, not
// returned as an error: Validate returns an error only when it cannot start,
// or when ctx ends, in which case the report is marked interrupted and lists
// the steps that ran to the end.
func Validate(ctx context.Context, client ValidateS3, cfg ValidateConfig) (*ValidateReport, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validate: %w", err)
	}
	st, err := ReadState(cfg.StatePath, cfg.Seed, cfg.KeyPrefix)
	if err != nil {
		return nil, err
	}
	v := &validator{client: client, cfg: cfg, hist: make(map[string]*Histogram)}
	v.report = &ValidateReport{
		Label:  cfg.Label,
		Sample: cfg.Sample,
		Steps:  []string{},
		Point:  TestPoint{Buckets: len(cfg.Buckets), MinBulk: ^uint64(0)},
	}
	for _, name := range cfg.Buckets {
		bs, ok := st.Buckets[name]
		if !ok || !bs.Ready {
			return nil, fmt.Errorf("validate: bucket %s has not been loaded (state %s)", name, cfg.StatePath)
		}
		v.buckets = append(v.buckets, validateBucket{name: name, state: bs})
		v.report.Point.MinBulk = min(v.report.Point.MinBulk, bs.Bulk)
		v.report.Point.MaxBulk = max(v.report.Point.MaxBulk, bs.Bulk)
		v.report.Point.Real += uint64(len(bs.Real))
	}

	start := time.Now()
	for _, step := range ValidateSteps {
		if !cfg.runs(step) {
			continue
		}
		switch step {
		case StepGet:
			err = v.run(ctx, v.getTasks())
		case StepHead:
			err = v.run(ctx, v.headTasks())
		case StepList:
			err = v.run(ctx, v.listTasks())
		case StepWrite:
			err = v.write(ctx)
		}
		v.progress(step, err != nil)
		if err != nil {
			break
		}
		v.report.Steps = append(v.report.Steps, step)
	}
	v.report.Interrupted = err != nil
	v.finish(start)
	return v.report, err
}

type validateBucket struct {
	name  string
	state *BucketState
}

type validator struct {
	client  ValidateS3
	cfg     ValidateConfig
	buckets []validateBucket

	mu     sync.Mutex // guards hist and report
	hist   map[string]*Histogram
	report *ValidateReport
}

// run runs tasks with cfg.Workers of them in flight, until all have run or
// ctx ends. It returns once every task it started has finished, with ctx's
// error as it stands then.
func (v *validator) run(ctx context.Context, tasks []func(context.Context)) error {
	ch := make(chan func(context.Context))
	var wg sync.WaitGroup
	for range v.cfg.Workers {
		wg.Go(func() {
			for t := range ch {
				t(ctx)
			}
		})
	}
feed:
	for _, t := range tasks {
		select {
		case ch <- t:
		case <-ctx.Done():
			break feed
		}
	}
	close(ch)
	wg.Wait()
	return ctx.Err()
}

// call runs one request under the request timeout and returns how long it
// took.
func (v *validator) call(ctx context.Context, req func(context.Context) error) (time.Duration, error) {
	if v.cfg.RequestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, v.cfg.RequestTimeout)
		defer cancel()
	}
	start := time.Now()
	err := req(ctx)
	return time.Since(start), err
}

func (v *validator) record(op string, d time.Duration) {
	v.mu.Lock()
	defer v.mu.Unlock()
	h, ok := v.hist[op]
	if !ok {
		h = new(Histogram)
		v.hist[op] = h
	}
	h.Record(d)
}

// fail counts a failure, ignoring those caused by ctx ending: they say
// nothing about the gateway.
func (v *validator) fail(ctx context.Context, format string, args ...any) {
	if ctx.Err() != nil {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.report.Failures++
	if len(v.report.FailureSample) < maxFailureSample {
		v.report.FailureSample = append(v.report.FailureSample, fmt.Sprintf(format, args...))
	}
}

// sampleRand returns the random source that picks what bucket's step reads,
// the same for every run with the same seed and sample.
func (v *validator) sampleRand(bucket, step string) *mrand.Rand {
	h := sha256.New()
	writeField(h, "sample")
	writeField(h, v.cfg.Seed)
	writeField(h, v.cfg.Sample)
	writeField(h, bucket)
	writeField(h, step)
	var seed [32]byte
	h.Sum(seed[:0])
	return mrand.New(mrand.NewChaCha8(seed))
}

// shuffle spreads tasks across buckets, in an order fixed by the sample.
func (v *validator) shuffle(step string, tasks []func(context.Context)) []func(context.Context) {
	r := v.sampleRand("", step)
	r.Shuffle(len(tasks), func(i, j int) { tasks[i], tasks[j] = tasks[j], tasks[i] })
	return tasks
}

// getTasks reads one range of each real object and checks its bytes.
func (v *validator) getTasks() []func(context.Context) {
	var tasks []func(context.Context)
	for _, b := range v.buckets {
		r := v.sampleRand(b.name, StepGet)
		for _, obj := range b.state.Real {
			off, n := int64(0), obj.Size
			if obj.Size > v.cfg.RangeSize {
				off, n = r.Int64N(obj.Size-v.cfg.RangeSize+1), v.cfg.RangeSize
			}
			tasks = append(tasks, func(ctx context.Context) { v.getRange(ctx, b.name, obj, off, n) })
		}
	}
	return v.shuffle(StepGet, tasks)
}

func (v *validator) getRange(ctx context.Context, bucket string, obj RealObject, off, n int64) {
	var out *s3.GetObjectOutput
	var data []byte
	elapsed, err := v.call(ctx, func(ctx context.Context) error {
		var err error
		out, err = v.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(obj.Key),
			Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, off+n-1)),
		})
		if err != nil {
			return err
		}
		defer out.Body.Close()
		data, err = io.ReadAll(out.Body)
		return err
	})
	if err != nil {
		v.fail(ctx, "get %s/%s bytes %d-%d: %v", bucket, obj.Key, off, off+n-1, err)
		return
	}
	v.record(OpGetRange, elapsed)
	if total, ok := rangeTotal(aws.ToString(out.ContentRange)); !ok || total != obj.Size {
		v.fail(ctx, "get %s/%s bytes %d-%d: Content-Range %q, object is %d bytes", bucket, obj.Key, off, off+n-1, aws.ToString(out.ContentRange), obj.Size)
		return
	}
	want := make([]byte, n)
	if _, err := NewBody(realSeed(v.cfg.Seed, bucket, obj.Index), obj.Size).ReadAt(want, off); err != nil {
		v.fail(ctx, "get %s/%s: regenerate body: %v", bucket, obj.Key, err)
		return
	}
	if !bytes.Equal(data, want) {
		v.fail(ctx, "get %s/%s bytes %d-%d: %d bytes back, not the ones written", bucket, obj.Key, off, off+n-1, len(data))
	}
}

// rangeTotal returns the object size a Content-Range header names.
func rangeTotal(cr string) (int64, bool) {
	_, total, ok := strings.Cut(cr, "/")
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(total, 10, 64)
	return n, err == nil
}

// headTasks reads the metadata of a sample of each bucket's bulk objects.
func (v *validator) headTasks() []func(context.Context) {
	var tasks []func(context.Context)
	for _, b := range v.buckets {
		for _, n := range sampleIndexes(v.sampleRand(b.name, StepHead), b.state.Bulk, v.cfg.Samples) {
			key := BulkKey(v.cfg.KeyPrefix, v.cfg.Seed, b.name, n)
			tasks = append(tasks, func(ctx context.Context) { v.headBulk(ctx, b.name, key) })
		}
	}
	return v.shuffle(StepHead, tasks)
}

// sampleIndexes returns up to k distinct indexes below n.
func sampleIndexes(r *mrand.Rand, n uint64, k int) []uint64 {
	if n <= uint64(k) {
		out := make([]uint64, n)
		for i := range out {
			out[i] = uint64(i)
		}
		return out
	}
	seen := make(map[uint64]bool, k)
	out := make([]uint64, 0, k)
	for len(out) < k {
		i := r.Uint64N(n)
		if !seen[i] {
			seen[i] = true
			out = append(out, i)
		}
	}
	return out
}

func (v *validator) headBulk(ctx context.Context, bucket, key string) {
	var out *s3.HeadObjectOutput
	elapsed, err := v.call(ctx, func(ctx context.Context) error {
		var err error
		out, err = v.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		return err
	})
	if err != nil {
		v.fail(ctx, "head %s/%s: %v", bucket, key, err)
		return
	}
	v.record(OpHead, elapsed)
	switch {
	case out.ContentLength == nil:
		v.fail(ctx, "head %s/%s: no Content-Length", bucket, key)
	case *out.ContentLength != 0:
		v.fail(ctx, "head %s/%s: %d bytes, want an empty object", bucket, key, *out.ContentLength)
	}
}

// listTasks lists the first cfg.ListBuckets buckets in full.
func (v *validator) listTasks() []func(context.Context) {
	var tasks []func(context.Context)
	for _, b := range v.buckets[:min(v.cfg.ListBuckets, len(v.buckets))] {
		tasks = append(tasks, func(ctx context.Context) { v.list(ctx, b) })
	}
	return tasks
}

// list pages through b's keys under the key prefix, timing each page, and
// checks the keys arrive in order and include every object the load
// recorded. Keys the load did not record are counted but allowed: a load
// stopped part way may have written a few past its record.
func (v *validator) list(ctx context.Context, b validateBucket) {
	want := make([]string, 0, b.state.Bulk+uint64(len(b.state.Real)))
	for n := range b.state.Bulk {
		want = append(want, BulkKey(v.cfg.KeyPrefix, v.cfg.Seed, b.name, n))
	}
	for _, r := range b.state.Real {
		want = append(want, r.Key)
	}
	slices.Sort(want)

	start := time.Now()
	res := ListResult{Bucket: b.name}
	// gone holds the recorded keys the listing went past without returning.
	var gone []string
	// done records the listing. One that ran to its end lacked every
	// recorded key it didn't return. One that stopped part way lacked those
	// below where it stopped, unless it stopped at a key out of order: then
	// the keys it went past may only have come late.
	done := func(complete, inOrder bool) {
		missing := gone
		if complete {
			missing = append(missing, want...)
		}
		if inOrder {
			for _, key := range missing {
				res.Missing++
				v.fail(ctx, "list %s: %s is missing", b.name, key)
			}
		}
		res.Complete = complete
		res.DurationS = time.Since(start).Seconds()
		v.mu.Lock()
		v.report.Lists = append(v.report.Lists, res)
		v.mu.Unlock()
	}
	var token *string
	var last string
	for {
		var out *s3.ListObjectsV2Output
		elapsed, err := v.call(ctx, func(ctx context.Context) error {
			in := &s3.ListObjectsV2Input{Bucket: aws.String(b.name), ContinuationToken: token}
			if v.cfg.KeyPrefix != "" {
				in.Prefix = aws.String(v.cfg.KeyPrefix)
			}
			var err error
			out, err = v.client.ListObjectsV2(ctx, in)
			return err
		})
		if err != nil {
			v.fail(ctx, "list %s after %d objects: %v", b.name, res.Objects, err)
			done(false, true)
			return
		}
		v.record(OpListPage, elapsed)
		res.Pages++
		for _, o := range out.Contents {
			key := aws.ToString(o.Key)
			// A key out of order ends the listing: what follows it can't be
			// matched against the recorded keys, and a gateway whose next
			// page starts after it may return the same keys forever.
			if res.Objects > 0 && key <= last {
				v.fail(ctx, "list %s: page %d has %s after %s, out of order", b.name, res.Pages, key, last)
				done(false, false)
				return
			}
			last = key
			res.Objects++
			// Both lists are sorted, so a recorded key below this one that
			// has not been seen is missing.
			for len(want) > 0 && want[0] < key {
				gone = append(gone, want[0])
				want = want[1:]
			}
			if len(want) > 0 && want[0] == key {
				want = want[1:]
			} else {
				res.Extra++
			}
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		if len(out.Contents) == 0 || out.NextContinuationToken == nil {
			v.fail(ctx, "list %s: truncated page %d has %d keys and no continuation token, so the listing does not move on",
				b.name, res.Pages, len(out.Contents))
			done(false, true)
			return
		}
		token = out.NextContinuationToken
	}
	done(true, true)
}

// write writes cfg.Samples new zero-byte objects in each bucket and
// cfg.WriteReal real ones over the buckets in turn, deletes them, and checks
// they are gone. They are new on every run, so a run never meets an object
// another left behind. The deletes run even when ctx ends, each still
// bounded by the request timeout, so that an interrupted run leaves nothing
// behind.
func (v *validator) write(ctx context.Context) error {
	var tag [8]byte
	if _, err := rand.Read(tag[:]); err != nil {
		return err
	}
	run := hex.EncodeToString(tag[:])
	v.mu.Lock()
	v.report.WriteRun = run
	v.mu.Unlock()
	// A real object's key is its body's digest, as the pack server names a
	// pack, so its PUT task sets it once it has hashed the body. Each task
	// sets only its own object's key, and later steps read the keys after
	// every PUT task has returned.
	type object struct {
		bucket, key string
		real        bool
		index       uint64
	}
	var objects []object
	for _, b := range v.buckets {
		for i := range v.cfg.Samples {
			objects = append(objects, object{bucket: b.name, key: ValidateKey(v.cfg.KeyPrefix, v.cfg.Seed, b.name, run, uint64(i))})
		}
	}
	for k := range v.cfg.WriteReal {
		objects = append(objects, object{bucket: v.buckets[k%len(v.buckets)].name, real: true, index: uint64(k)})
	}
	// sent marks the objects whose PUT went out. Each of those gets a
	// delete, whatever its PUT's answer: a delete of an object that was
	// never written does nothing.
	sent := make([]atomic.Bool, len(objects))
	each := func(f func(ctx context.Context, i int, o *object)) []func(context.Context) {
		tasks := make([]func(context.Context), len(objects))
		for i := range objects {
			tasks[i] = func(ctx context.Context) { f(ctx, i, &objects[i]) }
		}
		return tasks
	}
	v.say("write  writing %d zero-byte objects in each of %d buckets and %d real ones, run %s", v.cfg.Samples, len(v.buckets), v.cfg.WriteReal, run)
	// An interrupt during the PUTs or the deletes is announced at once.
	announced := make(chan struct{})
	stopAnnounce := context.AfterFunc(ctx, func() {
		defer close(announced)
		v.say("write  interrupted: finishing the PUTs in flight, then deleting every object sent (run %s); interrupt again to stop at once and leave them", run)
	})
	// An interrupt stops new PUTs, but one already sent finishes, so that
	// its delete follows its answer. A PUT the client gives up on at the
	// request timeout has no answer, so the gateway may still commit it after
	// its delete.
	putErr := v.run(ctx, each(func(ctx context.Context, i int, o *object) {
		var body *Body
		var sum [sha256.Size]byte
		if o.real {
			body = validateRealBody(v.cfg.Seed, o.bucket, run, o.index, v.cfg.RealMinSize, v.cfg.RealMaxSize)
			var err error
			if sum, err = body.SHA256(); err != nil {
				v.fail(ctx, "hash real object %d of run %s: %v", o.index, run, err)
				return
			}
			o.key = RealKey(v.cfg.KeyPrefix, sum)
		}
		if ctx.Err() != nil {
			return
		}
		sent[i].Store(true)
		v.put(context.WithoutCancel(ctx), o.bucket, o.key, body, sum)
	}))
	if putErr != nil {
		// ctx has ended and the announcement is still armed, so it runs;
		// waiting for it keeps the lines in order.
		<-announced
		n := 0
		for i := range sent {
			if sent[i].Load() {
				n++
			}
		}
		v.say("write  deleting the %d objects whose PUT was sent (run %s)", n, run)
	}
	_ = v.run(context.WithoutCancel(ctx), each(func(ctx context.Context, i int, o *object) {
		if sent[i].Load() {
			v.delete(ctx, o.bucket, o.key, o.real)
		}
	}))
	if !stopAnnounce() {
		<-announced
	}
	if putErr != nil {
		return putErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return v.run(ctx, each(func(ctx context.Context, i int, o *object) {
		if sent[i].Load() {
			v.checkGone(ctx, o.bucket, o.key)
		}
	}))
}

// put writes key create-only: body, which hashes to sum, or a zero-byte
// object when body is nil.
func (v *validator) put(ctx context.Context, bucket, key string, body *Body, sum [sha256.Size]byte) {
	in := &s3.PutObjectInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String(key),
		Body:           bytes.NewReader(nil),
		ContentLength:  aws.Int64(0),
		IfNoneMatch:    aws.String("*"),
		ChecksumSHA256: aws.String(emptySHA256),
	}
	op := OpPut
	if body != nil {
		in.Body, in.ContentLength = body, aws.Int64(body.Size())
		in.ChecksumSHA256 = aws.String(base64.StdEncoding.EncodeToString(sum[:]))
		op = OpPutReal
	}
	elapsed, err := v.call(ctx, func(ctx context.Context) error {
		_, err := v.client.PutObject(ctx, in)
		return err
	})
	if err != nil {
		v.fail(ctx, "put %s/%s: %v", bucket, key, err)
		return
	}
	v.record(op, elapsed)
}

// delete deletes key, timing it as a real object's when real is set.
func (v *validator) delete(ctx context.Context, bucket, key string, real bool) {
	elapsed, err := v.call(ctx, func(ctx context.Context) error {
		_, err := v.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		return err
	})
	if err != nil {
		v.fail(ctx, "delete %s/%s: %v", bucket, key, err)
		return
	}
	op := OpDelete
	if real {
		op = OpDeleteReal
	}
	v.record(op, elapsed)
}

// checkGone fails unless a HEAD of a deleted object finds nothing.
func (v *validator) checkGone(ctx context.Context, bucket, key string) {
	_, err := v.call(ctx, func(ctx context.Context) error {
		_, err := v.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		return err
	})
	var re interface{ HTTPStatusCode() int }
	switch {
	case err == nil:
		v.fail(ctx, "head %s/%s after its delete: still there", bucket, key)
	case !errors.As(err, &re) || re.HTTPStatusCode() != 404:
		v.fail(ctx, "head %s/%s after its delete: %v", bucket, key, err)
	}
}

// say writes a line to the progress writer, if there is one.
func (v *validator) say(format string, args ...any) {
	if v.cfg.Progress == nil {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	fmt.Fprintf(v.cfg.Progress, format+"\n", args...)
}

// progress writes a line with the latencies of step's operations, marking a
// step that ctx ended part way.
func (v *validator) progress(step string, interrupted bool) {
	if v.cfg.Progress == nil {
		return
	}
	ops := map[string][]string{
		StepGet:   {OpGetRange},
		StepHead:  {OpHead},
		StepList:  {OpListPage},
		StepWrite: {OpPut, OpDelete, OpPutReal, OpDeleteReal},
	}[step]
	if step == StepWrite && v.cfg.WriteReal == 0 {
		ops = ops[:2]
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	line := fmt.Sprintf("%-6s", step)
	for _, op := range ops {
		s := LatencySummary{}
		if h, ok := v.hist[op]; ok {
			s = h.Summary()
		}
		line += fmt.Sprintf("  %s %d, p50 %.1fms p99 %.1fms max %.1fms", op, s.Count, s.P50, s.P99, s.Max)
	}
	if step == StepWrite {
		line += "  run " + v.report.WriteRun
	}
	line += fmt.Sprintf("  failures so far %d", v.report.Failures)
	if interrupted {
		line += "  (interrupted)"
	}
	fmt.Fprintln(v.cfg.Progress, line)
}

func (v *validator) finish(start time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.report.Time = time.Now().UTC()
	v.report.ElapsedS = time.Since(start).Seconds()
	v.report.Latency = make(map[string]LatencySummary, len(v.hist))
	for op, h := range v.hist {
		v.report.Latency[op] = h.Summary()
	}
	slices.SortFunc(v.report.Lists, func(a, b ListResult) int { return strings.Compare(a.Bucket, b.Bucket) })
}
