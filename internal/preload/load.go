package preload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// S3 is the part of the S3 API a load uses. *s3.Client implements it.
type S3 interface {
	CreateBucket(context.Context, *s3.CreateBucketInput, ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	PutBucketEncryption(context.Context, *s3.PutBucketEncryptionInput, ...func(*s3.Options)) (*s3.PutBucketEncryptionOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// Config is what a load writes, and how.
type Config struct {
	// Buckets are the buckets to load. A bucket that does not exist yet is
	// created.
	Buckets []string
	// KeyPrefix starts every object key.
	KeyPrefix string
	// Seed determines every key and body the load writes.
	Seed string
	// Until is the number of bulk objects each bucket ends with.
	Until uint64
	// Workers is the number of bulk PUTs in flight per bucket.
	Workers int

	// RealEvery places one real object in a bucket after every RealEvery
	// bulk objects; 0 writes none.
	RealEvery uint64
	// RealMinSize and RealMaxSize bound a real object's size in bytes.
	RealMinSize, RealMaxSize int64
	// RealWorkers is the number of real PUTs in flight across all buckets.
	RealWorkers int

	// Encryption, when set, is the SSEAlgorithm set as the default
	// encryption of each bucket the state file has not set up, an existing
	// bucket included: "none" stores objects as received, as the pack
	// server's buckets do. A bucket the state has set up keeps its own, and
	// a load asking for another is refused; a fresh state file over buckets
	// an earlier load filled resets theirs part way through their objects.
	Encryption string

	// StatePath is the state file the load resumes from and records its
	// progress in.
	StatePath string
	// CheckpointInterval is how often the state file is written.
	CheckpointInterval time.Duration
	// MaxAttempts is how many times a PUT that fails for a reason that may
	// pass (a 5xx, a 429, a 409, a dropped connection, a timeout) is tried
	// before the load stops.
	MaxAttempts int
	// AttemptTimeout bounds each attempt at a PUT; 0 leaves it unbounded. A
	// PUT that hangs would otherwise hold its bucket's watermark, and the
	// load's end, forever.
	AttemptTimeout time.Duration

	// ReportInterval is how often progress is reported.
	ReportInterval time.Duration
	// Metrics, when set, receives each report as a line of JSON.
	Metrics io.Writer
	// Progress, when set, receives each report as a line of text.
	Progress io.Writer

	// backoff returns the wait before attempt n+1; nil uses retryDelay.
	backoff func(n int) time.Duration
}

func (c *Config) validate() error {
	switch {
	case len(c.Buckets) == 0:
		return errors.New("no buckets")
	case c.Workers < 1:
		return errors.New("workers must be at least 1")
	case c.RealEvery > 0 && c.RealWorkers < 1:
		return errors.New("real workers must be at least 1")
	case c.RealEvery > 0 && (c.RealMinSize < 1 || c.RealMaxSize < c.RealMinSize):
		return fmt.Errorf("real object sizes %d to %d are not a range of positive sizes", c.RealMinSize, c.RealMaxSize)
	case c.StatePath == "":
		return errors.New("no state file")
	case c.MaxAttempts < 1:
		return errors.New("max attempts must be at least 1")
	case c.AttemptTimeout < 0:
		return errors.New("attempt timeout must not be negative")
	case c.CheckpointInterval <= 0 || c.ReportInterval <= 0:
		return errors.New("checkpoint and report intervals must be positive")
	}
	for _, b := range c.Buckets {
		if !ValidBucketName(b) {
			return fmt.Errorf("invalid bucket name %q", b)
		}
	}
	return nil
}

// Operation names, as reports key latencies by them.
const (
	OpPutBulk = "put_bulk"
	OpPutReal = "put_real"
)

// ErrStateNotSaved marks a load's error when its final progress could not be
// written to the state file: a later load resumes from the previous
// checkpoint instead.
var ErrStateNotSaved = errors.New("progress not saved")

// emptySHA256 is the base64 SHA-256 of an empty body.
var emptySHA256 = func() string {
	sum := sha256.Sum256(nil)
	return base64.StdEncoding.EncodeToString(sum[:])
}()

// Load writes cfg.Buckets' objects until each holds cfg.Until bulk objects
// and the real objects due by then, resuming from and recording progress in
// cfg.StatePath. It returns the final report. When ctx ends while objects
// are being written, Load records the progress made and returns ctx's cause
// (context.Cause); a later Load picks up from there. When that final record
// cannot be written, the error also carries ErrStateNotSaved, and a later
// Load resumes from the previous checkpoint. When ctx ends during bucket
// setup, Load returns that step's error and no report; each bucket set up
// by then is already recorded.
func Load(ctx context.Context, client S3, cfg Config) (*Report, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("preload: %w", err)
	}
	st, err := ReadState(cfg.StatePath, cfg.Seed, cfg.KeyPrefix)
	if err != nil {
		return nil, err
	}
	l := newLoader(client, cfg, st)
	if err := l.setupBuckets(ctx); err != nil {
		return nil, err
	}
	// Rates count from here, not from before the buckets were set up.
	l.start = time.Now()
	l.lastReport = l.start

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stop := make(chan struct{})
	var bg sync.WaitGroup
	bg.Go(func() { l.loop(stop) })

	var wg sync.WaitGroup
	fail := func(err error) {
		if err != nil {
			cancel(err)
		}
	}
	for _, b := range l.buckets {
		for range cfg.Workers {
			wg.Go(func() { fail(l.bulkWorker(runCtx, b)) })
		}
		if cfg.RealEvery > 0 {
			wg.Go(func() { fail(l.realWorker(runCtx, b)) })
		}
	}
	wg.Wait()
	close(stop)
	bg.Wait()

	saveErr := l.checkpoint()
	if saveErr != nil {
		saveErr = fmt.Errorf("%w: %w", ErrStateNotSaved, saveErr)
	}
	final := l.report(true)
	if err := context.Cause(runCtx); err != nil {
		if saveErr != nil {
			err = errors.Join(err, saveErr)
		}
		return final, err
	}
	return final, saveErr
}

// bucketRun is one bucket's part of a load.
type bucketRun struct {
	name  string
	state *BucketState // guarded by loader.mu
	wm    *watermark

	written, existed, errors atomic.Uint64
	real                     atomic.Uint64

	// prev is the count of PUTs at the last report, for the rate.
	prev uint64
}

type loader struct {
	client  S3
	cfg     Config
	buckets []*bucketRun
	start   time.Time
	// realSlots holds a token for each real PUT in flight, across buckets.
	realSlots chan struct{}

	mu    sync.Mutex // guards state, the histograms, lastReport and lastErr
	state *State

	hist struct{ interval, total map[string]*Histogram }

	realBytes  atomic.Uint64
	lastReport time.Time
	lastErr    string
}

func newLoader(client S3, cfg Config, st *State) *loader {
	l := &loader{
		client:    client,
		cfg:       cfg,
		state:     st,
		start:     time.Now(),
		realSlots: make(chan struct{}, max(cfg.RealWorkers, 1)),
	}
	l.lastReport = l.start
	l.hist.interval = make(map[string]*Histogram)
	l.hist.total = make(map[string]*Histogram)
	for _, name := range cfg.Buckets {
		bs := st.Bucket(name)
		b := &bucketRun{name: name, state: bs, wm: newWatermark(bs.Bulk, max(cfg.Until, bs.Bulk))}
		b.real.Store(uint64(len(bs.Real)))
		l.buckets = append(l.buckets, b)
	}
	return l
}

// setupBuckets creates each bucket not yet ready and sets its encryption. A
// bucket already set up with another encryption is refused: setting it now
// would change it part way through its objects.
func (l *loader) setupBuckets(ctx context.Context) error {
	for _, b := range l.buckets {
		if b.state.Ready {
			if b.state.Encryption != l.cfg.Encryption {
				return fmt.Errorf("bucket %s was set up with %s, not %s: load other buckets, with another state file",
					b.name, encryptionName(b.state.Encryption), encryptionName(l.cfg.Encryption))
			}
			continue
		}
		_, err := l.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b.name)})
		var ae smithy.APIError
		if err != nil && !(errors.As(err, &ae) && ae.ErrorCode() == "BucketAlreadyOwnedByYou") {
			return fmt.Errorf("create bucket %s: %w", b.name, err)
		}
		if l.cfg.Encryption != "" {
			_, err := l.client.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
				Bucket: aws.String(b.name),
				ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
					Rules: []types.ServerSideEncryptionRule{{
						ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
							SSEAlgorithm: types.ServerSideEncryption(l.cfg.Encryption),
						},
					}},
				},
			})
			if err != nil {
				return fmt.Errorf("set encryption of bucket %s to %s: %w", b.name, l.cfg.Encryption, err)
			}
		}
		l.mu.Lock()
		b.state.Ready = true
		b.state.Encryption = l.cfg.Encryption
		err = writeState(l.cfg.StatePath, l.state)
		l.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// encryptionName names a default encryption for a message.
func encryptionName(algo string) string {
	if algo == "" {
		return "the gateway's default encryption"
	}
	return fmt.Sprintf("encryption %q", algo)
}

// bulkWorker writes b's bulk objects until the watermark's limit.
func (l *loader) bulkWorker(ctx context.Context, b *bucketRun) error {
	for {
		n, ok := b.wm.take()
		if !ok {
			return nil
		}
		key := BulkKey(l.cfg.KeyPrefix, l.cfg.Seed, b.name, n)
		in := &s3.PutObjectInput{
			Bucket:         aws.String(b.name),
			Key:            aws.String(key),
			Body:           bytes.NewReader(nil),
			ContentLength:  aws.Int64(0),
			IfNoneMatch:    aws.String("*"),
			ChecksumSHA256: aws.String(emptySHA256),
		}
		if err := l.put(ctx, b, OpPutBulk, in, nil); err != nil {
			return err
		}
		b.wm.complete(n)
	}
}

// realWorker writes b's real objects, each once the bulk objects before it
// are written, so they spread through the bucket's load.
func (l *loader) realWorker(ctx context.Context, b *bucketRun) error {
	l.mu.Lock()
	first := uint64(len(b.state.Real))
	l.mu.Unlock()
	due := b.wm.limit / l.cfg.RealEvery
	for j := first; j < due; j++ {
		if err := b.wm.wait(ctx, (j+1)*l.cfg.RealEvery); err != nil {
			return err
		}
		obj, err := l.putReal(ctx, b, j)
		if err != nil {
			return err
		}
		l.mu.Lock()
		b.state.Real = append(b.state.Real, obj)
		l.mu.Unlock()
		b.real.Add(1)
	}
	return nil
}

// putReal writes real object j of b.
func (l *loader) putReal(ctx context.Context, b *bucketRun, j uint64) (RealObject, error) {
	select {
	case l.realSlots <- struct{}{}:
	case <-ctx.Done():
		return RealObject{}, ctx.Err()
	}
	defer func() { <-l.realSlots }()

	body := RealBody(l.cfg.Seed, b.name, j, l.cfg.RealMinSize, l.cfg.RealMaxSize)
	sum, err := body.SHA256()
	if err != nil {
		return RealObject{}, fmt.Errorf("hash real object %d of %s: %w", j, b.name, err)
	}
	key := RealKey(l.cfg.KeyPrefix, sum)
	in := &s3.PutObjectInput{
		Bucket:         aws.String(b.name),
		Key:            aws.String(key),
		Body:           body,
		ContentLength:  aws.Int64(body.Size()),
		IfNoneMatch:    aws.String("*"),
		ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(sum[:])),
	}
	rewind := func() error { _, err := body.Seek(0, io.SeekStart); return err }
	if err := l.put(ctx, b, OpPutReal, in, rewind); err != nil {
		return RealObject{}, err
	}
	l.realBytes.Add(uint64(body.Size()))
	return RealObject{Index: j, Key: key, Size: body.Size()}, nil
}

// outcome is what a PUT's error says to do next.
type outcome int

const (
	stored outcome = iota // the object was written
	taken                 // the key already held an object: a 412
	retry                 // the PUT may succeed if tried again
	fatal                 // it will not
)

func classify(err error) outcome {
	if err == nil {
		return stored
	}
	// No response: the SDK reports a send that failed, a dropped connection
	// included, as a RequestSendError, and an attempt whose context ended,
	// its timeout included, as a CanceledError. Both arrive inside a
	// response error whose status is 0, so they are matched first.
	var sendErr *smithyhttp.RequestSendError
	var canceled *smithy.CanceledError
	if errors.As(err, &sendErr) || errors.As(err, &canceled) {
		return retry
	}
	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) {
		switch code := re.HTTPStatusCode(); {
		case code == 412:
			return taken
		case code == 408 || code == 409 || code == 429 || code >= 500:
			return retry
		default:
			return fatal
		}
	}
	// Anything else came without a response.
	return retry
}

// put sends in until it is stored or its key is found taken, either of which
// counts as written. rewind, when set, runs before each attempt.
func (l *loader) put(ctx context.Context, b *bucketRun, op string, in *s3.PutObjectInput, rewind func() error) error {
	for attempt := 1; ; attempt++ {
		if rewind != nil {
			if err := rewind(); err != nil {
				return err
			}
		}
		actx, cancel := ctx, context.CancelFunc(func() {})
		if l.cfg.AttemptTimeout > 0 {
			actx, cancel = context.WithTimeout(ctx, l.cfg.AttemptTimeout)
		}
		start := time.Now()
		_, err := l.client.PutObject(actx, in)
		elapsed := time.Since(start)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch classify(err) {
		case stored:
			b.written.Add(1)
			l.record(op, elapsed)
			return nil
		case taken:
			b.existed.Add(1)
			return nil
		case fatal:
			return fmt.Errorf("put %s/%s: %w", b.name, *in.Key, err)
		}
		b.errors.Add(1)
		l.mu.Lock()
		l.lastErr = fmt.Sprintf("put %s/%s: %v", b.name, *in.Key, err)
		l.mu.Unlock()
		if attempt >= l.cfg.MaxAttempts {
			return fmt.Errorf("put %s/%s: giving up after %d attempts: %w", b.name, *in.Key, attempt, err)
		}
		delay := retryDelay(attempt)
		if l.cfg.backoff != nil {
			delay = l.cfg.backoff(attempt)
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// retryDelay is the wait after a PUT's n'th failed attempt: exponential from
// 200ms, capped at 30s, with jitter.
func retryDelay(n int) time.Duration {
	d := 200 * time.Millisecond << min(n-1, 8)
	d = min(d, 30*time.Second)
	return d/2 + rand.N(d/2+1)
}

// record adds a successful PUT's latency to op's histograms.
func (l *loader) record(op string, d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range []map[string]*Histogram{l.hist.interval, l.hist.total} {
		h, ok := m[op]
		if !ok {
			h = new(Histogram)
			m[op] = h
		}
		h.Record(d)
	}
}

// checkpoint records each bucket's watermark in the state file.
func (l *loader) checkpoint() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, b := range l.buckets {
		b.state.Bulk = b.wm.Done()
	}
	return writeState(l.cfg.StatePath, l.state)
}

// loop checkpoints and reports at their intervals until stop closes.
func (l *loader) loop(stop <-chan struct{}) {
	cp := time.NewTicker(l.cfg.CheckpointInterval)
	defer cp.Stop()
	rp := time.NewTicker(l.cfg.ReportInterval)
	defer rp.Stop()
	for {
		select {
		case <-stop:
			return
		case <-cp.C:
			if err := l.checkpoint(); err != nil {
				l.mu.Lock()
				l.lastErr = err.Error()
				l.mu.Unlock()
			}
		case <-rp.C:
			l.report(false)
		}
	}
}
