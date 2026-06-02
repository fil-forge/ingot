package testing

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot"
	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/uploader"
)

// DefaultAccessKey / DefaultSecretKey are the sigv4 credentials a
// freshly started Harness uses unless overridden via WithCredentials.
// They are not secrets — the harness binds to 127.0.0.1 only.
const (
	DefaultAccessKey = "ingot-test-access"
	DefaultSecretKey = "ingot-test-secret"
)

// Harness is an in-process ingot S3 listener backed by in-memory deps.
// No Postgres, no piri, no indexer: a sealed segment's flush is a
// no-op that just advances bookkeeping. Sufficient for driving the
// upstream versitygw integration suite against the listener via
// Run + Suite.
//
// The server is constructed through ingot's exported fx ServerModule —
// the same wiring production uses — with the in-memory fakes supplied
// in place of the Postgres registry / Forge reader / Forge uploader.
type Harness struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Region    string

	app     *fx.App
	dataDir string
}

// HarnessOption customizes StartHarness. Each option mutates a
// HarnessOptions value in place.
type HarnessOption func(*harnessOptions)

type harnessOptions struct {
	logger     *zap.Logger
	region     string
	accessKey  string
	secretKey  string
	chunkSize  int64
	sealBytes  int64
	sealAge    time.Duration
	retain     int
	readyAfter time.Duration
}

// WithLogger sets the zap logger supplied to the ingot fx module (and
// used for fx's own lifecycle logging). Default nop.
func WithLogger(l *zap.Logger) HarnessOption {
	return func(o *harnessOptions) { o.logger = l }
}

// WithRegion overrides the default "us-east-1" sigv4 region.
func WithRegion(r string) HarnessOption {
	return func(o *harnessOptions) { o.region = r }
}

// WithCredentials overrides DefaultAccessKey / DefaultSecretKey.
func WithCredentials(access, secret string) HarnessOption {
	return func(o *harnessOptions) {
		o.accessKey = access
		o.secretKey = secret
	}
}

// WithChunkSize overrides the per-object body chunk size.
// 0 means use bucket.DefaultChunkSize.
func WithChunkSize(n int64) HarnessOption {
	return func(o *harnessOptions) { o.chunkSize = n }
}

// WithSealConfig forwards SealBytes / SealAge / Retain to logstore.
// Tests that exercise seal-on-size or seal-on-age behavior use this;
// the default leaves all three zero so logstore picks its own
// defaults.
func WithSealConfig(sealBytes int64, sealAge time.Duration, retain int) HarnessOption {
	return func(o *harnessOptions) {
		o.sealBytes = sealBytes
		o.sealAge = sealAge
		o.retain = retain
	}
}

// WithReadyTimeout caps how long StartHarness will dial the listener
// before giving up. Default 5 s.
func WithReadyTimeout(d time.Duration) HarnessOption {
	return func(o *harnessOptions) { o.readyAfter = d }
}

// StartHarness stands up an in-process ingot listener — built through the
// exported ingot.ServerModule with in-memory fakes — bound to a random
// 127.0.0.1 port, and waits for it to accept TCP connections. The caller
// must call Stop to drain the log and remove scratch state.
func StartHarness(ctx context.Context, opts ...HarnessOption) (*Harness, error) {
	options := harnessOptions{
		logger:     zap.NewNop(),
		region:     "us-east-1",
		accessKey:  DefaultAccessKey,
		secretKey:  DefaultSecretKey,
		readyAfter: 5 * time.Second,
	}
	for _, o := range opts {
		o(&options)
	}

	addr, err := pickFreeAddr()
	if err != nil {
		return nil, fmt.Errorf("ingot harness: pick port: %w", err)
	}

	dataDir, err := os.MkdirTemp("", "ingot-harness-")
	if err != nil {
		return nil, fmt.Errorf("ingot harness: tempdir: %w", err)
	}

	mem := newMemStore()

	// Build the server through ingot's exported ServerModule, exactly as a
	// production host would, but supplying the in-memory fakes for the four
	// collaborator interfaces instead of the Postgres registry / Forge
	// reader / Forge uploader. No PreStartHooks are registered (there are no
	// migrations to run against the in-memory store).
	app := fx.New(
		// Route fx's own lifecycle logging through the supplied zap logger
		// (the test logger) instead of fx's default stderr writer.
		fx.WithLogger(func() fxevent.Logger { return &fxevent.ZapLogger{Logger: options.logger} }),
		fx.Supply(options.logger),
		fx.Supply(ingot.ServerConfig{
			Addr:       addr,
			DataDir:    dataDir,
			Region:     options.region,
			RootAccess: options.accessKey,
			RootSecret: options.secretKey,
			ChunkSize:  options.chunkSize,
			SealBytes:  options.sealBytes,
			SealAge:    options.sealAge,
			Retain:     options.retain,
		}),
		// memStore satisfies both registry.Registry and logstore.Meta; expose
		// it under each interface the module consumes.
		fx.Provide(
			fx.Annotate(func() *memStore { return mem }, fx.As(new(registry.Registry))),
			fx.Annotate(func() *memStore { return mem }, fx.As(new(logstore.Meta))),
			fx.Annotate(func() nopBaseReader { return nopBaseReader{} }, fx.As(new(blockstore.BlockReader))),
			fx.Annotate(func() nopUploader { return nopUploader{} }, fx.As(new(uploader.Uploader))),
		),
		ingot.ServerModule,
	)
	if err := app.Err(); err != nil {
		_ = os.RemoveAll(dataDir)
		return nil, fmt.Errorf("ingot harness: build app: %w", err)
	}

	if err := app.Start(ctx); err != nil {
		_ = os.RemoveAll(dataDir)
		return nil, fmt.Errorf("ingot harness: start app: %w", err)
	}

	if err := waitListening(ctx, addr, options.readyAfter); err != nil {
		_ = app.Stop(ctx)
		_ = os.RemoveAll(dataDir)
		return nil, fmt.Errorf("ingot harness: %w", err)
	}

	return &Harness{
		Endpoint:  "http://" + addr,
		AccessKey: options.accessKey,
		SecretKey: options.secretKey,
		Region:    options.region,
		app:       app,
		dataDir:   dataDir,
	}, nil
}

// Stop stops the fx app (which shuts the listener down and drains the
// log via the module's OnStop hook) and removes the scratch data
// directory. Safe to call once; subsequent calls no-op. Errors from
// each step are joined.
func (h *Harness) Stop(ctx context.Context) error {
	var errs []error
	if h.app != nil {
		// Shutdown must run even when the caller's context is already done —
		// e.g. a t.Cleanup gets a t.Context() that is canceled just before
		// cleanups run, and fx.App.Stop honors a canceled context by refusing
		// to run OnStop. Detach from the parent's cancellation but keep a
		// bound so a wedged drain can't hang the test.
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		if err := h.app.Stop(stopCtx); err != nil {
			errs = append(errs, err)
		}
		cancel()
		h.app = nil
	}
	if h.dataDir != "" {
		if err := os.RemoveAll(h.dataDir); err != nil {
			errs = append(errs, fmt.Errorf("remove dataDir: %w", err))
		}
		h.dataDir = ""
	}
	if len(errs) > 0 {
		return fmt.Errorf("ingot harness stop: %v", errs)
	}
	return nil
}

// Config returns a Config wired against the harness's listener,
// suitable for passing to Run.
func (h *Harness) Config() Config {
	return Config{
		Endpoint:  h.Endpoint,
		AccessKey: h.AccessKey,
		SecretKey: h.SecretKey,
		Region:    h.Region,
	}
}

// pickFreeAddr asks the kernel for a free 127.0.0.1 port by binding
// and immediately closing. There is a small race window between
// close and ingot's rebind, but for serial unit tests it is
// effectively zero.
func pickFreeAddr() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		return "", err
	}
	return addr, nil
}

// waitListening polls TCP connect to addr until it succeeds, ctx
// is canceled, or the timeout fires.
func waitListening(ctx context.Context, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var d net.Dialer
	for {
		if !time.Now().Before(deadline) {
			return fmt.Errorf("listener not ready at %s after %s", addr, timeout)
		}
		dialCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		conn, err := d.DialContext(dialCtx, "tcp", addr)
		cancel()
		if err == nil {
			_ = conn.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// memStore is an in-memory implementation of registry.Registry +
// logstore.Meta. The two interfaces overlap on bucket state because
// MarkSegmentFlushed advances forge_root_cid; production wires a
// single *registry.Postgres for both seams, and this fake follows
// suit so flush behavior matches.
type memStore struct {
	mu       sync.Mutex
	buckets  map[string]*registry.State
	segments map[uint64]*logstore.SegmentMeta
	nextSeq  uint64
}

func newMemStore() *memStore {
	return &memStore{
		buckets:  map[string]*registry.State{},
		segments: map[uint64]*logstore.SegmentMeta{},
	}
}

// Registry methods ===========================================================

func (m *memStore) Create(_ context.Context, name string, createdAt int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.buckets[name]; ok {
		return registry.ErrExists
	}
	m.buckets[name] = &registry.State{Name: name, CreatedAt: createdAt}
	return nil
}

func (m *memStore) Get(_ context.Context, name string) (*registry.State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.buckets[name]
	if !ok {
		return nil, registry.ErrNotFound
	}
	cp := *s
	return &cp, nil
}

func (m *memStore) List(_ context.Context) ([]*registry.State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*registry.State, 0, len(m.buckets))
	for _, s := range m.buckets {
		cp := *s
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (m *memStore) Delete(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.buckets[name]; !ok {
		return registry.ErrNotFound
	}
	delete(m.buckets, name)
	return nil
}

func (m *memStore) CASRoot(_ context.Context, name string, expect, next cid.Cid) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.buckets[name]
	if !ok {
		return registry.ErrNotFound
	}
	if !s.Root.Equals(expect) {
		return registry.ErrConflict
	}
	s.Root = next
	return nil
}

func (m *memStore) SetForgeRoot(_ context.Context, name string, root cid.Cid) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.buckets[name]
	if !ok {
		return registry.ErrNotFound
	}
	s.ForgeRoot = root
	return nil
}

// Meta methods ===============================================================

func (m *memStore) NextSegmentSeq(_ context.Context) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextSeq++
	return m.nextSeq, nil
}

func (m *memStore) InsertSegmentOpen(_ context.Context, seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.segments[seq]; ok {
		return nil
	}
	m.segments[seq] = &logstore.SegmentMeta{Seq: seq, State: logstore.StateOpen}
	return nil
}

func (m *memStore) MarkSegmentSealed(_ context.Context, seq uint64, sealedAt int64, sizeBytes int64, sha256 []byte, opRoots []blockstore.OpRoot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.segments[seq]
	if !ok || r.State != logstore.StateOpen {
		return nil
	}
	r.State = logstore.StateSealed
	r.SealedAt = sealedAt
	r.SizeBytes = sizeBytes
	r.SHA256 = append([]byte(nil), sha256...)
	r.OpRoots = append([]blockstore.OpRoot(nil), opRoots...)
	return nil
}

func (m *memStore) MarkSegmentFlushed(_ context.Context, seq uint64, flushedAt int64, opRoots []blockstore.OpRoot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.segments[seq]; ok {
		r.State = logstore.StateFlushed
		r.FlushedAt = flushedAt
	}
	for _, opr := range opRoots {
		if b, ok := m.buckets[opr.Bucket]; ok {
			b.ForgeRoot = opr.Root
		}
	}
	return nil
}

func (m *memStore) DeleteSegment(_ context.Context, seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.segments, seq)
	return nil
}

func (m *memStore) ListUnflushedSegments(_ context.Context) ([]logstore.SegmentMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []logstore.SegmentMeta
	for _, r := range m.segments {
		if r.State == logstore.StateOpen || r.State == logstore.StateSealed {
			out = append(out, *r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

func (m *memStore) RehydrateSegment(_ context.Context, sm logstore.SegmentMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := sm
	m.segments[sm.Seq] = &cp
	return nil
}

// nopBaseReader is the base tier of the layered read path for the
// harness: every miss past the log returns ErrNotFound. Production
// wires *blockstore.Forge here; tests don't have piri to talk to.
type nopBaseReader struct{}

func (nopBaseReader) GetBlock(_ context.Context, _ cid.Cid) (block.Block, error) {
	return nil, blockstore.ErrNotFound
}

// nopUploader is the flush sink for the harness: SubmitCAR returns
// nil so the segment is marked flushed without touching the network.
type nopUploader struct{}

func (nopUploader) SubmitCAR(_ context.Context, _ []cid.Cid, _ uploader.CARSource) error {
	return nil
}

// Compile-time guarantees the fakes still match the contracts after
// upstream interface drift.
var (
	_ registry.Registry      = (*memStore)(nil)
	_ logstore.Meta          = (*memStore)(nil)
	_ blockstore.BlockReader = nopBaseReader{}
	_ uploader.Uploader      = nopUploader{}
)
