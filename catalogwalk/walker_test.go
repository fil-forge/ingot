package catalogwalk

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	contentcmds "github.com/fil-forge/libforge/commands/content"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/ucan/delegation"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/cars"
	"github.com/fil-forge/ingot/iam"
	"github.com/fil-forge/ingot/inmem"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/registry"
)

// node returns a dag-cbor block holding name and links.
func node(t *testing.T, name string, links ...block.Block) block.Block {
	t.Helper()
	var buf bytes.Buffer
	cw := cbg.NewCborWriter(&buf)
	require.NoError(t, cw.WriteMajorTypeHeader(cbg.MajArray, uint64(1+len(links))))
	require.NoError(t, cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(name))))
	_, err := cw.WriteString(name)
	require.NoError(t, err)
	for _, l := range links {
		require.NoError(t, cbg.WriteCid(cw, l.Cid()))
	}
	mh, err := multihash.Sum(buf.Bytes(), multihash.SHA2_256, -1)
	require.NoError(t, err)
	blk, err := block.NewBlockWithCid(buf.Bytes(), cid.NewCidV1(cid.DagCBOR, mh))
	require.NoError(t, err)
	return blk
}

func frames(blks ...block.Block) int64 {
	var n int64
	for _, b := range blks {
		n += cars.FrameSize(b.Cid(), uint64(len(b.RawData())))
	}
	return n
}

// fakeReads serves blocks from a map and counts reads. onRead, when set,
// runs before each read.
type fakeReads struct {
	mu     sync.Mutex
	blocks map[cid.Cid]block.Block
	reads  int
	onRead func(ctx context.Context)
	fail   error
}

func (f *fakeReads) put(blks ...block.Block) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, b := range blks {
		f.blocks[b.Cid()] = b
	}
}

func (f *fakeReads) GetBlock(ctx context.Context, _ did.DID, c cid.Cid) (block.Block, error) {
	if f.onRead != nil {
		f.onRead(ctx)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return nil, f.fail
	}
	if _, ok := reqscope.ProofStore(ctx); !ok {
		return nil, errors.New("read without a proof store")
	}
	f.reads++
	b, ok := f.blocks[c]
	if !ok {
		return nil, blockstore.ErrNotFound
	}
	return b, nil
}

// fixture is a bucket with shipped catalog segments, a walker over it, a
// clock both read, and an access key whose proof store can retrieve in the
// bucket's space for the agent.
type fixture struct {
	t      *testing.T
	ctx    context.Context
	mem    *inmem.MemStore
	reads  *fakeReads
	auth   *Authority
	walker *Walker
	space  did.DID
	clock  time.Time

	keys  *iam.KeyProofs
	key   did.DID
	grant interface{ Link() cid.Cid } // the space→agent retrieve delegation
}

func newFixture(t *testing.T, cfg Config) *fixture {
	t.Helper()
	spaceKey, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	agent, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	accessKey, err := ed25519.GenerateIssuer()
	require.NoError(t, err)
	grant, err := contentcmds.Retrieve.Delegate(spaceKey, agent.DID(), spaceKey.DID(), delegation.WithNoExpiration())
	require.NoError(t, err)
	f := &fixture{
		t:     t,
		ctx:   context.Background(),
		mem:   inmem.NewMemStore(),
		reads: &fakeReads{blocks: map[cid.Cid]block.Block{}},
		auth:  NewAuthority(agent.DID()),
		clock: time.Unix(1_000_000, 0),
		space: spaceKey.DID(),
		keys:  iam.NewKeyProofs(),
		key:   accessKey.DID(),
		grant: grant,
	}
	f.keys.Deposit(f.key, grant)
	require.NoError(t, f.mem.Create(f.ctx, "bk", f.space, registry.CreateState{}))
	seq, err := f.mem.NextSegmentSeq(f.ctx)
	require.NoError(t, err)
	require.NoError(t, f.mem.InsertSegmentOpen(f.ctx, blockstore.PlaneCatalog, seq, "bk"))
	require.NoError(t, f.mem.MarkSegmentSealed(f.ctx, blockstore.PlaneCatalog, seq, 1, 10_000, nil, nil))
	require.NoError(t, f.mem.MarkSegmentShipped(f.ctx, blockstore.PlaneCatalog, seq, 2, multihash.Multihash("index"), 1_000, nil))

	f.auth.now = f.now
	f.walker = New(cfg, Deps{Reads: f.reads, Usage: f.mem, Walks: f.mem, Authority: f.auth})
	f.walker.now = f.now
	return f
}

func (f *fixture) now() time.Time { return f.clock }

// setRoot moves the bucket's root to root.
func (f *fixture) setRoot(root block.Block) {
	st, err := f.mem.Get(f.ctx, "bk")
	require.NoError(f.t, err)
	require.NoError(f.t, f.mem.CASRoot(f.ctx, "bk", st.Root, root.Cid()))
}

// request captures authority as a request by the access key reading in the
// space would.
func (f *fixture) request() {
	ctx := context.WithValue(f.ctx, reqscope.ProofStoreKey(), f.keys.For(f.key))
	ctx = context.WithValue(ctx, reqscope.BorrowableKey(), true)
	f.auth.Capture(ctx, f.space)
}

// walkNext walks the first due bucket, as the server's loop would; ok is
// false when none is due.
func (f *fixture) walkNext() (Walked, bool, error) {
	f.t.Helper()
	due, err := f.walker.Due(f.ctx)
	require.NoError(f.t, err)
	if len(due) == 0 {
		return Walked{}, false, nil
	}
	walked, err := f.walker.Walk(f.ctx, due[0])
	return walked, true, err
}

func (f *fixture) lastWalk() *registry.CatalogWalk {
	usage, err := f.mem.CatalogUsage(f.ctx)
	require.NoError(f.t, err)
	require.Len(f.t, usage, 1)
	return usage[0].Walk
}

// TestWalkRecordsLiveBytes: a walk counts the frames of every block reachable
// from the root, a block once per link to it, and records them with the
// bucket's usage as of the walk. A block no link reaches is not counted.
func TestWalkRecordsLiveBytes(t *testing.T) {
	f := newFixture(t, Config{})
	shared := node(t, "shared")
	a := node(t, "a", shared)
	b := node(t, "b", shared)
	root := node(t, "root", a, b)
	f.reads.put(shared, a, b, root, node(t, "dead"))
	f.setRoot(root)
	f.request()

	walked, ok, err := f.walkNext()
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, walked.Completed)
	live := frames(root, a, b, shared, shared)
	require.Equal(t, live, walked.LiveBytes)
	require.EqualValues(t, 5, walked.Reads)

	w := f.lastWalk()
	require.NotNil(t, w)
	require.Equal(t, registry.CatalogWalk{
		Root: root.Cid(), StartedAt: f.clock.Unix(), FinishedAt: f.clock.Unix(),
		LiveBytes: live, ForgeBytes: 10_000, DeadOnArrivalBytes: 1_000,
	}, *w)
}

// TestWalkNextPicksOnlyDueBuckets: a bucket is not walked without borrowed
// authority, nor again while its root is unchanged, nor within Interval of
// its last walk.
func TestWalkNextPicksOnlyDueBuckets(t *testing.T) {
	f := newFixture(t, Config{Interval: time.Hour})
	leaf := node(t, "leaf")
	root := node(t, "root", leaf)
	f.reads.put(leaf, root)
	f.setRoot(root)

	_, ok, err := f.walkNext()
	require.NoError(t, err)
	require.False(t, ok, "walked without authority")

	f.request()
	_, ok, err = f.walkNext()
	require.NoError(t, err)
	require.True(t, ok)

	_, ok, err = f.walkNext()
	require.NoError(t, err)
	require.False(t, ok, "walked again with the root unchanged")

	root2 := node(t, "root2", leaf)
	f.reads.put(root2)
	f.setRoot(root2)
	f.clock = f.clock.Add(30 * time.Minute)
	f.request()
	_, ok, err = f.walkNext()
	require.NoError(t, err)
	require.False(t, ok, "walked again within the interval")

	f.clock = f.clock.Add(31 * time.Minute)
	f.request()
	_, ok, err = f.walkNext()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, root2.Cid(), f.lastWalk().Root)
}

// TestWalkOfEmptyBucket: a bucket with no root has nothing live, and its walk
// needs no authority.
func TestWalkOfEmptyBucket(t *testing.T) {
	f := newFixture(t, Config{})
	walked, ok, err := f.walkNext()
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, walked.Completed)
	require.Zero(t, walked.LiveBytes)
	require.Zero(t, walked.Reads)
	require.False(t, f.lastWalk().Root.Defined())
}

// TestWalkReusesUnchangedSubtrees: a later walk reads only what changed since
// an earlier one, gets the same figures a fresh walk would, and drops the
// remembered subtrees the new root no longer reaches.
func TestWalkReusesUnchangedSubtrees(t *testing.T) {
	f := newFixture(t, Config{Interval: time.Minute, MinSubtreeBlocks: 2})
	l1, l2, l3 := node(t, "l1"), node(t, "l2"), node(t, "l3")
	kept := node(t, "kept", l1, l2)
	dropped := node(t, "dropped", l3)
	root := node(t, "root", kept, dropped)
	f.reads.put(l1, l2, l3, kept, dropped, root)
	f.setRoot(root)
	f.request()
	_, _, err := f.walkNext()
	require.NoError(t, err)

	l4 := node(t, "l4")
	root2 := node(t, "root2", kept, l4)
	f.reads.put(l4, root2)
	f.setRoot(root2)
	f.clock = f.clock.Add(2 * time.Minute)
	f.request()
	walked, ok, err := f.walkNext()
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 2, walked.Reads, "only root2 and l4 are read")
	require.EqualValues(t, 1, walked.Reused)
	require.Equal(t, frames(root2, kept, l1, l2, l4), walked.LiveBytes)

	remembered, err := f.mem.CatalogSubtrees(f.ctx, "bk")
	require.NoError(t, err)
	var got []cid.Cid
	for _, st := range remembered {
		got = append(got, st.CID)
	}
	require.ElementsMatch(t, []cid.Cid{kept.Cid(), root2.Cid()}, got)
}

// TestWalkStopsWhenAuthorityLapses: a walk whose borrowed authority lapses
// stops without recording a walk, keeps the subtrees it finished, and the
// bucket is due again once authority is held.
func TestWalkStopsWhenAuthorityLapses(t *testing.T) {
	f := newFixture(t, Config{MinSubtreeBlocks: 2})
	l1, l2, l3 := node(t, "l1"), node(t, "l2"), node(t, "l3")
	first := node(t, "first", l1, l2)
	second := node(t, "second", l3)
	root := node(t, "root", first, second)
	f.reads.put(l1, l2, l3, first, second, root)
	f.setRoot(root)
	f.request()
	f.reads.onRead = func(context.Context) {
		if f.reads.reads == 3 { // root, first, l1 read: lapse before l2's read returns
			f.clock = f.clock.Add(borrowTTL)
		}
	}

	walked, ok, err := f.walkNext()
	require.True(t, ok)
	require.ErrorIs(t, err, errNoAuthority)
	require.False(t, walked.Completed)
	require.Nil(t, f.lastWalk())
	remembered, err := f.mem.CatalogSubtrees(f.ctx, "bk")
	require.NoError(t, err)
	require.Len(t, remembered, 1, "the finished subtree is kept")
	require.Equal(t, first.Cid(), remembered[0].CID)

	f.reads.onRead = nil
	f.request()
	walked, ok, err = f.walkNext()
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, walked.Completed)
	require.EqualValues(t, 1, walked.Reused)
	require.Equal(t, frames(root, first, l1, l2, second, l3), walked.LiveBytes)
}

// TestFailedWalkWaits: a walk that fails on a read holds the bucket back for
// retryAfter.
func TestFailedWalkWaits(t *testing.T) {
	f := newFixture(t, Config{})
	root := node(t, "root")
	f.reads.put(root)
	f.setRoot(root)
	f.request()
	f.reads.fail = errors.New("piri is down")

	_, ok, err := f.walkNext()
	require.True(t, ok)
	require.ErrorContains(t, err, "piri is down")

	f.reads.fail = nil
	_, ok, err = f.walkNext()
	require.NoError(t, err)
	require.False(t, ok, "walked again before retryAfter")

	f.clock = f.clock.Add(retryAfter)
	f.request()
	walked, ok, err := f.walkNext()
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, walked.Completed)
}

// TestDueOrder: the bucket longest without a completed walk goes first, the
// larger between two never walked.
func TestDueOrder(t *testing.T) {
	f := newFixture(t, Config{Interval: time.Minute})
	f.setRoot(node(t, "root"))
	f.request()
	small, err := did.Parse("did:web:small.example")
	require.NoError(t, err)
	large, err := did.Parse("did:web:large.example")
	require.NoError(t, err)
	for _, b := range []struct {
		name  string
		space did.DID
		size  int64
	}{{"never-small", small, 1}, {"never-large", large, 5}} {
		require.NoError(t, f.mem.Create(f.ctx, b.name, b.space, registry.CreateState{}))
		seq, err := f.mem.NextSegmentSeq(f.ctx)
		require.NoError(t, err)
		require.NoError(t, f.mem.InsertSegmentOpen(f.ctx, blockstore.PlaneCatalog, seq, b.name))
		require.NoError(t, f.mem.MarkSegmentSealed(f.ctx, blockstore.PlaneCatalog, seq, 1, b.size, nil, nil))
		require.NoError(t, f.mem.MarkSegmentShipped(f.ctx, blockstore.PlaneCatalog, seq, 2, multihash.Multihash("index"), 0, nil))
	}
	require.NoError(t, f.mem.CompleteCatalogWalk(f.ctx, "bk", registry.CatalogWalk{FinishedAt: 100}, nil))

	due, err := f.walker.Due(f.ctx)
	require.NoError(t, err)
	var got []string
	for _, u := range due {
		got = append(got, u.Bucket)
	}
	require.Equal(t, []string{"never-large", "never-small", "bk"}, got)
}

// TestWalkReadsAtTheConfiguredRate: the walker's limiter allows Rate reads a
// second, one at a time.
func TestWalkReadsAtTheConfiguredRate(t *testing.T) {
	w := New(Config{Rate: 7}, Deps{})
	require.EqualValues(t, 7, w.limiter.Limit())
	require.Equal(t, 1, w.limiter.Burst())
	require.EqualValues(t, DefaultRate, New(Config{}, Deps{}).limiter.Limit())
}

// TestWalkKeepsReusedSubtreesInside: a remembered subtree reused whole keeps
// the remembered subtrees inside it, so a later change deep inside reads only
// the changed path.
func TestWalkKeepsReusedSubtreesInside(t *testing.T) {
	f := newFixture(t, Config{Interval: time.Minute, MinSubtreeBlocks: 2})
	x, y, z, v := node(t, "x"), node(t, "y"), node(t, "z"), node(t, "v")
	a1, a2 := node(t, "a1", x, y), node(t, "a2", z, v)
	a := node(t, "a", a1, a2)
	root := node(t, "root", a, node(t, "b"))
	f.reads.put(x, y, z, v, a1, a2, a, root)
	f.reads.put(node(t, "b"))
	f.setRoot(root)
	f.request()
	_, _, err := f.walkNext()
	require.NoError(t, err)

	// a is reused whole: a1 and a2 stay remembered although the walk never
	// reaches them itself.
	b2 := node(t, "b2")
	root2 := node(t, "root2", a, b2)
	f.reads.put(b2, root2)
	f.setRoot(root2)
	f.clock = f.clock.Add(2 * time.Minute)
	f.request()
	_, _, err = f.walkNext()
	require.NoError(t, err)

	// Change inside a, under a2: only the changed path and its new leaf are
	// read; a1 is reused.
	w := node(t, "w")
	a2b := node(t, "a2b", z, w)
	ab := node(t, "ab", a1, a2b)
	root3 := node(t, "root3", ab, b2)
	f.reads.put(w, a2b, ab, root3)
	f.setRoot(root3)
	f.clock = f.clock.Add(2 * time.Minute)
	f.request()
	walked, ok, err := f.walkNext()
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 6, walked.Reads, "root3, ab, a2b, z, w and b2 are read; a1 is reused")
	require.Equal(t, frames(root3, ab, a1, x, y, a2b, z, w, b2), walked.LiveBytes)
}
