package revocation

import (
	"context"
	"errors"
	"iter"
	"testing"
	"time"

	jsg "github.com/alanshaw/dag-json-gen"
	"github.com/fil-forge/swarf/pkg/api"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func adapterCid(t *testing.T, s string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte(s), multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.DagCBOR, mh)
}

// fakeFirehose yields a fixed script and records the `since` it was called
// with. The swarf client itself needs a live SSE endpoint, so the adapter is
// tested against this narrow seam.
type fakeFirehose struct {
	records []api.FirehoseRevocation
	err     error
	since   time.Time
}

func (f *fakeFirehose) Stream(_ context.Context, from time.Time) iter.Seq2[api.FirehoseRevocation, error] {
	f.since = from
	return func(yield func(api.FirehoseRevocation, error) bool) {
		for _, rec := range f.records {
			if !yield(rec, nil) {
				return
			}
		}
		if f.err != nil {
			yield(api.FirehoseRevocation{}, f.err)
		}
	}
}

func TestSwarfSourceMapsRevocations(t *testing.T) {
	at := time.Date(2026, 8, 6, 12, 0, 1, 0, time.UTC)
	rec := api.FirehoseRevocation{
		Revoke:     adapterCid(t, "revoked"),
		Cause:      adapterCid(t, "cause"),
		Path:       []cid.Cid{adapterCid(t, "root")},
		RecordedAt: jsg.DagJsonTime(at),
	}
	fh := &fakeFirehose{records: []api.FirehoseRevocation{rec}}

	var got []Event
	for ev, err := range newSwarfSource(fh).Stream(context.Background(), at.Add(-time.Hour)) {
		require.NoError(t, err)
		got = append(got, ev)
	}

	require.Equal(t, at.Add(-time.Hour), fh.since, "the since cursor reaches the client unchanged")
	require.Len(t, got, 1)
	require.False(t, got[0].IsPrincipal(), "a firehose revocation is a revocation event")
	require.True(t, got[0].Revoke.Equals(rec.Revoke))
	require.True(t, got[0].Cause.Equals(rec.Cause))
	require.True(t, got[0].RecordedAt.Equal(at))
	require.Empty(t, got[0].Principal)
	require.Zero(t, got[0].Tenant)
}

func TestSwarfSourceForwardsErrors(t *testing.T) {
	want := errors.New("stream died")
	fh := &fakeFirehose{err: want}

	var errs []error
	for _, err := range newSwarfSource(fh).Stream(context.Background(), time.Time{}) {
		errs = append(errs, err)
	}
	require.Equal(t, []error{want}, errs)
}
