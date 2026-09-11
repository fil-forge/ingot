package revocation

import (
	"context"
	"errors"
	"iter"
	"testing"
	"time"

	jsg "github.com/alanshaw/dag-json-gen"
	"github.com/fil-forge/swarf/pkg/api"
	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var adapterTenant = did.MustParse("did:plc:ewvi7nxzyoun6zhxrhs64oiz")

func adapterCid(t *testing.T, s string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte(s), multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.DagCBOR, mh)
}

// fakeFirehose yields a fixed script of events and records the `since` it
// was called with. The swarf client itself needs a live SSE endpoint, so the
// adapter is tested against this narrow seam.
type fakeFirehose struct {
	events []api.FirehoseEvent
	err    error
	since  time.Time
}

func (f *fakeFirehose) StreamEvents(_ context.Context, from time.Time) iter.Seq2[api.FirehoseEvent, error] {
	f.since = from
	return func(yield func(api.FirehoseEvent, error) bool) {
		for _, fe := range f.events {
			if !yield(fe, nil) {
				return
			}
		}
		if f.err != nil {
			yield(api.FirehoseEvent{}, f.err)
		}
	}
}

// collect drains the adapter's stream into the events and errors it yielded,
// in order.
func collect(t *testing.T, src Source, from time.Time) ([]Event, []error) {
	t.Helper()
	var got []Event
	var errs []error
	for ev, err := range src.Stream(context.Background(), from) {
		if err != nil {
			errs = append(errs, err)
			continue
		}
		got = append(got, ev)
	}
	return got, errs
}

func TestSwarfSourceMapsRevocations(t *testing.T) {
	at := time.Date(2026, 8, 6, 12, 0, 1, 0, time.UTC)
	rec := api.FirehoseRevocation{
		Revoke:     adapterCid(t, "revoked"),
		Cause:      adapterCid(t, "cause"),
		Path:       []cid.Cid{adapterCid(t, "root")},
		RecordedAt: jsg.DagJsonTime(at),
	}
	fh := &fakeFirehose{events: []api.FirehoseEvent{{Revocation: &rec}}}

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

func TestSwarfSourceMapsPrincipalInvalidations(t *testing.T) {
	at := time.Date(2026, 9, 9, 10, 0, 0, 0, time.UTC)
	rec := api.FirehosePrincipalRevocation{
		Tenant:     adapterTenant,
		Principal:  "8f2c1b7e",
		Cause:      adapterCid(t, "principal-cause"),
		RecordedAt: jsg.DagJsonTime(at),
	}
	fh := &fakeFirehose{events: []api.FirehoseEvent{{PrincipalRevocation: &rec}}}

	var got []Event
	for ev, err := range newSwarfSource(fh).Stream(context.Background(), at.Add(-time.Hour)) {
		require.NoError(t, err)
		got = append(got, ev)
	}

	require.Equal(t, at.Add(-time.Hour), fh.since, "the since cursor reaches the client unchanged")
	require.Len(t, got, 1)
	require.True(t, got[0].IsPrincipal(), "a firehose principal record is a principal event")
	require.Equal(t, rec.Tenant, got[0].Tenant)
	require.Equal(t, rec.Principal, got[0].Principal)
	require.True(t, got[0].Cause.Equals(rec.Cause))
	require.True(t, got[0].RecordedAt.Equal(at))
	require.False(t, got[0].Revoke.Defined(), "a principal invalidation withdraws no delegation")
}

func TestSwarfSourceInterleavesBothKinds(t *testing.T) {
	at := time.Date(2026, 9, 9, 10, 0, 0, 0, time.UTC)
	revoke := api.FirehoseRevocation{
		Revoke:     adapterCid(t, "revoked"),
		Cause:      adapterCid(t, "cause-1"),
		RecordedAt: jsg.DagJsonTime(at),
	}
	principal := api.FirehosePrincipalRevocation{
		Tenant:     adapterTenant,
		Principal:  "8f2c1b7e",
		Cause:      adapterCid(t, "cause-2"),
		RecordedAt: jsg.DagJsonTime(at.Add(time.Second)),
	}
	fh := &fakeFirehose{events: []api.FirehoseEvent{
		{Revocation: &revoke},
		{PrincipalRevocation: &principal},
	}}

	got, errs := collect(t, newSwarfSource(fh), at)

	require.Empty(t, errs)
	require.Len(t, got, 2)
	require.False(t, got[0].IsPrincipal())
	require.True(t, got[0].Revoke.Equals(revoke.Revoke))
	require.True(t, got[1].IsPrincipal())
	require.Equal(t, principal.Principal, got[1].Principal)
	require.True(t, got[1].RecordedAt.After(got[0].RecordedAt), "order is preserved")
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

func TestSwarfSourceRejectsEventWithNoRecord(t *testing.T) {
	at := time.Date(2026, 9, 9, 10, 0, 0, 0, time.UTC)
	rec := api.FirehoseRevocation{
		Revoke:     adapterCid(t, "revoked"),
		Cause:      adapterCid(t, "cause"),
		RecordedAt: jsg.DagJsonTime(at),
	}
	// An event with neither record set is a client or wire fault. It must
	// surface as a stream error, never as a revocation of the undefined CID,
	// and must not swallow the records around it.
	fh := &fakeFirehose{events: []api.FirehoseEvent{
		{},
		{Revocation: &rec},
	}}

	got, errs := collect(t, newSwarfSource(fh), at)

	require.Len(t, errs, 1)
	require.ErrorIs(t, errs[0], errEmptyFirehoseEvent)
	var malformed *MalformedRecordError
	require.ErrorAs(t, errs[0], &malformed)
	require.True(t, malformed.RecordedAt.IsZero(), "an empty event has no position to skip past")
	require.Len(t, got, 1, "the empty event yields no Event")
	require.True(t, got[0].Revoke.Equals(rec.Revoke))
}

func TestSwarfSourceRejectsUnnamedPrincipal(t *testing.T) {
	at := time.Date(2026, 9, 9, 10, 0, 0, 0, time.UTC)
	unnamed := api.FirehosePrincipalRevocation{Tenant: adapterTenant, Cause: adapterCid(t, "cause-1"), RecordedAt: jsg.DagJsonTime(at)}
	untenanted := api.FirehosePrincipalRevocation{Principal: "8f2c", Cause: adapterCid(t, "cause-2"), RecordedAt: jsg.DagJsonTime(at)}
	named := api.FirehosePrincipalRevocation{Tenant: adapterTenant, Principal: "8f2c", Cause: adapterCid(t, "cause-3"), RecordedAt: jsg.DagJsonTime(at)}
	// Event distinguishes the kinds by a non-empty Principal, so an
	// invalidation naming none would read as a revocation of the undefined
	// CID. Both malformed shapes surface as stream errors and the record
	// after them is still delivered.
	fh := &fakeFirehose{events: []api.FirehoseEvent{
		{PrincipalRevocation: &unnamed},
		{PrincipalRevocation: &untenanted},
		{PrincipalRevocation: &named},
	}}

	got, errs := collect(t, newSwarfSource(fh), at)

	require.Len(t, errs, 2)
	require.ErrorIs(t, errs[0], errUnnamedPrincipal)
	require.ErrorIs(t, errs[1], errUnnamedPrincipal)
	// The error carries the record's position so the consumer can move past it.
	var malformed *MalformedRecordError
	require.ErrorAs(t, errs[0], &malformed)
	require.Equal(t, at, malformed.RecordedAt)
	require.True(t, malformed.Cause.Equals(unnamed.Cause))
	require.Len(t, got, 1)
	require.True(t, got[0].IsPrincipal())
	require.Equal(t, "8f2c", got[0].Principal)
}

func TestSwarfSourceStopsWhenConsumerStops(t *testing.T) {
	at := time.Date(2026, 9, 9, 10, 0, 0, 0, time.UTC)
	first := api.FirehoseRevocation{Revoke: adapterCid(t, "a"), Cause: adapterCid(t, "c1"), RecordedAt: jsg.DagJsonTime(at)}
	second := api.FirehoseRevocation{Revoke: adapterCid(t, "b"), Cause: adapterCid(t, "c2"), RecordedAt: jsg.DagJsonTime(at)}
	fh := &fakeFirehose{events: []api.FirehoseEvent{{Revocation: &first}, {Revocation: &second}}}

	var seen int
	for _, err := range newSwarfSource(fh).Stream(context.Background(), at) {
		require.NoError(t, err)
		seen++
		break
	}
	require.Equal(t, 1, seen, "breaking out of the range stops the adapter after one event")
}
