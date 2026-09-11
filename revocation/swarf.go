package revocation

import (
	"context"
	"errors"
	"iter"
	"time"

	"github.com/fil-forge/swarf/pkg/api"
	swarfclient "github.com/fil-forge/swarf/pkg/client"
)

// firehose is the slice of the swarf client the adapter reads: the SSE
// firehose of revocations and principal invalidations from a `since` cursor.
type firehose interface {
	StreamEvents(ctx context.Context, from time.Time) iter.Seq2[api.FirehoseEvent, error]
}

// Compile-time assertions: the real swarf client is a firehose, and the
// adapter over it is a Source.
var (
	_ firehose = (*swarfclient.Client)(nil)
	_ Source   = (*swarfSource)(nil)
)

// errEmptyFirehoseEvent is the stream error the adapter yields for a firehose
// event that carries neither a revocation nor a principal invalidation. The
// client promises exactly one is set; an event with neither is a client or
// wire fault, and passing it on as a revocation of cid.Undef would silently
// advance the cursor over it.
var errEmptyFirehoseEvent = errors.New("revocation: firehose event carries no record")

// errUnnamedPrincipal is the stream error the adapter yields for a principal
// invalidation with an empty principal or an undefined tenant. Event tells
// the two kinds apart by a non-empty Principal, so mapping such a record
// would turn it into a revocation of cid.Undef and advance the cursor over
// it silently.
var errUnnamedPrincipal = errors.New("revocation: principal invalidation names no principal")

// swarfSource adapts the swarf client's firehose to Source.
type swarfSource struct {
	fh firehose
}

// NewSwarfSource returns the consumer's Source over the swarf client.
//
// The client's firehose carries two event kinds, revocations and principal
// invalidations, and this adapter is the single place that maps the wire
// shape of each to an Event. It discriminates on which record the client
// set, which follows the SSE event name, rather than on any field's value.
func NewSwarfSource(c *swarfclient.Client) Source { return newSwarfSource(c) }

// newSwarfSource is the fakeable constructor NewSwarfSource wraps.
func newSwarfSource(fh firehose) Source { return &swarfSource{fh: fh} }

// Stream yields each firehose event as an ingot Event and each stream error
// unchanged. A record it cannot map (neither kind set, or a principal
// invalidation naming no principal) is yielded as a [MalformedRecordError]
// carrying the record's position, so the consumer skips it and moves on
// instead of applying a zero revocation or stalling on it.
func (s *swarfSource) Stream(ctx context.Context, from time.Time) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		for fe, err := range s.fh.StreamEvents(ctx, from) {
			if err != nil {
				if !yield(Event{}, err) {
					return
				}
				continue
			}
			ev, err := toEvent(fe)
			if err != nil {
				malformed := &MalformedRecordError{Err: err}
				if fe.PrincipalRevocation != nil {
					malformed.RecordedAt = fe.PrincipalRevocation.RecordedAt.Time()
					malformed.Cause = fe.PrincipalRevocation.Cause
				}
				if !yield(Event{}, malformed) {
					return
				}
				continue
			}
			if !yield(ev, nil) {
				return
			}
		}
	}
}

// toEvent maps one firehose event to an ingot Event by the record the client
// set. Neither set is errEmptyFirehoseEvent.
func toEvent(fe api.FirehoseEvent) (Event, error) {
	switch {
	case fe.Revocation != nil:
		return revocationEvent(*fe.Revocation), nil
	case fe.PrincipalRevocation != nil:
		if fe.PrincipalRevocation.Principal == "" || !fe.PrincipalRevocation.Tenant.Defined() {
			return Event{}, errUnnamedPrincipal
		}
		return principalEvent(*fe.PrincipalRevocation), nil
	default:
		return Event{}, errEmptyFirehoseEvent
	}
}

// revocationEvent maps one firehose revocation to an ingot Event. Principal
// and Tenant stay zero, which is what makes it a revocation event.
func revocationEvent(rec api.FirehoseRevocation) Event {
	return Event{
		RecordedAt: rec.RecordedAt.Time(),
		Cause:      rec.Cause,
		Revoke:     rec.Revoke,
	}
}

// principalEvent maps one firehose principal invalidation to an ingot Event.
// Revoke stays undefined: the record withdraws no delegation.
func principalEvent(rec api.FirehosePrincipalRevocation) Event {
	return Event{
		RecordedAt: rec.RecordedAt.Time(),
		Cause:      rec.Cause,
		Tenant:     rec.Tenant,
		Principal:  rec.Principal,
	}
}
