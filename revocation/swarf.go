package revocation

import (
	"context"
	"iter"
	"time"

	"github.com/fil-forge/swarf/pkg/api"
	swarfclient "github.com/fil-forge/swarf/pkg/client"
)

// firehose is the slice of the swarf client the adapter reads: the SSE
// revocation stream from a `since` cursor.
type firehose interface {
	Stream(ctx context.Context, from time.Time) iter.Seq2[api.FirehoseRevocation, error]
}

// Compile-time assertions: the real swarf client is a firehose, and the
// adapter over it is a Source.
var (
	_ firehose = (*swarfclient.Client)(nil)
	_ Source   = (*swarfSource)(nil)
)

// swarfSource adapts the swarf client's firehose to Source.
type swarfSource struct {
	fh firehose
}

// NewSwarfSource returns the consumer's Source over the swarf client.
//
// The client streams revocations only, so every event it yields is a
// revocation event. Principal invalidations reach the consumer once the
// client exposes them; this adapter grows a second case then, and nothing
// upstream of it changes.
func NewSwarfSource(c *swarfclient.Client) Source { return newSwarfSource(c) }

// newSwarfSource is the fakeable constructor NewSwarfSource wraps.
func newSwarfSource(fh firehose) Source { return &swarfSource{fh: fh} }

// Stream yields each firehose record as an ingot Event, and each stream
// error unchanged.
func (s *swarfSource) Stream(ctx context.Context, from time.Time) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		for rec, err := range s.fh.Stream(ctx, from) {
			if err != nil {
				if !yield(Event{}, err) {
					return
				}
				continue
			}
			if !yield(revocationEvent(rec), nil) {
				return
			}
		}
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
