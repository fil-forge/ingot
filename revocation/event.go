package revocation

import (
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
)

// Event is one record off the revocation service's firehose, in ingot's own
// terms. The firehose carries two kinds and the consumer dispatches on them:
//
//   - a revocation, which withdraws one delegation (Revoke), and
//   - a principal invalidation, which announces that what (Tenant,
//     Principal) may do has changed.
//
// Both kinds carry RecordedAt (the service's own timeline, which the resume
// cursor tracks) and Cause (the CID of the invocation that produced the
// record). Only a revocation sets Revoke; only a principal invalidation sets
// Tenant and Principal.
//
// The type is ingot's, not the client's, so the consumer and its tests do
// not depend on the wire shape: the adapter in swarf.go is the single place
// that knows it.
type Event struct {
	RecordedAt time.Time
	Cause      cid.Cid
	Revoke     cid.Cid
	Tenant     did.DID
	Principal  string
}

// IsPrincipal reports whether the event is a principal invalidation rather
// than a revocation. A principal is always named, so a non-empty Principal
// is the discriminator.
func (e Event) IsPrincipal() bool { return e.Principal != "" }
