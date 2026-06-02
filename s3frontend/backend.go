// Package s3frontend implements versitygw's backend.Backend by
// orchestrating directly over the ingot domain primitives. It is the
// only S3 frontend ingot ships; it is wired into the process via
// pkg/ingot.Server.
//
// The Backend type is a thin protocol adapter:
//   - Read paths drive a single ReadStore that exposes both
//     CBOR-decoded reads (manifest, MST nodes) and raw block reads
//     (body chunks). The interface has no Put method, so write paths
//     can't accidentally route through it.
//   - Write paths drive a per-op bucketop.Tx, which owns the
//     staging buffer, MST CBOR view, bucket-Root CAS, and per-bucket
//     locking.
//
// Operations not implemented (multipart, lifecycle, locking,
// versioning, etc.) inherit ErrNotImplemented from the embedded
// backend.BackendUnsupported. The few unsupported-by-default
// methods that versitygw nevertheless calls on every request
// (GetBucketAcl, GetBucketPolicy, GetObjectLockConfiguration,
// GetBucketVersioning) are stubbed in bucket.go.
package s3frontend

import (
	"context"

	"github.com/versity/versitygw/backend"

	"github.com/fil-forge/ingot/blockstore"
	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/bucketop"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
)

// Backend implements versitygw's backend.Backend directly over the
// ingot domain primitives. The embedded BackendUnsupported supplies
// ErrNotImplemented defaults for every operation; we override only
// the ones we actually serve.
type Backend struct {
	backend.BackendUnsupported

	read  blockstore.ReadStore
	reg   registry.Registry
	txns  *bucketop.Coordinator
	codec msbucket.BodyCodec
}

// Compile-time assertion that Backend satisfies versitygw's interface.
var _ backend.Backend = (*Backend)(nil)

// New constructs a Backend wired over ingot's domain primitives.
// rs is the layered read blockstore (log → forge); log is the
// LSM-style write log; codec is the body-DAG codec used for both
// chunking on PUT and streaming on GET — typically a *FixedChunker.
func New(reg registry.Registry, rs blockstore.ReadStore, log *logstore.Store, codec msbucket.BodyCodec) *Backend {
	return &Backend{
		read:  rs,
		reg:   reg,
		txns:  bucketop.NewCoordinator(bucketop.Deps{Reg: reg, Log: log, Reads: rs}),
		codec: codec,
	}
}

// String identifies this backend in versitygw logs.
func (*Backend) String() string { return "ingot" }

// Shutdown is a no-op; lifecycle for the underlying registry/log is
// owned by pkg/ingot.Server's Stop hook, not by versitygw.
func (*Backend) Shutdown() {}

// Recover is a no-op in the LSM design: logstore.Open already
// scanned the segment directory, reconciled with Postgres, and
// re-enqueued any pending segments for the background flusher.
// Recover is retained as the lifecycle seam in case future
// invariants need verifying before the listener accepts traffic.
func (b *Backend) Recover(_ context.Context) error { return nil }

// Drain shuts the log down via the Coordinator: seals the open
// segment, drains the flush queue, and updates per-bucket
// forge_root_cid for every op_root that landed in a flushed
// segment. After Drain returns cleanly, no acked write is
// unrepresented in Postgres.
func (b *Backend) Drain(ctx context.Context) error {
	if b.txns == nil {
		return nil
	}
	return b.txns.Close(ctx)
}
