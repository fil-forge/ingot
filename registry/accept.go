package registry

import (
	"bytes"
	"fmt"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/taskrunner"
)

// AcceptQueue is the task queue a multipart upload's parked parts are
// accepted on. Its subject is the upload ID, so an upload has one accept
// running at a time, apart from the upload's other work: a pass makes network
// calls and can run for minutes.
const AcceptQueue = "queue-multipart-upload-shard"

// AcceptAction concludes an upload's parked parts and marks them accepted.
const AcceptAction = "accept"

// AcceptTask is a queue-multipart-upload-shard.accept task's arguments.
type AcceptTask struct {
	// Parts limits the accept to these part numbers; empty accepts every
	// parked part. Complete names the parts it lists, so a part it drops
	// cannot fail it.
	Parts []int64
}

// AcceptSpec returns the accept an UploadPart queues for uploadID: a
// replacement due now, so an upload has at most one pending.
func AcceptSpec(uploadID string) taskrunner.Spec {
	now := DueNow()
	return taskrunner.Spec{
		Queue:     AcceptQueue,
		Subject:   uploadID,
		Action:    AcceptAction,
		NotBefore: &now,
		Replace:   true,
	}
}

// CompleteAcceptSpec returns the accept Complete runs for uploadID's parts,
// to start by expires.
func CompleteAcceptSpec(uploadID string, parts []int64, expires time.Time) (taskrunner.Spec, error) {
	var buf bytes.Buffer
	if err := (&AcceptTask{Parts: parts}).MarshalCBOR(&buf); err != nil {
		return taskrunner.Spec{}, fmt.Errorf("registry: encode accept: %w", err)
	}
	return taskrunner.Spec{
		Queue:   AcceptQueue,
		Subject: uploadID,
		Action:  AcceptAction,
		Args:    buf.Bytes(),
		Expires: &expires,
	}, nil
}

// AcceptTaskFrom decodes an accept task's arguments.
func AcceptTaskFrom(t taskrunner.Task) (AcceptTask, error) {
	var args AcceptTask
	if len(t.Args) == 0 {
		return args, nil
	}
	if err := args.UnmarshalCBOR(bytes.NewReader(t.Args)); err != nil {
		return AcceptTask{}, fmt.Errorf("registry: decode accept: %w", err)
	}
	return args, nil
}

// acceptedIntent marks the space's intent for a located blob accepted. A
// published intent stays published.
const acceptedIntent = `UPDATE ingot.upload_intents SET state = 'accepted', updated_at = now()
	 WHERE space = $1 AND digest = $2 AND state IN ('uploading', 'parked')
	   AND EXISTS (SELECT 1 FROM ingot.blob_locations l WHERE l.space = $1 AND l.digest = $2)`

// ConcludedBlob is the writes that record a parked blob's conclusion: the
// park whose allocation was concluded, named by its add task, is swapped for
// the location, and the intent marked accepted. A park released meanwhile
// leaves nothing to swap: its release aborted the allocation or removed the
// blob, and a location written after it would let a later write reuse a
// blob that is gone. A park another write recorded since names an allocation
// this conclude did not accept, and stays.
func ConcludedBlob(space did.DID, digest multihash.Multihash, addTask []byte, loc BlobLocation) []Statement {
	return []Statement{
		{`WITH swapped AS (
		    DELETE FROM ingot.blob_parks WHERE space = $1 AND digest = $2 AND add_task = $3
		    RETURNING space, digest)
		  INSERT INTO ingot.blob_locations (space, digest, provider, url, size)
		  SELECT space, digest, $4, $5, $6 FROM swapped
		  ON CONFLICT (space, digest) DO UPDATE
		    SET provider = EXCLUDED.provider, url = EXCLUDED.url, size = EXCLUDED.size`,
			[]any{space, []byte(digest), addTask, loc.Provider, loc.URL, loc.Size}},
		{acceptedIntent, []any{space, []byte(digest)}},
	}
}

// LocatedBlob is the writes that record a parked blob found already
// accepted for the space (a dedup): a park left beside the location is
// dropped, and the intent marked accepted, while the location stands.
func LocatedBlob(space did.DID, digest multihash.Multihash) []Statement {
	return []Statement{
		{`DELETE FROM ingot.blob_parks WHERE space = $1 AND digest = $2
		    AND EXISTS (SELECT 1 FROM ingot.blob_locations l WHERE l.space = $1 AND l.digest = $2)`,
			[]any{space, []byte(digest)}},
		{acceptedIntent, []any{space, []byte(digest)}},
	}
}

// AcceptedPart is the write that marks p, a part of a session in space,
// accepted once every one of its blobs is located there, and while its row
// still names the blobs it was read with: a part replaced or dropped
// meanwhile is left, as is one whose conclude found its park taken.
func AcceptedPart(space did.DID, p MultipartPart) Statement {
	return Statement{
		`UPDATE ingot.multipart_parts p SET state = 'accepted'
		  WHERE p.upload_id = $1 AND p.part_number = $2 AND p.blob_digests = $3 AND p.state = 'parked'
		    AND NOT EXISTS (
		      SELECT 1 FROM unnest(p.blob_digests) AS b(digest)
		       WHERE NOT EXISTS (SELECT 1 FROM ingot.blob_locations l WHERE l.space = $4 AND l.digest = b.digest))`,
		[]any{p.UploadID, p.PartNumber, p.BlobDigests, space},
	}
}
