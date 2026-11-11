package registry

import (
	"github.com/fil-forge/ucantone/did"
	"github.com/multiformats/go-multihash"
)

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

// AcceptedParts is the write that takes digest off the list of blobs still
// to be accepted of every parked part of an open session in space that names
// it, and marks accepted each part it leaves with none. It follows the writes
// that locate digest in the same commit, and applies only while digest is
// located: a conclude whose park was taken, or a location gone before the
// commit, leaves the parts as they are. Each part's row is locked by the
// update, so two accepts of one part's blobs, which run under different
// subjects, take turns on it, and the second sees the first's removal. A part
// written since it was listed lists every one of its own blobs, each with an
// accept of its own to come, and a part replaced or dropped meanwhile is not
// there to mark.
func AcceptedParts(space did.DID, digest multihash.Multihash) []Statement {
	return []Statement{{
		`UPDATE ingot.multipart_parts p
		    SET unaccepted = array_remove(p.unaccepted, $2::bytea),
		        state = CASE WHEN cardinality(array_remove(p.unaccepted, $2::bytea)) = 0 THEN 'accepted' ELSE p.state END
		   FROM ingot.multipart_sessions s
		  WHERE s.upload_id = p.upload_id AND s.space = $1 AND s.state = $3
		    AND p.blob_digests @> ARRAY[$2::bytea] AND p.unaccepted @> ARRAY[$2::bytea] AND p.state = 'parked'
		    AND EXISTS (SELECT 1 FROM ingot.blob_locations l WHERE l.space = $1 AND l.digest = $2)`,
		[]any{space, []byte(digest), SessionOpen},
	}}
}

// AcceptedEmptyPart is the write that marks p, a parked part with no blobs,
// accepted: it has nothing to conclude.
func AcceptedEmptyPart(p MultipartPart) Statement {
	return Statement{
		`UPDATE ingot.multipart_parts SET state = 'accepted'
		  WHERE upload_id = $1 AND part_number = $2 AND state = 'parked' AND cardinality(blob_digests) = 0`,
		[]any{p.UploadID, p.PartNumber},
	}
}
