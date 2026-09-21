-- +goose Up
-- A committed blob's upload intent is 'published', written with its first
-- reference claim; a release recognises a committed blob by it once the claims
-- are gone and keeps the spool copy (the insurance copy until eviction) and the
-- intent. Blobs committed before the state existed carry 'accepted' intents and
-- their claims are the only trace: mark them, so their eventual release does not
-- take them for never-committed part blobs and remove the spool copy.
UPDATE ingot.upload_intents i
   SET state = 'published', updated_at = now()
 WHERE i.state <> 'published'
   AND EXISTS (SELECT 1 FROM ingot.blob_refs r WHERE r.digest = i.digest);

-- +goose Down
-- Nothing to undo: the rows were committed blobs before and after.
