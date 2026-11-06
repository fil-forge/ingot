package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/taskrunner"
)

// Compile-time assertions: *Postgres satisfies every store interface.
var (
	_ BlobRefStore          = (*Postgres)(nil)
	_ IntentStore           = (*Postgres)(nil)
	_ LocationStore         = (*Postgres)(nil)
	_ EncryptionParamsStore = (*Postgres)(nil)
	_ InclusionStore        = (*Postgres)(nil)
	_ MultipartStore        = (*Postgres)(nil)
	_ GCStore               = (*Postgres)(nil)
	_ CatalogUsageStore     = (*Postgres)(nil)
	_ RevocationCursorStore = (*Postgres)(nil)
)

// BlobRefStore ===============================================================

func (r *Postgres) CountReferences(ctx context.Context, space did.DID, digest multihash.Multihash, except taskrunner.TaskID) (int, error) {
	var n int
	err := r.pool.QueryRow(ctx, countReferencesSQL, space, digest, except, SessionOpen).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("registry: count references: %w", err)
	}
	return n, nil
}

func (r *Postgres) ExpiriesOf(ctx context.Context, refs []taskrunner.TaskID) ([]taskrunner.TaskID, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	var expires []taskrunner.TaskID
	if err := r.pool.QueryRow(ctx,
		`SELECT coalesce(array_agg(expire_task), '{}') FROM ingot.blob_pending_refs WHERE add_task = ANY($1)`,
		refs).Scan(&expires); err != nil {
		return nil, fmt.Errorf("registry: expiries of pending references: %w", err)
	}
	return expires, nil
}

func (r *Postgres) PendingRefByExpiry(ctx context.Context, expire taskrunner.TaskID) (taskrunner.TaskID, error) {
	var add taskrunner.TaskID
	err := r.pool.QueryRow(ctx, `SELECT add_task FROM ingot.blob_pending_refs WHERE expire_task = $1`, expire).Scan(&add)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, ErrNotFound
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("registry: pending reference by expiry: %w", err)
	}
	return add, nil
}

// countReferencesSQL counts a space's references to a digest in one
// statement, so one snapshot reads them all: a row that takes another's
// reference over in one transaction (a claim or a part row taking a pending
// reference) is counted before or after, never neither.
const countReferencesSQL = `SELECT
	  (SELECT count(*) FROM ingot.blob_refs WHERE space = $1 AND digest = $2)
	+ (SELECT count(*) FROM ingot.blob_pending_refs WHERE space = $1 AND digest = $2 AND add_task <> $3)
	+ (SELECT count(*) FROM ingot.multipart_parts p
	     JOIN ingot.multipart_sessions s ON s.upload_id = p.upload_id
	    WHERE p.blob_digests @> ARRAY[$2::bytea] AND s.space = $1 AND s.state = $4)`

func (r *Postgres) QueueReleases(ctx context.Context, space did.DID, digests []multihash.Multihash, notBefore time.Time, release ReleaseTask) error {
	if len(digests) == 0 {
		return nil
	}
	if r.tasks == nil {
		return fmt.Errorf("registry: releases to queue but the registry has no task runner")
	}
	specs := make([]taskrunner.Spec, len(digests))
	for i, d := range digests {
		var err error
		if specs[i], err = release.Spec(space, d, notBefore); err != nil {
			return err
		}
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("registry: queue releases: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := r.tasks.SubmitAllTx(ctx, tx, specs); err != nil {
		return fmt.Errorf("registry: queue releases: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("registry: queue releases: commit: %w", err)
	}
	return nil
}

// IntentStore ================================================================

func (r *Postgres) PutIntent(ctx context.Context, in UploadIntent) error {
	if _, err := r.pool.Exec(ctx,
		`INSERT INTO ingot.upload_intents (space, digest, size, state, bucket)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (space, digest) DO UPDATE
		   SET size       = EXCLUDED.size,
		       state      = EXCLUDED.state,
		       bucket     = EXCLUDED.bucket,
		       updated_at = now()`,
		in.Space, in.Digest, in.Size, in.State, nullString(in.Bucket)); err != nil {
		return fmt.Errorf("registry: put intent: %w", err)
	}
	return nil
}

func (r *Postgres) SetIntentState(ctx context.Context, space did.DID, digest multihash.Multihash, state string) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE ingot.upload_intents
		 SET state = $3, updated_at = CASE WHEN state = $3 THEN updated_at ELSE now() END
		 WHERE space = $1 AND digest = $2`,
		space, digest, state)
	if err != nil {
		return fmt.Errorf("registry: set intent state: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Postgres) GetIntent(ctx context.Context, space did.DID, digest multihash.Multihash) (*UploadIntent, error) {
	in := &UploadIntent{Space: space, Digest: digest}
	var bucket *string
	err := r.pool.QueryRow(ctx,
		`SELECT size, state, bucket, updated_at
		   FROM ingot.upload_intents WHERE space = $1 AND digest = $2`,
		space, digest).Scan(&in.Size, &in.State, &bucket, &in.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("registry: get intent: %w", err)
	}
	if bucket != nil {
		in.Bucket = *bucket
	}
	return in, nil
}

func (r *Postgres) ListIntentsByState(ctx context.Context, state string) ([]UploadIntent, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT `+intentColumns+` FROM ingot.upload_intents i WHERE i.state = $1`,
		state)
	if err != nil {
		return nil, fmt.Errorf("registry: list intents: %w", err)
	}
	return scanIntents(rows)
}

func (r *Postgres) CountOtherIntents(ctx context.Context, space did.DID, digest multihash.Multihash) (int, error) {
	var n int
	err := r.pool.QueryRow(ctx,
		`SELECT count(*) FROM ingot.upload_intents WHERE digest = $2 AND space <> $1`,
		space, digest).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("registry: count other intents: %w", err)
	}
	return n, nil
}

func (r *Postgres) StalledBytes(ctx context.Context, before time.Time) (int64, error) {
	// The state literal matches the partial index upload_intents_stalled_idx
	// (migration 00035): the planner uses it only when the query names the
	// same constant, not a parameter. The index carries size, so the sum
	// comes from it alone.
	var n int64
	err := r.pool.QueryRow(ctx,
		`SELECT COALESCE(sum(size), 0)::bigint
		   FROM ingot.upload_intents
		  WHERE state = 'uploading'
		    AND updated_at < $1`,
		before).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("registry: sum stalled intents: %w", err)
	}
	return n, nil
}

// intentColumns is the column list scanIntents reads, qualified for the
// alias i (unqualified names resolve against it too).
const intentColumns = `i.space, i.digest, i.size, i.state, i.bucket, i.updated_at`

// scanIntents reads rows of intentColumns and closes them.
func scanIntents(rows pgx.Rows) ([]UploadIntent, error) {
	defer rows.Close()
	var out []UploadIntent
	for rows.Next() {
		var in UploadIntent
		var space string
		var bucket *string
		if err := rows.Scan(&space, &in.Digest, &in.Size, &in.State, &bucket, &in.UpdatedAt); err != nil {
			return nil, fmt.Errorf("registry: list intents scan: %w", err)
		}
		sp, err := did.Parse(space)
		if err != nil {
			return nil, fmt.Errorf("registry: list intents: parse space %q: %w", space, err)
		}
		in.Space = sp
		if bucket != nil {
			in.Bucket = *bucket
		}
		out = append(out, in)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: list intents rows: %w", err)
	}
	return out, nil
}

func (r *Postgres) DeleteIntent(ctx context.Context, space did.DID, digest multihash.Multihash) error {
	_, err := r.pool.Exec(ctx,
		`DELETE FROM ingot.upload_intents WHERE space = $1 AND digest = $2`, space, digest)
	if err != nil {
		return fmt.Errorf("registry: delete intent: %w", err)
	}
	return nil
}

// LocationStore ==============================================================

func (r *Postgres) PutLocation(ctx context.Context, loc BlobLocation) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO ingot.blob_locations (space, digest, provider, url, size)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (space, digest) DO UPDATE
		   SET provider = EXCLUDED.provider, url = EXCLUDED.url, size = EXCLUDED.size`,
		loc.Space, loc.Digest, loc.Provider, loc.URL, loc.Size)
	if err != nil {
		return fmt.Errorf("registry: put location: %w", err)
	}
	return nil
}

func (r *Postgres) GetLocation(ctx context.Context, space did.DID, digest multihash.Multihash) (*BlobLocation, error) {
	loc := &BlobLocation{Space: space, Digest: digest}
	err := r.pool.QueryRow(ctx,
		`SELECT provider, url, size
		 FROM ingot.blob_locations WHERE space = $1 AND digest = $2`,
		space, digest).Scan(&loc.Provider, &loc.URL, &loc.Size)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("registry: get location: %w", err)
	}
	return loc, nil
}

func (r *Postgres) GetLocations(ctx context.Context, space did.DID, digests []multihash.Multihash) (map[string]BlobLocation, error) {
	out := make(map[string]BlobLocation, len(digests))
	if len(digests) == 0 {
		return out, nil
	}
	rows, err := r.pool.Query(ctx,
		`SELECT digest, provider, url, size
		 FROM ingot.blob_locations WHERE space = $1 AND digest = ANY($2::bytea[])`,
		space, digestArray(digests))
	if err != nil {
		return nil, fmt.Errorf("registry: get locations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		loc := BlobLocation{Space: space}
		var d []byte
		if err := rows.Scan(&d, &loc.Provider, &loc.URL, &loc.Size); err != nil {
			return nil, fmt.Errorf("registry: get locations scan: %w", err)
		}
		if loc.Digest, err = multihash.Cast(d); err != nil {
			return nil, fmt.Errorf("registry: get locations: stored digest %x: %w", d, err)
		}
		out[string(d)] = loc
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: get locations rows: %w", err)
	}
	return out, nil
}

// digestArray converts digests to the [][]byte pgx encodes as bytea[].
func digestArray(digests []multihash.Multihash) [][]byte {
	raw := make([][]byte, len(digests))
	for i, d := range digests {
		raw[i] = d
	}
	return raw
}

func (r *Postgres) DeleteLocation(ctx context.Context, space did.DID, digest multihash.Multihash) error {
	_, err := r.pool.Exec(ctx,
		`DELETE FROM ingot.blob_locations WHERE space = $1 AND digest = $2`, space, digest)
	if err != nil {
		return fmt.Errorf("registry: delete location: %w", err)
	}
	return nil
}

// EncryptionParamsStore ======================================================

func (r *Postgres) PutEncryptionParams(ctx context.Context, params BlobEncryptionParams) error {
	// Upsert: a re-encryption replaces the parameter set for a blob already
	// stored. Every column is NOT NULL with a CHECK, so an incomplete set is
	// rejected by the constraint rather than by a second check here.
	_, err := r.pool.Exec(ctx,
		`INSERT INTO ingot.blob_encryption_params
		   (space, digest, region_wrapped_cek, region_key_version, header_len, aad)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (space, digest) DO UPDATE
		   SET region_wrapped_cek = EXCLUDED.region_wrapped_cek,
		       region_key_version = EXCLUDED.region_key_version,
		       header_len         = EXCLUDED.header_len,
		       aad                = EXCLUDED.aad`,
		params.Space, params.Digest, params.RegionWrappedCEK, params.RegionKeyVersion,
		params.HeaderLen, params.AAD)
	if err != nil {
		return fmt.Errorf("registry: put encryption params: %w", err)
	}
	return nil
}

func (r *Postgres) GetEncryptionParams(ctx context.Context, space did.DID, digest multihash.Multihash) (*BlobEncryptionParams, error) {
	params := &BlobEncryptionParams{Space: space, Digest: digest}
	err := r.pool.QueryRow(ctx,
		`SELECT region_wrapped_cek, region_key_version, header_len, aad
		 FROM ingot.blob_encryption_params WHERE space = $1 AND digest = $2`,
		space, digest).Scan(&params.RegionWrappedCEK, &params.RegionKeyVersion,
		&params.HeaderLen, &params.AAD)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("registry: get encryption params: %w", err)
	}
	return params, nil
}

func (r *Postgres) GetEncryptionParamsBatch(ctx context.Context, space did.DID, digests []multihash.Multihash) (map[string]BlobEncryptionParams, error) {
	out := make(map[string]BlobEncryptionParams, len(digests))
	if len(digests) == 0 {
		return out, nil
	}
	rows, err := r.pool.Query(ctx,
		`SELECT digest, region_wrapped_cek, region_key_version, header_len, aad
		 FROM ingot.blob_encryption_params WHERE space = $1 AND digest = ANY($2::bytea[])`,
		space, digestArray(digests))
	if err != nil {
		return nil, fmt.Errorf("registry: get encryption params batch: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		params := BlobEncryptionParams{Space: space}
		var d []byte
		if err := rows.Scan(&d, &params.RegionWrappedCEK, &params.RegionKeyVersion, &params.HeaderLen, &params.AAD); err != nil {
			return nil, fmt.Errorf("registry: get encryption params batch scan: %w", err)
		}
		if params.Digest, err = multihash.Cast(d); err != nil {
			return nil, fmt.Errorf("registry: get encryption params batch: stored digest %x: %w", d, err)
		}
		out[string(d)] = params
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: get encryption params batch rows: %w", err)
	}
	return out, nil
}

func (r *Postgres) DeleteEncryptionParams(ctx context.Context, space did.DID, digest multihash.Multihash) error {
	_, err := r.pool.Exec(ctx,
		`DELETE FROM ingot.blob_encryption_params WHERE space = $1 AND digest = $2`, space, digest)
	if err != nil {
		return fmt.Errorf("registry: delete encryption params: %w", err)
	}
	return nil
}

func (r *Postgres) RewrapEncryptionParams(ctx context.Context, space did.DID, digest multihash.Multihash, wrappedCEK []byte, keyVersion string) error {
	// The CHECK constraints reject an empty wrapped CEK or key version, so a
	// rotation cannot blank out the material the decrypt path needs.
	tag, err := r.pool.Exec(ctx,
		`UPDATE ingot.blob_encryption_params
		    SET region_wrapped_cek = $3, region_key_version = $4
		  WHERE space = $1 AND digest = $2`,
		space, digest, wrappedCEK, keyVersion)
	if err != nil {
		return fmt.Errorf("registry: rewrap encryption params: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ParkStore ==================================================================

func (r *Postgres) PutPark(ctx context.Context, p BlobPark) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`INSERT INTO ingot.blob_parks (space, digest, add_task, accept_task, put_invocation, size)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (space, digest) DO NOTHING`,
		p.Space, p.Digest, p.AddTask, p.AcceptTask, p.PutInvocation, p.Size)
	if err != nil {
		return false, fmt.Errorf("registry: put park: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *Postgres) GetPark(ctx context.Context, space did.DID, digest multihash.Multihash) (*BlobPark, error) {
	park := &BlobPark{Space: space, Digest: digest}
	err := r.pool.QueryRow(ctx,
		`SELECT add_task, accept_task, put_invocation, size, created_at
		 FROM ingot.blob_parks WHERE space = $1 AND digest = $2`,
		space, digest).Scan(&park.AddTask, &park.AcceptTask, &park.PutInvocation, &park.Size, &park.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("registry: get park: %w", err)
	}
	return park, nil
}

func (r *Postgres) DeletePark(ctx context.Context, space did.DID, digest multihash.Multihash) error {
	_, err := r.pool.Exec(ctx,
		`DELETE FROM ingot.blob_parks WHERE space = $1 AND digest = $2`, space, digest)
	if err != nil {
		return fmt.Errorf("registry: delete park: %w", err)
	}
	return nil
}

// InclusionStore =============================================================

func (r *Postgres) PutInclusions(ctx context.Context, incs []BlobInclusion) error {
	if len(incs) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, inc := range incs {
		batch.Queue(
			`INSERT INTO ingot.shard_inclusions (space, digest, shard_digest, range_start, range_end)
			 VALUES ($1, $2, $3, $4, $5)
			 ON CONFLICT (space, digest) DO UPDATE
			   SET shard_digest = EXCLUDED.shard_digest,
			       range_start  = EXCLUDED.range_start,
			       range_end    = EXCLUDED.range_end`,
			inc.Space, inc.Digest, inc.ShardDigest, inc.RangeStart, inc.RangeEnd)
	}
	if err := r.pool.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("registry: put inclusions: %w", err)
	}
	return nil
}

func (r *Postgres) GetInclusion(ctx context.Context, space did.DID, digest multihash.Multihash) (*BlobInclusion, error) {
	inc := &BlobInclusion{Space: space, Digest: digest}
	err := r.pool.QueryRow(ctx,
		`SELECT shard_digest, range_start, range_end
		 FROM ingot.shard_inclusions WHERE space = $1 AND digest = $2`,
		space, digest).Scan(&inc.ShardDigest, &inc.RangeStart, &inc.RangeEnd)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("registry: get inclusion: %w", err)
	}
	return inc, nil
}

// MultipartStore =============================================================

func (r *Postgres) CreateSession(ctx context.Context, s MultipartSession) error {
	st, err := CreatedSession(s)
	if err != nil {
		return err
	}
	if _, err := r.pool.Exec(ctx, st.SQL, st.Args...); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return ErrExists
		}
		return fmt.Errorf("registry: create session: %w", err)
	}
	return nil
}

func (r *Postgres) GetSession(ctx context.Context, uploadID string) (*MultipartSession, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT upload_id, bucket, object_key, state, content_type, metadata, created_at,
		        content_encoding, content_disposition, content_language, cache_control, expires,
		        website_redirect_location, checksum_algorithm, checksum_type,
		        lock_mode, lock_retain_until, lock_legal_hold, tagging,
		        committed_etag, committed_version_id, space, state_changed_at, plaintext
		 FROM ingot.multipart_sessions WHERE upload_id = $1`,
		uploadID)
	s, err := scanSession(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("registry: get session: %w", err)
	}
	return s, nil
}

// scanSession scans one multipart_sessions row in the canonical column order
// (see GetSession/ListSessions selects).
func scanSession(row pgx.Row) (*MultipartSession, error) {
	s := &MultipartSession{}
	var contentType, ce, cd, cl, cc, exp, wrl, ckAlgo, ckType, lockMode, lockHold, tagging *string
	var committedETag, committedVersionID *string
	var meta []byte
	var space string
	err := row.Scan(&s.UploadID, &s.Bucket, &s.ObjectKey, &s.State, &contentType, &meta, &s.CreatedAt,
		&ce, &cd, &cl, &cc, &exp, &wrl, &ckAlgo, &ckType,
		&lockMode, &s.LockRetainUntil, &lockHold, &tagging,
		&committedETag, &committedVersionID, &space, &s.StateChangedAt, &s.Plaintext)
	if err != nil {
		return nil, err
	}
	if space != "" {
		if s.Space, err = did.Parse(space); err != nil {
			return nil, fmt.Errorf("registry: session %s space %q: %w", s.UploadID, space, err)
		}
	}
	setIfNotNil := func(dst *string, src *string) {
		if src != nil {
			*dst = *src
		}
	}
	setIfNotNil(&s.ContentType, contentType)
	setIfNotNil(&s.ContentEncoding, ce)
	setIfNotNil(&s.ContentDisposition, cd)
	setIfNotNil(&s.ContentLanguage, cl)
	setIfNotNil(&s.CacheControl, cc)
	setIfNotNil(&s.Expires, exp)
	setIfNotNil(&s.WebsiteRedirectLocation, wrl)
	setIfNotNil(&s.ChecksumAlgorithm, ckAlgo)
	setIfNotNil(&s.ChecksumType, ckType)
	setIfNotNil(&s.LockMode, lockMode)
	setIfNotNil(&s.LockLegalHold, lockHold)
	setIfNotNil(&s.Tagging, tagging)
	setIfNotNil(&s.CommittedETag, committedETag)
	setIfNotNil(&s.CommittedVersionID, committedVersionID)
	if s.Metadata, err = unmarshalMetadata(meta); err != nil {
		return nil, err
	}
	return s, nil
}

func (r *Postgres) DeleteSession(ctx context.Context, uploadID string) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM ingot.multipart_sessions WHERE upload_id = $1`, uploadID)
	if err != nil {
		return fmt.Errorf("registry: delete session: %w", err)
	}
	return nil
}

// digestsDropped returns the digests of replaced that kept does not name,
// each once, in the form the bytea[] parameter takes.
func digestsDropped(replaced, kept []multihash.Multihash) [][]byte {
	skip := make(map[string]bool, len(kept))
	for _, d := range kept {
		skip[string(d)] = true
	}
	var out [][]byte
	for _, d := range replaced {
		if skip[string(d)] {
			continue
		}
		skip[string(d)] = true
		out = append(out, []byte(d))
	}
	return out
}

func (r *Postgres) ListParkedParts(ctx context.Context, uploadID string, after, limit int) ([]MultipartPart, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT part_number, etag_md5, size, checksum, blob_digests, span_lengths, state, created_at
		 FROM ingot.multipart_parts WHERE upload_id = $1 AND state = $2 AND part_number > $3
		 ORDER BY part_number ASC LIMIT $4`,
		uploadID, PartParked, after, limit)
	if err != nil {
		return nil, fmt.Errorf("registry: list parked parts: %w", err)
	}
	return scanParts(rows, uploadID)
}

func (r *Postgres) OldestParkedPart(ctx context.Context) (time.Time, bool, error) {
	var oldest *time.Time
	if err := r.pool.QueryRow(ctx,
		`SELECT min(p.created_at) FROM ingot.multipart_parts p
		   JOIN ingot.multipart_sessions s ON s.upload_id = p.upload_id
		  WHERE p.state = $1 AND s.state = $2`,
		PartParked, SessionOpen).Scan(&oldest); err != nil {
		return time.Time{}, false, fmt.Errorf("registry: oldest parked part: %w", err)
	}
	if oldest == nil {
		return time.Time{}, false, nil
	}
	return *oldest, true, nil
}

func (r *Postgres) DeletePartTrees(ctx context.Context, uploadID string) error {
	if _, err := r.pool.Exec(ctx, `DELETE FROM ingot.multipart_part_trees WHERE upload_id = $1`, uploadID); err != nil {
		return fmt.Errorf("registry: delete part trees: %w", err)
	}
	return nil
}

func (r *Postgres) ListParts(ctx context.Context, uploadID string) ([]MultipartPart, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT part_number, etag_md5, size, checksum, blob_digests, span_lengths, state, created_at
		 FROM ingot.multipart_parts WHERE upload_id = $1 ORDER BY part_number ASC`,
		uploadID)
	if err != nil {
		return nil, fmt.Errorf("registry: list parts: %w", err)
	}
	return scanParts(rows, uploadID)
}

// scanParts reads uploadID's part rows as ListParts selects them.
func scanParts(rows pgx.Rows, uploadID string) ([]MultipartPart, error) {
	defer rows.Close()
	var out []MultipartPart
	for rows.Next() {
		p := MultipartPart{UploadID: uploadID}
		if err := rows.Scan(&p.PartNumber, &p.ETagMD5, &p.Size, &p.Checksum, &p.BlobDigests, &p.SpanLengths, &p.State, &p.CreatedAt); err != nil {
			return nil, fmt.Errorf("registry: list parts scan: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: list parts rows: %w", err)
	}
	return out, nil
}

func (r *Postgres) ListPartTrees(ctx context.Context, uploadID string) (map[int]*PartTree, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT part_number, tree_offset, chunk_log, nodes, blocks, head, tail, root
		 FROM ingot.multipart_part_trees WHERE upload_id = $1`,
		uploadID)
	if err != nil {
		return nil, fmt.Errorf("registry: list part trees: %w", err)
	}
	defer rows.Close()

	out := map[int]*PartTree{}
	for rows.Next() {
		var n int
		var chunkLog int16
		t := &PartTree{}
		if err := rows.Scan(&n, &t.Offset, &chunkLog, &t.Nodes, &t.Blocks, &t.Head, &t.Tail, &t.Root); err != nil {
			return nil, fmt.Errorf("registry: list part trees scan: %w", err)
		}
		t.ChunkLog = uint8(chunkLog)
		out[n] = t
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: list part trees rows: %w", err)
	}
	return out, nil
}

func (r *Postgres) ListSessions(ctx context.Context, bucket string) ([]MultipartSession, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT upload_id, bucket, object_key, state, content_type, metadata, created_at,
		        content_encoding, content_disposition, content_language, cache_control, expires,
		        website_redirect_location, checksum_algorithm, checksum_type,
		        lock_mode, lock_retain_until, lock_legal_hold, tagging,
		        committed_etag, committed_version_id, space, state_changed_at, plaintext
		 FROM ingot.multipart_sessions WHERE bucket = $1
		 ORDER BY object_key ASC, created_at ASC, upload_id ASC`,
		bucket)
	if err != nil {
		return nil, fmt.Errorf("registry: list sessions: %w", err)
	}
	defer rows.Close()

	var out []MultipartSession
	for rows.Next() {
		s, err := scanSession(rows)
		if err != nil {
			return nil, fmt.Errorf("registry: list sessions scan: %w", err)
		}
		out = append(out, *s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: list sessions rows: %w", err)
	}
	return out, nil
}

func (r *Postgres) CountPartRefs(ctx context.Context, digest multihash.Multihash, excludeUploadID string) (int, error) {
	var n int
	err := r.pool.QueryRow(ctx,
		`SELECT count(*) FROM ingot.multipart_parts
		 WHERE blob_digests @> ARRAY[$1::bytea] AND upload_id <> $2`,
		digest, excludeUploadID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("registry: count part refs: %w", err)
	}
	return n, nil
}

// GCStore ====================================================================

func (r *Postgres) AddGCCandidate(ctx context.Context, cidBytes []byte, bucket string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO ingot.gc_candidates (cid, bucket) VALUES ($1, $2)
		 ON CONFLICT (cid) DO NOTHING`,
		cidBytes, nullString(bucket))
	if err != nil {
		return fmt.Errorf("registry: add gc candidate: %w", err)
	}
	return nil
}

// RevocationCursorStore ======================================================

func (r *Postgres) GetRevocationCursor(ctx context.Context) (*RevocationCursor, error) {
	var cur RevocationCursor
	var revoke []byte
	err := r.pool.QueryRow(ctx,
		`SELECT recorded_at, revoke FROM ingot.revocation_cursor WHERE id`).
		Scan(&cur.RecordedAt, &revoke)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("registry: get revocation cursor: %w", err)
	}
	if cur.Revoke, err = cid.Cast(revoke); err != nil {
		return nil, fmt.Errorf("registry: get revocation cursor: decode revoke cid: %w", err)
	}
	return &cur, nil
}

func (r *Postgres) PutRevocationCursor(ctx context.Context, cur RevocationCursor) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO ingot.revocation_cursor (id, recorded_at, revoke)
		 VALUES (true, $1, $2)
		 ON CONFLICT (id) DO UPDATE
		   SET recorded_at = EXCLUDED.recorded_at,
		       revoke      = EXCLUDED.revoke,
		       updated_at  = now()`,
		cur.RecordedAt, cur.Revoke.Bytes())
	if err != nil {
		return fmt.Errorf("registry: put revocation cursor: %w", err)
	}
	return nil
}

// helpers ====================================================================

// nullString maps "" to a SQL NULL so optional text columns stay NULL rather
// than storing an empty string.
func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func marshalMetadata(m map[string]string) ([]byte, error) {
	if len(m) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("registry: marshal metadata: %w", err)
	}
	return b, nil
}

func unmarshalMetadata(b []byte) (map[string]string, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("registry: unmarshal metadata: %w", err)
	}
	return m, nil
}
