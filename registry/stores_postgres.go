package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Compile-time assertions: *Postgres satisfies every store interface.
var (
	_ BlobRefStore   = (*Postgres)(nil)
	_ IntentStore    = (*Postgres)(nil)
	_ LocationStore  = (*Postgres)(nil)
	_ InclusionStore = (*Postgres)(nil)
	_ MultipartStore = (*Postgres)(nil)
	_ GCStore        = (*Postgres)(nil)
)

// BlobRefStore ===============================================================

func (r *Postgres) AddBlobClaim(ctx context.Context, c BlobClaim) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO ingot.blob_refs (digest, bucket, object_key, version_id, space)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (digest, bucket, object_key, version_id) DO NOTHING`,
		c.Digest, c.Bucket, c.ObjectKey, c.VersionID, c.Space)
	if err != nil {
		return fmt.Errorf("registry: add blob claim: %w", err)
	}
	return nil
}

func (r *Postgres) DeleteBlobClaim(ctx context.Context, digest []byte, bucket, objectKey, versionID string) error {
	_, err := r.pool.Exec(ctx,
		`DELETE FROM ingot.blob_refs
		 WHERE digest = $1 AND bucket = $2 AND object_key = $3 AND version_id = $4`,
		digest, bucket, objectKey, versionID)
	if err != nil {
		return fmt.Errorf("registry: delete blob claim: %w", err)
	}
	return nil
}

func (r *Postgres) CountClaims(ctx context.Context, space did.DID, digest []byte) (int, error) {
	var n int
	err := r.pool.QueryRow(ctx,
		`SELECT count(*) FROM ingot.blob_refs WHERE space = $1 AND digest = $2`,
		space, digest).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("registry: count claims: %w", err)
	}
	return n, nil
}

// IntentStore ================================================================

func (r *Postgres) PutIntent(ctx context.Context, in UploadIntent) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO ingot.upload_intents (digest, local_path, size, state, bucket)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (digest) DO UPDATE
		   SET local_path = EXCLUDED.local_path,
		       size       = EXCLUDED.size,
		       state      = EXCLUDED.state,
		       bucket     = EXCLUDED.bucket,
		       updated_at = now()`,
		in.Digest, in.LocalPath, in.Size, in.State, nullString(in.Bucket))
	if err != nil {
		return fmt.Errorf("registry: put intent: %w", err)
	}
	return nil
}

func (r *Postgres) SetIntentState(ctx context.Context, digest []byte, state string) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE ingot.upload_intents SET state = $2, updated_at = now() WHERE digest = $1`,
		digest, state)
	if err != nil {
		return fmt.Errorf("registry: set intent state: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Postgres) GetIntent(ctx context.Context, digest []byte) (*UploadIntent, error) {
	in := &UploadIntent{Digest: digest}
	var bucket *string
	err := r.pool.QueryRow(ctx,
		`SELECT local_path, size, state, bucket FROM ingot.upload_intents WHERE digest = $1`,
		digest).Scan(&in.LocalPath, &in.Size, &in.State, &bucket)
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
		`SELECT digest, local_path, size, state, bucket FROM ingot.upload_intents WHERE state = $1`,
		state)
	if err != nil {
		return nil, fmt.Errorf("registry: list intents: %w", err)
	}
	defer rows.Close()

	var out []UploadIntent
	for rows.Next() {
		var in UploadIntent
		var bucket *string
		if err := rows.Scan(&in.Digest, &in.LocalPath, &in.Size, &in.State, &bucket); err != nil {
			return nil, fmt.Errorf("registry: list intents scan: %w", err)
		}
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

func (r *Postgres) DeleteIntent(ctx context.Context, digest []byte) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM ingot.upload_intents WHERE digest = $1`, digest)
	if err != nil {
		return fmt.Errorf("registry: delete intent: %w", err)
	}
	return nil
}

// LocationStore ==============================================================

func (r *Postgres) PutLocation(ctx context.Context, loc BlobLocation) error {
	// Reject a partial FEE set before it reaches storage (see ValidateFEE).
	if err := loc.ValidateFEE(); err != nil {
		return err
	}
	// The FEE columns are nullable: an unencrypted blob writes them NULL (nullBytes
	// / nullString / nullInt64 map empty/zero to NULL, so an empty-but-non-nil
	// slice never lands as a non-NULL empty bytea). The upsert overwrites the whole
	// row state, so a rotation re-wrap is a read-modify-write of the full record.
	_, err := r.pool.Exec(ctx,
		`INSERT INTO ingot.blob_locations
		   (space, digest, provider, url, size,
		    region_wrapped_cek, region_key_version, tenant_recipient_kid,
		    base_nonce, chunk_size, protected_header)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		 ON CONFLICT (space, digest) DO UPDATE
		   SET provider = EXCLUDED.provider, url = EXCLUDED.url, size = EXCLUDED.size,
		       region_wrapped_cek   = EXCLUDED.region_wrapped_cek,
		       region_key_version   = EXCLUDED.region_key_version,
		       tenant_recipient_kid = EXCLUDED.tenant_recipient_kid,
		       base_nonce           = EXCLUDED.base_nonce,
		       chunk_size           = EXCLUDED.chunk_size,
		       protected_header     = EXCLUDED.protected_header`,
		loc.Space, loc.Digest, loc.Provider, loc.URL, loc.Size,
		nullBytes(loc.RegionWrappedCEK), nullString(loc.RegionKeyVersion), nullString(loc.TenantRecipientKID),
		nullBytes(loc.BaseNonce), nullInt64(loc.ChunkSize), nullBytes(loc.ProtectedHeader))
	if err != nil {
		return fmt.Errorf("registry: put location: %w", err)
	}
	return nil
}

func (r *Postgres) GetLocation(ctx context.Context, space did.DID, digest []byte) (*BlobLocation, error) {
	loc := &BlobLocation{Space: space, Digest: digest}
	var regionKeyVersion, tenantRecipientKID *string
	var chunkSize *int64
	err := r.pool.QueryRow(ctx,
		`SELECT provider, url, size,
		        region_wrapped_cek, region_key_version, tenant_recipient_kid,
		        base_nonce, chunk_size, protected_header
		 FROM ingot.blob_locations WHERE space = $1 AND digest = $2`,
		space, digest).Scan(&loc.Provider, &loc.URL, &loc.Size,
		&loc.RegionWrappedCEK, &regionKeyVersion, &tenantRecipientKID,
		&loc.BaseNonce, &chunkSize, &loc.ProtectedHeader)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("registry: get location: %w", err)
	}
	if regionKeyVersion != nil {
		loc.RegionKeyVersion = *regionKeyVersion
	}
	if tenantRecipientKID != nil {
		loc.TenantRecipientKID = *tenantRecipientKID
	}
	if chunkSize != nil {
		loc.ChunkSize = *chunkSize
	}
	return loc, nil
}

func (r *Postgres) DeleteLocation(ctx context.Context, space did.DID, digest []byte) error {
	_, err := r.pool.Exec(ctx,
		`DELETE FROM ingot.blob_locations WHERE space = $1 AND digest = $2`, space, digest)
	if err != nil {
		return fmt.Errorf("registry: delete location: %w", err)
	}
	return nil
}

// ParkStore ==================================================================

func (r *Postgres) PutPark(ctx context.Context, p BlobPark) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO ingot.blob_parks (digest, add_task, accept_task, put_invocation, size)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (digest) DO UPDATE
		   SET add_task = EXCLUDED.add_task, accept_task = EXCLUDED.accept_task,
		       put_invocation = EXCLUDED.put_invocation, size = EXCLUDED.size`,
		p.Digest, p.AddTask, p.AcceptTask, p.PutInvocation, p.Size)
	if err != nil {
		return fmt.Errorf("registry: put park: %w", err)
	}
	return nil
}

func (r *Postgres) GetPark(ctx context.Context, digest []byte) (*BlobPark, error) {
	park := &BlobPark{Digest: digest}
	err := r.pool.QueryRow(ctx,
		`SELECT add_task, accept_task, put_invocation, size, created_at
		 FROM ingot.blob_parks WHERE digest = $1`,
		digest).Scan(&park.AddTask, &park.AcceptTask, &park.PutInvocation, &park.Size, &park.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("registry: get park: %w", err)
	}
	return park, nil
}

func (r *Postgres) DeletePark(ctx context.Context, digest []byte) error {
	_, err := r.pool.Exec(ctx,
		`DELETE FROM ingot.blob_parks WHERE digest = $1`, digest)
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

func (r *Postgres) GetInclusion(ctx context.Context, space did.DID, digest []byte) (*BlobInclusion, error) {
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
	state := s.State
	if state == "" {
		state = SessionOpen
	}
	meta, err := marshalMetadata(s.Metadata)
	if err != nil {
		return err
	}
	_, err = r.pool.Exec(ctx,
		`INSERT INTO ingot.multipart_sessions
		   (upload_id, bucket, object_key, state, content_type, metadata,
		    content_encoding, content_disposition, content_language, cache_control, expires,
		    website_redirect_location, checksum_algorithm, checksum_type,
		    lock_mode, lock_retain_until, lock_legal_hold, tagging)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`,
		s.UploadID, s.Bucket, s.ObjectKey, state, nullString(s.ContentType), meta,
		nullString(s.ContentEncoding), nullString(s.ContentDisposition),
		nullString(s.ContentLanguage), nullString(s.CacheControl), nullString(s.Expires),
		nullString(s.WebsiteRedirectLocation), nullString(s.ChecksumAlgorithm), nullString(s.ChecksumType),
		nullString(s.LockMode), s.LockRetainUntil, nullString(s.LockLegalHold), nullString(s.Tagging))
	if err != nil {
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
		        lock_mode, lock_retain_until, lock_legal_hold, tagging
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
	var meta []byte
	err := row.Scan(&s.UploadID, &s.Bucket, &s.ObjectKey, &s.State, &contentType, &meta, &s.CreatedAt,
		&ce, &cd, &cl, &cc, &exp, &wrl, &ckAlgo, &ckType,
		&lockMode, &s.LockRetainUntil, &lockHold, &tagging)
	if err != nil {
		return nil, err
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
	if s.Metadata, err = unmarshalMetadata(meta); err != nil {
		return nil, err
	}
	return s, nil
}

func (r *Postgres) LatchSession(ctx context.Context, uploadID, from, to string) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE ingot.multipart_sessions SET state = $3 WHERE upload_id = $1 AND state = $2`,
		uploadID, from, to)
	if err != nil {
		return false, fmt.Errorf("registry: latch session: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *Postgres) DeleteSession(ctx context.Context, uploadID string) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM ingot.multipart_sessions WHERE upload_id = $1`, uploadID)
	if err != nil {
		return fmt.Errorf("registry: delete session: %w", err)
	}
	return nil
}

func (r *Postgres) PutPart(ctx context.Context, p MultipartPart) error {
	state := p.State
	if state == "" {
		state = PartParked
	}
	_, err := r.pool.Exec(ctx,
		`INSERT INTO ingot.multipart_parts (upload_id, part_number, etag_md5, size, checksum, blob_digests, state)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (upload_id, part_number) DO UPDATE
		   SET etag_md5 = EXCLUDED.etag_md5, size = EXCLUDED.size, checksum = EXCLUDED.checksum,
		       blob_digests = EXCLUDED.blob_digests, state = EXCLUDED.state`,
		p.UploadID, p.PartNumber, p.ETagMD5, p.Size, p.Checksum, p.BlobDigests, state)
	if err != nil {
		return fmt.Errorf("registry: put part: %w", err)
	}
	return nil
}

func (r *Postgres) ListParts(ctx context.Context, uploadID string) ([]MultipartPart, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT part_number, etag_md5, size, checksum, blob_digests, state, created_at
		 FROM ingot.multipart_parts WHERE upload_id = $1 ORDER BY part_number ASC`,
		uploadID)
	if err != nil {
		return nil, fmt.Errorf("registry: list parts: %w", err)
	}
	defer rows.Close()

	var out []MultipartPart
	for rows.Next() {
		p := MultipartPart{UploadID: uploadID}
		if err := rows.Scan(&p.PartNumber, &p.ETagMD5, &p.Size, &p.Checksum, &p.BlobDigests, &p.State, &p.CreatedAt); err != nil {
			return nil, fmt.Errorf("registry: list parts scan: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: list parts rows: %w", err)
	}
	return out, nil
}

func (r *Postgres) ListSessions(ctx context.Context, bucket string) ([]MultipartSession, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT upload_id, bucket, object_key, state, content_type, metadata, created_at,
		        content_encoding, content_disposition, content_language, cache_control, expires,
		        website_redirect_location, checksum_algorithm, checksum_type,
		        lock_mode, lock_retain_until, lock_legal_hold, tagging
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

func (r *Postgres) ListStaleSessions(ctx context.Context, state string, cutoff time.Time) ([]MultipartSession, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT upload_id, bucket, object_key, state, content_type, metadata, created_at,
		        content_encoding, content_disposition, content_language, cache_control, expires,
		        website_redirect_location, checksum_algorithm, checksum_type,
		        lock_mode, lock_retain_until, lock_legal_hold, tagging
		 FROM ingot.multipart_sessions WHERE state = $1 AND created_at < $2
		 ORDER BY created_at ASC`,
		state, cutoff)
	if err != nil {
		return nil, fmt.Errorf("registry: list stale sessions: %w", err)
	}
	defer rows.Close()

	var out []MultipartSession
	for rows.Next() {
		s, err := scanSession(rows)
		if err != nil {
			return nil, fmt.Errorf("registry: list stale sessions scan: %w", err)
		}
		out = append(out, *s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: list stale sessions rows: %w", err)
	}
	return out, nil
}

func (r *Postgres) CountPartRefs(ctx context.Context, digest []byte, excludeUploadID string) (int, error) {
	var n int
	err := r.pool.QueryRow(ctx,
		`SELECT count(*) FROM ingot.multipart_parts
		 WHERE $1 = ANY(blob_digests) AND upload_id <> $2`,
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

// helpers ====================================================================

// nullString maps "" to a SQL NULL so optional text columns stay NULL rather
// than storing an empty string.
func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// nullInt64 maps 0 to a SQL NULL so an optional bigint column stays NULL rather
// than storing 0. Used for blob_locations.chunk_size, which is 0 (absent) for an
// unencrypted blob and always positive for a FEE envelope.
func nullInt64(n int64) *int64 {
	if n == 0 {
		return nil
	}
	return &n
}

// nullBytes maps an empty (nil or zero-length) slice to a SQL NULL so an
// optional bytea column stays NULL rather than storing a non-NULL empty value.
// Used for the FEE wrap columns, whose absence must read back as nil.
func nullBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
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
