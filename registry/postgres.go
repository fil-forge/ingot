package registry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// uniqueViolation is the Postgres SQLSTATE for a unique constraint
// violation (matches the literal used elsewhere in sprue's stores).
const uniqueViolation = "23505"

// Postgres is a *pgxpool.Pool-backed Registry (and, via its sibling
// files, the segment Meta and the relational stores). Schema is owned
// by the migrations package and lives in the `ingot` Postgres schema.
// The pool is borrowed, never closed by this type.
//
// Bucket authority (which buckets exist, who may act on them) lives
// with the Hilt tenant service: s3frontend consults it through the
// bucketauthority package before touching these rows, which hold the
// per-bucket root/versioning state plus the owning tenant (read only by
// the copy paths, whose source bucket hilt never sees). Create and Delete
// here are plain SQL and read nothing from ctx.
type Postgres struct {
	pool *pgxpool.Pool
	// tasks queues a commit's upload registrations in its transaction.
	tasks TaskSubmitter
}

// PostgresOption configures a Postgres registry.
type PostgresOption func(*Postgres)

// WithTasks queues each commit's upload registrations through ts, in the
// commit's transaction. A registry without it refuses a commit that carries
// any.
func WithTasks(ts TaskSubmitter) PostgresOption {
	return func(r *Postgres) { r.tasks = ts }
}

// NewPostgres wraps an existing pool. Callers are responsible for
// running pkg/ingot/migrations.Up against the same pool before any
// registry method is called.
func NewPostgres(pool *pgxpool.Pool, opts ...PostgresOption) *Postgres {
	r := &Postgres{pool: pool}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Compile-time assertion.
var _ Registry = (*Postgres)(nil)

func (r *Postgres) Create(ctx context.Context, name string, space did.DID, init CreateState) error {
	// An undefined tenant would store "" — accepted by NOT NULL, rejected by
	// did.Parse on every later Get — so refuse it here rather than write an
	// unreadable row.
	if !init.Tenant.Defined() {
		return fmt.Errorf("registry: create %q: tenant required", name)
	}
	// root_cid stays NULL (empty bucket); created_at from the column default.
	v := init.Versioning
	if v == "" {
		v = VersioningUnversioned
	}
	_, err := r.pool.Exec(ctx,
		`INSERT INTO ingot.buckets (name, space, tenant, versioning, object_lock_config) VALUES ($1, $2, $3, $4, $5)`,
		name, space.String(), init.Tenant.String(), string(v), init.ObjectLockConfig)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return ErrExists
		}
		return fmt.Errorf("registry: create %q: %w", name, err)
	}
	return nil
}

func (r *Postgres) Get(ctx context.Context, name string) (*State, error) {
	var rootBytes, forgeBytes, lockCfg, tagging, corsCfg []byte
	var createdAt time.Time
	var spaceStr, tenantStr, versioning string
	var encryption *string
	err := r.pool.QueryRow(ctx,
		`SELECT root_cid, forge_root_cid, created_at, space, tenant, versioning, object_lock_config, bucket_tagging, cors_config, encryption FROM ingot.buckets WHERE name = $1`, name).
		Scan(&rootBytes, &forgeBytes, &createdAt, &spaceStr, &tenantStr, &versioning, &lockCfg, &tagging, &corsCfg, &encryption)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("registry: get %q: %w", name, err)
	}
	space, err := did.Parse(spaceStr)
	if err != nil {
		return nil, fmt.Errorf("registry: parse space %q: %w", spaceStr, err)
	}
	tenant, err := did.Parse(tenantStr)
	if err != nil {
		return nil, fmt.Errorf("registry: parse tenant %q: %w", tenantStr, err)
	}
	st := &State{Name: name, Space: space, Tenant: tenant, Versioning: VersioningState(versioning), ObjectLockConfig: lockCfg, BucketTagging: tagging, CORSConfig: corsCfg, CreatedAt: createdAt}
	if encryption != nil {
		st.Encryption = BucketEncryption(*encryption)
	}
	if err := setCidPg(&st.Root, rootBytes, name, "root_cid"); err != nil {
		return nil, err
	}
	if err := setCidPg(&st.ForgeRoot, forgeBytes, name, "forge_root_cid"); err != nil {
		return nil, err
	}
	return st, nil
}

func (r *Postgres) Delete(ctx context.Context, name string) error {
	if _, err := r.pool.Exec(ctx, `DELETE FROM ingot.buckets WHERE name = $1`, name); err != nil {
		return fmt.Errorf("registry: delete %q: %w", name, err)
	}
	return nil
}

// CatalogJournalStats reports the journal's row count, bytes and oldest row.
func (r *Postgres) CatalogJournalStats(ctx context.Context) (CatalogJournalStats, error) {
	var st CatalogJournalStats
	var oldest *time.Time
	if err := r.pool.QueryRow(ctx,
		`SELECT count(*), COALESCE(sum(octet_length(data)), 0), min(created_at) FROM ingot.catalog_journal`,
	).Scan(&st.Blocks, &st.Bytes, &oldest); err != nil {
		return CatalogJournalStats{}, fmt.Errorf("registry: catalog journal stats: %w", err)
	}
	if oldest != nil {
		st.Oldest = *oldest
	}
	return st, nil
}

// EmptyRoot swaps the bucket root from expect to the empty tree, and in the
// same transaction clears forge_root_cid and drops the bucket's journaled
// catalog blocks, which nothing the empty tree reaches needs (Registry).
func (r *Postgres) EmptyRoot(ctx context.Context, name string, expect cid.Cid) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("registry: begin empty root %q: %w", name, err)
	}
	defer tx.Rollback(ctx)
	// Lock the bucket row as a deletion does before deleting its journal
	// rows: a ship of one of its segments takes KEY SHARE on the row before
	// deleting the same rows (MarkSegmentShipped).
	if _, err := tx.Exec(ctx, `SELECT 1 FROM ingot.buckets WHERE name = $1 FOR UPDATE`, name); err != nil {
		return fmt.Errorf("registry: empty root %q: lock bucket: %w", name, err)
	}
	var expectBytes []byte
	if expect.Defined() {
		expectBytes = expect.Bytes()
	}
	tag, err := tx.Exec(ctx,
		`UPDATE ingot.buckets SET root_cid = NULL, forge_root_cid = NULL
		  WHERE name = $1 AND root_cid IS NOT DISTINCT FROM $2`, name, expectBytes)
	if err != nil {
		return fmt.Errorf("registry: empty root %q: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		var exists bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM ingot.buckets WHERE name = $1)`, name).Scan(&exists); err != nil {
			return fmt.Errorf("registry: empty root %q: %w", name, err)
		}
		if !exists {
			return ErrNotFound
		}
		return ErrConflict
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM ingot.catalog_journal WHERE bucket = $1`, name); err != nil {
		return fmt.Errorf("registry: empty root %q: drop journal: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("registry: commit empty root %q: %w", name, err)
	}
	return nil
}

func (r *Postgres) SetForgeRoot(ctx context.Context, name string, root cid.Cid) error {
	var rootBytes []byte
	if root.Defined() {
		rootBytes = root.Bytes()
	}
	tag, err := r.pool.Exec(ctx,
		`UPDATE ingot.buckets SET forge_root_cid = $1 WHERE name = $2`,
		rootBytes, name)
	if err != nil {
		return fmt.Errorf("registry: set forge root %q: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Postgres) SetBucketTagging(ctx context.Context, name string, tags []byte) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE ingot.buckets SET bucket_tagging = $1 WHERE name = $2`,
		tags, name)
	if err != nil {
		return fmt.Errorf("registry: set bucket tagging %q: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Postgres) SetCORSConfig(ctx context.Context, name string, cfg []byte) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE ingot.buckets SET cors_config = $1 WHERE name = $2`,
		cfg, name)
	if err != nil {
		return fmt.Errorf("registry: set cors config %q: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Postgres) SetEncryption(ctx context.Context, name string, enc BucketEncryption) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE ingot.buckets SET encryption = $1 WHERE name = $2`,
		nullString(string(enc)), name)
	if err != nil {
		return fmt.Errorf("registry: set encryption %q: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Postgres) AllocVersionSeq(ctx context.Context, name string) (uint64, error) {
	var seq int64
	err := r.pool.QueryRow(ctx,
		`UPDATE ingot.buckets SET next_version_seq = next_version_seq + 1 WHERE name = $1 RETURNING next_version_seq`,
		name).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("registry: alloc version seq %q: %w", name, err)
	}
	return uint64(seq), nil
}

func setCidPg(dst *cid.Cid, raw []byte, name, field string) error {
	if len(raw) == 0 {
		*dst = cid.Undef
		return nil
	}
	c, err := cid.Cast(raw)
	if err != nil {
		return fmt.Errorf("registry: bad %s for %q: %w", field, name, err)
	}
	*dst = c
	return nil
}
