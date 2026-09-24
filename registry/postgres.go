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

// pgxQuerier is the surface shared by *pgxpool.Pool and pgx.Tx, so a statement
// can run standalone or inside a caller's transaction. Everything a statement
// needs has to be here: reaching back to the pool from inside a transaction
// would hold one connection while waiting for another, which deadlocks on a
// single-connection pool.
type pgxQuerier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

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
}

// NewPostgres wraps an existing pool. Callers are responsible for
// running pkg/ingot/migrations.Up against the same pool before any
// registry method is called.
func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
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
	var rootBytes, forgeBytes, lockCfg, tagging []byte
	var createdAt time.Time
	var spaceStr, tenantStr, versioning string
	err := r.pool.QueryRow(ctx,
		`SELECT root_cid, forge_root_cid, created_at, space, tenant, versioning, object_lock_config, bucket_tagging FROM ingot.buckets WHERE name = $1`, name).
		Scan(&rootBytes, &forgeBytes, &createdAt, &spaceStr, &tenantStr, &versioning, &lockCfg, &tagging)
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
	st := &State{Name: name, Space: space, Tenant: tenant, Versioning: VersioningState(versioning), ObjectLockConfig: lockCfg, BucketTagging: tagging, CreatedAt: createdAt}
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

func (r *Postgres) CASRoot(ctx context.Context, name string, expect, next cid.Cid) error {
	return r.casRoot(ctx, r.pool, name, expect, next)
}

// CASRootEnqueue advances the root and records the commit's upload
// registrations in one transaction, so the outbox row exists exactly when the
// version it describes committed. A conflict or a missing bucket rolls the
// rows back with the CAS.
func (r *Postgres) CASRootEnqueue(ctx context.Context, name string, expect, next cid.Cid, regs []UploadRegistration) error {
	if len(regs) == 0 {
		return r.CASRoot(ctx, name, expect, next)
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("registry: begin cas %q: %w", name, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := r.casRoot(ctx, tx, name, expect, next); err != nil {
		return err
	}
	for _, reg := range regs {
		// seq comes from the sequence, so the rows of one bucket are numbered
		// in the order its lock let them commit.
		if _, err := tx.Exec(ctx, `
			INSERT INTO ingot.upload_registrations (bucket, object_key, space, root, op, proofs)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, reg.Bucket, reg.ObjectKey, reg.Space.String(), reg.Root.Bytes(), string(reg.Op), reg.Proofs); err != nil {
			return fmt.Errorf("registry: enqueue upload registration for %q: %w", name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("registry: commit cas %q: %w", name, err)
	}
	return nil
}

func (r *Postgres) casRoot(ctx context.Context, q pgxQuerier, name string, expect, next cid.Cid) error {
	var (
		expectBytes []byte
		nextBytes   []byte
	)
	if expect.Defined() {
		expectBytes = expect.Bytes()
	}
	if next.Defined() {
		nextBytes = next.Bytes()
	}

	var (
		tag pgconn.CommandTag
		err error
	)
	if expectBytes == nil {
		tag, err = q.Exec(ctx,
			`UPDATE ingot.buckets SET root_cid = $1 WHERE name = $2 AND root_cid IS NULL`,
			nextBytes, name)
	} else {
		tag, err = q.Exec(ctx,
			`UPDATE ingot.buckets SET root_cid = $1 WHERE name = $2 AND root_cid = $3`,
			nextBytes, name, expectBytes)
	}
	if err != nil {
		return fmt.Errorf("registry: cas %q: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		// Either the bucket doesn't exist or the expected root didn't match.
		// Asked through the same querier: from inside CASRootEnqueue's
		// transaction this would otherwise want a second pooled connection
		// while holding one, which deadlocks a single-connection pool.
		var exists bool
		if gerr := q.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM ingot.buckets WHERE name = $1)`, name,
		).Scan(&exists); gerr != nil {
			return fmt.Errorf("registry: cas %q: resolving conflict: %w", name, gerr)
		}
		if !exists {
			return ErrNotFound
		}
		return ErrConflict
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

func (r *Postgres) SetVersioning(ctx context.Context, name string, v VersioningState) error {
	if v != VersioningEnabled && v != VersioningSuspended {
		return fmt.Errorf("registry: set versioning %q: invalid state %q", name, v)
	}
	tag, err := r.pool.Exec(ctx,
		`UPDATE ingot.buckets SET versioning = $1 WHERE name = $2`,
		string(v), name)
	if err != nil {
		return fmt.Errorf("registry: set versioning %q: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Postgres) SetObjectLockConfig(ctx context.Context, name string, cfg []byte) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE ingot.buckets SET object_lock_config = $1 WHERE name = $2`,
		cfg, name)
	if err != nil {
		return fmt.Errorf("registry: set object lock config %q: %w", name, err)
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
