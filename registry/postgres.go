package registry

import (
	"context"
	"errors"
	"fmt"
	"time"

	hiltclient "github.com/fil-forge/hilt/pkg/client"
	bucketrpc "github.com/fil-forge/hilt/pkg/rpc/service/bucket"
	"github.com/fil-forge/ingot/internal/fasthttputil"
	"github.com/fil-forge/ucantone/did"
	ucanerr "github.com/fil-forge/ucantone/errors"
	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// uniqueViolation is the Postgres SQLSTATE for a unique constraint
// violation (matches the literal used elsewhere in sprue's stores).
const uniqueViolation = "23505"

// Postgres is a *pgxpool.Pool-backed Registry. Schema is owned by
// pkg/ingot/migrations and lives in the `ingot` Postgres schema. The
// pool is borrowed, never closed by this type.
//
// Bucket operations (Create, Delete, List) are forwarded to the Hilt
// tenant service — the authority on which buckets exist and who may act
// on them — with the local table holding only per-bucket root state.
// Each of those methods recovers the original signed S3 request from
// ctx (see hiltclient.RequestFromContext), so they must be called on a
// request-serving path.
type Postgres struct {
	pool       *pgxpool.Pool
	authorizer *hiltclient.Client
}

// NewPostgres wraps an existing pool. Callers are responsible for
// running pkg/ingot/migrations.Up against the same pool before any
// registry method is called. The authorizer (the Hilt client) is
// required: bucket create/delete/list are Hilt operations.
func NewPostgres(pool *pgxpool.Pool, authorizer *hiltclient.Client) *Postgres {
	return &Postgres{pool: pool, authorizer: authorizer}
}

// Compile-time assertion.
var _ Registry = (*Postgres)(nil)

func (r *Postgres) Create(ctx context.Context, name string) error {
	req, ok := fasthttputil.RequestFromContext(ctx)
	if !ok {
		return fmt.Errorf("registry: create %q: no request in context", name)
	}
	createOK, _, err := r.authorizer.CreateBucket(ctx, req)
	if err != nil {
		var namedErr ucanerr.Named
		if errors.As(err, &namedErr) && namedErr.Name() == bucketrpc.BucketExistsErrorName {
			return ErrExists
		}
		return err
	}
	space := createOK.Bucket

	// root_cid stays NULL (empty bucket); created_at from the column default.
	_, err = r.pool.Exec(ctx,
		`INSERT INTO ingot.buckets (name, space) VALUES ($1, $2)`,
		name, space.String())
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
	var rootBytes, forgeBytes []byte
	var createdAt time.Time
	var spaceStr string
	err := r.pool.QueryRow(ctx,
		`SELECT root_cid, forge_root_cid, created_at, space FROM ingot.buckets WHERE name = $1`, name).
		Scan(&rootBytes, &forgeBytes, &createdAt, &spaceStr)
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
	st := &State{Name: name, Space: space, CreatedAt: createdAt}
	if err := setCidPg(&st.Root, rootBytes, name, "root_cid"); err != nil {
		return nil, err
	}
	if err := setCidPg(&st.ForgeRoot, forgeBytes, name, "forge_root_cid"); err != nil {
		return nil, err
	}
	return st, nil
}

// List forwards the signed S3 request to Hilt — the authority on which
// buckets exist for the requesting tenant, including pagination — then
// queries the local table for just the names in the returned page. The
// ListOptions are unused here: Hilt derives the same parameters from the
// request itself.
func (r *Postgres) List(ctx context.Context, _ ListOptions) (*Page, error) {
	req, ok := fasthttputil.RequestFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("registry: list: no request in context")
	}
	listOK, err := r.authorizer.ListBuckets(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("registry: list: %w", err)
	}

	names := make([]string, 0, len(listOK.Buckets))
	for _, b := range listOK.Buckets {
		names = append(names, b.Name)
	}
	local, err := r.statesByName(ctx, names)
	if err != nil {
		return nil, err
	}

	// Merge in Hilt's order. A bucket Hilt knows but the local table
	// doesn't (state not materialized here yet) still appears, with
	// undef roots and Hilt's creation date.
	page := &Page{ContinuationToken: listOK.ContinuationToken}
	for _, b := range listOK.Buckets {
		if st, ok := local[b.Name]; ok {
			page.Buckets = append(page.Buckets, st)
			continue
		}
		createdAt, err := time.Parse(time.RFC3339, b.CreationDate)
		if err != nil {
			createdAt = time.Time{}
		}
		page.Buckets = append(page.Buckets, &State{Name: b.Name, CreatedAt: createdAt})
	}
	return page, nil
}

// statesByName loads local bucket state for exactly the given names,
// keyed by name. Names with no local row are absent from the map.
func (r *Postgres) statesByName(ctx context.Context, names []string) (map[string]*State, error) {
	if len(names) == 0 {
		return map[string]*State{}, nil
	}
	rows, err := r.pool.Query(ctx,
		`SELECT name, root_cid, forge_root_cid, created_at, space FROM ingot.buckets WHERE name = ANY($1)`,
		names)
	if err != nil {
		return nil, fmt.Errorf("registry: list states: %w", err)
	}
	defer rows.Close()

	out := make(map[string]*State, len(names))
	for rows.Next() {
		var name string
		var rootBytes, forgeBytes []byte
		var createdAt time.Time
		var spaceStr string
		if err := rows.Scan(&name, &rootBytes, &forgeBytes, &createdAt, &spaceStr); err != nil {
			return nil, fmt.Errorf("registry: list scan: %w", err)
		}
		space, err := did.Parse(spaceStr)
		if err != nil {
			return nil, fmt.Errorf("registry: parse space %q: %w", spaceStr, err)
		}
		st := &State{Name: name, Space: space, CreatedAt: createdAt}
		if err := setCidPg(&st.Root, rootBytes, name, "root_cid"); err != nil {
			return nil, err
		}
		if err := setCidPg(&st.ForgeRoot, forgeBytes, name, "forge_root_cid"); err != nil {
			return nil, err
		}
		out[name] = st
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: list rows: %w", err)
	}
	return out, nil
}

func (r *Postgres) Delete(ctx context.Context, name string) error {
	req, ok := fasthttputil.RequestFromContext(ctx)
	if !ok {
		return fmt.Errorf("registry: delete %q: no request in context", name)
	}
	if err := r.authorizer.DeleteBucket(ctx, req); err != nil {
		var namedErr ucanerr.Named
		if errors.As(err, &namedErr) {
			switch namedErr.Name() {
			case bucketrpc.UnknownBucketErrorName:
				return ErrNotFound
			case bucketrpc.BucketNotEmptyErrorName:
				return ErrNotEmpty
			}
		}
		return err
	}
	// Hilt has removed the bucket; drop the local root state. A missing
	// local row is fine — the bucket may never have materialized here.
	if _, err := r.pool.Exec(ctx, `DELETE FROM ingot.buckets WHERE name = $1`, name); err != nil {
		return fmt.Errorf("registry: delete %q: %w", name, err)
	}
	return nil
}

func (r *Postgres) CASRoot(ctx context.Context, name string, expect, next cid.Cid) error {
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
		tag, err = r.pool.Exec(ctx,
			`UPDATE ingot.buckets SET root_cid = $1 WHERE name = $2 AND root_cid IS NULL`,
			nextBytes, name)
	} else {
		tag, err = r.pool.Exec(ctx,
			`UPDATE ingot.buckets SET root_cid = $1 WHERE name = $2 AND root_cid = $3`,
			nextBytes, name, expectBytes)
	}
	if err != nil {
		return fmt.Errorf("registry: cas %q: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		// Either the bucket doesn't exist or the expected root didn't match.
		if _, gerr := r.Get(ctx, name); errors.Is(gerr, ErrNotFound) {
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
