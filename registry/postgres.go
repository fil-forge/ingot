package registry

import (
	"context"
	"errors"
	"fmt"

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

func (r *Postgres) Create(ctx context.Context, name string, createdAt int64) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO ingot.buckets (name, root_cid, created_at) VALUES ($1, NULL, $2)`,
		name, createdAt)
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
	var createdAt int64
	err := r.pool.QueryRow(ctx,
		`SELECT root_cid, forge_root_cid, created_at FROM ingot.buckets WHERE name = $1`, name).
		Scan(&rootBytes, &forgeBytes, &createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("registry: get %q: %w", name, err)
	}

	st := &State{Name: name, CreatedAt: createdAt}
	if err := setCidPg(&st.Root, rootBytes, name, "root_cid"); err != nil {
		return nil, err
	}
	if err := setCidPg(&st.ForgeRoot, forgeBytes, name, "forge_root_cid"); err != nil {
		return nil, err
	}
	return st, nil
}

func (r *Postgres) List(ctx context.Context) ([]*State, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT name, root_cid, forge_root_cid, created_at FROM ingot.buckets ORDER BY name ASC`)
	if err != nil {
		return nil, fmt.Errorf("registry: list: %w", err)
	}
	defer rows.Close()

	var out []*State
	for rows.Next() {
		var name string
		var rootBytes, forgeBytes []byte
		var createdAt int64
		if err := rows.Scan(&name, &rootBytes, &forgeBytes, &createdAt); err != nil {
			return nil, fmt.Errorf("registry: list scan: %w", err)
		}
		st := &State{Name: name, CreatedAt: createdAt}
		if err := setCidPg(&st.Root, rootBytes, name, "root_cid"); err != nil {
			return nil, err
		}
		if err := setCidPg(&st.ForgeRoot, forgeBytes, name, "forge_root_cid"); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: list rows: %w", err)
	}
	return out, nil
}

func (r *Postgres) Delete(ctx context.Context, name string) error {
	tag, err := r.pool.Exec(ctx, `DELETE FROM ingot.buckets WHERE name = $1`, name)
	if err != nil {
		return fmt.Errorf("registry: delete %q: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
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
