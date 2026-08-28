# CLAUDE.md

Guidance for Claude Code when working in this repository. For *how the system
works*, read [`DESIGN_NOTES.md`](./DESIGN_NOTES.md); this file is about building,
testing, and the conventions/gotchas of the codebase.

## What this is

`github.com/fil-forge/ingot` is an **S3 gateway over the Forge network**. It runs
two ways: an embeddable Go **library** a host imports in-process, and a
standalone **daemon** (`ingot serve`, in `cmd/`). It presents each S3 bucket as
a per-bucket MST, uploads object bodies to Forge as content-addressed blobs
before a write is acked, journals the catalog (MST nodes + manifests) to a
local **per-bucket** log (`logstore`), and ships sealed catalog segments to
Forge (sprue → piri) as a guppy-style edge client, with **hilt** authorizing
every non-root request and owning tenancy. See DESIGN_NOTES for the current
architecture, `docs/architecture.md` for the target, and `docs/diagrams.md`
for the as-built diagrams.

## Build, test, run

**Critical: use `GOWORK=off`** (or the Makefile, which sets it). The workspace
`go.work` at `…/fil-forge/go.work` exists for cross-repo work and drags sibling
modules in; ingot is a standalone module and must build on its own `go.mod`:

```bash
make build      # GOWORK=off go build ./...
make test       # unit tests: GOWORK=off go test ./... (fast, no Docker)
make itest      # integration tests: boots the Forge stack in Docker (~6 min)
make gen        # regenerate bucket/cbor_gen.go after changing bucket types
GOWORK=off go vet ./...
GOWORK=off go test -tags itest ./itest -run 'TestForgeVersity/PutObject' -v  # one S3 category
GOWORK=off go build -o /tmp/ingot ./cmd/ingot               # the daemon binary
```

**go directive: 1.26.4.**

**The test pattern — unit first, integration when you're ready to wait.**
`make test` runs library/unit tests in seconds with no Docker. `make itest`
runs `itest/` (build tag `itest`): it boots the full smelt Forge stack in
Docker, mounts THIS working tree's binary over the published ingot image, and
validates the real network path — including the curated S3 conformance
partition (`itest/versity_*_test.go`); see `itest/README.md`. CI mirrors the
same ordering: the `itest` job only runs after the unit job passes
(`.github/workflows/go-test.yml`).

## Dependency stack

ingot depends only on these — it must **never** import `fil-forge/sprue` or
`fil-forge/guppy` (guppy embeds ingot; importing it would cycle):

- **`ucantone`** — UCAN 1.0 primitives: `principal` (ed25519), `did`,
  `ucan/{delegation,invocation,receipt,command}`, `binding`, `client`, `execution`.
- **`libforge`** — Forge capability bindings + helpers:
  `commands/{blob,content,http,assert,ucan,index,provider,access}`, `blobindex`
  (sharded-dag-index), `ucan` (ProofStore), `ucan/retrieval`, `didmailto`, `receipt`.
- **`fil-forge/hilt/pkg/{client,rpc/service,s3perm,sigv4}`** — the auth/tenant
  service client: `/s3/request/authorize`, `/s3/bucket/*`, SigV4 verification
  helpers, permission→command mapping.
- **`indexing-service/pkg/{client,types}`** — indexer query client (the
  indexer-backed locator compiles but is not wired; reads use the local tables).
- **`swarf/pkg/{client,api}`** — the UCAN revocation service client: the SSE
  revocation firehose the `revocation/` consumer subscribes to.
- **`fil-forge/versitygw`** — our fork of versity/versitygw, the S3 REST front
  end (we implement `backend.Backend`). The fork adds externally derived SigV4
  signing keys (`auth.Account.SigningKey`, `middlewares.RequestIAMService`) for
  the Hilt flow.
- Plumbing: `go-cid`, `go-block-format`, `whyrusleeping/cbor-gen` (**not**
  go-ipld-prime), `multiformats/*`, `pgx/v5`, `goose/v3`, `spf13/{cobra,viper}`,
  `uber-go/fx`, `zap`.

UCAN uses the **binding pattern**: a capability is `binding.Bind[*Args,*OK](command)`
(e.g. `blob.Allocate`) with `.Invoke`/`.Delegate`; proof chains come from a
`libforge/ucan.ProofStore`. Materially different from go-ucanto — the UCAN code is
a real port, not an import rewrite.

## Package map

Public surface (what hosts import):

- **`ingot` (root)** — `Module(cfg) fx.Option`, `ServerModule`,
  `ServiceIdentity`, `PreStartHook`; the non-fx `New(ctx, ServerConfig,
  ServerDeps)` + `Server.{Start,Stop}`. `module.go` (fx wiring), `server.go`
  (`New`, lifecycle, `newBucketFlushFunc`, the versitygw `s3api` mount).
- **`config/`** — the host `Config`, `Config.ServerConfig()` (the single
  mapping site), the `CatalogPlane` block, validation.

Internal:

- **`cmd/`** — the daemon (cobra/viper/fx): `serve`, `whoami`, `version`;
  `deps.go` (identity PEM + pgx pool).
- **`s3frontend/`** — versitygw `backend.Backend`: `object.go`
  (Put/Get/Head/Delete/List), `version.go` (resolveVersion / commitVersion,
  the per-key version tree), `multipart.go`, `bucket.go`, `listversions.go`,
  `backend.go`.
- **`bucketop/`** — `Coordinator`/`Tx`: per-bucket write transaction (lock,
  snapshot root, staging buffer, CAS commit).
- **`blockstore/`** — block I/O contracts + impls: `log.go` (`Log`, `Plane`
  [catalog-only], `OpRoot`), `staging.go` (`OpStaging`), `spool.go` (`Spool`),
  `layered.go`, `forge.go` (the network read tier), `cache.go` (`Cached` LRU),
  `locator/` (carried from guppy; the indexer-backed locator is never
  injected).
- **`logstore/`** — the per-bucket catalog log: `manager.go` (`Manager`, the
  production `blockstore.Log`), `store.go`, `planelog.go`, `segment.go`,
  `recovery.go`. See `logstore/README.md`.
- **`registry/`** — Postgres metadata: `postgres.go` (`Registry` + `State`),
  `segments.go` (`logstore.Meta`), `stores*.go` (intents, locations,
  inclusions, blob_refs, GC, multipart sessions/parts, parks),
  `locallocator.go` (the read tier's `Locator`). One `*Postgres` satisfies
  all of them.
- **`uploader/`** — `forge.go`/`blob.go`: `Forge` behind `Uploader`
  (`SubmitShard`), `BodyUploader`/`DeferredBodyUploader` (`UploadBlob`,
  `ConcludeBlob`, `AbortBlob`), and `BlobRemover`; captures the per-space
  ship authority (`shipProofs`, 1h TTL) from in-request writes.
- **`bucketauthority/`** — hilt bucket ops: forwards CreateBucket /
  DeleteBucket / ListBuckets to `/s3/bucket/*`, recovering the signed S3
  request from ctx.
- **`iam/`** — the hilt IAM integration over `fil-forge/hilt/pkg/client`:
  versitygw `IAMService`/`RequestIAMService` authorizing each non-root
  request via `/s3/request/authorize` (derived SigV4 key, with a local fast
  path over cached delegations), plus `KeyProofs`/`DelegationCache` — per-
  access-key TTL caches of hilt-issued delegations that the uploader and the
  network read tier consume via `internal/reqscope`.
- **`forgeclient/`** — carried-from-guppy sprue edge client: `/blob/add`
  (with a deferrable conclude), `/ucan/conclude`, `/blob/abort`,
  `/blob/remove`, `/index/add`, receipt polling. The `/access` login and
  `/provider/add` flows are dormant (no CLI drives them).
- **`revocation/`** — the Swarf firehose consumer (optional,
  `revocation_service_url`/`_did`): streams UCAN revocations and clears the
  affected access key's iam caches via `iam.Revoker`; resumes from the
  `registry.RevocationCursorStore` cursor (no cursor → subscribe from now).
- **`tokenstore/`** — carried-from-guppy delegation store (`tokens.cbor`);
  empty today, read only by the dormant login paths.
- **`bucket/`** — the per-object model: `manifest.go` (`ObjectManifest`,
  `Body`), `leaf.go` (`ValueUnion`, `ObjectLeaf`, `VersionNode`),
  `chunker.go` (`SplitBody`, body readers), `cbor_gen.go`.
- **`mst/`** — the forked MST (deps: go-cid, blockstore, ucantone/did — trees
  carry their bucket's space for network-backed reads).
- **`inmem/`** — in-memory fakes (`MemStore`, `NopBaseReader`, `NopUploader`);
  test-only.
- **`cars/`**, **`migrations/`**,
  **`internal/{reqscope,ucanexec,fasthttputil,cors,build}`**, **`gen/`**,
  **`testing/`** — CAR codec, goose SQL (`ingot` schema), request-scoped ctx
  keys, generic `Execute[T]`, fasthttp adapters, CORS, the version stamp, the
  cborgen driver, S3-client test glue.

## Interface seams

| Contract | Production | Test |
|---|---|---|
| `versitygw/backend.Backend` | `s3frontend.Backend` | (same) |
| versitygw `auth.IAMService` + `middlewares.RequestIAMService` | `iam.Service` (the root account is checked before IAM) | root account only |
| `blockstore.Log` | `logstore.Manager` (one `Store` per bucket) | in-memory fake |
| `blockstore.BlockReader` | `blockstore.Forge` (in `Cached`) | `inmem.NopBaseReader` |
| `registry.Registry` + the store seams + `logstore.Meta` | `*registry.Postgres` (all of them) | `inmem.MemStore` |
| `locator.Locator` | `registry.LocalLocator` | (indexer-backed locator exists, unwired) |
| `uploader.{Uploader,BodyUploader,DeferredBodyUploader,BlobRemover}` | `uploader.Forge` | `inmem.NopUploader` |

## fx module

- **`ServerModule`** (composable core) — consumes `ServerConfig`,
  `*zap.Logger`, and the collaborator seams (`blockstore.BlockReader`; the
  four `uploader` seams; `bucketauthority.BucketAuthority`;
  `registry.Registry` plus the intent/location/inclusion/blob-ref/GC/
  multipart/park stores; `logstore.Meta`; an optional `auth.IAMService`) +
  a `PreStartHook` group; runs pre-start → `New` → `Start` on the fx
  lifecycle.
- **`Module(cfg)`** (production wrapper) — supplies Postgres as Registry +
  Meta + every store (one `*registry.Postgres` behind them all), the Forge
  reader (`LocalLocator` inside `Cached`), the sprue edge client + uploader,
  the hilt client + `iam.Service` + `KeyProofs`, `bucketauthority`, the
  (empty) token store, and the goose migration `PreStartHook`. Empty option
  when `cfg.Enabled` is false.

A host provides `*zap.Logger`, `*pgxpool.Pool`, `ingot.ServiceIdentity` (the
agent) and sets `Config.UploadServiceURL`/`UploadServiceDID` (sprue) +
`AuthServiceURL`/`AuthServiceDID` (hilt). The non-fx escape hatch is
`New(ctx, ServerConfig, ServerDeps)`.

## Configuration (`config.Config`)

Viper/yaml-bindable (env prefix `INGOT_`, `.` → `_`). Key fields: `Enabled`,
`Addr` (default `0.0.0.0:9000`), `DataDir`, `Region`, `RootAccess`/`RootSecret`
(the versitygw root account), `MaxBlobSize`, top-level
`SealBytes`/`SealAge`/`Retain` with a `CatalogPlane` `{SealBytes, SealAge,
Ship, Retain}` override block (the only plane), `ReadCacheBytes` (0 → 256 MiB,
<0 → off), `UploadServiceURL`/`UploadServiceDID`/`UploadReceiptsURL` (sprue),
`AuthServiceURL`/`AuthServiceDID`/`AuthServiceProofs` (hilt; proofs = file
path or string-encoded UCAN container, required alongside the URL and
validated at startup down to holding at least one delegation),
`TokenStoreDir` (→
`DataDir`), `MultipartSessionTTL` (0 → 7d, negative → sweeper off),
`CORSAllowedOrigins`, `LogLevel`. `Config.ServerConfig()` is the single
mapping site. The daemon's config (cmd/) adds `postgres_dsn` and
`identity.key_file`.

## Testing

There is no in-memory ingot: the deployment under test is always the real
forge-mode daemon. Two tiers:

- **`make test` — unit** (seconds, no Docker): library/unit tests across the
  packages, plus the thin S3-client glue in `testing/` (`Config`/`NewS3Conf`,
  roundtrip helpers).
- **`make itest` — integration** (`itest/`, build tag `itest`, Docker):
  boots the smelt Forge stack with THIS working tree's binary mounted over
  the published image.
  - **`versity_{bucket,object,multipart,versioning}_test.go`** — the S3
    conformance partition: per upstream versitygw group, a curated pass table (every case
    must pass) and an XFail table (known-failing, reported as SKIP; an
    *unexpected pass* fails the test — the cue to promote the row). One
    shared stack serves all categories (`TestForgeVersity`).
  - **`scenarios_test.go`** — ingot-unique behaviors upstream can't assert
    (blob-split/spool-by-digest, zero-byte objects, part-spans-blobs
    multipart, failed-Complete session recovery), on a small-`max_blob_size`
    config (`testdata/config-smallblob.yaml` via smelt's WithServiceConfig).
  - **`forge_*_test.go`** — forge-native behaviors on dedicated stacks:
    provisioning (`forge_native`), delete/release (`forge_delete`), deferred
    multipart accept (`forge_multipart_deferred`), catalog retention
    (`forge_retention`), and the read-after-eviction network tier
    (`forge_eviction`).
- **Suite-composition-sensitive upstream cases** — a few versitygw cases
  depend on run position rather than S3 semantics: `ListBuckets_truncated`
  names buckets from a process-global counter and asserts *creation-order*
  pagination (ingot lists lexicographically; whether they diverge depends on
  the counter's digit boundary), and the CompleteMultipartUpload racey cases
  depend on host load. Such cases carry a `skip` hook in their table row with
  the reason. Don't move them to the XFail table — that table fails on an
  *unexpected pass*, so a position-dependent case would flip there.
- **When bumping versitygw:** new upstream cases are not picked up
  automatically — diff `group-tests.go` dispatch lists against the itest
  tables and curate the additions (see `itest/README.md`).

## Code generation & migrations

- **cborgen:** `make gen` (`go run ./gen`) regenerates `bucket/cbor_gen.go`.
  `mst/cbor_gen.go` is separate. Verify a no-op diff after touching `bucket` types.
- **migrations:** SQL in `migrations/sql/*.sql` (`go:embed`), applied by
  `migrations.Up` under the `ingot` schema at startup via a `PreStartHook`.
  The schema is dev-only; reshape migrations in place and reset any
  persistent dev DB.

## Docker images & release

Two publishing paths, both to `ghcr.io/fil-forge/ingot`. Mirrors sprue's
pattern so smelt treats every forge service alike; where sprue and piri differ,
we follow **sprue** (goreleaser builds the release container; `publish-ghcr.yml`
stays main-only).

**`Dockerfile`** is multi-stage / multi-target: `build` (base) → `build-prod`
(stripped `-s -w`) / `build-dev` (`-gcflags=all=-N -l` + `dlv`) → runtime
targets `prod` and `dev`. Both are `debian:bookworm-slim` + `curl` (smelt's
healthcheck hits `/health`); `dev` adds a network/debug toolset + the `dlv`
debugger and EXPOSEs `2345`. ENTRYPOINT is the **bare binary** — the compose
`command:` supplies `serve …` (and lets the dev image run under dlv by
overriding `command`). BuildKit cache mounts speed the module + build cache.

- **On merge to `main`** (`.github/workflows/publish-ghcr.yml`): publishes
  `:main` + `:sha-<short>` (prod) and `:main-dev` + `:sha-<short>-dev` (dev),
  multi-arch amd64/arm64. PRs get a single-arch, no-push build-check of both
  targets. `:main-dev` slots into smelt's `compose.debug.yml` / `make
  debug-<svc>` flow.

**Releases** produce immutable `:vX.Y.Z` multi-arch containers **and**
cross-platform binaries + a GitHub release, all via goreleaser. Cut one by
bumping **`version.json`** on `main`:

- `version.json` change → `releaser.yml` (ipdxco unified) tags `vX.Y.Z` +
  creates the release → `release-binaries.yml` runs `goreleaser release`
  (`.goreleaser.yaml` builds `./cmd/ingot`; the container uses
  `Dockerfile.release`, which only *packages* the prebuilt binary — no compile).
  `tagpush.yml` / `release-check.yml` are the tag / PR-bump checks.
- The version is stamped into **`internal/build`** via `-ldflags -X` and
  surfaced by `ingot version` / `--version`; a plain `go build` reports `dev`
  plus the VCS revision from `debug.ReadBuildInfo()`.
- After the first publish, set the GHCR package **Public** (like piri/sprue).

**No `GOWORK=off` in the container/release builds** — deliberate, don't re-add
it. CI checkouts and the Docker build context don't contain the parent `go.work`
(it lives above the repo), so those builds are already hermetic on ingot's own
`go.mod`. Only *local* `go`/`goreleaser` runs need it; to release from a
workspace checkout: `GOWORK=off goreleaser release --clean`.

## Conventions & gotchas

- **`GOWORK=off` for every *local* go command** (build/test/vet/run/tidy). The
  Docker and goreleaser builds intentionally omit it — their contexts have no
  `go.work` (see *Docker images & release*); don't re-add it there.
- **No `fil-forge/sprue` or `fil-forge/guppy` imports** (cycle). The carried
  copies — `forgeclient/`, `tokenstore/`, `blockstore/locator/`,
  `internal/ucanexec/` — are deliberate duplicates of guppy/sprue code; keep them
  in sync (header comments point at upstream). DESIGN_NOTES wants a shared
  `forge-client` lib to remove them.
- **The write path holds `blockstore.Log`; production wires `logstore.Manager`**
  (one per-bucket `Store`). Reads carry no bucket context, so `Manager.Get`
  linear-scans open bucket stores — the known hot spot.
- **The Bash tool buffers / returns out of order under load** — when verifying git
  or test state, prefer a single fresh `go test -count=1` run and read its result
  over trusting interleaved buffered output.
- Match the surrounding comment density and naming; this codebase is heavily
  doc-commented at the type/function level.

## Architecture diagrams

`docs/diagrams.md` is a living artifact; two of its diagrams live in
`logstore/README.md`. A PR that changes behavior a diagram draws updates the
diagram in the same PR. The package-to-diagram table at the bottom of
`docs/diagrams.md` maps changed packages to the diagrams to review; each
diagram's `Sources:` footer lists its ground-truth files. Diagrams draw the
code as built; target-state divergences are labeled notes, and the target
lives in `docs/architecture.md`.

## Companion docs
- [`DESIGN_NOTES.md`](./DESIGN_NOTES.md) — how the system operates today.
- [`docs/architecture.md`](./docs/architecture.md) — the target architecture +
  implementation status (§12).
- [`docs/diagrams.md`](./docs/diagrams.md) — the as-built diagram set (living).
- [`docs/s3-versioning.md`](./docs/s3-versioning.md) — the per-key
  version-tree design.
- [`docs/aggregation-gate.md`](./docs/aggregation-gate.md) — why this S3
  design is gated on Piri's aggregation.
- [`docs/IMPLEMENTATION_PLAN.md`](./docs/IMPLEMENTATION_PLAN.md) — the phase
  tracker for the architecture migration.
- [`logstore/README.md`](./logstore/README.md) — the catalog log internals.
- [`itest/README.md`](./itest/README.md) — the integration harness and the
  conformance ratchet.
- [`README.md`](./README.md) — orientation / deploy modes.
