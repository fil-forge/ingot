# CLAUDE.md

Guidance for Claude Code when working in this repository. For *how the system
works*, read [`DESIGN_NOTES.md`](./DESIGN_NOTES.md); this file is about building,
testing, and the conventions/gotchas of the codebase.

## What this is

`github.com/fil-forge/ingot` is an **S3 gateway over the Forge network**. It runs
two ways: an embeddable Go **library** a host (piri/guppy/sprue) imports
in-process, and a standalone **daemon** (`ingot serve`, in `cmd/`). It presents
each S3 bucket as a per-bucket MST, journals mutations to a local **two-plane**
LSM log (data + catalog), and ships sealed segments to Forge (sprue → piri +
indexing-service) as a guppy-style edge client. See DESIGN_NOTES for the full
architecture.

## Build, test, run

**Critical: use `GOWORK=off`** (or the Makefile, which sets it). The workspace
`go.work` at `…/fil-forge/go.work` declares `go 1.26.1` (above the installed
toolchain) and exists for cross-repo work; ingot is a standalone module:

```bash
make build      # GOWORK=off go build ./...
make test       # unit tests: GOWORK=off go test ./... (fast, no Docker)
make itest      # integration tests: boots the Forge stack in Docker (~6 min)
make gen        # regenerate bucket/cbor_gen.go after changing bucket types
GOWORK=off go vet ./...
GOWORK=off go test -tags itest ./itest -run 'TestForgeVersity/PutObject' -v  # one S3 category
GOWORK=off go build -o /tmp/ingot ./cmd/ingot               # the daemon binary
```

**go directive: 1.26.4** (matches the swarf dep; indexing-service needs ≥ 1.25.7).

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
- **`indexing-service/pkg/{client,types}`** — indexer query client.
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

- **`ingot` (root)** — `Module(cfg) fx.Option`, `ServerModule`, `Config`,
  `ServiceIdentity`, `PreStartHook`; the non-fx `New(ctx, ServerConfig, ServerDeps)`
  + `Server.{Start,Stop}`. `module.go` (fx wiring), `config.go` (host `Config` +
  `ServerConfig()` mapping + per-plane blocks), `server.go` (`New`, lifecycle,
  `newPlaneFlushFunc`, versitygw `s3api`), `util.go` (`LoadOrCreateSigner` — the
  space key).

Internal:

- **`cmd/`** — the daemon (cobra/viper/fx): `serve` (standalone | forge modes),
  `login`, `space generate`/`ls`, `whoami`; `config.go` (`DaemonConfig`), `deps.go`.
- **`s3frontend/`** — versitygw `backend.Backend`: `object.go` (Put/Get/Head/
  Delete/List), `bucket.go`, `backend.go`.
- **`bucketop/`** — `Coordinator`/`Tx`: per-bucket write transaction (lock,
  snapshot Root, staging buffer, CAS-commit).
- **`blockstore/`** — block I/O contracts + impls. `log.go` (`Log`, `Plane`,
  `OpRoot`, `BlockLoc`), `staging.go` (`OpStaging` + codec plane-classification),
  `layered.go`, `forge.go` (network read tier), `cache.go` (`Cached` LRU),
  `locator/` (carried from guppy).
- **`logstore/`** — the two-plane LSM log. `store.go` (`Store` coordinator),
  `planelog.go` (`PlaneLog`, the per-plane pipeline), `segment.go` (single-plane
  CAR + `.idx` + `.ops`), `recovery.go`, `config.go` (`PlaneConfig`), `types.go`
  (`Meta`, `SegmentMeta`). See `logstore/README.md`.
- **`registry/`** — Postgres bucket + segment metadata. `postgres.go` (`Registry`
  + `State`), `segments.go` (`logstore.Meta`). One `*Postgres` satisfies both.
- **`uploader/`** — `forge.go`: `Uploader` / `CARShard` / `Forge.SubmitShard` —
  the per-plane ship via the edge client.
- **`forgeclient/`** — carried-from-guppy edge client: `/blob/add`,
  `/ucan/conclude`, `/index/add`, `/provider/add`, `/access/delegate`, and the
  `/access` login flow.
- **`iam/`** — the Hilt (auth-service) integration over the external
  `github.com/fil-forge/hilt/pkg/client` (RFC: forge-s3-tenant-management):
  versitygw `IAMService`/`RequestIAMService` authorizing each non-root
  request via `/s3/request/authorize` (derived SigV4 key), plus
  `DelegationCache` — a TTL cache (go-cache) of Hilt-issued delegations
  (authorize re-delegations + `/s3/bucket/info` chains) that the network
  read tier consumes as its per-space `/content/retrieve` proof store.
  **Forge mode requires Hilt** (`auth_service_url`/`auth_service_did`): the
  postgres registry forwards bucket create/delete/list to it, recovering
  the signed S3 request from the method's ctx.
- **`revocation/`** — the Swarf firehose consumer (optional,
  `revocation_service_url`/`_did`): streams UCAN revocations and clears the
  affected access key's iam caches via `iam.Revoker`; resumes from the
  `registry.RevocationCursorStore` cursor (no cursor → subscribe from now).
- **`tokenstore/`** — carried-from-guppy delegation store (`tokens.cbor`).
- **`bucket/`** — per-object model: `manifest.go` (`ObjectManifest`, `Body`),
  `chunker.go` (`BodyCodec`/`FixedChunker`), `cbor_gen.go`.
- **`mst/`** — the forked MST (deps: go-cid, blockstore, ucantone/did — trees
  carry their bucket's space for network-backed reads).
- **`inmem/`** — `MemStore` (Registry+Meta), `NopBaseReader`, `NopUploader`; backs
  standalone mode (slated for removal).
- **`cars/`**, **`migrations/`**, **`internal/ucanexec/`**, **`gen/`**,
  **`testing/`** — CAR codec, goose SQL (`ingot` schema), generic `Execute[T]`,
  cborgen driver, S3-client test glue (Config/NewS3Conf + roundtrip helpers).

## Interface seams

| Contract | Production | Test / standalone |
|---|---|---|
| `versitygw/backend.Backend` | `s3frontend.Backend` | (same) |
| `blockstore.Log` | `logstore.Store` | (same) |
| `blockstore.BlockReader` | `blockstore.Forge` (in `Cached`) | `inmem.NopBaseReader` |
| `registry.Registry` + `logstore.Meta` | `*registry.Postgres` (both) | `inmem.MemStore` (both) |
| `uploader.Uploader` | `uploader.Forge` | `inmem.NopUploader` |
| `bucket.BodyCodec` | `*bucket.FixedChunker` | (same) |

## fx module

- **`ServerModule`** (composable core) — consumes `ServerConfig`, `*zap.Logger`,
  and the four collaborator interfaces (`registry.Registry`, `logstore.Meta`,
  `blockstore.BlockReader`, `uploader.Uploader`) + a `PreStartHook` group; runs
  pre-start → `New` → `Start` on the fx lifecycle.
- **`Module(cfg)`** (production wrapper) — supplies Postgres as both Registry+Meta
  (`fx.Out`), the Forge reader, the edge-client uploader, the space signer, the
  token store + a seed hook (self-issued space→agent delegations), and the goose
  migration `PreStartHook`. Empty option when `cfg.Enabled` is false.

A host provides `*zap.Logger`, `*pgxpool.Pool`, `ingot.ServiceIdentity` (the
agent) and sets `Config.UploadServiceURL`/`UploadServiceDID` (sprue) +
`IndexerEndpoint`/`IndexerDID`. The non-fx escape hatch is
`New(ctx, ServerConfig, ServerDeps)`.

## Configuration (`Config`)

Viper/yaml-bindable. Key fields: `Enabled`, `Addr`, `DataDir`, `Region`,
`RootAccess`/`RootSecret` (single-account IAM), `ChunkSize`, top-level
`SealBytes`/`SealAge`/`Retain` (defaults for both planes), per-plane
`DataPlane`/`CatalogPlane` `{SealBytes, SealAge, Ship, Retain}` overrides,
`IndexerEndpoint`/`IndexerDID`, `ReadCacheBytes` (0 → 256 MiB, <0 → off),
`UploadServiceURL`/`UploadServiceDID`/`UploadReceiptsURL`, `TokenStoreDir`
(→ `DataDir`), `HiltURL`/`HiltDID`/`HiltProofs` (tenant-management service;
proofs = file path or string-encoded UCAN container, optional).
`Config.ServerConfig()` is the single mapping site. The daemon's
`DaemonConfig` (cmd/config.go) embeds `Config` + `Mode`/`PostgresDSN`/`Identity`.

## Testing

There is no in-memory ingot: the deployment under test is always the real
forge-mode daemon. Two tiers:

- **`make test` — unit** (seconds, no Docker): `module_test.go` (root),
  `logstore/store_test.go`, `blockstore/{cache,staging}_test.go`,
  `forgeclient/accounts_test.go`, `cmd/space_test.go`, plus library helpers in
  `testing/` (thin S3-client glue: `Config`/`NewS3Conf`, roundtrip helpers).
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
  - **`forge_native_test.go` / `forge_eviction_test.go`** — provisioning and
    the read-after-eviction network tier, each on its own stack.
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
  `migrations.Up` under the `ingot` schema at startup (forge mode, via a
  `PreStartHook`). The schema is dev-only; reshape migrations in place and reset
  any persistent dev DB.

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
- **`Store` preserves `AppendBatch`'s signature**, so the write path
  (`OpStaging`, `bucketop`, `s3frontend`) is unaware of the plane split — it lives
  entirely inside `logstore` (the `Store` coordinator over two `PlaneLog`s).
- **The Bash tool buffers / returns out of order under load** — when verifying git
  or test state, prefer a single fresh `go test -count=1` run and read its result
  over trusting interleaved buffered output.
- Match the surrounding comment density and naming; this codebase is heavily
  doc-commented at the type/function level.

## Companion docs
- [`DESIGN_NOTES.md`](./DESIGN_NOTES.md) — how the whole system operates today +
  known gaps.
- [`logstore/README.md`](./logstore/README.md) — the two-plane log internals.
- [`README.md`](./README.md) — orientation / deploy modes.
