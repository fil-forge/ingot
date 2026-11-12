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
every request and owning tenancy (versitygw's root account is disabled). See DESIGN_NOTES for the current
architecture, `docs/architecture.md` for the target, and `docs/diagrams.md`
for the as-built diagrams.

## Build, test, run

**Critical: use `GOWORK=off`** (or the Makefile, which sets it). The workspace
`go.work` at `…/fil-forge/go.work` exists for cross-repo work and drags sibling
modules in; ingot is a standalone module and must build on its own `go.mod`:

```bash
make build      # the daemon binary, ./ingot (GOWORK=off go build -o ingot ./cmd/ingot)
make test       # every package's tests, with Postgres and OpenBao in Docker
make itest      # integration tests: boots the Forge stack in Docker (~20 min)
make itest-group GROUP=auth   # one CI group: encryption|conformance|uploads|releases|auth|localdisk|rest
make s3compat   # the S3 compatibility corpus, written as an HTML report
make gen        # regenerate the cbor-gen marshalers (go generate ./... runs gen/)
make gen-check  # make gen, then fail if bucket/cbor_gen*.go changed
GOWORK=off go vet ./...
GOWORK=off go test -tags itest ./itest -run 'TestForgeVersity/PutObject' -v  # one S3 category
GOWORK=off go build -o /tmp/ingot ./cmd/ingot               # the daemon binary
```

**go directive: 1.27.0.**

**The test pattern — the suite first, integration when you're ready to wait.**
`make test` (`scripts/test.sh`) runs every package's tests in a minute or two.
They need Postgres, and the region-key provider's need OpenBao: it starts both
in Docker unless `INGOT_TEST_DSN` and `INGOT_TEST_BAO_ADDR` name running ones,
and a test fails, rather than skips, without them. `internal/pgtest` gives
each test a database of its own, copied from a migrated template, so tests
share nothing; `internal/testfakes` stands in for the network. `make itest`
runs `itest/` (build tag `itest`): it boots the full smelt Forge stack in
Docker, mounts THIS working tree's binary over the published ingot image, and
validates the real network path — including the curated S3 conformance
partition (`itest/versity_*_test.go`); see `itest/README.md`. CI mirrors the
same ordering: the `itest-groups` jobs (one runner per `make itest-group`
group) and the `s3compat` job run only after the `test` job passes, and the
`itest` job reports the groups as one check (`.github/workflows/go-test.yml`).

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
  the Hilt flow, and the `Blake3` object attribute (`s3response.Blake3Tree`)
  that GetObjectAttributes returns the object's CID and Bao outboard in.
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
  `PreStartHook`; the non-fx `New(ctx, ServerConfig,
  ServerDeps)` + `Server.{Start,Stop}`. `module.go` (fx wiring), `server.go`
  (`New`, lifecycle, `newBucketFlushFunc`, the versitygw `s3api` mount, the
  local blob sweeper), `auditlog.go` (the versitygw audit-log sink, which
  names each request's span for its S3 action and logs failed requests).
- **`config/`** — the host `Config`, `Config.ServerConfig()` (the single
  mapping site), the `CatalogPlane` block, validation.

Internal:

- **`cmd/`** — the daemon (cobra/viper/fx): `serve`, `whoami`, `version`,
  `blake3` (hash / verify / range: client-side checks of `x-cid` and the
  `Blake3` attribute, over `blake3tree`), `catalog usage` (each bucket's
  catalog bytes on Forge, dead on arrival, and live via
  `logstore.WalkCatalog`, which reads segments retired from disk from Forge
  with an access key from the environment, as JSON, for benchmark runs);
  `deps.go` (agent identity from the PEM key + optional did:web, pgx pool).
- **`cmd/ingot-preload/`** + **`internal/preload`** — a separate binary, a
  plain S3 client that loads a gateway's catalog to estate scale for
  measuring it: zero-byte bulk objects plus a few pack-sized ones with
  deterministic pseudo-random bodies, keys and PUTs shaped as the pack
  server's (create-only, `x-amz-checksum-sha256`), progress in a state file it
  resumes from, reports as JSON lines (`load`); then timed, checked reads,
  listings, writes and deletes against what the state file says was loaded
  (`validate`). `internal/preload` imports nothing else from ingot.
- **`s3frontend/`** — versitygw `backend.Backend`: `object.go`
  (Put/Get/Head/Delete/List), `version.go` (resolveVersion, reading the
  per-key version tree), `bucketqueue.go` (the requests' commits through
  `queue/bucketqueue`), `conditions.go` (its errors' S3 answers),
  `bucketadmin.go` (bucket settings through `queue/bucketqueue`),
  `multipart.go` (the multipart verbs through `queue/multipartqueue`),
  `multipartqueue.go` (its errors' S3 answers), `bucket.go`
  (DeleteBucket: the close, the uploads' aborts, then
  `queue-bucket.delete`), `listversions.go`,
  `backend.go`, `stream.go` (each body blob streams to its
  provider as the body arrives, teeing an object's bytes into the cache; a
  failed stream fails the request and aborts its allocation, and an
  allocation a dead request leaves is expired by the provider).
- **`bucketop/`** — `Coordinator`/`Tx`: per-bucket write transaction
  (snapshot root, staging buffer, `Mark`/`Restore` per change, `Append`:
  one fsynced batch to the log). It takes no lock: the bucket's
  `queue-bucket` subject serializes its commits.
- **`blockstore/`** — block I/O contracts + impls: `log.go` (`Log`, `Plane`
  [catalog-only], `OpRoot`), `staging.go` (`OpStaging`), `blobcache.go`
  (`BlobCache`: copies the provider holds, and writes in progress as
  `TempBlob`s it adopts), `blobdir.go`,
  `layered.go`, `forge.go` (the network read tier), `cache.go` (`Cached` LRU),
  `locator/` (carried from guppy; the indexer-backed locator is never
  injected).
- **`logstore/`** — the per-bucket catalog log: `manager.go` (`Manager`, the
  production `blockstore.Log`), `store.go`, `planelog.go`, `segment.go`,
  `recovery.go`. See `logstore/README.md`.
- **`registry/`** — Postgres metadata: `postgres.go` (`Registry` + `State`;
  `EmptyRoot`, DeleteBucket's first write), `bucketcommit.go` (`RootCommitWrites`: the guarded root advance, claims, drops, GC
  candidates and the journal rows of each commit's catalog blocks in
  `ingot.catalog_journal` until their segment ships, written with the commit
  task), `segments.go` (`logstore.Meta`, and `CatalogUsage`, the per-bucket
  sums the catalog usage gauges report), `stores*.go` (intents, locations,
  inclusions, blob_refs and pending refs (`CountReferences`),
  multipart sessions/parts, parks, encryption params, the revocation
  cursor),
  `locallocator.go` (the read tier's `Locator`). One `*Postgres` satisfies
  all of them.
- **`queue/`** — one package per task queue, each built over the runner
  with what its handlers need, registering them in its `New`, with typed
  calls for its sync tasks and specs for the async ones other queues spawn.
  `s3frontend.New` builds them from the end of the queue order back, each
  over the ones after it, which are the only queues a package imports; the
  bucket queue hands the catalog log its ship callback.
  The packages:
  `shardqueue` (`queue-shard`, the cache's copies), `spaceuploadqueue`
  (`queue-space-upload`, the object-count changes), `spaceshardqueue`
  (`queue-space-shard`, the tasks on one blob in one space; the releases
  bucket commits and part writes owe come back from `registry` as data,
  and the queue that applies those writes queues them through it),
  `bucketqueue` (`queue-bucket`,
  every change to a bucket: `.commit` applies an ordered list of key
  changes to one Tx under the version tree's write rule, failing a change
  on its own terms with one of a fixed set of errors; `.create`,
  `.set-versioning`, `.set-object-lock-config`, `.set-tagging`,
  `.set-cors`, `.set-encryption`, `.close`, `.admit-upload` and `.delete`;
  `.ship`, queued on each seal), `multipartqueue`
  (`queue-multipart-upload`, every change to a multipart upload: its create,
  parts, accept, Complete, abort and reap; the accept queues a
  `queue-space-shard.accept` per parked blob, which concludes it while the
  client uploads, and Complete validates the part list, assembles the
  object and commits it as a `queue-bucket.commit` child).
- **`taskrunner/`** — durable task queues in Postgres (one tasks table and
  one leases table per queue), serialized per (queue, subject) by a lease
  row. A task's tree (its handler plus sync children in later queues) adds
  writes to an ordered list that commits in one transaction at the end, with
  the rows of the tasks it spawned; sibling children taking leases in
  opposite orders resolve by the younger tree giving way. Async tasks can be
  replacements (the later `NotBefore` wins), cancelled before they start,
  batched (`HandleBatch`, each task with its own outcome) and retried
  (`WithRetry`; `Permanent` stops retries). Failed tasks stay as the
  dead-letter queue (`ListFailed`, `Resubmit`); finished ones are deleted
  past their queue's `Retention`. `Spec.Node` places a task on the runner whose `Options.Node`
  matches. Metrics (`taskrunner.*`: ready, delayed and failed tasks, oldest
  ready age, lease waits and takeovers, task duration by outcome, busy
  workers and the worker limit) go to
  `Options.MeterProvider`, or the global provider when unset. Imports nothing
  else from ingot (a test enforces it), so it can ship as a library.
  `taskrunnertest` makes a fresh schema per test and runs a tree with writes
  held and applied straight away (`CheckOwnWrites`). The daemon runs one
  runner (`provideTaskRunner` in `module.go`) over the `ingot` schema; its
  tables are migrated in the migration hook, and `Server` starts and stops
  it. Queues in order: `queue-multipart-upload`, `queue-bucket`,
  `queue-space-upload`, `queue-space-shard`, `queue-shard`.
  `queue-multipart-upload` (`queue/multipartqueue`), subject the
  upload ID: an upload's create, add-part, accept, complete, abort and reap,
  one at a time, the accept queuing a
  `queue-space-shard.accept` for each blob of the upload's parked parts;
  `queue-bucket` (`queue/bucketqueue`), subject the bucket's space
  DID: every change to a bucket, placed on the instance holding its catalog
  log; `queue-space-upload` (`queue/spaceuploadqueue`), the
  object-count changes a `queue-bucket.commit` spawns with its own
  commit; `queue-shard` (`queue/shardqueue`), subject the hex
  digest: a cached blob's time to live, `queue-shard.cache` scheduling its
  `queue-shard.evict` at `CacheTTL`, run again by reads with under half
  left, and the budget pass's evictions as sync `queue-shard.evict`s;
  `queue-space-shard` (`queue/spaceshardqueue`), subject space/hex digest: a blob's release,
  a parked blob's `queue-space-shard.accept` (its conclude), a write's sync
  `queue-space-shard.add` (its pending reference, in `blob_pending_refs`)
  and the reference's `queue-space-shard.expire` at 24 h. Its own tests each use a schema of their own
  (`taskrunnertest`), so the runner tests need no Ingot tables.
- **`uploader/`** — `forge.go`/`blob.go`/`stream.go`/`upload.go`: `Forge`
  behind `Uploader` (`SubmitShard`), `DeferredBodyUploader` (`ConcludeBlobs`,
  `AbortBlob`), `StreamingBodyUploader` (`StartBlob`, `PutBlob`: allocate by
  digest code, send as the body arrives), `BlobRemover`, and
  `UploadRegistrar` (`RegisterUploads`/`RetractUploads`, the object-count
  entries `queue-space-upload` sends, each with authority captured in the
  request by `CaptureAuthority`); captures the per-space
  ship authority (1h TTL) from in-request writes: `shipProofs` from body
  uploads (also the background releases' and aborts' fallback), and
  `commitProofs` from every `queue-bucket.commit` append (`CaptureShipAuthority`), which
  the ship prefers. Both take only stores `reqscope.BorrowableProofStore`
  returns, and their chains skip revoked delegations.
- **`bucketauthority/`** — hilt bucket ops: forwards CreateBucket /
  DeleteBucket / ListBuckets to `/s3/bucket/*`, recovering the signed S3
  request from ctx.
- **`iam/`** — the hilt IAM integration over `fil-forge/hilt/pkg/client`:
  versitygw `IAMService`/`RequestIAMService` authorizing each request via
  `/s3/request/authorize` (derived SigV4 key, with a local fast
  path over cached delegations, which accepts only chains ending in a
  delegation the access key issued: Hilt's tenant-issued cleanup delegations
  outlive the key and must not authorize requests), plus `KeyProofs`/`DelegationCache` — per-
  access-key TTL caches of hilt-issued delegations that the uploader and the
  network read tier consume via `internal/reqscope`. `KeyProofs` also
  remembers every revoked CID for 48h past its last sighting; a response
  carrying one is authorized but not cached. `AuthorizeSigned` authorizes a
  request signed in process (the catalog walk's Forge reads) into a store of
  its own.
- **`forgeclient/`** — carried-from-guppy sprue edge client: `/blob/add`
  (with a deferrable conclude, or by digest code for a streamed blob), `/ucan/conclude`, `/blob/abort`,
  `/blob/remove`, `/index/add`, receipt polling. The `/access` login and
  `/provider/add` flows are dormant (no CLI drives them).
- **`revocation/`** — the Swarf firehose consumer (optional,
  `revocation_service_url`/`_did`): streams UCAN revocations, records each
  revoked CID and clears the affected access key's iam caches via
  `iam.Revoker`; resumes after the Swarf cursor token in the
  `registry.RevocationCursorStore` cursor (no cursor → Swarf's head, read from
  its own database and saved before the stream opens; a cursor Swarf refuses
  → its head again, after clearing every access key's iam caches with
  `iam.Revoker.ClearAll`).
- **`tokenstore/`** — carried-from-guppy delegation store (`tokens.cbor`);
  empty, read only by the dormant login paths.
- **`regionkey/`** — the region CEK wrap (`Provider`): `openbao` (the
  region's OpenBao transit engine, production) or `inprocess` (tests and
  development), selected by `regionkey.provider`.
- **`tenantkey/`** — resolves a tenant's wrap key, the FEE tenant recipient,
  from its did:plc document (`tenantkey.plc_directory_url`, cached for
  `tenantkey.cache_ttl`).
- **`blake3tree/`** — the BLAKE3 Merkle-tree material of a body: the
  whole-object digest (the `x-cid` header), the block-aligned block chaining
  values the manifest records for verifying ranged reads, and the aligned
  subtrees of a range hashed at an offset (for assembling a multipart
  object's tree from its parts). Wraps `lukechampine.com/blake3/guts`.
- **`bucket/`** — the per-object model: `manifest.go` (`ObjectManifest`,
  `Body`), `leaf.go` (`ValueUnion`, `ObjectLeaf`, `VersionNode`),
  `chunker.go` (`SplitBody`, body readers), `sized.go` (`SplitSizedBody`, for
  a body of declared length), `cbor_gen.go`.
- **`mst/`** — the forked MST (deps: go-cid, blockstore, ucantone/did — trees
  carry their bucket's space for network-backed reads).
- **`internal/pgtest`** — a fresh, migrated Postgres database per test;
  **`internal/testfakes`** — the network's stand-ins (`NopBaseReader`,
  `NopUploader`, a bucket `Authority` in place of hilt). Both test-only.
- **`internal/authority`** — the authority queued tasks make their network
  calls with (`Resolver`): a store a request lends for the space while it
  waits on them, then the chain the task carries, then the space's captured
  ship authority.
- **`cars/`**, **`migrations/`**,
  **`internal/{reqscope,ucanexec,fasthttputil,cors,build}`**, **`gen/`**,
  **`testing/`** — CAR codec, goose SQL (`ingot` schema), request-scoped ctx
  keys, generic `Execute[T]`, fasthttp adapters, CORS, the version stamp, the
  cborgen driver, S3-client test glue.
- **`internal/tracing`** — OpenTelemetry: the per-request server span
  middleware and the instrumented HTTP client every outbound caller is built
  with. The daemon's exporter setup is `cmd/telemetry.go` (traces and, under
  the same `OTEL_EXPORTER_OTLP_*` variables, metrics). Local blob storage's
  instruments (`ingot.local_blobs.*`: usage by directory, budget and
  stalled-upload gauges, removals by reason, reads by tier), the multipart
  accept's (`ingot.multipart.*`: parked-part age, blob concludes by outcome,
  deferred Completes) and the catalog journal's (`ingot.catalog_journal.*`:
  blocks, bytes, oldest age) are created on the global meter provider,
  through `s3frontend.Deps.MeterProvider` and `Layered.CountBlobReads`, and
  the runner's `taskrunner.*` on it too; with no provider installed they are
  no-ops. So are the catalog usage gauges (`ingot.catalog.*`: each bucket's
  shipped catalog bytes and how much was dead on arrival, labeled by space
  DID, never bucket name), which a `server.go` sweep refreshes every five
  minutes from `registry.CatalogUsageStore`.
  versitygw passes
  the backend the bare `*fasthttp.RequestCtx` as its context, so the
  middleware stores the span as a request user value under the trace API's
  own key; `trace.SpanFromContext` on that context then finds it. A new
  outbound HTTP client should use `tracing.NewHTTPClient()`. Manual spans use
  `tracing.Start` / `tracing.End` (an S3 error table outcome does not mark a
  span failed); reads report their tier with `tracing.CountRead`, which lands
  on the server span as `ingot.reads.*`. A streamed response (GetObject) keeps
  its server span open until fasthttp has written the body. The local blob
  sweeper (`server.go`) starts a root span per run (`sweep.local_blobs`), so
  its registry queries, which `otelpgx` traces only under a recording span,
  show up too. A sync task runs in its caller's context, inside the request's
  span; an async task's handler has no parent span.

## Interface seams

| Contract | Production | Test |
|---|---|---|
| `versitygw/backend.Backend` | `s3frontend.Backend` | (same) |
| versitygw `auth.IAMService` + `middlewares.RequestIAMService` | `iam.Service` (versitygw's root account is disabled; every key resolves here) | (same; hilt-issued credentials) |
| `blockstore.Log` | `logstore.Manager` (one `Store` per bucket) | in-memory fake |
| `blockstore.BlockReader` | `blockstore.Forge` (in `Cached`) | `testfakes.NopBaseReader` |
| `registry.Registry` + the store seams + `logstore.Meta` | `*registry.Postgres` (all of them) | (same, on a database per test) |
| `locator.Locator` | `registry.LocalLocator` | (indexer-backed locator exists, unwired) |
| `uploader.{Uploader,StreamingBodyUploader,DeferredBodyUploader,BlobRemover,UploadRegistrar}` | `uploader.Forge` | `testfakes.NopUploader` |

## fx module

- **`ServerModule`** (composable core) — consumes `ServerConfig`,
  `*zap.Logger`, the `*taskrunner.Runner`, the agent `identity.Identity`, and
  the collaborator seams (`blockstore.BlockReader`; the five `uploader`
  seams; `bucketauthority.BucketAuthority`; `registry.Registry` plus the
  intent/location/inclusion/blob-ref/GC/multipart/park/encryption-params
  stores; `logstore.Meta`; `auth.IAMService`; `regionkey.Provider`;
  `tenantkey.Source`; optionally `registry.CatalogUsageStore`) + a
  `PreStartHook` group; runs pre-start → `New` → `Start` on the fx
  lifecycle.
- **`Module(cfg)`** (production wrapper) — supplies Postgres as Registry +
  Meta + every store (one `*registry.Postgres` behind them all), the Forge
  reader (`LocalLocator` inside `Cached`), the task runner
  (`provideTaskRunner`), the sprue edge client + uploader, the hilt client +
  `iam.Service` + `KeyProofs`, `bucketauthority`, the region-key provider,
  the tenant key source, the (empty) token store, and the migration
  `PreStartHook` (goose, then the runner's tables); with
  `revocation_service_url` set, the revocation consumer too. Empty option
  when `cfg.Enabled` is false.

A host provides `*zap.Logger`, `*pgxpool.Pool`, libforge's `identity.Identity` (the
agent) and sets `Config.UploadServiceURL`/`UploadServiceDID` (sprue) +
`AuthServiceURL`/`AuthServiceDID` (hilt). The non-fx escape hatch is
`New(ctx, ServerConfig, ServerDeps)`.

## Configuration (`config.Config`)

Viper/yaml-bindable (env prefix `INGOT_`, `.` → `_`; `Load` binds every
field, so each has a variable whether or not the YAML names it). Key fields:
`Enabled`, `Addr` (default `0.0.0.0:8080`), `DataDir`, `Region` (default
`us-east-1`), `MaxBlobSize` (0 → `bucket.DefaultMaxBlobSize`; at least
1 KiB, and its encrypted envelope must fit piri's blob ceiling),
`MultipartRehashBudget` (0 → 5 GiB; the re-read Complete allows for non-final
parts hashed at a wrong assumed offset, past which Complete is refused with
InvalidPartLayout; the final part is always re-hashed),
`CompleteMultipartKeepalive` (empty → 5s, negative → off; a Complete's
response commits to 200 with the version id once the request validates, as
S3 does, then sends a newline at this interval until the result), top-level
`SealBytes`/`SealAge`/`Retain` with a `CatalogPlane` `{SealBytes, SealAge,
Ship, Retain}` override block (the only plane), `ReadCacheBytes` (0 → 256 MiB,
<0 → off), `UploadServiceURL`/`UploadServiceDID`/`UploadReceiptsURL` (sprue),
`AuthServiceURL`/`AuthServiceDID`/`AuthServiceProofs` (hilt; proofs = file
path or string-encoded UCAN container, required alongside the URL and
validated at startup down to holding at least one delegation),
`RevocationServiceURL`/`RevocationServiceDID` (Swarf; optional, both or
neither), `TokenStoreDir` (→ `DataDir`), `MultipartSessionTTL` (empty → 7d,
negative → never: an open upload's auto-abort and a completed one's reap are
due this long out), `ReleaseGrace` (empty → 60s, negative → 0: how long a
blob's release waits after its last claim drops),
`LocalBlobMaxBytes` (0 → no budget for the cache, writes in progress
included) with
`CacheTTL` (1h: how long a body stays cached after it was cached or last
read) and
`LocalBlobOrphanAge` (24h, ≥ 1h) for the local blob sweeper
(`s3frontend.SweepLocalBlobs`), `CacheWrites` (unset → true; false drops
each body's local copy once accepted),
`CORSAllowedOrigins`, `LogLevel` (default `info`), `PostgresDSN`,
`Identity` (`key_file`: the agent's PEM key; `service_id`: optional did:web
the key is wrapped with), `RegionKey` (`provider`: `openbao` with
`openbao.{address,token,mount,key}`, mount default `transit`, or
`inprocess` with `inprocess.{kek,version}`, version default `v1`),
`TenantKey` (`plc_directory_url`, required; `cache_ttl`, default 10m) and
`Encryption.AllowNone` (lets PutBucketEncryption choose `none`).
`Config.ServerConfig()` is the single mapping site; `Config.Validate` checks
the daemon's required fields. The listener always serves the agent's DID
document at `/.well-known/did.json` — see `didDocumentHandler` in
`server.go`, mounted via versitygw's `s3api.WithRoute` ahead of the S3
route table.

## Testing

There is no in-memory ingot: the deployment under test is always the real
forge-mode daemon. Two tiers:

- **`make test`** (a minute or two, Postgres and OpenBao in Docker): every
  package's tests, the registry on Postgres and the network faked, plus the
  thin S3-client glue in `testing/` (`Config`/`NewS3Conf`, roundtrip helpers).
- **`make itest` — integration** (`itest/`, build tag `itest`, Docker):
  boots the smelt Forge stack with THIS working tree's binary mounted over
  the published image.
  - **`versity_{bucket,object,multipart,versioning,lock,tagging}_test.go`** — the S3
    conformance partition: per upstream versitygw group, a curated pass table (every case
    must pass) and an XFail table (known-failing, reported as SKIP; an
    *unexpected pass* fails the test — the cue to promote the row). One
    shared stack serves all categories (`TestForgeVersity`).
  - **`scenarios_test.go`** — ingot-unique behaviors upstream can't assert
    (blob-split/cache-by-digest, zero-byte objects, part-spans-blobs
    multipart, failed-Complete session recovery), on a small-`max_blob_size`
    config (`testdata/config-smallblob.yaml` via smelt's WithServiceConfig).
  - **`forge_*_test.go`** — forge-native behaviors on dedicated stacks:
    provisioning (`forge_native`), delete/release (`forge_delete`), the
    catalog rebuilt from the journal after a wiped volume
    (`forge_lost_volume`), deferred
    multipart accept (`forge_multipart_deferred`), catalog retention
    (`forge_retention`), `ingot catalog usage` inside the container
    (`forge_catalog_usage`), the read-after-eviction network tier and the
    local blob budget sweeper (`forge_eviction`), a real `aws s3 cp`
    multipart round trip from the official CLI image (`forge_awscli`), and
    encryption, authorization, object counts, bucket deletes and the
    Complete keepalive in the other `forge_*_test.go` files.
  - **`forge_s3compat_test.go`** — the S3 compatibility corpus behind
    `make s3compat` (skips unless `INGOT_S3COMPAT` is set).
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

- **cborgen:** `make gen` (`go generate ./...`, whose one directive runs
  `gen/`) regenerates `bucket/cbor_gen.go`, `bucket/cbor_gen_tuple.go`,
  `registry/cbor_gen.go` and `s3frontend/cbor_gen.go` (the task argument
  types). `mst/cbor_gen.go` is separate. `make gen-check` fails on a stale
  `bucket/cbor_gen*.go`; verify a no-op diff after touching any generated
  type.
- **migrations:** SQL in `migrations/sql/*.sql` (`go:embed`), applied by
  `migrations.Up` under the `ingot` schema at startup via a `PreStartHook`.
  Once a migration has merged, change the schema with a new migration, never
  by editing the merged one. The dev node (fil-forge/infra-nodes) deploys
  every image `main` publishes and keeps its database across deploys, and
  goose never re-runs a version it has recorded, so an in-place edit never
  reaches that database, and a later migration that assumes the edit fails
  there at startup. Editing a migration that hasn't merged is fine. Other dev
  data is still disposable: reshape stored formats in place and reset dev
  data rather than migrating it.

## Docker images & release

Three publishing paths, all to `ghcr.io/fil-forge/ingot`. Mirrors sprue's
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
  multi-arch amd64/arm64. PR pushes get a single-arch, no-push build-check
  of both targets. `:main-dev` slots into smelt's `compose.debug.yml` /
  `make debug-<svc>` flow.
- **On request from a PR** (`.github/workflows/forge-perf.yml`): a
  `/forge-perf` comment from an owner, member or collaborator builds the PR's
  head as the `prod` target, linux/arm64 only, and pushes
  `:pr-<n>-<sha7>` for fil-forge/forge-perf to benchmark against main. It
  never tags `:main` or `:sha-*`; the jobs live in forge-perf's `pr-run.yml`.

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

## Observability

When a change adds or alters something operational, decide how on-call would
know it had broken, and add what is useful in the same PR: metrics, trace
spans, and an alert or dashboard panel in fil-forge/infra-central. Skip it for
changes nothing operational depends on. List each new metric in a metrics
table in the README, beside the local blob, multipart and catalog journal
ones. "Adding a
signal" in [`docs/observability.md`](docs/observability.md) says how to do
each.

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
- [`docs/s3-cors.md`](./docs/s3-cors.md) — the gateway-wide CORS document
  and the per-bucket one that replaces it.
- [`docs/aggregation-gate.md`](./docs/aggregation-gate.md) — why this S3
  design is gated on Piri's aggregation.
- [`docs/IMPLEMENTATION_PLAN.md`](./docs/IMPLEMENTATION_PLAN.md) — the phase
  tracker for the architecture migration.
- [`logstore/README.md`](./logstore/README.md) — the catalog log internals.
- [`itest/README.md`](./itest/README.md) — the integration harness and the
  conformance ratchet.
- [`README.md`](./README.md) — orientation / deploy modes.
