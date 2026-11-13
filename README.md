# ingot

> **⚠️ Work in progress.** ingot is under active development and its design is
> changing rapidly: interfaces, on-disk formats, and the Forge upload path are
> all still in flux. Expect breaking changes.

**ingot is an S3 gateway over the [Forge](https://github.com/fil-forge) network,
built around a Merkle Search Tree (MST) ported from
[bluesky-social/indigo](https://github.com/bluesky-social/indigo/tree/main/mst).**

It speaks the S3 REST protocol on one side and the Forge UCAN control plane on
the other, and runs **two ways**: as an embeddable Go **library** a host
process imports in-process, or as a standalone **daemon** (`ingot serve`).
Either way it presents each S3 bucket as a per-bucket MST, uploads object
bodies to Forge storage as content-addressed blobs before a write is acked,
journals the catalog to a local per-bucket log, and ships sealed catalog
segments to Forge as a guppy-style edge client. Reads fall through local
tiers and finally to the network.

For the architecture as it operates today, see
[`DESIGN_NOTES.md`](./DESIGN_NOTES.md); for the target design,
[`docs/architecture.md`](./docs/architecture.md); for the as-built diagram
set, [`docs/diagrams.md`](./docs/diagrams.md); for the log internals,
[`logstore/README.md`](./logstore/README.md).

### Why a forked MST, not a direct dependency

The MST in [`mst/`](./mst) is a fork of indigo's, not an import of it. atproto's
MST is exactly the data structure ingot wants — an ordered, content-addressed
key/value map that commits to an entire keyspace under a single root CID — but
ingot ports it rather than depending on it, for three reasons:

- **A different key space.** atproto validates keys as repo record paths
  (`collection/rkey`, with charset and length limits). ingot relaxes this to
  accept arbitrary S3 object keys (any non-empty UTF-8 string up to 1024 bytes,
  NUL excluded) — a behavioral change to the structure, not just a repackage.
- **A small dependency surface.** Importing indigo's `mst` subpackage would pull
  the broader atproto module graph in for one self-contained data structure. The
  fork's only external dependency is `go-cid`.
- **Freedom to diverge.** The on-disk format starts identical to atproto's, but
  cross-implementation compatibility is intentionally not a goal.

## How buckets map onto the MST and Postgres

Each bucket is one MST: an ordered map from **object key → value block**. A
key's value is its object manifest (size, BLAKE3 digest and tree, md5, the S3 system
headers and user metadata, and the ordered list of content-addressed body
blobs that cover it); a key with retained versions groups them under a
per-key leaf ([`docs/s3-versioning.md`](./docs/s3-versioning.md)). Because
every node is addressed by its own hash, a bucket rolls up to a single
**root CID** — a cryptographic commitment to the exact set of objects it
holds — and the tree's ordered keys make S3 prefix/delimiter listings fall
out of ordinary traversal.

Writes are functional: a PUT or DELETE rewrites only the nodes on the path from
the changed key up to the root (every other node is immutable and shared),
producing a **new root CID**. Each change to a bucket runs as a task on the
bucket's `queue-bucket` subject, one at a time: ingot journals the changed blocks to the local
log, then **advances the bucket's root in Postgres** from the old CID to the
new one, guarded on the old. That split is the heart of the design:

- **The MST is the data** — immutable, content-addressed, self-verifying, and
  shippable to Forge exactly as it sits on disk.
- **Postgres is the mutable index** — the authoritative *current* root per
  bucket (the bucket's queue keeps it single-writer), the segment
  lifecycle, and the blob claim ledger, all under the `ingot` schema.

## Bodies and catalog

A write splits into two paths:

- **Bodies** split into blobs of at most `max_blob_size` as they arrive (the
  BLAKE3 tree and md5 in the same pass) and stream to Forge storage
  synchronously: `/blob/add` via sprue, an HTTP PUT to the allocated piri as
  the bytes arrive, and the accepted location commitment, all before the
  catalog commit.
  Identical bytes dedup by digest; a reference index (`blob_refs`) counts
  which versions still claim each blob and releases it (`/blob/remove`) when
  the count reaches zero.
- **The catalog** (the dag-cbor MST nodes and manifests) journals to a local
  per-bucket log, seals into CAR segments by size or age, and ships to the
  bucket's Forge space as a `queue-bucket.ship` task queued on each seal;
  shipping advances the bucket's `forge_root_cid` under a guard. See
  [`logstore/README.md`](./logstore/README.md).

Reads fall through tiers: the local cache, the local log
(catalog blocks), and finally the network, resolved by a local locator
(`blob_locations` + `shard_inclusions`) and fetched with a ranged
`content/retrieve` against the storing piri. The two routes are drawn side by side in
[`docs/diagrams.md`](./docs/diagrams.md#two-block-routes-body-blobs-and-catalog-blocks).

## Running it

**As a library** (fx). A host adds the module and provides a logger, a
Postgres pool, and the agent identity (via `Config`):

```go
app := fx.New(
    // host provides: *zap.Logger, *pgxpool.Pool, libforge identity.Identity
    ingot.Module(cfg), // cfg sets UploadServiceURL/DID, AuthServiceURL/DID, ...
)
```

There is also a non-fx escape hatch — `New(ctx, ServerConfig, ServerDeps)` +
`Server.Start`/`Server.Stop` — for hosts and tests that construct the
collaborators themselves.

**As a daemon** (`cmd/`, cobra/viper/fx):

```bash
ingot serve --config /etc/ingot/config.yaml   # the gateway
ingot whoami                                  # agent DID (and key DID) + sprue/hilt endpoints
ingot version
ingot blake3 hash < file                      # the object CID (x-cid) of the data
ingot blake3 hash --bao < file                # plus the ChunkLog and Bao Outboard the Blake3 attribute carries
ingot blake3 hash --bao --chunk-log 4 < file  # an outboard at iroh's 16 KiB block instead of ingot's block size
ingot blake3 verify <cid> < file              # whole-object check against an x-cid
ingot blake3 range --outboard "$OUTBOARD" --chunk-log 12 5000-200000   # aligned Range to fetch
ingot blake3 verify --outboard "$OUTBOARD" --chunk-log 12 --offset 0 <cid> < part  # ranged check
ingot catalog usage                           # each bucket's catalog bytes: on Forge, dead on arrival, live (JSON; benchmarks)
```

`blake3 range` and the ranged `verify` take the `Blake3` attribute of
`GetObjectAttributes`: its `ChunkLog` (the Bao block size as a base-2 exponent of
1 KiB chunks), and its `Outboard` either inline as the
base64 string from the XML (`--outboard`) or from a file holding that string
or the raw bytes (`--outboard-file`). A mismatch exits 1; any other failure
exits 2.

`serve` requires `addr` (default `0.0.0.0:8080`) and `data_dir`, Postgres
(`postgres_dsn`), the agent's key (`identity.key_file`), the sprue edge client
(`upload_service_url`/`_did`), the hilt auth/tenant service
(`auth_service_url`/`_did` plus the `auth_service_proofs` delegation chains
hilt issues to this agent), a region-key provider (`regionkey.provider`:
`openbao`, or `inprocess` for development) and the did:plc directory tenant
wrap keys resolve from (`tenantkey.plc_directory_url`). Every key can also be
set from the environment as `INGOT_` plus the key, `.` replaced by `_`
(`INGOT_TENANTKEY_PLC_DIRECTORY_URL`). Tenants, access keys, and buckets are owned by
[hilt](https://github.com/fil-forge/hilt): it authorizes every S3 request
signed with a hilt-issued access key, mints each bucket's Forge space, and
issues the S3 credentials; ingot never self-provisions. The gateway has no
root account, and a key that is not a `did:key` is rejected before hilt is
consulted. The deployment context is the
[Forge deployment RFC](https://github.com/fil-one/RFC/blob/main/2026-05-filone-forge-deployment-proposal.md):
the S3 facade runs **at the edge**, co-located with a provider's piri or as a
standalone client — not inside the central upload-service.

`serve` exports OpenTelemetry traces over OTLP/HTTP when
`OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`) names a
collector; with no endpoint, tracing is off. Each S3 request is a trace named for its S3 action, with the hilt, sprue,
piri and Postgres calls it makes as child spans. The other standard `OTEL_*`
environment variables apply: `OTEL_EXPORTER_OTLP_HEADERS` authenticates to the
collector, and `OTEL_TRACES_SAMPLER_ARG` sets the fraction of requests traced
(`0.01` traces 1%; the default traces every request). As a library, ingot
records spans on the global tracer provider, so an embedding host decides
where they go. Metrics go to the same collector under the same variables
(`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` overrides the endpoint for them alone,
and `OTEL_METRIC_EXPORT_INTERVAL` sets how often they are pushed); as a
library, ingot records them on the global meter provider.

### Local disk

`serve` keeps copies of object bodies in `<data_dir>/cache`. A write tees
each body blob into a temporary file there as it streams the blob to its
provider, and keeps the file once the provider holds the blob, before the
PUT answers, so a read soon after a write is served from local disk.

A cached copy is not needed, so it goes:

- when its time to live runs out: `cache_ttl` (default `1h`) after it was
  cached or last read;
- when a deleted or overwritten object's `queue-space-shard.release` runs
  (`release_grace`, default 60s, after its last reference drops). The release also removes the
  provider's copy, so a GET still streaming the old object once the grace
  has passed fails when it reaches a blob it has not yet opened:
  `release_grace` bounds how long such a read may take;
- with `local_blob_max_bytes` set, when a sweeper, checking every 30 seconds,
  finds the cache over the budget and evicts cached bodies, the least
  recently cached or read first, down to 90% of the budget, however much of
  their time to live is left.

Later reads of an evicted body go to the provider and take provider-read
latency. A read from the cache extends the body's time to live only once
less than half of it is left, so most reads cost nothing. Each time to live
is a `queue-shard.evict` task in Postgres, so it survives a restart.

A multipart part keeps no local copy, so Complete relies on the provider
keeping each parked allocation until it runs; keep `multipart_session_ttl`
(default `168h`) well under how long the provider keeps parked allocations.
The TTL counts from the upload's creation, when its auto-abort is
scheduled, so no parked part outlives it. The budget is off by default (`local_blob_max_bytes: 0`): without it
the cache grows with every live object's bodies. A node that rarely reads
back what it just wrote can keep no written bodies at all: with
`cache_writes: false`, a write keeps no copy and every read goes to the
provider. Bodies cached before the setting was turned off stay until they
are released or evicted. With or without a budget, the sweeper also
deletes, hourly, unfinished `.tmp-*` writes once they are older than
`local_blob_orphan_age` (default `24h`, at least `1h`); startup deletes
every one it finds.

**Sizing.** The filesystem needs room for:

- `local_blob_max_bytes`, plus 10% headroom that must exceed the ingest rate
  × 30 seconds (60 GB at 2 GB/s). To keep bodies for their whole
  `cache_ttl`, the budget must also exceed the bodies written or read within
  it (7.2 TB at 2 GB/s of ingest for the default `1h`); below that, sweeps
  evict bodies before their time runs out;
- writes in progress, when they outgrow the budget: each concurrent object
  write tees its body into the cache as it streams. The budget counts
  those bytes as they land, so the sweeper evicts cached bodies to make room
  for them. But a write in progress cannot be evicted, so if writes alone
  exceed the budget, usage runs over it by the difference;
- the catalog log, if it shares the filesystem: `<data_dir>/segments`, per
  bucket about (`retain` + the open and unshipped segments) × `seal_bytes`.

**Metrics.** With metrics on, local blob storage reports:

| Metric | Meaning |
| -- | -- |
| `ingot.local_blobs.usage` | Bytes held, by `dir` (always `cache`): cached bodies, and writes in progress (which eviction cannot touch) |
| `ingot.local_blobs.budget` | `local_blob_max_bytes` (0: no budget) |
| `ingot.local_blobs.stalled_bytes` | Bytes of bodies whose upload has stalled, by `state`: intents still `uploading` an hour after their last state change. They hold no local bytes; the provider expires an allocation that is never concluded. Growth means uploads are failing |
| `ingot.local_blobs.removals`, `ingot.local_blobs.removed_bytes` | Files and bytes removed, by `reason`: `released`, `expired` (its `cache_ttl` ran out), `budget`, `orphan` (an unfinished write's temporary file) |
| `ingot.local_blobs.reads` | Body-blob reads, by `tier`: `local` or `network` (the local hit ratio) |

Multipart uploads' parts are accepted while the client uploads them, each
blob by a `queue-space-shard.accept` task the upload's
`queue-multipart-upload.accept` queues (see DESIGN_NOTES), and report:

| Metric | Meaning |
| -- | -- |
| `ingot.multipart.parked_age` | Seconds the oldest parked part of an open upload has waited for acceptance, read from the part rows when collected; 0 when none waits. Parts are accepted within seconds, so growth means the acceptance queue is stuck |
| `ingot.multipart.accept_passes` | Part blobs concluded, by `outcome`: `accepted` or `failed` (the blob's accept retries; a timed-out conclude included) |
| `ingot.multipart.complete_deferred` | CompleteMultipartUpload requests answered OperationAborted because the parts' accepts did not finish in time. Botocore clients (the AWS CLI) treat that as a failed upload |

Object traffic is counted in body bytes. Copies stay inside Ingot and count
as neither:

| Metric | Meaning |
| -- | -- |
| `ingot.s3.object_egress_bytes` | Bytes GetObject sent to clients: those read from the body before it closed, so an abandoned download counts what went out |
| `ingot.s3.object_ingress_bytes` | Bytes PutObject and UploadPart received, whether or not the write succeeded |

Each commit also journals its catalog blocks in Postgres until their segment
ships (see `logstore/README.md`), so a copy of the database holds the
catalog only this host's disk would otherwise hold. The journal reports:

| Metric | Meaning |
| -- | -- |
| `ingot.catalog_journal.blocks`, `ingot.catalog_journal.bytes` | Catalog blocks, and their bytes, journaled in Postgres whose segment has not shipped yet |
| `ingot.catalog_journal.oldest_age` | Seconds since the oldest journaled block was written; 0 when the journal is empty. Segments ship within seconds of sealing, so growth means shipping has stalled and the unshipped catalog is building up in Postgres |

The task queues report queue depth, the oldest ready task's age, lease waits
and takeovers, task duration and busy workers as `taskrunner.*`, listed in
[`taskrunner/README.md`](./taskrunner/README.md#metrics).

**Manual cleanup.** Every blob file in `<data_dir>/cache` is safe to
delete: the provider holds its body. Restart `serve` after deleting files by
hand: the byte count sees such a change at startup, or only at an hourly
scan that no write overlapped.

### Catalog storage on Forge

Nothing collects dead catalog blocks yet: each shipped catalog segment stays
on Forge until its bucket is deleted, retired segments included. With metrics
on, these gauges show how much catalog storage each bucket holds there and how
much of it was dead before it shipped. Each is labeled by the bucket's `space`
DID, not its name, and refreshed every five minutes: each refresh reads every
segment row in Postgres.

| Metric | Meaning |
| -- | -- |
| `ingot.catalog.forge_bytes` | Bytes of the bucket's shipped catalog segments, retired ones included (their CARs; their index blobs are not counted) |
| `ingot.catalog.dead_on_arrival_bytes` | Bytes of those segments' blocks that were already dead when their segment shipped: superseded by later writes in the same segment |
| `ingot.catalog.unmeasured_bytes` | Bytes of shipped segments whose dead-on-arrival bytes were not measured, such as every segment shipped before the measurement existed |

Blocks that die after their segment ships are not measured: that would take a
walk of the bucket's live catalog, most of which is only on Forge.

A test stack can measure them. `ingot catalog usage` prints, as JSON, each
bucket's figures above beside its local segment files and the live bytes its
root reaches. The walk reads a block no local segment holds, such as one of a
retired segment, with the rest of its segment from Forge, authorized by an
access key the command takes from `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`. Without one such blocks count as missing, and so do
those of a bucket the key may not list. Dead after shipping is then roughly
what's left: CAR headers count only in the bytes on Forge, blocks of segments
not yet shipped count as live, an unmeasured segment's dead-on-arrival bytes
count as dead after shipping, and a block can count both as live and as dead
on arrival in an older segment.
The walk holds every segment it reads in memory, so it is for benchmark runs,
not production nodes. A production node could not run it anyway: it holds no
tenant's secret, and its authority over a bucket arrives only with that
tenant's signed requests.

`forge_bytes` undercounts a bucket whose segments retired before retired rows
were kept (migration `00033`): those rows were deleted, so their CARs, still on
Forge, count nowhere, not even as unmeasured. Read such a bucket's figures as a
floor, or reset its data.

### Loading a catalog to estate scale

`ingot-preload` (`cmd/ingot-preload`) fills a gateway's buckets through its S3
API, to measure the catalog at sizes no test reaches by accident. It is a plain
S3 client, with its access key and region taken from the AWS SDK's usual
sources (smelt's `make s3-key` writes a profile it can use):

```bash
GOWORK=off go run ./cmd/ingot-preload load --profile smelt --encryption "" \
  --buckets 3 --until 2000 --real-every 500 --real-min-mib 16 --real-max-mib 32
```

Most objects are zero-byte, which runs the whole commit, seal and ship without
uploading a blob. One object per `--real-every` has a pseudo-random body of a
pack's size, for timing reads and deletes. Keys and PUTs are shaped as the pack
server writes them: 64 hex digits after an optional prefix, create-only, with
the body's SHA-256. New buckets get the `none` default encryption, which needs
`encryption.allow_none`. smelt's ingot doesn't allow it, so the example passes
`--encryption ""` and keeps ingot's default; to try `none` on smelt, add
`encryption: {allow_none: true}` to its `systems/ingot/config/config.yaml`,
recreate ingot, and load other buckets with another state file
(`--bucket-prefix`, `--state`): a state file remembers each bucket's encryption
and refuses another. Progress is saved in a state file, so a second `load` with
a higher `--until` carries on from the first, and reports go to stdout and, as
JSON lines, to a metrics file.

Real objects at their default size add up. At estate scale, `--buckets 150
--until 600000 --real-every 30000` writes 20 per bucket, about 540 GiB of
bodies in all.

`ingot-preload validate` then times requests against the loaded buckets and
checks every answer, reading which objects exist from the load's state file:
pass it the load's `--buckets`, `--bucket-prefix`, `--seed`, `--key-prefix`,
`--state`, `--real-min-mib` and `--real-max-mib`. It reads a range of every
real object and checks its bytes, reads the metadata of a sample of bulk
objects, lists a few buckets in full and checks every recorded object is there,
and writes, deletes and re-checks new objects shaped like the load's: zero-byte
ones in every bucket, and `--write-real` real ones (20 by default, about 3.6
GiB uploaded at the default sizes) over the buckets in turn. Its access key
must also be allowed to read, list and delete. It deletes only what it wrote,
and deletes it even after an interrupt; a second interrupt stops it at once,
and a later listing of a bucket counts what it left there as extra keys. A test
point is a load to that size, a gateway restart, two reading runs with the same
`--sample`, then the listing and writes:

```bash
ingot-preload load --until 600000 --real-every 30000      # loaded to the test point
# restart ingot
ingot-preload validate --label 0.6M-cold --steps get,head
ingot-preload validate --label 0.6M-warm --steps get,head
ingot-preload validate --label 0.6M-list-write --steps list,write
```

The restart empties the catalog's block cache and the authorization caches,
so the first run reads them cold and the second warm. A full listing reads
every listed key's manifest through that block cache (`read_cache_bytes`,
256 MiB by default): three buckets at the 0.6M point come to an estimated
0.6 GB, more than it holds, so the listing runs after the warm run.

Body bytes are a different matter: a range is served from the local blob
cache, which survives a restart and holds a blob for `cache_ttl` (1h by
default) after it was written or last read, and otherwise from Piri, which
reads don't copy back into the cache. Both reading runs read bodies from the
same place. To time body reads
from Piri, start the first run more than `cache_ttl` after the load wrote its
last real object, or load with `cache_writes: false`.

Each step's latencies go to stdout as it ends, and the whole report to a JSON
lines file (`--report`). A request that failed or answered wrongly is listed
there and makes the command exit non-zero.

`ingot footprint`, run where the daemon runs and with its config, prints what
the node keeps, as JSON: each table of the `ingot` schema with its bytes and
estimated rows, the database's size and the server's write-ahead log position,
and each entry under `data_dir` with its files and bytes. Run it and
`ingot catalog usage` before a load and at each test point. The difference in
`wal_bytes` between two reports is the WAL written in between, and each table's
and directory's growth per object loaded is what a projection scales.

It also reports how blob releases keep up: how long the finished ones took from
the change that queued them, any grace included, how far past their due time
they ran, and how many are still pending, past due or failed. Finished releases
are kept for a day, the task runner's retention for finished tasks, so those
figures are a rolling window, not totals to subtract. The load queues no
releases, since it writes each object once. `validate`'s write step queues the
releases of the real objects it deletes, one per blob, due once the release
grace has passed, so run `ingot footprint` after that to see them finished.

Neither command is listed in `ingot --help` unless `INGOT_DEV_COMMANDS` is set
to `1` or `true`; both run either way.

## Build & test

```bash
make build                 # the daemon binary, ./ingot (GOWORK=off)
make test                  # every package's tests (starts Postgres and OpenBao in Docker)
make itest                 # integration tests (boots the Forge stack in Docker, ~20 min)
make itest-group GROUP=auth  # one of the groups CI runs on its own runner
make s3compat              # the S3 compatibility corpus, written as an HTML report
make gen                   # regenerate CBOR marshalers after changing bucket, registry or s3frontend task types
make gen-check             # fail if the generated bucket marshalers are stale
```

The `itest/` package boots the real Forge stack via smelt's Go SDK and runs
this working tree's binary against it — including the curated S3 conformance
partition (per-group expected-pass and known-fail tables of versitygw cases).
CI runs it, split into `make itest-group` groups, after unit tests pass. See
`itest/README.md`.

## Deploying to the dev node

Every merge to `main` publishes `ghcr.io/fil-forge/ingot:main` and dispatches the digest of the
**prod** image to
[infra-nodes](https://github.com/fil-forge/infra-nodes/blob/main/.github/workflows/bump-deployed-image.yml).
That workflow rewrites the pin the FilOne Appliance dev node runs and opens a pull request with
auto-merge armed. Merging queues the image; the node picks it up on its next reconcile pass, waits
for a safe proving window, and then restarts.

The `dev` matrix leg dispatches nothing. It carries delve and a debug build, and the node pins the
prod image.

The dispatch needs two repository credentials: the Actions variable `FORGE_BOT_APP_ID` and the
Actions secret `FORGE_BOT_PRIVATE_KEY`, which mint a token scoped to `infra-nodes` alone. Without
them the publish step fails and the image still lands in GHCR.

## Dependencies

ingot depends only on the Forge stack — `ucantone` (UCAN 1.0 primitives),
`libforge` (Forge capability definitions), the `hilt` client (request
authorization + tenancy), the `indexing-service` query client, `versitygw`
(the S3 front end) — plus standard plumbing (pgx, goose, fx, zap, go-cid). It
must never import `fil-forge/sprue` or `fil-forge/guppy` (guppy embeds ingot
→ cycle); the Forge-client subset it needs is carried in `forgeclient/` +
`tokenstore/`.

## Status

The S3 core is exercised two ways: `make test` runs every package's tests
against a real Postgres, and `make itest` boots the real Forge stack (smelt, in Docker) and runs this
working tree's binary against it, including a curated partition of the
upstream versitygw S3 conformance suite. Known gaps are tracked in
[`DESIGN_NOTES.md`](./DESIGN_NOTES.md).
