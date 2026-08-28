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
key's value is its object manifest (size, sha256 and md5, the S3 system
headers and user metadata, and the ordered list of content-addressed body
blobs that cover it); a key with retained versions groups them under a
per-key leaf ([`docs/s3-versioning.md`](./docs/s3-versioning.md)). Because
every node is addressed by its own hash, a bucket rolls up to a single
**root CID** — a cryptographic commitment to the exact set of objects it
holds — and the tree's ordered keys make S3 prefix/delimiter listings fall
out of ordinary traversal.

Writes are functional: a PUT or DELETE rewrites only the nodes on the path from
the changed key up to the root (every other node is immutable and shared),
producing a **new root CID**. ingot journals the changed blocks to the local
log, then **compare-and-swaps the bucket's root in Postgres** from the old CID
to the new one. That split is the heart of the design:

- **The MST is the data** — immutable, content-addressed, self-verifying, and
  shippable to Forge exactly as it sits on disk.
- **Postgres is the mutable index** — the authoritative *current* root per
  bucket (the CAS keeps a bucket single-writer-correct), the segment
  lifecycle, and the blob claim ledger, all under the `ingot` schema.

## Bodies and catalog

A write splits into two paths:

- **Bodies** stream into a local spool (sha256 and md5 in one pass), split
  into blobs of at most `max_blob_size`, and upload to Forge storage
  synchronously: `/blob/add` via sprue, an HTTP PUT to the allocated piri,
  and the accepted location commitment, all before the catalog commit.
  Identical bytes dedup by digest; a reference index (`blob_refs`) counts
  which versions still claim each blob and releases it (`/blob/remove`) when
  the count reaches zero.
- **The catalog** (the dag-cbor MST nodes and manifests) journals to a local
  per-bucket log, seals into CAR segments by size or age, and ships to the
  bucket's Forge space in the background; shipping advances the bucket's
  `forge_root_cid` under a guard. See
  [`logstore/README.md`](./logstore/README.md).

Reads fall through tiers: the spool, the local log (catalog blocks), and
finally the network, resolved by a local locator (`blob_locations` +
`shard_inclusions`) and fetched with a ranged `content/retrieve` against the
storing piri. The two routes are drawn side by side in
[`docs/diagrams.md`](./docs/diagrams.md#two-block-routes-body-blobs-and-catalog-blocks).

## Running it

**As a library** (fx). A host adds the module and provides a logger, a
Postgres pool, and the agent identity (via `Config`):

```go
app := fx.New(
    // host provides: *zap.Logger, *pgxpool.Pool, ingot.ServiceIdentity
    ingot.Module(cfg), // cfg sets UploadServiceURL/DID, AuthServiceURL/DID, ...
)
```

There is also a non-fx escape hatch — `New(ctx, ServerConfig, ServerDeps)` +
`Server.Start`/`Server.Stop` — for hosts and tests that construct the
collaborators themselves.

**As a daemon** (`cmd/`, cobra/viper/fx):

```bash
ingot serve --config /etc/ingot/config.yaml   # the gateway
ingot whoami                                  # agent DID + sprue/hilt endpoints
ingot version
```

`serve` requires Postgres (`postgres_dsn`), the sprue edge client
(`upload_service_url`/`_did`), and the hilt auth/tenant service
(`auth_service_url`/`_did` plus the `auth_service_proofs` delegation chains
hilt issues to this agent). Tenants, access keys, and buckets are owned by
[hilt](https://github.com/fil-forge/hilt): it authorizes every non-root S3
request, mints each bucket's Forge space, and issues the S3 credentials;
ingot never self-provisions. The deployment context is the
[Forge deployment RFC](https://github.com/fil-one/RFC/blob/main/2026-05-filone-forge-deployment-proposal.md):
the S3 facade runs **at the edge**, co-located with a provider's piri or as a
standalone client — not inside the central upload-service.

## Build & test

```bash
make build   # GOWORK=off go build ./...
make test    # unit tests (seconds, no Docker)
make itest   # integration tests (boots the Forge stack in Docker, ~6 min)
make gen     # regenerate CBOR marshalers after changing bucket types
```

The `itest/` package boots the real Forge stack via smelt's Go SDK and runs
this working tree's binary against it — including the curated S3 conformance
partition (per-group expected-pass and known-fail tables of versitygw cases).
CI runs it after unit tests pass. See `itest/README.md`.

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

The S3 core is exercised two ways: `make test` runs the unit tier in seconds,
and `make itest` boots the real Forge stack (smelt, in Docker) and runs this
working tree's binary against it, including a curated partition of the
upstream versitygw S3 conformance suite. Known gaps are tracked in
[`DESIGN_NOTES.md`](./DESIGN_NOTES.md).
