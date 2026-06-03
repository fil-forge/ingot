# ingot

> **⚠️ Work in progress.** ingot is under active development and its design is
> changing rapidly — interfaces, on-disk formats, and the Forge upload path are
> all still in flux. Expect breaking changes.

**ingot is an S3 gateway over the [Forge](https://github.com/fil-forge) network,
built around a Merkle Search Tree (MST) ported from
[bluesky-social/indigo](https://github.com/bluesky-social/indigo/tree/main/mst).**

It speaks the S3 REST protocol on one side and the Forge UCAN control plane on
the other, and runs **two ways**: as an embeddable Go **library** that a host
process (piri, guppy, sprue) imports in-process, or as a standalone **daemon**
(`ingot serve`). Either way it presents each S3 bucket as a per-bucket MST,
journals mutations to a local LSM-style log, and ships sealed segments to Forge
as a guppy-style edge client. Reads fall through local tiers and finally to the
network.

For the full architecture, see [`DESIGN_NOTES.md`](./DESIGN_NOTES.md); for the
log internals, [`logstore/README.md`](./logstore/README.md).

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

Each bucket is one MST: an ordered map from **object key → object-manifest CID**.
A manifest records the object's size, its sha256 and md5, the S3 system headers
and user metadata, and a pointer to the chunked body. Because every node is
addressed by its own hash, a bucket rolls up to a single **root CID** — a
cryptographic commitment to the exact set of objects it holds — and the tree's
ordered keys make S3 prefix/delimiter listings fall out of ordinary traversal.

Writes are functional: a PUT or DELETE rewrites only the nodes on the path from
the changed key up to the root (every other node is immutable and shared),
producing a **new root CID**. ingot journals the changed blocks to the local log,
then **compare-and-swaps the bucket's root in Postgres** from the old CID to the
new one. That split is the heart of the design:

- **The MST is the data** — immutable, content-addressed, self-verifying, and
  shippable to Forge exactly as it sits on disk.
- **Postgres is the mutable index** — the authoritative *current* root per bucket
  (the CAS keeps a bucket single-writer-correct) plus each log segment's
  hot → warm → cold lifecycle, all under the `ingot` schema.

## Data plane vs catalog plane

The log splits each write into **two independent pipelines**, configured
separately:

- **data plane** — raw object-body chunks (the bytes a client GETs).
- **catalog plane** — the dag-cbor MST nodes, manifests, and chunk indexes.

Each plane seals, ships, and retains on its own — so (for example) the catalog
can be kept local-only while the data plane ships to Forge. The catalog
references data only by CID; byte location is resolved at read time through the
indexing-service. Sealed CARs ship as a guppy-style edge client
(`/blob/add` → PUT → `/ucan/conclude` → `/blob/accept` → `/index/add` against
sprue). See [`logstore/README.md`](./logstore/README.md).

### Storage tiers (LSM)

- **Hot** — the open segment per plane on local disk; fsynced before a PUT is
  acked (data before catalog).
- **Warm** — sealed segments retained locally for fast reads (per-plane
  size/age seal triggers and retention window).
- **Cold** — segments shipped to Forge; reads fall through to the network on a
  local miss.

## Running it

**As a library** (fx). A host adds the module and provides a logger, a Postgres
pool, the agent identity, and the sprue endpoint (via `Config`):

```go
app := fx.New(
    // host provides: *zap.Logger, *pgxpool.Pool, ingot.ServiceIdentity
    ingot.Module(cfg), // cfg sets UploadServiceURL/DID, IndexerEndpoint/DID, ...
)
```

There is also a non-fx escape hatch — `New(ctx, ServerConfig, ServerDeps)` +
`Server.Start`/`Server.Stop` — for hosts and tests that construct the
collaborators themselves.

**As a daemon** (`cmd/`, cobra/viper/fx):

```bash
ingot serve                       # standalone (in-memory, no Forge) or forge mode, per config
ingot login <email>               # authorize the agent against sprue (email flow)
ingot space generate --provision-to <email>   # mint/provision/grant the space
```

`serve` runs in **standalone** mode (in-memory registry, no Forge/Postgres, both
planes retained on disk — for local/dev S3) or **forge** mode (Postgres + the
sprue edge client + a login token store). The deployment context is the
[Forge deployment RFC](https://github.com/fil-one/RFC/blob/main/2026-05-filone-forge-deployment-proposal.md):
the S3 facade runs **at the edge**, co-located with a provider's piri or as a
standalone client — not inside the central upload-service.

## Build & test

```bash
make build   # GOWORK=off go build ./...
make test    # GOWORK=off go test ./...
make gen     # regenerate CBOR marshalers after changing bucket types
```

The `testing/` package boots a full in-process S3 listener backed by in-memory
fakes — the way to exercise the S3 → MST → LSM core without Postgres, piri, or
the indexing-service.

## Dependencies

ingot depends only on the Forge stack — `ucantone` (UCAN 1.0 primitives),
`libforge` (Forge capability definitions), the `indexing-service` query client,
`versitygw` (the S3 front end) — plus standard plumbing (pgx, goose, fx, zap,
go-cid). It must never import `fil-forge/sprue` or `fil-forge/guppy` (guppy
embeds ingot → cycle); the Forge-client subset it needs is carried in
`forgeclient/` + `tokenstore/`.

## Status

The S3 → MST → LSM core is exercised by an in-memory smoke suite (~66 pass), and
forge mode is verified end-to-end against a live sprue + piri + indexing-service
by smelt's `TestIngotNativeProvision` e2e (PUT → ship → GET round trip on a
self-provisioned space). Known gaps are tracked in
[`DESIGN_NOTES.md`](./DESIGN_NOTES.md).
