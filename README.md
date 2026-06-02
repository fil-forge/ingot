# ingot

> **⚠️ Work in progress.** ingot is under active development and its design is
> changing rapidly — interfaces, on-disk formats, and the Forge upload path are
> all still in flux. Expect breaking changes.

**ingot is an embeddable S3 gateway over the [Forge](https://github.com/fil-forge)
network, built around a Merkle Search Tree (MST) ported from
[bluesky-social/indigo](https://github.com/bluesky-social/indigo/tree/main/mst).**

It is a Go library — not a standalone daemon — that a host process (piri, guppy,
or sprue) imports and runs in-process. It speaks the S3 REST protocol on one
side and the Forge UCAN control plane on the other.

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
  fork's only external dependency is `go-cid`, keeping ingot's build and
  dependency tree lean.
- **Freedom to diverge.** The on-disk format starts identical to atproto's, but
  cross-implementation compatibility is intentionally not a goal — a fork lets
  the structure evolve alongside ingot instead of tracking indigo's releases.

## What it does

ingot presents each S3 bucket as a per-bucket **Merkle Search Tree (MST)**,
journals mutations to a local **LSM-style log**, and asynchronously ships sealed
segments to the Forge network (piri storage nodes + the indexing-service). Reads
fall through local tiers and finally to the network.

```
versitygw (S3 REST: sigv4, path-style)
   → s3frontend.Backend
      WRITE: per-bucket txn → chunk body → MST → local log (CAR, fsync)
             → CAS root in Postgres → 200 OK
             → [background] ship sealed segments to piri + indexer
      READ:  open segment → sealed segments → Forge network
```

### How buckets map onto the MST and Postgres

Each bucket is one MST: an ordered map from **object key → object-manifest CID**.
A manifest records the object's size, its sha256 and md5, the S3 system headers
and user metadata, and a pointer to the chunked body DAG. Because every node is
addressed by its own hash, a bucket rolls up to a single **root CID** — a
cryptographic commitment to the exact set of objects it holds — and the tree's
ordered keys make S3 prefix/delimiter listings fall out of ordinary traversal.

Writes are functional: a PUT or DELETE rewrites only the nodes on the path from
the changed key up to the root (every other node is immutable and shared),
producing a **new root CID**. ingot journals the changed blocks to the local
log, then **compare-and-swaps the bucket's root in Postgres** from the old CID
to the new one. That split is the heart of the design:

- **The MST is the data** — immutable, content-addressed, self-verifying, and
  shippable to the Forge network exactly as it sits on disk.
- **Postgres is the mutable index** — it holds the authoritative *current* root
  per bucket (the compare-and-swap is what keeps a bucket single-writer-correct)
  and tracks each log segment's hot → warm → cold lifecycle, all under the
  `ingot` schema.

A GET resolves the bucket's current root from Postgres, walks the MST down to
the key's manifest, and streams the object's blocks back through the layered
blockstore — local segments first, then the Forge network.

### Storage tiers (LSM)

- **Hot** — current open segment on local disk; fsynced before a PUT is acked.
- **Warm** — sealed segments retained locally for fast reads (64 MiB / 5s seal).
- **Cold** — segments shipped to Forge; reads fall through to the network on a
  local miss.

## Why a library, not a service

Per the [Forge deployment RFC](https://github.com/fil-one/RFC/blob/main/2026-05-filone-forge-deployment-proposal.md),
the S3 facade runs **at the edge** — co-located
with a provider's guppy+piri, or as a standalone client — not inside the central
upload-service. Packaging it as a library lets:

1. A storage provider's piri/guppy embed it: clients talk S3 to the local
   gateway, bytes land on the local piri, orchestration goes to sprue.
2. A client run it locally (guppy-style): talk S3 locally, ship to Forge.

## Using it

ingot wires up with [uber-go/fx](https://github.com/uber-go/fx). A host adds the
module to its graph and provides a logger, a Postgres pool, a service identity,
and a provider selector (or a single home-piri via config):

```go
app := fx.New(
    // host provides: *zap.Logger, *pgxpool.Pool, ingot.ServiceIdentity
    ingot.Module(cfg),
)
```

There is also a non-fx escape hatch — `New(ctx, ServerConfig, ServerDeps)` plus
`Server.Start` / `Server.Stop` — for hosts and tests that construct the
collaborators themselves.

## Build & test

ingot is a standalone module; build it with the workspace disabled.

```bash
# Build
make build

# Test
make test

# Regenerate CBOR marshalers (after changing bucket types)
make gen

```

The `testing/` package boots a full in-process S3 listener backed by in-memory
fakes — the way to exercise ingot end-to-end without Postgres, piri, or the
indexing-service.

## Dependencies

ingot depends only on the Forge stack — `ucantone` (UCAN 1.0 primitives),
`libforge` (Forge capability definitions), the `indexing-service` query client,
`versitygw` (the S3 front end) — plus standard plumbing (pgx, goose, fx, zap,
go-cid).

## Status

The S3 → MST → LSM core is exercised by an in-memory smoke suite (~87 pass).
The network-facing Forge glue is compiled but not yet verified against live
infrastructure.