# FIL-1197: request isolation and held proof-store invalidation

Review baseline: Ingot `82a70c9` (2026-11-10 checkout). This is a local,
source-backed regression result, not a reproduction against deployed Forge.
The ticket's November 3 correction remains authoritative: its earlier black-box
run did not isolate the cached authority and is not confirmed exploitation.

## What changed since the ticket

Current main already shares a revoked-CID set between key stores. Chain assembly
checks it, including through pointers the uploader retained after registry
eviction. `authorityFor` also requires a live chain for the actual command before
calling Forge. DeleteBucket no longer masks its request proof store. Durable
release tasks carry explicit cleanup proofs and can receive a currently lent
request store. Tenant-issued cleanup delegations intentionally outlive an access
key; invalidating an access key must not be confused with revoking these separate
tenant grants.

Two source boundaries still needed correction:

1. A context carrying an S3 request but missing/masking its proof store could
   fall back to the latest per-space upload or commit capture. That capture can
   belong to another access key.
2. `KeyProofs.ClearAll`, used when a revocation cursor is rejected and revocations
   may have been missed, flushed only the registry. Held pointers could still
   yield delegations and effective action sets, including ones no longer present
   in the registry. A late response could continue populating them.

## Resulting behavior

- A principal-bearing request must supply its own proof store. Both uploader
  cache lookup paths fail closed when the S3 request marker is present but that
  store is missing or masked. An explicit empty store still cannot borrow a
  different key's grants.
- `ClearAll` advances a shared atomic generation before flushing the registry.
  Every older key store becomes unusable for Add, Contains, ProofChain,
  ProofChainVia, permission insertion and permission checks. Previously captured
  iterators also check the generation before yielding. This reaches held and
  evicted pointers without enumerating their owners.
- A subsequent `For(key)` returns a new store; fresh authority can be added after
  Hilt authorizes the request. The revoked-CID record is retained.
- Explicit tenant-issued cleanup stores and valid background catalog shipping
  retain their existing contract. The per-space cache layout is unchanged:
  principal-bearing requests cannot borrow it at all, so adding an access-key
  dimension is unnecessary for that boundary. Every network operation still
  needs its own valid command chain.

The reset does not erase independently serialized task proofs, cancel an
already issued invocation, or revoke a tenant cleanup grant. Individual known
CID revocations retain their existing narrower behavior. Separate-worker and
deployed end-to-end behavior remain outside these unit tests.

## Verification

On cached Go 1.27.1 with `GOWORK=off`:

- New regressions fail on untouched `82a70c9` in a separate checkout: an iterator
  yields old authority after reset; a late refill/reset race retains a chain;
  request B borrows request A's captured store.
- The patched regressions pass. The two-principal case checks both cache paths,
  missing and masked request stores, actual remove/abort entry points, an empty
  B store, valid explicit tenant cleanup, preserved background catalog shipping,
  and rejection after a generation reset.
- `go test -race ./iam ./uploader -count=1` passes, including held/evicted cache
  pointers, late Add and permission refill, concurrent reset/refill, fresh-store
  recovery and the existing known-CID revocation tests.
- `go vet ./...` passes.
- `go test ./... -run '^$'` compiles all package/test targets.
- The standalone daemon builds to a temporary output path.
- `gofmt` and `git diff --check` pass.

The full database/OpenBao suite and Docker-backed smelt integration suite were
not run because the Docker daemon is unavailable. No live service, tenant data,
credential, deployment or authorization configuration was changed. Existing
cache-reset logs and task errors expose resets and missing-authority refusals;
no new operational service or metric is introduced.
