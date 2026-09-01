# Integration tests (itest)

Docker-backed integration tests behind the `itest` build tag. Everything here
runs against a **forge-mode ingot deployed in the smelt Forge stack** (sprue,
piri, indexing-service, postgres, ...) with a build of **this working tree**
bind-mounted over the published image — what you just edited is what runs.
There is no in-memory ingot anywhere: the deployment under test is the real
one. No smelt checkout needed; compose files travel with the Go import.

The pattern: `make test` while iterating (seconds, no Docker), `make itest`
when you're ready to wait for the real thing. CI does the same — unit tests
first, integration only after they pass.

```bash
make itest                                                  # everything (~10 min)
go test -tags itest ./itest -run 'TestForgeVersity/PutObject' -v          # one category
go test -tags itest ./itest -run 'TestForgeVersity/PutObject/success' -v  # one case
```

## Hilt-era provisioning and credentials

Since the hilt (tenant-management) integration, forge mode requires hilt and
the tests sign with **hilt-issued tenant credentials**, not the root account:
each test calls `hiltProvisionTenant` (stack_test.go), which provisions a
tenant + all-permission access key through hilt's Tenant API (curl inside the
hilt container, local-dev partner key) and returns the SigV4 credentials.
The old `ingot login` / `ingot space generate` self-provisioning CLI is gone.

The stack's service images are mutable `:main` tags that Docker never
re-pulls — if hilt errors with "unsupported DID method in did:web" or sprue
with "handler not found" (did:plc resolution and `/blob/list` landed on
sprue main 2026-07-17), re-pull `ghcr.io/fil-forge/{hilt,sprue,...}:main`.
To run against an upload-service (sprue) image the registry doesn't have
yet — e.g. one built from an unmerged branch — point the stack at it:

```bash
INGOT_ITEST_UPLOAD_IMAGE=<image> make itest
```

**Teardown-blocked XFail rows (historical):** before `DeleteObject` released
network blobs (FIL-588), a bucket that ever held a non-empty object body
could not be deleted — hilt's `/s3/bucket/delete` refuses non-empty spaces —
so dozens of cases passed their S3 assertions and failed their bucket-delete
teardown, and sat in the XFail tables marked "teardown-blocked". The
unexpected-pass ratchet flagged them all when the release path landed; they
now live in the pass tables.

## The conformance partition — `TestForgeVersity`

The S3 surface is gated by curated lists of upstream
[versitygw](https://github.com/versity/versitygw) integration cases in
`versity_{bucket,object,multipart,versioning}_test.go`: per upstream group, a table of
cases ingot must PASS and a table of known failures (**XFail**, reported as
SKIP). One shared stack serves all categories.

**The ratchet:** an expected-pass case failing fails the build; an XFail case
*unexpectedly passing* also errors — move its line from the xfail table to
the pass table (that's the promotion). The XFail tables are the precise,
reviewable statement of what ingot's S3 surface doesn't do yet.

**When bumping the versitygw dependency:** new upstream cases are NOT picked
up automatically — nothing enforces list exhaustiveness at runtime. Diff the
group dispatch lists (`tests/integration/group-tests.go`) against the tables
here and add new cases to the pass table (demote to xfail if they fail).

## Scenario tests

| Test | Covers | ~time |
|---|---|---|
| `TestForgeScenarios` | Ingot-unique behaviors upstream can't assert, on a stack whose ingot config lowers `max_blob_size` to 64 KiB (`testdata/config-smallblob.yaml`): coarse blob-split round-trip with spooled-by-digest proof (spool counted inside the container), zero-byte objects, a multipart part spanning multiple internal blobs, abort deleting the parts' spooled blobs, and failed-Complete session recovery. | ~3 min |
| `TestForgeNativeProvision` | Hilt onboarding end-to-end on a fresh stack: tenant + access key via hilt's Tenant API, then a PUT/GET round-trip over the real ship path. | ~1.7 min |
| `TestForgeReadAfterEviction` | The appliance read tier: PUT, wipe `/data/spool`, GET must re-fetch body blobs from piri via the local locator + `/content/retrieve`. | ~1.3 min |
| `TestForgeEncryption` | The end-to-end encryption suite, on a default-config stack (~254 MiB blobs, 256 KiB chunks): tampered-ciphertext rejection on whole and ranged GETs (corrupt the spooled envelope's final chunk — the failure is a mid-body stream error, headers are already out), DELETE of a multipart-created object releasing every part blob through piri, and the cheap 5 GiB+1 `EntityTooLarge` probe. The skipped subtests are assertions blocked on unbuilt features (overwrite atomicity, DELETE cryptoshred, multipart key-row hygiene) — each skip names the missing capability. | ~3 min |
| `TestForgeMaxSizePart` | Exactly 5 GiB — the AWS-matching max part size — as one multipart part (21 internal blobs at the default `max_blob_size`): HEAD, ranged spot checks across chunk and blob boundaries, full stream-compared read-back. The end-to-end regression gate for `bucket.DefaultMaxBlobSize`'s envelope allowance under piri's 266338304-byte piece cap. **Skipped unless `INGOT_ITEST_BIG=1`** (CI sets it): ~10–15 GiB of disk churn, several minutes. | minutes (gated) |

Notes:

- `GOWORK=off` matches the Makefile convention (a parent `go.work` may declare
  a newer Go than the installed toolchain); `make itest` sets it for you.
- **One itest run per Docker host at a time** — the pre-test sweep removes
  every `smeltery-*` container, including another suite's live stack.
- The other services run their published `:main` images — `docker pull` them
  occasionally; compose won't refresh an existing tag.
- CI runs this suite on every PR after unit tests pass
  (`.github/workflows/go-test.yml`, job `itest`).
