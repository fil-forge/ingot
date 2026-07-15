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

## The conformance partition — `TestForgeVersity`

The S3 surface is gated by curated lists of upstream
[versitygw](https://github.com/versity/versitygw) integration cases in
`versity_{bucket,object,multipart}_test.go`: per upstream group, a table of
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
| `TestForgeNativeProvision` | Guppy-free onboarding: `ingot login` + `space generate --provision-to` in the container, then a PUT/GET round-trip over the real ship path. | ~1.7 min |
| `TestForgeReadAfterEviction` | The appliance read tier: PUT, wipe `/data/spool`, GET must re-fetch body blobs from piri via the local locator + `/content/retrieve`. | ~1.3 min |

Notes:

- `GOWORK=off` matches the Makefile convention (a parent `go.work` may declare
  a newer Go than the installed toolchain); `make itest` sets it for you.
- **One itest run per Docker host at a time** — the pre-test sweep removes
  every `smeltery-*` container, including another suite's live stack.
- The other services run their published `:main` images — `docker pull` them
  occasionally; compose won't refresh an existing tag.
- CI runs this suite on every PR after unit tests pass
  (`.github/workflows/go-test.yml`, job `itest`).
