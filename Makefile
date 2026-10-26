# ingot — embeddable S3 gateway over the Forge network.
#
# ingot is a standalone module that must be built with the workspace disabled
# (the parent go.work declares a go version above the installed toolchain), so
# every go invocation here forces GOWORK=off.

# Force the workspace off for every recipe (see CLAUDE.md "Build, test, run").
export GOWORK := off

GO ?= go

.DEFAULT_GOAL := build

.PHONY: build test itest itest-group s3compat gen gen-check clean help

## build: compile the daemon binary (-> ./ingot)
build:
	$(GO) build -o ingot ./cmd/ingot

## test: run the unit test suite (fast, no Docker)
test:
	$(GO) test ./...

## itest: run the integration test suite (boots the Forge stack in Docker; ~20 min)
itest:
	$(GO) test -tags itest -v -timeout 30m ./itest

# itest groups. The suite is one Go package, so its tests run serially and
# every top-level test boots its own Forge stack (~40s of the runtime below).
# CI runs the groups on separate runners so the wall clock is the slowest
# group rather than the sum (.github/workflows/go-test.yml, job
# `itest-groups`).
#
# Groups are balanced by measured runtime, not by file or theme. The comments
# are the time each test takes with its stack boot included; rebalance here
# when `go test -tags itest -v` shows one has moved. The first test to run in
# a group also absorbs the runner's image pulls, about a minute.
GROUP_encryption  := TestForgeEncryption                  # 6m50
GROUP_conformance := TestForgeVersity                     # 3m45
GROUP_conformance += TestForgeAWSCLI                      # 2m36
GROUP_uploads     := TestForgeMaxSizePart                 # 3m02, needs INGOT_ITEST_BIG
GROUP_uploads     += TestForgeScenarios                   # 2m16
GROUP_uploads     += TestForgeMultipartExpiryShred        # 1m33
GROUP_releases    := TestForgeDeleteReleasesNetworkBlob   # 2m48, waits on piri's removal sweep
GROUP_releases    += TestForgeObjectCount                 # 2m19, waits on the registration sweeper
GROUP_releases    += TestForgeDeleteBucketReleases        # 1m52
GROUP_auth        := TestForgeAccessKeyExpiry             # 2m29, waits for the key to lapse
GROUP_auth        += TestForgeCopyAuthorization           # 0m51
GROUP_auth        += TestForgeRoutePermissions            # 0m50
GROUP_auth        += TestForgeBucketEncryptionPermissions # 0m49
GROUP_auth        += TestForgeBucketPolicy                # skips unless INGOT_ITEST_IAM=1 or a hilt override

# TestForgeS3Compat runs in its own CI job and belongs to no group.
GROUP_CLAIMED := $(GROUP_encryption) $(GROUP_conformance) $(GROUP_uploads) $(GROUP_releases) $(GROUP_auth) TestForgeS3Compat

# `rest` is the complement, so a test added to itest/ runs there rather than
# matching no group and silently never running: LocalBlobBudget (1m21),
# BucketEncryption (1m16), ReadAfterCatalogRetention (0m56), and
# ReadAfterEviction, UncachedWrites, NativeProvision and DeferredMultipart at
# ~0m50 each (~6m50 in total); MultipartManyParts skips unless
# INGOT_ITEST_MP_BENCH=1.
GROUP_ALL  = $(filter Test%,$(shell GOWORK=off $(GO) test -tags itest -list '.*' ./itest))
GROUP_rest = $(filter-out $(GROUP_CLAIMED),$(GROUP_ALL))

# Space-to-pipe: make has no join function, so $(space) is the usual way to
# name a literal space for $(subst). $(strip) collapses the runs of whitespace
# the trailing comments above leave behind.
empty :=
space := $(empty) $(empty)
group-regex = ^($(subst $(space),|,$(strip $(GROUP_$(1)))))$$

## itest-group: run one group of the integration suite (GROUP=encryption|conformance|uploads|releases|auth|rest)
itest-group:
	@test -n "$(strip $(GROUP))" || { echo "itest-group: set GROUP=encryption|conformance|uploads|releases|auth|rest"; exit 2; }
	@test -n "$(strip $(GROUP_$(GROUP)))" || { echo "itest-group: GROUP=$(GROUP) selects no tests (unknown group, or 'go test -list' failed above)"; exit 2; }
	$(GO) test -tags itest -v -timeout 20m -run '$(call group-regex,$(GROUP))' ./itest

# s3compat knobs (all optional): make s3compat TAGS=tier-1 CONCURRENCY=8 OUT=/tmp/r.html
S3COMPAT_OUT ?= $(CURDIR)/itest/ingot-s3compat.html
S3COMPAT_ENV := INGOT_S3COMPAT=1 INGOT_S3COMPAT_OUT="$(S3COMPAT_OUT)"
ifdef GROUPS
S3COMPAT_ENV += INGOT_S3COMPAT_GROUPS=$(GROUPS)
endif
ifdef TAGS
S3COMPAT_ENV += INGOT_S3COMPAT_TAGS=$(TAGS)
endif
ifdef CONCURRENCY
S3COMPAT_ENV += INGOT_S3COMPAT_CONCURRENCY=$(CONCURRENCY)
endif

## s3compat: run the S3 compatibility corpus against the Forge stack, write the HTML report (Docker; knobs: GROUPS= TAGS= CONCURRENCY= OUT=)
s3compat:
	@rm -f "$(S3COMPAT_OUT)"
	@echo "s3compat: running the corpus (vector failures are expected — the report is the output)"
	-$(S3COMPAT_ENV) $(GO) test -tags itest -v -timeout 30m -run TestForgeS3Compat ./itest
	@test -f "$(S3COMPAT_OUT)" || { echo "s3compat: no report produced — the run failed before writing it"; exit 1; }
	@echo "s3compat: report written to $(S3COMPAT_OUT)"

## gen: regenerate CBOR marshalers (idempotent)
gen:
	$(GO) generate ./...

## gen-check: regenerate, then fail if the committed generated files are stale
gen-check: gen
	@git diff --exit-code -- bucket/cbor_gen.go bucket/cbor_gen_tuple.go || { \
		echo "bucket/cbor_gen*.go is stale; run 'make gen' and commit the result"; \
		exit 1; \
	}

## clean: remove the go build/test cache
clean:
	$(GO) clean -cache -testcache

## help: list available targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //'
