# ingot — embeddable S3 gateway over the Forge network.
#
# ingot is a standalone module that must be built with the workspace disabled
# (the parent go.work declares a go version above the installed toolchain), so
# every go invocation here forces GOWORK=off.

# Force the workspace off for every recipe (see CLAUDE.md "Build, test, run").
export GOWORK := off

GO ?= go

.DEFAULT_GOAL := build

.PHONY: build test itest itest-shard s3compat gen gen-check clean help

## build: compile the daemon binary (-> ./ingot)
build:
	$(GO) build -o ingot ./cmd/ingot

## test: run the unit test suite (fast, no Docker)
test:
	$(GO) test ./...

## itest: run the integration test suite (boots the Forge stack in Docker; ~20 min)
itest:
	$(GO) test -tags itest -v -timeout 30m ./itest

# itest sharding. The suite is one Go package, so its tests run serially and
# every top-level test boots its own Forge stack (~40s of the runtime below).
# CI runs the shards on separate runners so the wall clock is the slowest
# shard rather than the sum (.github/workflows/go-test.yml, job
# `itest-shards`).
#
# Shards are balanced by measured runtime, not by file or theme. The comments
# are the time each test takes with its stack boot included; rebalance here
# when `go test -tags itest -v` shows one has moved.
SHARD_encryption  := TestForgeEncryption              # 6m00
SHARD_conformance := TestForgeVersity                 # 3m48
SHARD_conformance += TestForgeAWSCLI                  # 1m09
SHARD_conformance += TestForgeCopyAuthorization       # 0m50
SHARD_uploads     := TestForgeMaxSizePart             # 2m10, needs INGOT_ITEST_BIG
SHARD_uploads     += TestForgeMultipartExpiryShred    # 1m38
SHARD_uploads     += TestForgeScenarios               # 1m17
SHARD_uploads     += TestForgeDeferredMultipart       # 0m52

# TestForgeS3Compat runs in its own CI job and belongs to no shard.
SHARD_CLAIMED := $(SHARD_encryption) $(SHARD_conformance) $(SHARD_uploads) TestForgeS3Compat

# `rest` is the complement, so a test added to itest/ runs there rather than
# matching no shard and silently never running: Delete, Retention, Eviction,
# NativeProvision today (~5m27 in total).
SHARD_ALL  = $(filter Test%,$(shell GOWORK=off $(GO) test -tags itest -list '.*' ./itest))
SHARD_rest = $(filter-out $(SHARD_CLAIMED),$(SHARD_ALL))

# Space-to-pipe: make has no join function, so $(space) is the usual way to
# name a literal space for $(subst). $(strip) collapses the runs of whitespace
# the trailing comments above leave behind.
empty :=
space := $(empty) $(empty)
shard-regex = ^($(subst $(space),|,$(strip $(SHARD_$(1)))))$$

## itest-shard: run one shard of the integration suite (SHARD=encryption|conformance|uploads|rest)
itest-shard:
	@test -n "$(strip $(SHARD))" || { echo "itest-shard: set SHARD=encryption|conformance|uploads|rest"; exit 2; }
	@test -n "$(strip $(SHARD_$(SHARD)))" || { echo "itest-shard: SHARD=$(SHARD) selects no tests (unknown shard, or 'go test -list' failed above)"; exit 2; }
	$(GO) test -tags itest -v -timeout 20m -run '$(call shard-regex,$(SHARD))' ./itest

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
	@git diff --exit-code -- bucket/cbor_gen.go || { \
		echo "bucket/cbor_gen.go is stale; run 'make gen' and commit the result"; \
		exit 1; \
	}

## clean: remove the go build/test cache
clean:
	$(GO) clean -cache -testcache

## help: list available targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //'
