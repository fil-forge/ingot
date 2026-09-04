# ingot — embeddable S3 gateway over the Forge network.
#
# ingot is a standalone module that must be built with the workspace disabled
# (the parent go.work declares a go version above the installed toolchain), so
# every go invocation here forces GOWORK=off.

# Force the workspace off for every recipe (see CLAUDE.md "Build, test, run").
export GOWORK := off

GO ?= go

.DEFAULT_GOAL := build

.PHONY: build test itest s3compat gen gen-check clean help

## build: compile the daemon binary (-> ./ingot)
build:
	$(GO) build -o ingot ./cmd/ingot

## test: run the unit test suite (fast, no Docker)
test:
	$(GO) test ./...

## itest: run the integration test suite (boots the Forge stack in Docker; ~6 min)
itest:
	$(GO) test -tags itest -v -timeout 30m ./itest

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
