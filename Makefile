# ingot — embeddable S3 gateway over the Forge network.
#
# ingot is a standalone module that must be built with the workspace disabled
# (the parent go.work declares a go version above the installed toolchain), so
# every go invocation here forces GOWORK=off.

# Force the workspace off for every recipe (see CLAUDE.md "Build, test, run").
export GOWORK := off

GO ?= go

.DEFAULT_GOAL := build

.PHONY: build test gen clean help

## build: compile all packages
build:
	$(GO) build ./...

## test: run the full test suite (uncached)
test:
	$(GO) test ./...

## gen: regenerate CBOR marshalers (idempotent)
gen:
	$(GO) generate ./...

## clean: remove the go build/test cache
clean:
	$(GO) clean -cache -testcache

## help: list available targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //'
