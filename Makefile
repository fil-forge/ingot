# ingot — embeddable S3 gateway over the Forge network.
#
# ingot is a standalone module that must be built with the workspace disabled
# (the parent go.work declares a go version above the installed toolchain), so
# every go invocation here forces GOWORK=off.

# Force the workspace off for every recipe (see CLAUDE.md "Build, test, run").
export GOWORK := off

GO ?= go

.DEFAULT_GOAL := build

.PHONY: build test dbtest itest gen gen-check clean help

# DSN for the live Postgres suite (`make dbtest`). Override to point at an
# existing database; the default matches the throwaway container below and the
# DSN the CI dbtest job uses (.github/workflows/go-test.yml).
INGOT_TEST_DSN ?= postgres://postgres:pw@127.0.0.1:55432/ingot

## build: compile the daemon binary (-> ./ingot)
build:
	$(GO) build -o ingot ./cmd/ingot

## test: run the unit test suite (fast, no Docker)
test:
	$(GO) test ./...

## dbtest: run the live migration + registry tests against a throwaway Postgres (Docker)
dbtest:
	@docker rm -f ingot-dbtest >/dev/null 2>&1 || true
	docker run -d --name ingot-dbtest \
		-e POSTGRES_PASSWORD=pw -e POSTGRES_DB=ingot \
		-p 55432:5432 postgres:17 >/dev/null
	@echo "waiting for postgres…"
	@for i in $$(seq 1 30); do \
		docker exec ingot-dbtest pg_isready -U postgres >/dev/null 2>&1 && break; \
		sleep 1; \
	done
	@INGOT_TEST_DSN="$(INGOT_TEST_DSN)" $(GO) test -count=1 -v -run Live ./registry/ ./migrations/; \
	status=$$?; \
	docker rm -f ingot-dbtest >/dev/null; \
	exit $$status

## itest: run the integration test suite (boots the Forge stack in Docker; ~6 min)
itest:
	$(GO) test -tags itest -v -timeout 30m ./itest

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
