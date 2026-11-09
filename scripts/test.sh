#!/usr/bin/env bash
# Runs the test suite against the services it needs: a Postgres the tests can
# create databases on, and a dev-mode OpenBao for the region-key provider. A
# service whose variable is already set (INGOT_TEST_DSN, INGOT_TEST_BAO_ADDR)
# is used as is; any other is started in Docker on a port Docker picks, and
# removed when the run ends. Arguments go to `go test`, ./... by default. The
# Makefile, which runs this, turns the Go workspace off.
set -euo pipefail

started=()
cleanup() {
	if ((${#started[@]})); then
		docker rm -f "${started[@]}" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

# port prints the host port Docker mapped to the container's port $2.
port() { docker port "$1" "$2/tcp" | head -n1 | sed 's/.*://'; }

# await runs the command after $1 until it succeeds, failing if container $1
# stops running or a minute passes first.
await() {
	local name=$1 deadline=$((SECONDS + 60))
	shift
	until "$@" >/dev/null 2>&1; do
		if [[ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" != true ]]; then
			echo "test: $name stopped before it was ready" >&2
			docker logs "$name" >&2 2>&1 || true
			exit 1
		fi
		if ((SECONDS >= deadline)); then
			echo "test: $name was not ready within a minute" >&2
			exit 1
		fi
		sleep 0.2
	done
}

if [[ -z "${INGOT_TEST_DSN:-}" ]]; then
	pg="ingot-test-pg-$$"
	docker run -d --rm --name "$pg" -e POSTGRES_PASSWORD=pw -p 127.0.0.1::5432 postgres:17 >/dev/null
	started+=("$pg")
	await "$pg" docker exec "$pg" pg_isready -U postgres -h 127.0.0.1
	export INGOT_TEST_DSN="postgres://postgres:pw@127.0.0.1:$(port "$pg" 5432)/postgres?sslmode=disable"
fi

if [[ -z "${INGOT_TEST_BAO_ADDR:-}" ]]; then
	bao="ingot-test-bao-$$"
	docker run -d --rm --name "$bao" -e BAO_DEV_ROOT_TOKEN_ID=root -p 127.0.0.1::8200 openbao/openbao:2.6.2 >/dev/null
	started+=("$bao")
	export INGOT_TEST_BAO_ADDR="http://127.0.0.1:$(port "$bao" 8200)"
	export INGOT_TEST_BAO_TOKEN=root
	await "$bao" curl -fs "$INGOT_TEST_BAO_ADDR/v1/sys/health"
fi

if (($# == 0)); then
	set -- ./...
fi
${GO:-go} test "$@"
