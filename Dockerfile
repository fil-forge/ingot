# ingot — embeddable S3 gateway over the Forge network, as a daemon.
#
# Multi-stage: cross-compile a static binary, then ship it on alpine with
# curl (smelt's healthcheck uses curl) + ca-certificates (TLS to sprue /
# the indexer). ingot is a standalone module — build with GOWORK=off.

FROM --platform=$BUILDPLATFORM golang:1.26-bookworm AS build
ARG TARGETOS=linux
ARG TARGETARCH
ENV GOWORK=off
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOWORK=off GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-s -w" -o /out/ingot ./cmd/ingot

FROM alpine:latest AS prod
RUN apk add --no-cache ca-certificates curl
COPY --from=build /out/ingot /usr/bin/ingot
# S3 listener (override addr in config; container should bind 0.0.0.0).
EXPOSE 9000
# The compose `command:` supplies the subcommand (e.g. serve --config ...),
# matching the smelt house style.
ENTRYPOINT ["/usr/bin/ingot"]
