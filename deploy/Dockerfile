# manifold demo image.
#
# One image holds both binaries manifold needs to demonstrate itself:
# `manifold` (the proxy) and `bench/backend` (the configurable upstream used
# to fake fast/slow/flaky backends). docker-compose.yml picks which one each
# container runs via `command:`, so one build serves every service in the
# demo topology instead of maintaining two Dockerfiles.
#
# Build from the repo root (the module root), not this directory:
#   docker build -f deploy/Dockerfile -t manifold-demo .
# docker-compose.yml sets that context/dockerfile pair for you.

# ---- build stage -----------------------------------------------------------
FROM golang:1.24-alpine AS build
WORKDIR /src

# Layer the module download below the source copy so an edit to main.go
# doesn't invalidate the (slow, network-bound) dependency layer.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd/    ./cmd/
COPY bench/  ./bench/
COPY internal/ ./internal/

# CGO_ENABLED=0 is what makes these binaries static: no libc dependency, so
# the final stage needs nothing but the binary itself. -trimpath drops build
# machine paths from the binary; -s -w strips debug symbols the demo image
# has no use for.
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /out/manifold ./cmd/manifold
RUN go build -trimpath -ldflags="-s -w" -o /out/backend  ./bench/backend

# ---- final stage ------------------------------------------------------------
# Alpine over distroless here specifically for `wget`, which both binaries'
# healthcheck: entries in docker-compose.yml rely on -- distroless/static has
# no shell and no HTTP client, and adding one back defeats the point of
# choosing it. Alpine's own package footprint stays under 10MB.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates && \
    addgroup -S manifold && adduser -S -G manifold -H manifold

COPY --from=build /out/manifold /usr/local/bin/manifold
COPY --from=build /out/backend  /usr/local/bin/backend

USER manifold:manifold

# No ENTRYPOINT/CMD: this image runs two different programs depending on
# which service in docker-compose.yml starts it, each supplying its own
# `command:`. A default would be misleading either way.
