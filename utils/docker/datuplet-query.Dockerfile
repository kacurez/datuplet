# Multi-stage build for the datuplet-query binary (RFC 022 Task 3.1).
#
# datuplet-query is the BYO-local (mode c) ad-hoc SQL tool: it runs DuckDB on
# the user's own machine against the remote warehouse, reusing queryengine.Run.
# It is a SEPARATE install from the duckdb-FREE root `datuplet` CLI — the root
# binary cannot run DuckDB by design, so local query requires this image/binary.
#
# Uses Debian bookworm (not Alpine): DuckDB's prebuilt library requires glibc.
# Mirrors utils/docker/query-worker.Dockerfile: same base images, BuildKit
# cache mounts, baked iceberg + httpfs extensions, non-root user.
#
# Phase 0 — DuckDB extension baking: the builder runs
# utils/docker/install-duckdb-ext to pre-install iceberg + httpfs into
# $HOME/.duckdb/extensions/, so a locked-down engine's `INSTALL iceberg`
# resolves the local copy instead of hitting the network (matching the
# query-worker rationale).
#
# syntax=docker/dockerfile:1.4
FROM golang:1.25-bookworm AS builder

WORKDIR /build

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy the full repo so replace directives in go.mod resolve correctly.
COPY . .

# Build the datuplet-query binary from the queryengine module (duckdb_arrow,
# CGO required for DuckDB).
WORKDIR /build/components/queryengine
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -tags=duckdb_arrow -o /datuplet-query ./cmd/datuplet-query

# Bake iceberg + httpfs into the image (network available in the builder).
WORKDIR /build/utils/docker/install-duckdb-ext
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 HOME=/root go run -tags=duckdb_arrow . && \
    echo "Baked extension directory:" && \
    find /root/.duckdb -type f | sort

# ---- Runtime image --------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    libstdc++6 && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd -g 1000 datuplet && \
    useradd -r -u 1000 -g datuplet -d /home/datuplet -m datuplet

# Baked extensions resolve INSTALL iceberg/httpfs as a local no-op.
COPY --from=builder --chown=datuplet:datuplet /root/.duckdb /home/datuplet/.duckdb

USER datuplet
ENV HOME=/home/datuplet

COPY --from=builder /datuplet-query /usr/local/bin/datuplet-query

ENTRYPOINT ["/usr/local/bin/datuplet-query"]
