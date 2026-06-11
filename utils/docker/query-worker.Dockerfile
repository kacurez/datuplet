# Multi-stage build for query-worker (RFC 022 Task 2.3).
#
# Uses Debian bookworm base (not Alpine): DuckDB's prebuilt library requires glibc.
# Mirrors components/sql-transform/Dockerfile conventions (same base images,
# BuildKit cache mounts, non-root user).
#
# Phase 0 — DuckDB extension baking:
#   Production pods run behind a NetworkPolicy that blocks outbound TCP, so
#   runtime `INSTALL iceberg` / `INSTALL httpfs` would fail.  Also,
#   engine.lock() applies `disabled_filesystems=LocalFileSystem` which would
#   block any post-lock INSTALL that touches the local FS.
#
#   To pre-bake: the builder stage runs utils/docker/install-duckdb-ext/main.go,
#   a tiny throwaway program that opens an embedded DuckDB (full network + FS
#   available in the builder) and executes INSTALL iceberg + INSTALL httpfs.
#   DuckDB writes the compiled extensions under $HOME/.duckdb/extensions/.
#   The COPY below transfers that directory into the runtime image at the
#   non-root user's $HOME so the query-worker's `INSTALL iceberg` resolves the
#   already-present local copy — treated as a no-op download — instead of
#   hitting the network.
#
# syntax=docker/dockerfile:1.4
FROM golang:1.25-bookworm AS builder

WORKDIR /build

# Build dependencies (CGO required for DuckDB).
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy the full repo so replace directives in go.mod resolve correctly.
COPY . .

# Build the query-worker binary from the queryengine module.
WORKDIR /build/components/queryengine
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -tags=duckdb_arrow -o /query-worker ./cmd/query-worker

# Phase 0: run the extension installer to bake iceberg + httpfs into the image.
# The installer is a standalone Go module under utils/docker/install-duckdb-ext/.
# It is compiled and run here in the builder (network available, local FS writable),
# writing extensions to /root/.duckdb/extensions/.  The installer binary itself
# is NOT copied to the runtime image.
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

# Non-root user with a home directory.
# DuckDB resolves INSTALL from $HOME/.duckdb/extensions/; having extensions
# pre-baked here means the engine's `INSTALL iceberg` is a local no-op.
RUN groupadd -g 1000 datuplet && \
    useradd -r -u 1000 -g datuplet -d /home/datuplet -m datuplet

# Copy baked extensions from the builder stage.
COPY --from=builder --chown=datuplet:datuplet /root/.duckdb /home/datuplet/.duckdb

# Scratch dir for DuckDB temp files: query-worker default TempDir=/scratch.
RUN mkdir -p /scratch && chown datuplet:datuplet /scratch

USER datuplet
ENV HOME=/home/datuplet

COPY --from=builder /query-worker /usr/local/bin/query-worker

ENTRYPOINT ["/usr/local/bin/query-worker"]
