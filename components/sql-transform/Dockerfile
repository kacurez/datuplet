# Multi-stage build for sql-transform component (RFC 010)
# Note: Uses Debian base (not Alpine) — DuckDB prebuilt library requires glibc.
# syntax=docker/dockerfile:1.4
FROM golang:1.25-bookworm AS builder

WORKDIR /build

# Install build dependencies (CGO required for DuckDB).
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy entire project (required for replace directives in go.mod).
COPY . .

WORKDIR /build/components/sql-transform

# Build with CGO enabled (DuckDB requires C bindings) and cache mounts.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -tags=duckdb_arrow -o /sql-transform .

# Runtime stage — minimal Debian with libstdc++ for DuckDB.
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    libstdc++6 && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user with home directory.
# DuckDB extension installation writes to $HOME/.duckdb/.
RUN groupadd -g 1000 datuplet && \
    useradd -r -u 1000 -g datuplet -d /home/datuplet -m datuplet

USER datuplet
ENV HOME=/home/datuplet

COPY --from=builder /sql-transform /usr/local/bin/sql-transform

ENTRYPOINT ["/usr/local/bin/sql-transform"]
