# app-worker service image (RFC 028 Part 3 W0)
#
# Stateless render service: renders user-authored dashboard apps inside
# per-request WASM sandboxes (pkg/appengine, wazero + QuickJS-ng). Root
# Go module, pure Go (wazero has no cgo dependency), so this mirrors
# Dockerfile.pipeline-observer/Dockerfile.pipeline-api's CGO_ENABLED=0
# alpine build rather than query-worker.Dockerfile's glibc/CGO setup
# (which is DuckDB-specific and does not apply here).
#
# syntax=docker/dockerfile:1.4
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY cmd/ ./cmd/
COPY pkg/ ./pkg/
COPY api/ ./api/

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -o /app-worker ./cmd/app-worker

FROM alpine:3.19
RUN apk add --no-cache ca-certificates

# Non-root: app-worker holds zero storage credentials and only needs to
# read its mounted service-token/cookie-key secrets + listen on a socket.
RUN addgroup -g 1000 datuplet && \
    adduser -D -u 1000 -G datuplet datuplet

COPY --from=builder /app-worker /usr/local/bin/app-worker

USER datuplet

# Configuration via environment variables (pkg/appworker.LoadConfig):
# - DATUPLET_API_URL                          pipeline-api base URL
# - DATUPLET_APPWORKER_LISTEN                 HTTP listen address (default :8090)
# - DATUPLET_APPWORKER_SERVICE_TOKEN_FILE     mounted service-credential path
# - DATUPLET_APPWORKER_COOKIE_KEY_FILE        mounted viewer-cookie HMAC key path
# - DATUPLET_APPWORKER_{TIMEOUT_S,MAX_TIMEOUT_S,MEMORY_MIB,MAX_MEMORY_MIB,
#   QUERIES_PER_RENDER,MAX_QUERIES_PER_RENDER,OUTPUT_DOC_MAX_BYTES,
#   BUNDLE_MAX_BYTES,PER_APP_INFLIGHT,CONCURRENCY}
#   render-limit overrides; see pkg/appworker/config.go for defaults/caps
#   (spec §7).

EXPOSE 8090

ENTRYPOINT ["app-worker"]
