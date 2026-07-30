# syntax=docker/dockerfile:1.7
#
# engine-wasm.Dockerfile — builds pkg/appengine/embed/engine.wasm: quickjs-ng
# compiled to wasm32-wasi with the Datuplet app-worker C shim
# (pkg/appengine/shim/shim.c, RFC 028 Part 0/3 "Engine ABI").
#
# Invoked via `make engine-wasm`:
#   docker build -f utils/docker/engine-wasm.Dockerfile -o pkg/appengine/embed .
# The `-o` (BuildKit local exporter) writes the final stage's filesystem —
# just engine.wasm — into pkg/appengine/embed/. The artifact is committed;
# CI does NOT rebuild it (contract-and-constraints.md, Engine ABI).
#
# Pinned versions (bump deliberately — this is a manually-triggered local
# build, not part of the automated dependency-bump flow):
#   quickjs-ng: v0.15.1  (https://github.com/quickjs-ng/quickjs)
#   wasi-sdk:   wasi-sdk-29 / 29.0 (https://github.com/WebAssembly/wasi-sdk)
#     — the release quickjs-ng's own CI (.github/workflows/ci.yml, `wasi`
#     job) builds and smoke-tests this exact quickjs-ng tag against.

FROM debian:bookworm-slim AS toolchain

ARG WASI_SDK_MAJOR=29
ARG WASI_SDK_VERSION=29.0
ARG QUICKJS_NG_TAG=v0.15.1

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl git xz-utils \
    && rm -rf /var/lib/apt/lists/*

# wasi-sdk ships prebuilt clang+sysroot tarballs per host arch; pick the one
# matching the machine actually running this RUN step (the wasm32-wasi
# *output* is architecture-independent — only the compiler toolchain itself
# needs to match the build host).
RUN set -eux; \
    case "$(uname -m)" in \
      x86_64)  WASI_ARCH=x86_64 ;; \
      aarch64) WASI_ARCH=arm64 ;; \
      *) echo "engine-wasm.Dockerfile: unsupported build host arch $(uname -m)" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/wasi-sdk.tar.gz \
      "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${WASI_SDK_MAJOR}/wasi-sdk-${WASI_SDK_VERSION}-${WASI_ARCH}-linux.tar.gz"; \
    mkdir -p /opt/wasi-sdk; \
    tar -xzf /tmp/wasi-sdk.tar.gz -C /opt/wasi-sdk --strip-components=1; \
    rm /tmp/wasi-sdk.tar.gz

RUN git clone --depth 1 --branch "${QUICKJS_NG_TAG}" \
      https://github.com/quickjs-ng/quickjs.git /src/quickjs-ng

FROM toolchain AS build

COPY pkg/appengine/shim/shim.c /src/quickjs-ng/shim.c

WORKDIR /src/quickjs-ng
RUN mkdir -p /out && /opt/wasi-sdk/bin/clang \
      --target=wasm32-wasi \
      --sysroot=/opt/wasi-sdk/share/wasi-sysroot \
      -O2 -D_GNU_SOURCE -D_WASI_EMULATED_SIGNAL \
      -I. \
      quickjs.c libregexp.c libunicode.c dtoa.c shim.c \
      -lwasi-emulated-signal \
      -Wl,--export=dtp_alloc,--export=dtp_render,--no-entry \
      -mexec-model=reactor \
      -o /out/engine.wasm

FROM scratch AS export
COPY --from=build /out/engine.wasm /engine.wasm
