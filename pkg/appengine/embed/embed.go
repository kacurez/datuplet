// Package embed carries the committed QuickJS engine artifact (RFC 028 E0).
//
// engine.wasm is a wasm32-wasi REACTOR module (quickjs-ng + the C shim in
// pkg/appengine/shim/). It is regenerated only via `make engine-wasm`
// (utils/docker/engine-wasm.Dockerfile); CI does not rebuild it.
package embed

import _ "embed"

// EngineWasm is the compiled QuickJS engine module consumed by pkg/appengine.
//
//go:embed engine.wasm
var EngineWasm []byte
