// Bundles app.js into bundle.js: a single self-contained IIFE exposing the
// module's exports on `globalThis.__dtp_app` (spec §6.2 / Appendix A). The
// Datuplet engine evaluates that IIFE and calls
// `globalThis.__dtp_app.render(ctx)` directly — there is no module loader
// in the guest, so this bundling step is not optional.
//
// Run via `npm install && npm run build` (see package.json). Equivalent to
// the raw esbuild CLI invocation from the spec:
//
//   esbuild app.js --bundle --format=iife --global-name=__dtp_app --outfile=bundle.js
//
// `datuplet apps put <name> --bundle bundle.js` uploads the result.
import * as esbuild from "esbuild";

await esbuild.build({
  entryPoints: ["app.js"],
  outfile: "bundle.js",
  bundle: true,
  format: "iife",
  globalName: "__dtp_app",
  // No DOM, no Node builtins in the guest — target neither.
  platform: "neutral",
  // quickjs-ng supports modern syntax (optional chaining, nullish
  // coalescing, async/await); es2020 avoids esbuild both under- and
  // over-transpiling relative to what the engine actually runs.
  target: "es2020",
});
