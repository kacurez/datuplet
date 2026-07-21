// resolveProduces: resolve a component's runtime-produced table names from
// its stored config, via the schema root's `x-datuplet-produces` annotation
// (RFC 027 §6, spec table under "New JSON Schema extension keywords").
//
// A component with a dynamic output (`outputs.defaultBucket`, no explicit
// `outputs.tables`) names its tables at runtime from its own config — the
// schema can point at where those names live with a small path expression,
// e.g. data-generator sets `x-datuplet-produces: "tables[*].name"`.
//
// Supported path grammar (exactly two segment kinds, joined by `.`):
//   - `key`      — plain object property access
//   - `key[*]`   — map over the array at `key`, resolving the REST of the
//                  path against each element
//
// This is intentionally not a general JSONPath: it's ~20 LOC so a reviewer
// can eyeball the whole resolution rule in one sitting.
//
// @param {string} pathExpr - dot-joined path, e.g. "tables[*].name".
// @param {*} config - the component's `config` object to resolve against.
// @returns {string[]} flat list of resolved table names (only string leaves
//   are kept). Never throws: a path that doesn't match the config shape
//   (missing key, non-array where `[*]` is expected, non-object where a key
//   is expected, non-string leaf) simply contributes nothing — this is a
//   best-effort UI helper, not a validator.
//
// @example
//   resolveProduces('tables[*].name', { tables: [{ name: 'events' }, { name: 'clicks' }] })
//   // => ['events', 'clicks']
export function resolveProduces(pathExpr, config) {
  if (typeof pathExpr !== 'string' || !pathExpr.trim()) return [];
  return walk(pathExpr.split('.'), 0, config);
}

// SEGMENT_RE splits one dot-delimited segment into its key and an optional
// trailing "[*]" wildcard marker.
const SEGMENT_RE = /^([^[\]]+)(\[\*\])?$/;

function walk(segments, i, value) {
  if (value === null || value === undefined) return [];
  if (i >= segments.length) return typeof value === 'string' ? [value] : [];

  const m = SEGMENT_RE.exec(segments[i]);
  if (!m) return []; // malformed segment — fail soft, not a validator.
  const [, key, isWildcard] = m;

  if (typeof value !== 'object' || Array.isArray(value)) return [];
  const next = value[key];

  if (isWildcard) {
    if (!Array.isArray(next)) return [];
    const out = [];
    for (const item of next) out.push(...walk(segments, i + 1, item));
    return out;
  }
  return walk(segments, i + 1, next);
}
