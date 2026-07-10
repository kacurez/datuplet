package validate

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/datuplet/datuplet/pkg/lib/secrets"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// refSentinel is substituted for every whole-scalar $[name] value before the
// config is handed to the JSON-Schema library. It is a valid non-empty string,
// so structural/type checks around a secret hold; content assertions that fire
// on it (pattern/enum/format/minLength) are anchored at the ref path and are
// dropped (RFC 026 §4.1) — placeholders must not be content-checked.
const refSentinel = "x"

const (
	msgSecretOnNonString = "secret reference not allowed on non-string field"
	msgSecretRequired    = "field requires a $[secret] reference"
	// xDatupletSecret is the vendor annotation marking a schema location whose
	// value must be a $[name] secret reference rather than a plaintext literal.
	xDatupletSecret = "x-datuplet-secret"
)

// CompileSchema parses and compiles a JSON Schema (2020-12) document. The
// vendor keyword x-datuplet-secret is unknown to the library and is ignored
// during compilation (jsonschema/v6 treats unknown keywords as annotations);
// its meaning is applied separately by ValidateConfig via a raw-JSON walk.
func CompileSchema(raw string) (*jsonschema.Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(raw))
	if err != nil {
		return nil, err
	}
	c := jsonschema.NewCompiler()
	const loc = "schema.json"
	if err := c.AddResource(loc, doc); err != nil {
		return nil, err
	}
	return c.Compile(loc)
}

// ValidateConfig validates cfg against schema, layering Datuplet's secret-ref
// semantics (RFC 026 §4.9) on top of standard JSON-Schema validation. rawSchema
// is the source text schema was compiled from; it is walked directly because
// the library does not surface the unknown x-datuplet-secret keyword. Finding
// paths are prefixed with pathPrefix (e.g. "stages[0].components[0].config").
//
// The four steps mirror the brief exactly:
//  1. Collect ref paths — instance locations holding a whole-scalar $[name].
//  2. A ref on a field whose schema type excludes "string" is a finding.
//  3. A field annotated x-datuplet-secret holding a present, non-ref value is a
//     finding; an absent value is not (that is `required`'s concern).
//  4. Replace every ref with a string sentinel and run library validation;
//     drop any error anchored exactly at a ref path (content assertions must
//     not evaluate the placeholder).
func ValidateConfig(schema *jsonschema.Schema, rawSchema string, cfg map[string]any, pathPrefix string) []Finding {
	var findings []Finding

	// Step 1: ref paths (whole-scalar $[name] string leaves).
	refPaths := map[string]bool{}
	walkStrings(cfg, "", func(path, s string) {
		if isWholeScalarRef(s) {
			refPaths[path] = true
		}
	})

	// Raw schema tree for the introspection in steps 2 and 3. If it fails to
	// parse, both steps become no-ops (every location reads as free-form).
	var schemaDoc any
	_ = json.Unmarshal([]byte(rawSchema), &schemaDoc)

	// Step 2: refs on non-string fields.
	for path := range refPaths {
		node := resolveSchemaNode(schemaDoc, path)
		if node == nil {
			continue // unresolvable / free-form location — allowed
		}
		if !typeAdmitsString(node) {
			findings = append(findings, Finding{
				Path:     withPrefix(pathPrefix, path),
				Message:  msgSecretOnNonString,
				Severity: severityError,
			})
		}
	}

	// Step 3: x-datuplet-secret fields must hold a ref when present.
	var secretPaths [][]segment
	collectSecretPaths(schemaDoc, nil, &secretPaths)
	for _, segs := range secretPaths {
		var hits []instanceHit
		lookupInstances(cfg, segs, "", &hits)
		for _, h := range hits {
			if refPaths[h.path] {
				continue // present and a ref — satisfied
			}
			findings = append(findings, Finding{
				Path:     withPrefix(pathPrefix, h.path),
				Message:  msgSecretRequired,
				Severity: severityError,
			})
		}
	}

	// Step 4: library validation over the sentinel-substituted config.
	sanitized := sanitizeRefs(cfg)
	err := schema.Validate(sanitized)
	if err == nil {
		return findings
	}
	verr, ok := err.(*jsonschema.ValidationError)
	if !ok {
		return append(findings, Finding{Path: pathPrefix, Message: err.Error(), Severity: severityError})
	}
	var leaves []jsonschema.OutputUnit
	collectLeafErrors(verr.BasicOutput(), &leaves)
	for _, u := range leaves {
		dotted := pointerToDotted(u.InstanceLocation)
		if refPaths[dotted] {
			continue // content assertion on a placeholder — drop it
		}
		findings = append(findings, Finding{
			Path:     withPrefix(pathPrefix, dotted),
			Message:  u.Error.String(),
			Severity: severityError,
		})
	}
	return findings
}

// isWholeScalarRef reports whether s is exactly a "$[name]" secret reference,
// reusing the shared pkg/lib/secrets whole-scalar matcher.
func isWholeScalarRef(s string) bool {
	names, err := secrets.Validate(s)
	return err == nil && len(names) > 0
}

// segment is one step of an instance/schema path: a named object key, an array
// element (index value irrelevant — JSON-Schema `items` applies to all), or a
// wildcard over every key of an object (schema-form `additionalProperties`,
// which applies its subschema to all dynamic keys).
type segment struct {
	key      string
	isIndex  bool
	isAnyKey bool
}

// pathSegments splits a walkStrings-style dotted path (e.g. "a.b[0].c") into
// its ordered segments.
func pathSegments(path string) []segment {
	var segs []segment
	for i := 0; i < len(path); {
		switch path[i] {
		case '.':
			i++
		case '[':
			j := strings.IndexByte(path[i:], ']')
			if j < 0 {
				return segs
			}
			segs = append(segs, segment{isIndex: true})
			i += j + 1
		default:
			start := i
			for i < len(path) && path[i] != '.' && path[i] != '[' {
				i++
			}
			segs = append(segs, segment{key: path[start:i]})
		}
	}
	return segs
}

// resolveSchemaNode walks the raw schema tree following an instance path,
// descending through properties / additionalProperties (object keys) and items
// (array elements). It returns the schema node at that location, or nil when
// the location is not pinned down by the schema (free-form).
func resolveSchemaNode(root any, path string) map[string]any {
	node, ok := root.(map[string]any)
	if !ok {
		return nil
	}
	for _, seg := range pathSegments(path) {
		if seg.isIndex {
			items, ok := node["items"].(map[string]any)
			if !ok {
				return nil
			}
			node = items
			continue
		}
		next := descendKey(node, seg.key)
		if next == nil {
			return nil
		}
		node = next
	}
	return node
}

// descendKey resolves an object property to its subschema, falling back to a
// schema-form additionalProperties. A missing/boolean additionalProperties
// makes the key free-form (nil).
func descendKey(node map[string]any, key string) map[string]any {
	if props, ok := node["properties"].(map[string]any); ok {
		if sub, ok := props[key].(map[string]any); ok {
			return sub
		}
	}
	if ap, ok := node["additionalProperties"].(map[string]any); ok {
		return ap
	}
	return nil
}

// typeAdmitsString reports whether a schema node's `type` permits a string
// value. A missing type constrains nothing and therefore admits string.
func typeAdmitsString(node map[string]any) bool {
	t, ok := node["type"]
	if !ok {
		return true
	}
	switch tv := t.(type) {
	case string:
		return tv == "string"
	case []any:
		for _, x := range tv {
			if s, ok := x.(string); ok && s == "string" {
				return true
			}
		}
		return false
	default:
		return true
	}
}

// collectSecretPaths walks the raw schema, recording the instance-path
// segments of every node annotated "x-datuplet-secret": true. It descends
// properties (named keys), items (array elements), and schema-form
// additionalProperties (a wildcard over every dynamic key of an object) — the
// concrete locations a config can carry. A boolean additionalProperties is not
// a subschema and is skipped.
func collectSecretPaths(node any, prefix []segment, out *[][]segment) {
	m, ok := node.(map[string]any)
	if !ok {
		return
	}
	if b, ok := m[xDatupletSecret].(bool); ok && b {
		clone := append([]segment(nil), prefix...)
		*out = append(*out, clone)
	}
	if props, ok := m["properties"].(map[string]any); ok {
		for k, sub := range props {
			collectSecretPaths(sub, append(prefix, segment{key: k}), out)
		}
	}
	if items, ok := m["items"].(map[string]any); ok {
		collectSecretPaths(items, append(prefix, segment{isIndex: true}), out)
	}
	if ap, ok := m["additionalProperties"].(map[string]any); ok {
		collectSecretPaths(ap, append(prefix, segment{isAnyKey: true}), out)
	}
}

// instanceHit is the dotted path of a present config location.
type instanceHit struct {
	path string
}

// lookupInstances resolves a schema segment path against the config, expanding
// index segments over the actual array elements. Only present locations are
// emitted; a missing key/array yields no hits (absence is not a finding).
func lookupInstances(node any, segs []segment, path string, out *[]instanceHit) {
	if len(segs) == 0 {
		*out = append(*out, instanceHit{path: path})
		return
	}
	seg := segs[0]
	if seg.isIndex {
		arr, ok := node.([]any)
		if !ok {
			return
		}
		for i, child := range arr {
			lookupInstances(child, segs[1:], indexPath(path, i), out)
		}
		return
	}
	if seg.isAnyKey {
		// schema-form additionalProperties: the subschema applies to every
		// present key of the instance object at this location.
		m, ok := node.(map[string]any)
		if !ok {
			return
		}
		for k, child := range m {
			lookupInstances(child, segs[1:], joinKey(path, k), out)
		}
		return
	}
	m, ok := node.(map[string]any)
	if !ok {
		return
	}
	child, present := m[seg.key]
	if !present {
		return
	}
	lookupInstances(child, segs[1:], joinKey(path, seg.key), out)
}

// sanitizeRefs deep-copies v, replacing every whole-scalar $[name] string with
// the sentinel so the JSON-Schema library never evaluates a placeholder.
func sanitizeRefs(v any) any {
	switch node := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(node))
		for k, child := range node {
			out[k] = sanitizeRefs(child)
		}
		return out
	case []any:
		out := make([]any, len(node))
		for i, child := range node {
			out[i] = sanitizeRefs(child)
		}
		return out
	case string:
		if isWholeScalarRef(node) {
			return refSentinel
		}
		return node
	default:
		return v
	}
}

// collectLeafErrors flattens a BasicOutput tree to the units that carry an
// actual error (leaves), skipping grouping nodes.
func collectLeafErrors(u *jsonschema.OutputUnit, out *[]jsonschema.OutputUnit) {
	if u == nil {
		return
	}
	if len(u.Errors) == 0 {
		if u.Error != nil {
			*out = append(*out, *u)
		}
		return
	}
	for i := range u.Errors {
		collectLeafErrors(&u.Errors[i], out)
	}
}

// pointerToDotted converts a JSON pointer ("/a/b/0/c") into the walkStrings
// dotted form ("a.b[0].c") so it can be compared against ref paths.
func pointerToDotted(ptr string) string {
	if ptr == "" {
		return ""
	}
	var b strings.Builder
	for _, tok := range strings.Split(strings.TrimPrefix(ptr, "/"), "/") {
		tok = strings.ReplaceAll(tok, "~1", "/")
		tok = strings.ReplaceAll(tok, "~0", "~")
		if isAllDigits(tok) {
			b.WriteByte('[')
			b.WriteString(tok)
			b.WriteByte(']')
			continue
		}
		if b.Len() > 0 {
			b.WriteByte('.')
		}
		b.WriteString(tok)
	}
	return b.String()
}

func indexPath(path string, i int) string {
	return path + "[" + strconv.Itoa(i) + "]"
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// withPrefix joins the caller-supplied prefix to a config-relative dotted path.
func withPrefix(prefix, path string) string {
	switch {
	case path == "":
		return prefix
	case prefix == "":
		return path
	default:
		return prefix + "." + path
	}
}
