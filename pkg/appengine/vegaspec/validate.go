// Package vegaspec validates chart-block specs against the restricted
// Vega-Lite subset of RFC 028 spec §6.4.
//
// A Vega-Lite spec is executable configuration rendered in the viewer's
// browser. Full Vega-Lite can load remote data (`data.url`), render remote
// images, follow links and evaluate expressions. This subset is the boundary
// that stops an app author from exfiltrating query results or phoning home
// from the trusted shell, so over-permissiveness here is a security hole, not
// a cosmetic bug.
//
// The normative artifact is schema.json, which the app-worker embeds and the
// browser shell byte-copies (Part 4 vendors it as
// ui/appshell/vegaspec.schema.json). Keep both in sync; the schema may only
// ever *narrow* §6.4.
package vegaspec

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// forbiddenKeys is the mandatory negative list of spec §6.4, hard-coded
// independently of schema.json. schema.json already rejects every one of
// these via `additionalProperties: false`, but this list is the belt to that
// suspenders: it names the offending key explicitly instead of emitting a
// generic additionalProperties error, and it keeps the rejection standing
// even if a future schema edit accidentally widens a level.
//
// `hconcat` and `vconcat` are the concrete Vega-Lite spellings of the `concat`
// composition §6.4 rejects.
var forbiddenKeys = map[string]string{
	"config":   "chart styling is platform-owned; the shell applies the theme at embed time",
	"usermeta": "arbitrary metadata is not part of the subset",
	"layer":    "composition is not supported in v1 (single view only)",
	"facet":    "composition is not supported in v1 (single view only)",
	"concat":   "composition is not supported in v1 (single view only)",
	"hconcat":  "composition is not supported in v1 (single view only)",
	"vconcat":  "composition is not supported in v1 (single view only)",
	"repeat":   "composition is not supported in v1 (single view only)",
	"resolve":  "composition is not supported in v1 (single view only)",
	"params":   "selection parameters are not supported in v1",
}

var (
	schema    *jsonschema.Schema
	once      sync.Once
	schemaErr error
)

func init() {
	once.Do(func() {
		if err := compileSchema(); err != nil {
			schemaErr = err
		}
	})
}

func compileSchema() error {
	// Parse the embedded schema
	var schemaObj interface{}
	if err := json.Unmarshal([]byte(schemaJSON), &schemaObj); err != nil {
		return fmt.Errorf("failed to parse schema JSON: %w", err)
	}

	// Compile the schema
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("https://datuplet.io/schemas/vegaspec-v1.json", schemaObj); err != nil {
		return fmt.Errorf("failed to add schema resource: %w", err)
	}

	s, err := compiler.Compile("https://datuplet.io/schemas/vegaspec-v1.json")
	if err != nil {
		return fmt.Errorf("failed to compile schema: %w", err)
	}
	schema = s
	return nil
}

// Validate validates a Vega-Lite chart spec against the restricted subset.
// It returns the first violation found, with a message naming the JSON pointer
// of the offending location.
//
// The forbidden-key scan runs before the schema so that the mandatory negative
// list of §6.4 produces a message that names the key; everything else is
// enforced by schema.json.
func Validate(spec []byte) error {
	if schemaErr != nil {
		return fmt.Errorf("schema compilation failed: %w", schemaErr)
	}

	var data interface{}
	if err := json.Unmarshal(spec, &data); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	if err := checkForbiddenKeys(data, ""); err != nil {
		return err
	}

	if err := schema.Validate(data); err != nil {
		// Try to extract the instance location for better error reporting
		var location string
		if ve, ok := err.(*jsonschema.ValidationError); ok {
			location = "/" + strings.Join(ve.InstanceLocation, "/")
		}
		if location != "/" {
			return fmt.Errorf("validation failed at %s: %v", location, err)
		}
		return fmt.Errorf("validation failed: %w", err)
	}

	return nil
}

// checkForbiddenKeys walks the spec looking for any key on the §6.4 negative
// list, at any depth.
//
// It deliberately does not descend into `data` at the spec root: the rows of
// an inline dataset are *data*, not spec, and a query result may legitimately
// contain a column named `config` or `params`. The `data` object's own shape
// is fully pinned by schema.json (`{values: [...]}` and nothing else), so
// skipping it here loses no coverage.
func checkForbiddenKeys(v interface{}, pointer string) error {
	switch node := v.(type) {
	case map[string]interface{}:
		for _, key := range sortedKeys(node) {
			if reason, bad := forbiddenKeys[key]; bad {
				return fmt.Errorf("forbidden key %q at %s/%s: %s", key, pointer, key, reason)
			}
			if pointer == "" && key == "data" {
				continue
			}
			if err := checkForbiddenKeys(node[key], pointer+"/"+key); err != nil {
				return err
			}
		}
	case []interface{}:
		for i, item := range node {
			if err := checkForbiddenKeys(item, fmt.Sprintf("%s/%d", pointer, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

// sortedKeys makes the reported violation deterministic when a spec carries
// more than one forbidden key.
func sortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// The schema definition, embedded at build time.
//
//go:embed schema.json
var schemaJSON string
