package outputdoc

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Structural caps from spec §6.3 / §7. Both are document-wide totals that
// JSON Schema cannot express (it cannot sum across sibling arrays), so
// Validate enforces them in Go.
const (
	// MaxDocBytes is the OutputDoc structural size cap (2 MiB), measured
	// against the raw payload.
	MaxDocBytes = 2 << 20
	// MaxBlocks is the maximum number of blocks in one document. Every block
	// object counts, including container blocks (`tabs`) and every block
	// nested inside a tab or a modal.
	MaxBlocks = 64
)

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
	if err := compiler.AddResource("https://datuplet.io/schemas/outputdoc-v1.json", schemaObj); err != nil {
		return fmt.Errorf("failed to add schema resource: %w", err)
	}

	s, err := compiler.Compile("https://datuplet.io/schemas/outputdoc-v1.json")
	if err != nil {
		return fmt.Errorf("failed to compile schema: %w", err)
	}
	schema = s
	return nil
}

// Validate validates an OutputDoc against the v1 schema.
// It returns the first violation found, with a message naming the JSON pointer
// of the offending location. Beyond the schema it enforces the two structural
// caps and the id-uniqueness rule of spec §6.3, none of which JSON Schema can
// express: the 2 MiB payload cap, the document-wide 64-block cap, and
// document-global uniqueness of block ids (the `block=<id>` partial-render key,
// §4.2).
func Validate(doc []byte) error {
	if schemaErr != nil {
		return fmt.Errorf("schema compilation failed: %w", schemaErr)
	}

	// Structural size cap (§6.3), checked against the raw payload before
	// unmarshalling so an oversized document is never materialized.
	if len(doc) > MaxDocBytes {
		return fmt.Errorf("outputDoc is %d bytes, exceeds the %d byte cap", len(doc), MaxDocBytes)
	}

	var data interface{}
	if err := json.Unmarshal(doc, &data); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	// Validate against schema
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

	return checkStructuralCaps(data)
}

// checkStructuralCaps walks every block in the document — root blocks, blocks
// nested in tabs, and blocks nested in block- or table-row-level modals —
// enforcing document-global id uniqueness and the document-wide block cap.
func checkStructuralCaps(data interface{}) error {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil
	}

	blocks, ok := m["blocks"].([]interface{})
	if !ok {
		return nil
	}

	w := blockWalker{ids: make(map[string]bool)}
	if err := w.walk(blocks, ""); err != nil {
		return err
	}
	if w.count > MaxBlocks {
		return fmt.Errorf("document has %d blocks, exceeds the %d block cap", w.count, MaxBlocks)
	}
	return nil
}

// blockWalker carries the document-global id set and block tally across the
// whole recursion — a fresh map per nesting level would let a root block and a
// nested block share an id, making the `block=<id>` lookup ambiguous.
type blockWalker struct {
	ids   map[string]bool
	count int
}

func (w *blockWalker) walk(blocks []interface{}, path string) error {
	for i, b := range blocks {
		block, ok := b.(map[string]interface{})
		if !ok {
			continue
		}

		blockPath := fmt.Sprintf("%s/blocks/%d", path, i)
		w.count++

		if id, ok := block["id"].(string); ok {
			if w.ids[id] {
				return fmt.Errorf("duplicate block id at %s/id: %q", blockPath, id)
			}
			w.ids[id] = true
		}

		// A block of any type may carry an inline modal, whose content is the
		// same block vocabulary (§6.3).
		if err := w.walkModal(block["modal"], blockPath+"/modal"); err != nil {
			return err
		}

		blockType, _ := block["type"].(string)
		switch blockType {
		case "tabs":
			tabs, ok := block["tabs"].([]interface{})
			if !ok {
				continue
			}
			for j, t := range tabs {
				tab, ok := t.(map[string]interface{})
				if !ok {
					continue
				}
				nested, ok := tab["blocks"].([]interface{})
				if !ok {
					continue
				}
				if err := w.walk(nested, fmt.Sprintf("%s/tabs/%d", blockPath, j)); err != nil {
					return err
				}
			}
		case "table":
			rows, ok := block["rows"].([]interface{})
			if !ok {
				continue
			}
			for j, r := range rows {
				row, ok := r.(map[string]interface{})
				if !ok {
					continue
				}
				modalPath := fmt.Sprintf("%s/rows/%d/modal", blockPath, j)
				if err := w.walkModal(row["modal"], modalPath); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// walkModal descends into an inline modal's blocks. The lazy form
// (`modal: {param}`) carries no blocks and is a no-op here.
func (w *blockWalker) walkModal(v interface{}, path string) error {
	modal, ok := v.(map[string]interface{})
	if !ok {
		return nil
	}
	blocks, ok := modal["blocks"].([]interface{})
	if !ok {
		return nil
	}
	return w.walk(blocks, path)
}

// The schema definition, embedded at build time.
//
//go:embed schema.json
var schemaJSON string
