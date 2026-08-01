package outputdoc

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

var (
	schema *jsonschema.Schema
	once   sync.Once
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
// of the offending location. It also enforces uniqueness of block IDs, which
// cannot be expressed in JSON Schema.
func Validate(doc []byte) error {
	if schemaErr != nil {
		return fmt.Errorf("schema compilation failed: %w", schemaErr)
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

	// Check for duplicate block IDs
	if err := checkDuplicateIds(data); err != nil {
		return err
	}

	return nil
}

// checkDuplicateIds recursively checks for duplicate block IDs within the document
// and within any nested tabs blocks.
func checkDuplicateIds(data interface{}) error {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil
	}

	blocks, ok := m["blocks"].([]interface{})
	if !ok {
		return nil
	}

	return checkBlockIds(blocks, "")
}

// checkBlockIds checks for duplicate IDs in a block array and in any nested tabs.
func checkBlockIds(blocks []interface{}, path string) error {
	ids := make(map[string]bool)

	for i, b := range blocks {
		block, ok := b.(map[string]interface{})
		if !ok {
			continue
		}

		id, ok := block["id"].(string)
		if !ok {
			continue
		}

		blockPath := fmt.Sprintf("%s/blocks/%d", path, i)

		// Check for duplicate
		if ids[id] {
			return fmt.Errorf("duplicate block id at %s/id: %q", blockPath, id)
		}
		ids[id] = true

		// If this is a tabs block, recursively check nested tabs
		blockType, _ := block["type"].(string)
		if blockType == "tabs" {
			tabs, ok := block["tabs"].([]interface{})
			if ok {
				for j, t := range tabs {
					tab, ok := t.(map[string]interface{})
					if !ok {
						continue
					}
					nestedBlocks, ok := tab["blocks"].([]interface{})
					if !ok {
						continue
					}
					tabPath := fmt.Sprintf("%s/tabs/%d", blockPath, j)
					if err := checkBlockIds(nestedBlocks, tabPath); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

// The schema definition as a JSON string. This will be embedded at build time
// via a build script or manually. For now, we read it from the schema.json file
// at runtime in the compile function above. The actual schema bytes will be
// provided via a go:embed directive below.
//
//go:embed schema.json
var schemaJSON string
