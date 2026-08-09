package outputdoc

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestValidateValidDoc(t *testing.T) {
	tests := []struct {
		name string
		doc  map[string]interface{}
	}{
		{
			name: "minimal valid doc",
			doc: map[string]interface{}{
				"outputDoc": 1,
				"title":     "Test",
				"blocks":    []interface{}{},
			},
		},
		{
			name: "doc with markdown block",
			doc: map[string]interface{}{
				"outputDoc": 1,
				"title":     "Test",
				"blocks": []interface{}{
					map[string]interface{}{
						"id":   "md1",
						"type": "markdown",
						"text": "Some markdown",
					},
				},
			},
		},
		{
			name: "doc with metric block",
			doc: map[string]interface{}{
				"outputDoc": 1,
				"title":     "Test",
				"blocks": []interface{}{
					map[string]interface{}{
						"id":   "m1",
						"type": "metric",
						"items": []interface{}{
							map[string]interface{}{
								"label":  "Revenue",
								"value":  1000,
								"format": "currency:EUR",
							},
						},
					},
				},
			},
		},
		{
			name: "doc with table block",
			doc: map[string]interface{}{
				"outputDoc": 1,
				"title":     "Test",
				"blocks": []interface{}{
					map[string]interface{}{
						"id":      "t1",
						"type":    "table",
						"title":   "My Table",
						"columns": []interface{}{"A", "B"},
						"rows":    []interface{}{},
					},
				},
			},
		},
		{
			name: "doc with chart block",
			doc: map[string]interface{}{
				"outputDoc": 1,
				"title":     "Test",
				"blocks": []interface{}{
					map[string]interface{}{
						"id":      "c1",
						"type":    "chart",
						"library": "vega-lite",
						"spec": map[string]interface{}{
							"mark": "bar",
						},
					},
				},
			},
		},
		{
			name: "doc with filter block",
			doc: map[string]interface{}{
				"outputDoc": 1,
				"title":     "Test",
				"blocks": []interface{}{
					map[string]interface{}{
						"id":     "f1",
						"type":   "filter",
						"fields": []interface{}{},
					},
				},
			},
		},
		{
			name: "doc with tabs block",
			doc: map[string]interface{}{
				"outputDoc": 1,
				"title":     "Test",
				"blocks": []interface{}{
					map[string]interface{}{
						"id":   "tb1",
						"type": "tabs",
						"tabs": []interface{}{},
					},
				},
			},
		},
		{
			name: "doc with refreshInterval",
			doc: map[string]interface{}{
				"outputDoc":       1,
				"title":           "Test",
				"blocks":          []interface{}{},
				"refreshInterval": 300,
			},
		},
		{
			name: "appendix A worked example",
			doc:  appendixAExample(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc, err := json.Marshal(tt.doc)
			if err != nil {
				t.Fatalf("failed to marshal doc: %v", err)
			}
			err = Validate(doc)
			if err != nil {
				t.Fatalf("expected valid doc but got error: %v", err)
			}
		})
	}
}

func TestValidateMissingOutputDoc(t *testing.T) {
	doc := map[string]interface{}{
		"title":  "Test",
		"blocks": []interface{}{},
	}
	data, _ := json.Marshal(doc)
	err := Validate(data)
	if err == nil {
		t.Fatal("expected error for missing outputDoc")
	}
	if err.Error() == "" {
		t.Fatal("expected error message")
	}
}

func TestValidateWrongOutputDocValue(t *testing.T) {
	tests := []interface{}{
		2,
		"1",
	}
	for _, val := range tests {
		doc := map[string]interface{}{
			"outputDoc": val,
			"title":     "Test",
			"blocks":    []interface{}{},
		}
		data, _ := json.Marshal(doc)
		err := Validate(data)
		if err == nil {
			t.Fatalf("expected error for outputDoc=%v", val)
		}
	}
}

func TestValidateMissingTitle(t *testing.T) {
	doc := map[string]interface{}{
		"outputDoc": 1,
		"blocks":    []interface{}{},
	}
	data, _ := json.Marshal(doc)
	err := Validate(data)
	if err == nil {
		t.Fatal("expected error for missing title")
	}
}

func TestValidateUnknownBlockType(t *testing.T) {
	doc := map[string]interface{}{
		"outputDoc": 1,
		"title":     "Test",
		"blocks": []interface{}{
			map[string]interface{}{
				"id":   "b1",
				"type": "unknown",
			},
		},
	}
	data, _ := json.Marshal(doc)
	err := Validate(data)
	if err == nil {
		t.Fatal("expected error for unknown block type")
	}
}

func TestValidateMissingBlockId(t *testing.T) {
	doc := map[string]interface{}{
		"outputDoc": 1,
		"title":     "Test",
		"blocks": []interface{}{
			map[string]interface{}{
				"type": "markdown",
				"text": "test",
			},
		},
	}
	data, _ := json.Marshal(doc)
	err := Validate(data)
	if err == nil {
		t.Fatal("expected error for missing block id")
	}
}

func TestValidateDuplicateBlockId(t *testing.T) {
	doc := map[string]interface{}{
		"outputDoc": 1,
		"title":     "Test",
		"blocks": []interface{}{
			map[string]interface{}{
				"id":   "b1",
				"type": "markdown",
				"text": "test",
			},
			map[string]interface{}{
				"id":   "b1",
				"type": "markdown",
				"text": "test2",
			},
		},
	}
	data, _ := json.Marshal(doc)
	err := Validate(data)
	if err == nil {
		t.Fatal("expected error for duplicate block id")
	}
}

func TestValidateTooManyBlocks(t *testing.T) {
	blocks := make([]interface{}, 65)
	for i := 0; i < 65; i++ {
		blocks[i] = map[string]interface{}{
			"id":   "b" + string(rune(i)),
			"type": "markdown",
			"text": "test",
		}
	}
	doc := map[string]interface{}{
		"outputDoc": 1,
		"title":     "Test",
		"blocks":    blocks,
	}
	data, _ := json.Marshal(doc)
	err := Validate(data)
	if err == nil {
		t.Fatal("expected error for >64 blocks")
	}
}

func TestValidateBlockExtraFields(t *testing.T) {
	doc := map[string]interface{}{
		"outputDoc": 1,
		"title":     "Test",
		"blocks": []interface{}{
			map[string]interface{}{
				"id":    "b1",
				"type":  "markdown",
				"text":  "test",
				"extra": "field",
			},
		},
	}
	data, _ := json.Marshal(doc)
	err := Validate(data)
	if err == nil {
		t.Fatal("expected error for extra block field")
	}
}

func TestValidateRefreshIntervalBounds(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		valid bool
	}{
		{"too low", 10, false},
		{"at min boundary", 15, true},
		{"in range", 300, true},
		{"at max boundary", 3600, true},
		{"too high", 3601, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := map[string]interface{}{
				"outputDoc":       1,
				"title":           "Test",
				"blocks":          []interface{}{},
				"refreshInterval": tt.value,
			}
			data, _ := json.Marshal(doc)
			err := Validate(data)
			if tt.valid && err != nil {
				t.Fatalf("expected valid but got error: %v", err)
			}
			if !tt.valid && err == nil {
				t.Fatalf("expected error for refreshInterval=%v", tt.value)
			}
		})
	}
}

func TestValidateChartRequiresLibrary(t *testing.T) {
	doc := map[string]interface{}{
		"outputDoc": 1,
		"title":     "Test",
		"blocks": []interface{}{
			map[string]interface{}{
				"id":   "c1",
				"type": "chart",
				"spec": map[string]interface{}{},
			},
		},
	}
	data, _ := json.Marshal(doc)
	err := Validate(data)
	if err == nil {
		t.Fatal("expected error for chart without library")
	}
}

func TestValidateChartRequiresVegaLite(t *testing.T) {
	doc := map[string]interface{}{
		"outputDoc": 1,
		"title":     "Test",
		"blocks": []interface{}{
			map[string]interface{}{
				"id":      "c1",
				"type":    "chart",
				"library": "other",
				"spec":    map[string]interface{}{},
			},
		},
	}
	data, _ := json.Marshal(doc)
	err := Validate(data)
	if err == nil {
		t.Fatal("expected error for chart with wrong library")
	}
}

func TestValidateChartRequiresSpec(t *testing.T) {
	doc := map[string]interface{}{
		"outputDoc": 1,
		"title":     "Test",
		"blocks": []interface{}{
			map[string]interface{}{
				"id":      "c1",
				"type":    "chart",
				"library": "vega-lite",
			},
		},
	}
	data, _ := json.Marshal(doc)
	err := Validate(data)
	if err == nil {
		t.Fatal("expected error for chart without spec")
	}
}

func TestValidateChartSpecMustBeObject(t *testing.T) {
	doc := map[string]interface{}{
		"outputDoc": 1,
		"title":     "Test",
		"blocks": []interface{}{
			map[string]interface{}{
				"id":      "c1",
				"type":    "chart",
				"library": "vega-lite",
				"spec":    "not an object",
			},
		},
	}
	data, _ := json.Marshal(doc)
	err := Validate(data)
	if err == nil {
		t.Fatal("expected error for chart spec that's not an object")
	}
}

func TestValidateRootExtraFields(t *testing.T) {
	doc := map[string]interface{}{
		"outputDoc": 1,
		"title":     "Test",
		"blocks":    []interface{}{},
		"extra":     "field",
	}
	data, _ := json.Marshal(doc)
	err := Validate(data)
	if err == nil {
		t.Fatal("expected error for extra root field")
	}
}

func TestValidateBlocksNotArray(t *testing.T) {
	doc := map[string]interface{}{
		"outputDoc": 1,
		"title":     "Test",
		"blocks":    "not an array",
	}
	data, _ := json.Marshal(doc)
	err := Validate(data)
	if err == nil {
		t.Fatal("expected error for blocks not being an array")
	}
}

func TestValidateFilterBlockFields(t *testing.T) {
	doc := map[string]interface{}{
		"outputDoc": 1,
		"title":     "Test",
		"blocks": []interface{}{
			map[string]interface{}{
				"id":   "f1",
				"type": "filter",
				"fields": []interface{}{
					map[string]interface{}{
						"name":  "days",
						"label": "Window",
						"kind":  "select",
					},
				},
			},
		},
	}
	data, _ := json.Marshal(doc)
	err := Validate(data)
	if err != nil {
		t.Fatalf("expected valid filter block but got error: %v", err)
	}
}

func TestValidateTabsBlock(t *testing.T) {
	doc := map[string]interface{}{
		"outputDoc": 1,
		"title":     "Test",
		"blocks": []interface{}{
			map[string]interface{}{
				"id":   "tb1",
				"type": "tabs",
				"tabs": []interface{}{
					map[string]interface{}{
						"label": "Tab 1",
						"blocks": []interface{}{
							map[string]interface{}{
								"id":   "b1",
								"type": "markdown",
								"text": "test",
							},
						},
					},
				},
			},
		},
	}
	data, _ := json.Marshal(doc)
	err := Validate(data)
	if err != nil {
		t.Fatalf("expected valid tabs block but got error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Fix round: spec §6.3 conformance (chart onClick, modals, doc-global id
// uniqueness, document-wide block cap, 2 MiB structural cap, filter options).
// ---------------------------------------------------------------------------

// docWith wraps blocks in a minimal valid root document and marshals it.
func docWith(t *testing.T, blocks ...interface{}) []byte {
	t.Helper()
	data, err := json.Marshal(map[string]interface{}{
		"outputDoc": 1,
		"title":     "Test",
		"blocks":    blocks,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

func markdownBlock(id string) map[string]interface{} {
	return map[string]interface{}{"id": id, "type": "markdown", "text": "x"}
}

// §6.3: "a chart block may declare onClick: {param: "…"}; the shell sets the
// param and re-renders."
func TestValidateChartOnClick(t *testing.T) {
	chart := func(onClick interface{}) map[string]interface{} {
		b := map[string]interface{}{
			"id":      "c1",
			"type":    "chart",
			"library": "vega-lite",
			"spec":    map[string]interface{}{"mark": "bar"},
		}
		if onClick != nil {
			b["onClick"] = onClick
		}
		return b
	}

	tests := []struct {
		name    string
		onClick interface{}
		valid   bool
	}{
		{"declared param", map[string]interface{}{"param": "country"}, true},
		{"missing param", map[string]interface{}{}, false},
		{"param not a string", map[string]interface{}{"param": 7}, false},
		{"extra key", map[string]interface{}{"param": "country", "value": "x"}, false},
		{"not an object", "country", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(docWith(t, chart(tt.onClick)))
			if tt.valid && err != nil {
				t.Fatalf("expected valid, got %v", err)
			}
			if !tt.valid && err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// §6.3: onClick is a chart-block affordance only.
func TestValidateOnClickOnlyOnChart(t *testing.T) {
	block := markdownBlock("md1")
	block["onClick"] = map[string]interface{}{"param": "country"}
	if err := Validate(docWith(t, block)); err == nil {
		t.Fatal("expected error for onClick on a non-chart block")
	}
}

// §6.3: "a block/table-row/button may declare modal: {title, blocks}
// (inline, shown client-side) or modal: {param} (lazy)."
func TestValidateBlockModalInline(t *testing.T) {
	for _, typ := range []string{"markdown", "metric", "table", "chart", "filter", "tabs"} {
		t.Run(typ, func(t *testing.T) {
			block := blockOfType(typ, typ+"1")
			block["modal"] = map[string]interface{}{
				"title":  "Details",
				"blocks": []interface{}{markdownBlock("modal-md")},
			}
			if err := Validate(docWith(t, block)); err != nil {
				t.Fatalf("expected inline modal to be valid on %s block, got %v", typ, err)
			}
		})
	}
}

func TestValidateBlockModalLazy(t *testing.T) {
	block := markdownBlock("md1")
	block["modal"] = map[string]interface{}{"param": "detail_id"}
	if err := Validate(docWith(t, block)); err != nil {
		t.Fatalf("expected lazy modal to be valid, got %v", err)
	}
}

func TestValidateModalRejectsBadShapes(t *testing.T) {
	tests := []struct {
		name  string
		modal interface{}
	}{
		{"mixes both forms", map[string]interface{}{"title": "T", "blocks": []interface{}{}, "param": "p"}},
		{"inline without blocks", map[string]interface{}{"title": "T"}},
		{"inline without title", map[string]interface{}{"blocks": []interface{}{}}},
		{"lazy with extra key", map[string]interface{}{"param": "p", "title": "T"}},
		{"param not a string", map[string]interface{}{"param": 7}},
		{"empty object", map[string]interface{}{}},
		{"not an object", "detail"},
		{"unknown key", map[string]interface{}{"title": "T", "blocks": []interface{}{}, "extra": 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block := markdownBlock("md1")
			block["modal"] = tt.modal
			if err := Validate(docWith(t, block)); err == nil {
				t.Fatalf("expected error for modal %v", tt.modal)
			}
		})
	}
}

// §6.3: "Modal content is the same block vocabulary" — nested modal blocks are
// schema-checked like any other block.
func TestValidateModalBlocksUseBlockVocabulary(t *testing.T) {
	block := markdownBlock("md1")
	block["modal"] = map[string]interface{}{
		"title":  "Details",
		"blocks": []interface{}{map[string]interface{}{"id": "bad", "type": "nope"}},
	}
	if err := Validate(docWith(t, block)); err == nil {
		t.Fatal("expected error for unknown block type inside a modal")
	}
}

// §6.3: table rows may declare a modal, so a row is either a plain cell array
// or an object carrying its cells plus the modal.
func TestValidateTableRowModal(t *testing.T) {
	tests := []struct {
		name  string
		row   interface{}
		valid bool
	}{
		{"plain cell array", []interface{}{"a", 1}, true},
		{
			"row object with lazy modal",
			map[string]interface{}{"cells": []interface{}{"a", 1}, "modal": map[string]interface{}{"param": "row"}},
			true,
		},
		{
			"row object with inline modal",
			map[string]interface{}{
				"cells": []interface{}{"a", 1},
				"modal": map[string]interface{}{"title": "Row", "blocks": []interface{}{markdownBlock("row-md")}},
			},
			true,
		},
		{"row object without cells", map[string]interface{}{"modal": map[string]interface{}{"param": "row"}}, false},
		{
			"row object with extra key",
			map[string]interface{}{"cells": []interface{}{"a"}, "extra": 1},
			false,
		},
		{"row scalar", "a", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block := map[string]interface{}{
				"id":      "t1",
				"type":    "table",
				"columns": []interface{}{"A", "B"},
				"rows":    []interface{}{tt.row},
			}
			err := Validate(docWith(t, block))
			if tt.valid && err != nil {
				t.Fatalf("expected valid, got %v", err)
			}
			if !tt.valid && err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// §6.3: block ids are "unique per doc" and are the partial-render key
// (§4.2 `block=<id>`), so uniqueness must hold across nesting levels.
func TestValidateDuplicateBlockIdAcrossNesting(t *testing.T) {
	tabsWith := func(inner ...interface{}) map[string]interface{} {
		return map[string]interface{}{
			"id":   "tabs1",
			"type": "tabs",
			"tabs": []interface{}{
				map[string]interface{}{"label": "One", "blocks": inner},
			},
		}
	}
	modalOn := func(id string, inner ...interface{}) map[string]interface{} {
		b := markdownBlock(id)
		b["modal"] = map[string]interface{}{"title": "T", "blocks": inner}
		return b
	}

	tests := []struct {
		name   string
		blocks []interface{}
	}{
		{"root block vs nested tab block", []interface{}{markdownBlock("dup"), tabsWith(markdownBlock("dup"))}},
		{"two nested tab blocks in different tabs", []interface{}{
			map[string]interface{}{
				"id":   "tabs1",
				"type": "tabs",
				"tabs": []interface{}{
					map[string]interface{}{"label": "One", "blocks": []interface{}{markdownBlock("dup")}},
					map[string]interface{}{"label": "Two", "blocks": []interface{}{markdownBlock("dup")}},
				},
			},
		}},
		{"root block vs modal block", []interface{}{markdownBlock("dup"), modalOn("holder", markdownBlock("dup"))}},
		{"tabs container id vs nested block id", []interface{}{tabsWith(markdownBlock("tabs1"))}},
		{"root block vs table-row modal block", []interface{}{
			markdownBlock("dup"),
			map[string]interface{}{
				"id":      "t1",
				"type":    "table",
				"columns": []interface{}{"A"},
				"rows": []interface{}{
					map[string]interface{}{
						"cells": []interface{}{"a"},
						"modal": map[string]interface{}{"title": "T", "blocks": []interface{}{markdownBlock("dup")}},
					},
				},
			},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Validate(docWith(t, tt.blocks...)); err == nil {
				t.Fatal("expected duplicate block id error")
			}
		})
	}
}

// §6.3/§7: "≤64 blocks/doc" — the cap is per document, counting nested blocks.
func TestValidateBlockCapIsDocumentWide(t *testing.T) {
	// One tabs container + N markdown blocks inside it.
	build := func(nested int) []byte {
		inner := make([]interface{}, nested)
		for i := 0; i < nested; i++ {
			inner[i] = markdownBlock(fmt.Sprintf("md%d", i))
		}
		return docWith(t, map[string]interface{}{
			"id":   "tabs1",
			"type": "tabs",
			"tabs": []interface{}{
				map[string]interface{}{"label": "One", "blocks": inner},
			},
		})
	}

	// 63 nested + the tabs container itself = 64 blocks: at the cap.
	if err := Validate(build(63)); err != nil {
		t.Fatalf("expected 64 total blocks to be valid, got %v", err)
	}
	// 64 nested + the tabs container = 65 blocks: over the cap.
	if err := Validate(build(64)); err == nil {
		t.Fatal("expected error for 65 total blocks across nesting")
	}
}

func TestValidateModalBlocksCountTowardCap(t *testing.T) {
	inner := make([]interface{}, 64)
	for i := 0; i < 64; i++ {
		inner[i] = markdownBlock(fmt.Sprintf("md%d", i))
	}
	holder := markdownBlock("holder")
	holder["modal"] = map[string]interface{}{"title": "T", "blocks": inner}
	if err := Validate(docWith(t, holder)); err == nil {
		t.Fatal("expected error: 65 blocks once modal content is counted")
	}
}

// §6.3: "Structural caps: … OutputDoc ≤2 MiB."
func TestValidateDocSizeCap(t *testing.T) {
	build := func(textLen int) []byte {
		block := map[string]interface{}{
			"id":   "md1",
			"type": "markdown",
			"text": strings.Repeat("a", textLen),
		}
		return docWith(t, block)
	}

	under := build(1024)
	if len(under) > MaxDocBytes {
		t.Fatalf("fixture setup: expected small doc, got %d bytes", len(under))
	}
	if err := Validate(under); err != nil {
		t.Fatalf("expected small doc to be valid, got %v", err)
	}

	over := build(MaxDocBytes + 1)
	if len(over) <= MaxDocBytes {
		t.Fatalf("fixture setup: expected oversize doc, got %d bytes", len(over))
	}
	err := Validate(over)
	if err == nil {
		t.Fatal("expected error for OutputDoc over 2 MiB")
	}
	if !strings.Contains(err.Error(), "2097152") {
		t.Fatalf("expected the cap in the message, got %v", err)
	}
}

// Appendix A shows both scalar options and {value,label} option objects.
func TestValidateFilterOptions(t *testing.T) {
	tests := []struct {
		name    string
		options interface{}
		valid   bool
	}{
		{"scalar strings", []interface{}{"ALL", "CZ"}, true},
		{"scalar numbers", []interface{}{7, 30, 90}, true},
		{"scalar booleans", []interface{}{true, false}, true},
		{"value/label objects", []interface{}{
			map[string]interface{}{"value": 7, "label": "Last 7 days"},
		}, true},
		{"object missing label", []interface{}{map[string]interface{}{"value": 7}}, false},
		{"object missing value", []interface{}{map[string]interface{}{"label": "x"}}, false},
		{"object with extra key", []interface{}{
			map[string]interface{}{"value": 7, "label": "x", "icon": "y"},
		}, false},
		{"nested array option", []interface{}{[]interface{}{"a"}}, false},
		{"null option", []interface{}{nil}, false},
		{"not an array", "ALL", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block := map[string]interface{}{
				"id":   "f1",
				"type": "filter",
				"fields": []interface{}{
					map[string]interface{}{
						"name":    "country",
						"label":   "Country",
						"kind":    "select",
						"options": tt.options,
					},
				},
			}
			err := Validate(docWith(t, block))
			if tt.valid && err != nil {
				t.Fatalf("expected valid, got %v", err)
			}
			if !tt.valid && err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// The id is the partial-render key (§4.2 `block=<id>`); an empty key is not
// addressable.
func TestValidateEmptyBlockID(t *testing.T) {
	if err := Validate(docWith(t, markdownBlock(""))); err == nil {
		t.Fatal("expected error for empty block id")
	}
}

// blockOfType returns a minimal valid block of the requested type.
func blockOfType(typ, id string) map[string]interface{} {
	switch typ {
	case "markdown":
		return markdownBlock(id)
	case "metric":
		return map[string]interface{}{"id": id, "type": "metric", "items": []interface{}{}}
	case "table":
		return map[string]interface{}{
			"id": id, "type": "table",
			"columns": []interface{}{"A"}, "rows": []interface{}{},
		}
	case "chart":
		return map[string]interface{}{
			"id": id, "type": "chart", "library": "vega-lite",
			"spec": map[string]interface{}{"mark": "bar"},
		}
	case "filter":
		return map[string]interface{}{"id": id, "type": "filter", "fields": []interface{}{}}
	case "tabs":
		return map[string]interface{}{"id": id, "type": "tabs", "tabs": []interface{}{}}
	}
	panic("unknown block type " + typ)
}

// Helper function to create the Appendix A worked example
func appendixAExample() map[string]interface{} {
	return map[string]interface{}{
		"outputDoc": 1,
		"title":     "Sales overview",
		"blocks": []interface{}{
			map[string]interface{}{
				"id":   "filters",
				"type": "filter",
				"fields": []interface{}{
					map[string]interface{}{
						"name":  "days",
						"label": "Window",
						"kind":  "select",
						"value": 30,
						"options": []interface{}{
							map[string]interface{}{"value": 7, "label": "Last 7 days"},
							map[string]interface{}{"value": 30, "label": "Last 30 days"},
							map[string]interface{}{"value": 90, "label": "Last 90 days"},
						},
					},
					map[string]interface{}{
						"name":    "country",
						"label":   "Country",
						"kind":    "select",
						"value":   "ALL",
						"options": []interface{}{"ALL", "CZ", "DE", "US"},
					},
				},
			},
			map[string]interface{}{
				"id":   "kpis",
				"type": "metric",
				"items": []interface{}{
					map[string]interface{}{
						"label":  "Revenue",
						"value":  100000,
						"format": "currency:EUR",
					},
					map[string]interface{}{
						"label": "Orders",
						"value": 500,
					},
					map[string]interface{}{
						"label":  "Avg order",
						"value":  200,
						"format": "currency:EUR",
					},
				},
			},
			map[string]interface{}{
				"id":      "daily-revenue",
				"type":    "chart",
				"library": "vega-lite",
				"title":   "Daily revenue",
				"spec": map[string]interface{}{
					"mark": "bar",
					"data": map[string]interface{}{
						"values": []interface{}{
							map[string]interface{}{"date": "2026-06-23", "revenue": 3120},
						},
					},
					"encoding": map[string]interface{}{
						"x": map[string]interface{}{
							"field": "date",
							"type":  "ordinal",
						},
						"y": map[string]interface{}{
							"field": "revenue",
							"type":  "quantitative",
						},
					},
				},
			},
			map[string]interface{}{
				"id":      "top-products",
				"type":    "table",
				"title":   "Top products",
				"columns": []interface{}{"Product", "Revenue", "Orders"},
				"rows":    []interface{}{},
			},
			map[string]interface{}{
				"id":   "footer",
				"type": "markdown",
				"text": "_rendered server-side_",
			},
		},
	}
}
