package outputdoc

import (
	"encoding/json"
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
								"label": "Revenue",
								"value": 1000,
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
				"outputDoc":     1,
				"title":         "Test",
				"blocks":        []interface{}{},
				"refreshInterval": 300,
			},
		},
		{
			name: "appendix A worked example",
			doc: appendixAExample(),
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
				"id":        "b1",
				"type":      "markdown",
				"text":      "test",
				"extra":     "field",
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
				"outputDoc":      1,
				"title":          "Test",
				"blocks":         []interface{}{},
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
						"name":  "country",
						"label": "Country",
						"kind":  "select",
						"value": "ALL",
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
						"label": "Avg order",
						"value": 200,
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
