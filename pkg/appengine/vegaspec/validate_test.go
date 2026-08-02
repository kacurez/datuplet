package vegaspec

import (
	"fmt"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Allowlists copied verbatim from spec §6.4. The tests below are driven by
// these tables so a spec change shows up as a table edit, not a scavenger hunt.
// ---------------------------------------------------------------------------

// §6.4: `mark`: string or object; type ∈ {bar, line, area, point, circle,
// square, tick, rect, rule, text, arc} (no `image`).
var allowedMarkTypes = []string{
	"bar", "line", "area", "point", "circle", "square",
	"tick", "rect", "rule", "text", "arc",
}

// §6.4: object keys limited to `type, tooltip, point, interpolate, opacity,
// filled, size, cornerRadius, orient, fontSize` (no `href`).
var allowedMarkKeys = map[string]string{
	"type":         `"bar"`,
	"tooltip":      `true`,
	"point":        `true`,
	"interpolate":  `"monotone"`,
	"opacity":      `0.7`,
	"filled":       `true`,
	"size":         `40`,
	"cornerRadius": `3`,
	"orient":       `"vertical"`,
	"fontSize":     `12`,
}

// §6.4: Encoding channels: `x, y, xOffset, yOffset, color, opacity, size,
// shape, theta, radius, text, tooltip, order, detail` (no `href`, no `url`).
var allowedChannels = []string{
	"x", "y", "xOffset", "yOffset", "color", "opacity", "size",
	"shape", "theta", "radius", "text", "tooltip", "order", "detail",
}

// §6.4: Channel-def keys: `field, type, value, aggregate, bin, timeUnit, sort,
// stack, title, format, scale, axis, legend, condition`.
var allowedChannelDefKeys = map[string]string{
	"field":     `"revenue"`,
	"type":      `"quantitative"`,
	"value":     `5`,
	"aggregate": `"sum"`,
	"bin":       `true`,
	"timeUnit":  `"yearmonth"`,
	"sort":      `"ascending"`,
	"stack":     `"zero"`,
	"title":     `"Revenue"`,
	"format":    `".2f"`,
	"scale":     `{"type": "linear"}`,
	"axis":      `{"title": "Revenue"}`,
	"legend":    `{"title": "Revenue"}`,
	"condition": `{"test": "datum.revenue > 10", "value": "red"}`,
}

// §6.4: `scale` keys `{type, domain, range, scheme, zero, nice}`.
var allowedScaleKeys = map[string]string{
	"type":   `"linear"`,
	"domain": `[0, 100]`,
	"range":  `["#fff", "#000"]`,
	"scheme": `"viridis"`,
	"zero":   `true`,
	"nice":   `true`,
}

// §6.4: `axis`/`legend` keys limited to presentation fields
// (`title, format, labelAngle, grid, orient, tickCount, values`).
var allowedGuideKeys = map[string]string{
	"title":      `"Revenue"`,
	"format":     `".2f"`,
	"labelAngle": `-45`,
	"grid":       `true`,
	"orient":     `"bottom"`,
	"tickCount":  `5`,
	"values":     `[0, 50, 100]`,
}

// §6.4 transforms, verbatim:
//
//	calculate{calculate, as}, filter{filter},
//	aggregate{aggregate[{op, field, as}], groupby}, bin{bin, field, as},
//	timeUnit{timeUnit, field, as},
//	window{window[{op, field, as}], groupby, sort, frame},
//	fold{fold, as}, pivot{pivot, value, groupby, op}
var allowedTransforms = map[string]string{
	"calculate": `{"calculate": "datum.a + datum.b", "as": "sum"}`,
	"filter":    `{"filter": "datum.revenue > 0"}`,
	"aggregate": `{"aggregate": [{"op": "sum", "field": "amount", "as": "total"}], "groupby": ["country"]}`,
	"bin":       `{"bin": true, "field": "amount", "as": "amount_binned"}`,
	"timeUnit":  `{"timeUnit": "yearmonth", "field": "date", "as": "month"}`,
	"window":    `{"window": [{"op": "rank", "field": "amount", "as": "rk"}], "groupby": ["country"], "sort": [{"field": "amount", "order": "descending"}], "frame": [null, 0]}`,
	"fold":      `{"fold": ["a", "b"], "as": ["key", "value"]}`,
	"pivot":     `{"pivot": "country", "value": "amount", "groupby": ["date"], "op": "sum"}`,
}

// §6.4 top-level keys: `title, description, width, height, data, mark,
// encoding, transform` (plus `$schema`).
var allowedTopLevelKeys = map[string]string{
	"title":       `"Daily revenue"`,
	"description": `"Revenue per day"`,
	"width":       `400`,
	"height":      `300`,
	"data":        `{"values": [{"a": 1}]}`,
	"mark":        `"bar"`,
	"encoding":    `{"x": {"field": "a", "type": "ordinal"}}`,
	"transform":   `[{"filter": "datum.a > 0"}]`,
	"$schema":     `"https://vega.github.io/schema/vega-lite/v5.json"`,
}

// appendixASpec is the chart-block spec from Appendix A of the design spec,
// verbatim (the wire form the viewer receives).
const appendixASpec = `{
  "mark": "bar",
  "data": { "values": [{ "date": "2026-06-23", "revenue": 3120 }] },
  "encoding": {
    "x": { "field": "date", "type": "ordinal" },
    "y": { "field": "revenue", "type": "quantitative" }
  }
}`

// baseSpec wraps an encoding channel-def in an otherwise minimal valid spec.
func specWithChannel(channel, def string) string {
	return fmt.Sprintf(`{"mark":"bar","data":{"values":[{"a":1}]},"encoding":{%q:%s}}`, channel, def)
}

func specWithMark(mark string) string {
	return fmt.Sprintf(`{"mark":%s,"data":{"values":[{"a":1}]}}`, mark)
}

func specWithTransform(tf string) string {
	return fmt.Sprintf(`{"mark":"bar","data":{"values":[{"a":1}]},"transform":[%s]}`, tf)
}

func mustAccept(t *testing.T, spec string) {
	t.Helper()
	if err := Validate([]byte(spec)); err != nil {
		t.Fatalf("expected spec to be accepted, got error: %v\nspec: %s", err, spec)
	}
}

func mustReject(t *testing.T, spec string) error {
	t.Helper()
	err := Validate([]byte(spec))
	if err == nil {
		t.Fatalf("expected spec to be REJECTED but it was accepted\nspec: %s", spec)
	}
	return err
}

// ---------------------------------------------------------------------------
// Positive coverage
// ---------------------------------------------------------------------------

func TestAppendixASpecAccepted(t *testing.T) {
	mustAccept(t, appendixASpec)
}

func TestAllowedTopLevelKeys(t *testing.T) {
	for key, value := range allowedTopLevelKeys {
		t.Run(key, func(t *testing.T) {
			// `mark` is the one structurally required key, so every case
			// carries it and the case under test adds its own key.
			spec := fmt.Sprintf(`{"mark":"bar",%q:%s}`, key, value)
			mustAccept(t, spec)
		})
	}
}

func TestAllowedMarkTypesAsString(t *testing.T) {
	for _, mark := range allowedMarkTypes {
		t.Run(mark, func(t *testing.T) {
			mustAccept(t, specWithMark(fmt.Sprintf("%q", mark)))
		})
	}
}

func TestAllowedMarkTypesAsObject(t *testing.T) {
	for _, mark := range allowedMarkTypes {
		t.Run(mark, func(t *testing.T) {
			mustAccept(t, specWithMark(fmt.Sprintf(`{"type":%q}`, mark)))
		})
	}
}

func TestAllowedMarkKeys(t *testing.T) {
	for key, value := range allowedMarkKeys {
		t.Run(key, func(t *testing.T) {
			mustAccept(t, specWithMark(fmt.Sprintf(`{"type":"bar",%q:%s}`, key, value)))
		})
	}
}

func TestAllowedEncodingChannels(t *testing.T) {
	for _, channel := range allowedChannels {
		t.Run(channel, func(t *testing.T) {
			mustAccept(t, specWithChannel(channel, `{"field":"a","type":"quantitative"}`))
		})
	}
}

func TestAllowedChannelDefKeys(t *testing.T) {
	for key, value := range allowedChannelDefKeys {
		t.Run(key, func(t *testing.T) {
			mustAccept(t, specWithChannel("x", fmt.Sprintf(`{%q:%s}`, key, value)))
		})
	}
}

func TestAllowedScaleKeys(t *testing.T) {
	for key, value := range allowedScaleKeys {
		t.Run(key, func(t *testing.T) {
			def := fmt.Sprintf(`{"field":"a","type":"quantitative","scale":{%q:%s}}`, key, value)
			mustAccept(t, specWithChannel("x", def))
		})
	}
}

func TestAllowedAxisKeys(t *testing.T) {
	for key, value := range allowedGuideKeys {
		t.Run(key, func(t *testing.T) {
			def := fmt.Sprintf(`{"field":"a","type":"quantitative","axis":{%q:%s}}`, key, value)
			mustAccept(t, specWithChannel("x", def))
		})
	}
}

func TestAllowedLegendKeys(t *testing.T) {
	for key, value := range allowedGuideKeys {
		t.Run(key, func(t *testing.T) {
			def := fmt.Sprintf(`{"field":"a","type":"nominal","legend":{%q:%s}}`, key, value)
			mustAccept(t, specWithChannel("color", def))
		})
	}
}

func TestAllowedTransforms(t *testing.T) {
	for name, tf := range allowedTransforms {
		t.Run(name, func(t *testing.T) {
			mustAccept(t, specWithTransform(tf))
		})
	}
}

func TestConditionShapes(t *testing.T) {
	// §6.4: `condition` = `{test, value|field}`.
	mustAccept(t, specWithChannel("color", `{"condition":{"test":"datum.a > 1","value":"red"},"value":"blue"}`))
	mustAccept(t, specWithChannel("color", `{"condition":{"test":"datum.a > 1","field":"b"},"field":"a","type":"nominal"}`))
}

func TestInlineDataValuesAccepted(t *testing.T) {
	mustAccept(t, `{"mark":"line","data":{"values":[{"x":1,"y":2},{"x":2,"y":4}]}}`)
	// An empty inline dataset is still a valid inline dataset.
	mustAccept(t, `{"mark":"line","data":{"values":[]}}`)
}

func TestDataRowMayCarryForbiddenKeyNames(t *testing.T) {
	// Forbidden *spec* keys are not forbidden *column names* — a query result
	// may legitimately contain a column called `config` or `url`.
	mustAccept(t, `{"mark":"bar","data":{"values":[{"config":"a","url":"b","usermeta":1,"href":"x"}]}}`)
}

// ---------------------------------------------------------------------------
// Mandatory negative list (spec §6.4 / task brief). One test per item.
// ---------------------------------------------------------------------------

func TestRejectDataURL(t *testing.T) {
	err := mustReject(t, `{"mark":"bar","data":{"url":"https://evil.example/x.json"}}`)
	t.Logf("rejected: %v", err)
}

func TestRejectDataName(t *testing.T) {
	mustReject(t, `{"mark":"bar","data":{"name":"mytable"}}`)
	// Named dataset alongside inline values is rejected too.
	mustReject(t, `{"mark":"bar","data":{"values":[{"a":1}],"name":"mytable"}}`)
}

func TestRejectHrefEncodingChannel(t *testing.T) {
	mustReject(t, specWithChannel("href", `{"field":"link","type":"nominal"}`))
}

func TestRejectURLEncodingChannel(t *testing.T) {
	mustReject(t, specWithChannel("url", `{"field":"img","type":"nominal"}`))
}

func TestRejectImageMark(t *testing.T) {
	mustReject(t, specWithMark(`"image"`))
	mustReject(t, specWithMark(`{"type":"image"}`))
}

func TestRejectMarkHref(t *testing.T) {
	mustReject(t, specWithMark(`{"type":"bar","href":"https://evil.example"}`))
}

func TestRejectLookupTransform(t *testing.T) {
	mustReject(t, specWithTransform(`{"lookup":"key","from":{"data":{"url":"https://evil.example/x.json"},"key":"key"}}`))
}

func TestRejectExcludedStatisticalTransforms(t *testing.T) {
	for _, tf := range []string{
		`{"loess":"y","on":"x"}`,
		`{"regression":"y","on":"x"}`,
		`{"density":"x"}`,
		`{"quantile":"x"}`,
	} {
		t.Run(tf, func(t *testing.T) {
			mustReject(t, specWithTransform(tf))
		})
	}
}

func TestRejectCompositionLayer(t *testing.T) {
	mustReject(t, `{"layer":[{"mark":"bar"}]}`)
	mustReject(t, `{"mark":"bar","layer":[{"mark":"line"}]}`)
}

func TestRejectCompositionFacet(t *testing.T) {
	mustReject(t, `{"mark":"bar","facet":{"field":"c","type":"nominal"}}`)
}

func TestRejectCompositionConcat(t *testing.T) {
	mustReject(t, `{"mark":"bar","concat":[{"mark":"line"}]}`)
	mustReject(t, `{"mark":"bar","hconcat":[{"mark":"line"}]}`)
	mustReject(t, `{"mark":"bar","vconcat":[{"mark":"line"}]}`)
}

func TestRejectCompositionRepeat(t *testing.T) {
	mustReject(t, `{"mark":"bar","repeat":["a","b"]}`)
}

func TestRejectResolve(t *testing.T) {
	mustReject(t, `{"mark":"bar","resolve":{"scale":{"y":"independent"}}}`)
}

func TestRejectParams(t *testing.T) {
	mustReject(t, `{"mark":"bar","params":[{"name":"sel","select":"point"}]}`)
}

func TestRejectUsermeta(t *testing.T) {
	mustReject(t, `{"mark":"bar","usermeta":{"anything":true}}`)
}

func TestRejectAnyConfigKey(t *testing.T) {
	// §6.4: "any `config` key at all" — top level and at every nested level.
	cases := map[string]string{
		"top level":  `{"mark":"bar","config":{"background":"red"}}`,
		"empty":      `{"mark":"bar","config":{}}`,
		"in mark":    `{"mark":{"type":"bar","config":{}},"data":{"values":[]}}`,
		"in data":    `{"mark":"bar","data":{"values":[],"config":{}}}`,
		"in channel": `{"mark":"bar","encoding":{"x":{"field":"a","config":{}}}}`,
		"in scale":   `{"mark":"bar","encoding":{"x":{"field":"a","scale":{"config":{}}}}}`,
		"in axis":    `{"mark":"bar","encoding":{"x":{"field":"a","axis":{"config":{}}}}}`,
		"in legend":  `{"mark":"bar","encoding":{"x":{"field":"a","legend":{"config":{}}}}}`,
		"in transf":  `{"mark":"bar","transform":[{"filter":"datum.a","config":{}}]}`,
	}
	for name, spec := range cases {
		t.Run(name, func(t *testing.T) {
			mustReject(t, spec)
		})
	}
}

// ---------------------------------------------------------------------------
// Additional lockdown coverage
// ---------------------------------------------------------------------------

func TestRejectUnknownTopLevelKey(t *testing.T) {
	mustReject(t, `{"mark":"bar","autosize":"fit"}`)
	mustReject(t, `{"mark":"bar","projection":{"type":"albersUsa"}}`)
	mustReject(t, `{"mark":"bar","datasets":{"d":[{"a":1}]}}`)
	mustReject(t, `{"mark":"bar","selection":{"s":{"type":"single"}}}`)
	mustReject(t, `{"mark":"bar","background":"url(https://evil.example)"}`)
}

func TestRejectUnknownMarkKey(t *testing.T) {
	mustReject(t, specWithMark(`{"type":"bar","xOffset":5}`))
}

func TestRejectUnknownChannelDefKey(t *testing.T) {
	mustReject(t, specWithChannel("x", `{"field":"a","bandPosition":0.5}`))
}

func TestRejectUnknownScaleKey(t *testing.T) {
	mustReject(t, specWithChannel("x", `{"field":"a","scale":{"reverse":true}}`))
}

func TestRejectUnknownAxisKey(t *testing.T) {
	mustReject(t, specWithChannel("x", `{"field":"a","axis":{"labelExpr":"datum.label"}}`))
}

func TestRejectUnknownScheme(t *testing.T) {
	mustReject(t, specWithChannel("color", `{"field":"a","type":"nominal","scale":{"scheme":"https://evil.example/scheme.json"}}`))
	mustReject(t, specWithChannel("color", `{"field":"a","type":"nominal","scale":{"scheme":"not-a-real-scheme"}}`))
}

func TestRejectForeignSchemaURL(t *testing.T) {
	mustReject(t, `{"mark":"bar","$schema":"https://evil.example/vega-lite/v5.json"}`)
}

func TestRejectMissingMark(t *testing.T) {
	mustReject(t, `{"data":{"values":[{"a":1}]}}`)
}

func TestRejectNonObjectSpec(t *testing.T) {
	mustReject(t, `[]`)
	mustReject(t, `"bar"`)
	mustReject(t, `null`)
}

func TestRejectInvalidJSON(t *testing.T) {
	err := mustReject(t, `{"mark":`)
	if !strings.Contains(err.Error(), "invalid JSON") {
		t.Fatalf("expected an invalid-JSON error, got: %v", err)
	}
}

func TestRejectUnknownTransformKind(t *testing.T) {
	mustReject(t, specWithTransform(`{"impute":"y","key":"x"}`))
	mustReject(t, specWithTransform(`{"sample":100}`))
	mustReject(t, specWithTransform(`{"flatten":["a"]}`))
	mustReject(t, specWithTransform(`{}`))
}

func TestErrorMessagesCarryJSONPointer(t *testing.T) {
	err := mustReject(t, `{"mark":"bar","data":{"url":"https://evil.example/x.json"}}`)
	if !strings.Contains(err.Error(), "/data") {
		t.Fatalf("expected the error to name the JSON pointer of the offending location, got: %v", err)
	}
}
