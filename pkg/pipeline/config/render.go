package config

import (
	"bytes"

	"gopkg.in/yaml.v3"
)

// RenderYAML deterministically renders a Pipeline as YAML for human editing
// (RFC 027 §3). Determinism comes from two sources: struct field order (fixed
// by the Go type definition) and yaml.v3's key-sorting behavior for map[string]any
// values (e.g. Component.Config) — the CI-owned golden file pins this exactly.
// Two renders of the same doc are byte-for-byte identical, and the output
// re-parses through Parse without loss.
func RenderYAML(p *Pipeline) ([]byte, error) {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(p); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
