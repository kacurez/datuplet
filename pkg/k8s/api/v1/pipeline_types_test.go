package v1

import (
	"testing"

	"sigs.k8s.io/yaml"
)

func TestComponentSpecStructuredConfig(t *testing.T) {
	in := []byte(`
name: gen
component: x
config:
  sql: |
    SELECT 1;
  threads: 4
  tables:
    - name: t1
      random: {schema: {id: int}}
`)
	var c ComponentSpec
	if err := yaml.UnmarshalStrict(in, &c); err != nil {
		t.Fatalf("strict decode: %v", err)
	}
	m, err := c.ConfigMap()
	if err != nil {
		t.Fatalf("ConfigMap: %v", err)
	}
	if m["threads"].(float64) != 4 {
		t.Errorf("threads = %v", m["threads"])
	}
	if _, ok := m["tables"].([]any); !ok {
		t.Errorf("tables not a list: %T", m["tables"])
	}
	cp := c.DeepCopy()
	if string(cp.Config.Raw) != string(c.Config.Raw) {
		t.Error("DeepCopy lost config")
	}
}

func TestComponentSpecEmptyConfig(t *testing.T) {
	var c ComponentSpec
	m, err := c.ConfigMap()
	if err != nil || m != nil {
		t.Fatalf("want (nil,nil), got (%v,%v)", m, err)
	}
}
