// Package framework — dev-DX ComponentDefinition bootstrap for the K8s e2e
// harness (RFC 026 P2, Task R11).
//
// Every locally-built e2e component image (built by `make
// build-components-e2e` + `build-component-sql-transform`, and referenced by
// every fixture under tests/e2e/pipelines/k8s/*.yaml via `component:`) needs a
// registered ComponentDefinition before any scenario can resolve it — R6's
// resolve-&-freeze admission path (pkg/k8s/controllers/pipelinerun_controller.go)
// rejects an unregistered component reference with FailedUser.
//
// RegisterBuiltinComponents kubectl-applies one ComponentDefinition per
// built-in image, mirroring the same "apply a manifest via kubectl" pattern
// K8sBackend already uses for the per-project Namespace (see
// ensureNamespace in k8s_runner.go).
package framework

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

// permissiveConfigSchema is the schema every "dev" prerelease version
// registers with: wide open, so local iteration on component config never
// blocks on schema drift while the image is still moving. This is the
// dev-DX path RFC 026 §4 describes — register once, iterate freely.
const permissiveConfigSchema = `{"type":"object"}`

// dataGeneratorStableConfigSchema is the REAL data-generator config schema,
// copied verbatim from the built-in registration this project ships in
// charts/datuplet-app/templates/components/data-generator.yaml (Task R10) —
// derived from docs/components.md ("data-generator" section) and
// components/data-generator/config.go (Table/RandomSpec/LiteralSpec/Limit).
// Registered on data-generator's stable "v0.0.1" e2e version so the
// resolution and schema-invalid scenarios have a real schema-enforcing
// target to pin against (the "dev" version's schema is deliberately
// permissive and cannot reject anything).
const dataGeneratorStableConfigSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["tables"],
  "properties": {
    "tables": {
      "type": "array",
      "minItems": 1,
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["name"],
        "properties": {
          "name": {"type": "string"},
          "rowInsertSpeed": {"type": "integer"},
          "random": {
            "type": "object",
            "additionalProperties": false,
            "properties": {
              "schema": {
                "type": "object",
                "additionalProperties": {
                  "type": "string",
                  "enum": ["string", "int", "long", "float", "double", "boolean", "date", "timestamp", "now", "uuid"]
                }
              },
              "limit": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                  "rowsCount": {"type": "integer", "minimum": 0},
                  "sizeInBytes": {"type": "integer", "minimum": 0},
                  "timeoutInSeconds": {"type": "integer", "minimum": 0}
                }
              },
              "userErrorMessage": {"type": "string"}
            }
          },
          "literal": {
            "type": "object",
            "additionalProperties": false,
            "properties": {
              "columns": {"type": "array", "items": {"type": "string"}},
              "rows": {"type": "array", "items": {"type": "array"}}
            }
          }
        }
      }
    }
  }
}`

// DataGeneratorStableVersion / DataGeneratorStableImage are the stable
// "second tag" registration data-generator gets in addition to its "dev"
// prerelease entry — see the Makefile's build-components-e2e target, which
// tags the same locally-built image as both `:latest` and `:v0.0.1`.
const (
	DataGeneratorStableVersion = "v0.0.1"
	DataGeneratorStableImage   = "datuplet/data-generator:" + DataGeneratorStableVersion
)

// builtinE2EComponents lists every component instantiated by
// tests/e2e/pipelines/k8s/*.yaml, i.e. every image `make build-components-e2e`
// (+ build-component-sql-transform) builds locally as `datuplet/<name>:latest`.
var builtinE2EComponents = []string{
	"data-generator",
	"http-json-extractor",
	"sql-transform",
	"stdout-writer",
}

// RegisterBuiltinComponents kubectl-applies a ComponentDefinition for every
// built-in e2e component image, run once from TestMain before any scenario.
//
// Every component gets a mutable "dev" prerelease version pointing at its
// locally docker-built `datuplet/<name>:latest` image with the permissive
// schema — proving the dev-DX registration path continuously. data-generator
// additionally gets a STABLE "v0.0.1" version (a second local tag applied by
// the Makefile) carrying the real config schema, so the unpinned-resolution
// and schema-invalid scenarios have a stable, schema-enforcing target.
//
// Idempotent: kubectl apply on an unchanged manifest is a no-op, and
// re-applying after a scenario mutates the live object (the freeze scenario
// patches the registry mid-run) restores the registered baseline.
func RegisterBuiltinComponents(ctx context.Context) error {
	for _, name := range builtinE2EComponents {
		def := devComponentDefinition(name)
		manifest, err := yaml.Marshal(def)
		if err != nil {
			return fmt.Errorf("marshal ComponentDefinition %q: %w", name, err)
		}
		if err := kubectlApplyManifest(ctx, string(manifest)); err != nil {
			return fmt.Errorf("register component %q: %w", name, err)
		}
	}
	return nil
}

// devComponentDefinition builds the ComponentDefinition object for a
// built-in e2e component: always a "dev" prerelease version pointing at the
// local `:latest` image; data-generator additionally carries the stable
// "v0.0.1" version with the real schema (see DataGeneratorStableVersion).
//
// DefaultVersion is deliberately left empty: unpinned resolution
// (StaticRegistry.Resolve, pkg/pipeline/validate/registry.go) falls through
// to LatestStable(), which skips prerelease entries — so unpinned
// data-generator resolves to "v0.0.1" (its only stable version) without
// needing an explicit default.
func devComponentDefinition(name string) *datupletv1.ComponentDefinition {
	versions := []datupletv1.VersionSpec{
		{
			Version:      "dev",
			Image:        "datuplet/" + name + ":latest",
			Prerelease:   true,
			ConfigSchema: permissiveConfigSchema,
		},
	}
	if name == "data-generator" {
		versions = append(versions, datupletv1.VersionSpec{
			Version:      DataGeneratorStableVersion,
			Image:        DataGeneratorStableImage,
			ConfigSchema: dataGeneratorStableConfigSchema,
		})
	}
	return &datupletv1.ComponentDefinition{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "datuplet.io/v1",
			Kind:       "ComponentDefinition",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: datupletv1.ComponentDefinitionSpec{
			DisplayName: name,
			Description: "e2e dev registration (RFC 026 P2 Task R11) — local docker-built image, not for production use.",
			Maintainer:  "datuplet-e2e",
			Versions:    versions,
		},
	}
}

// kubectlApplyManifest applies a single YAML manifest via `kubectl apply -f
// -`, mirroring ensureNamespace's pattern in k8s_runner.go.
func kubectlApplyManifest(ctx context.Context, manifest string) error {
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl apply: %w\n%s", err, string(out))
	}
	return nil
}
