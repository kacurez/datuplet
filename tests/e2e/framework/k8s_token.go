// Package framework — K8s run-token plumbing for e2e scenarios.
//
// pipeline-api would normally mint + attach the per-run JWT for HTTP-
// triggered runs. The e2e harness skips the HTTP path and applies
// PipelineRun YAML directly via kubectl, so we mint the same shape
// of token in-process and write it into a Secret the operator
// references via spec.runTokenRef.
//
// The token is a SINGLE per-run JWT
// (sub=<run-uuid>, actor=<user-uuid>, aud=datuplet-catalog,
// jti=run-tok-<run-uuid>) projected under the Secret data key `token`.
package framework

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"

	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
)

// RunTokenLifetime is how long the test-minted JWT lives. Any value
// below 24h is acceptable — tests run for seconds; the JWT just has
// to outlast the slowest scenario.
const RunTokenLifetime = 15 * time.Minute

// extractPipelineDoc splits a multi-doc YAML and returns the bytes of
// the first document whose kind is "Pipeline". The caller derives the
// pipeline-name claim from this for the per-run JWT.
func extractPipelineDoc(yamlBytes []byte) ([]byte, error) {
	dec := yaml.NewDecoder(bytes.NewReader(yamlBytes))
	for {
		var node yaml.Node
		if err := dec.Decode(&node); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode YAML: %w", err)
		}
		kind := yamlKind(&node)
		if kind == "Pipeline" {
			return yaml.Marshal(&node)
		}
	}
	return nil, errors.New("no Pipeline document found in YAML")
}

// yamlKind returns the value of the top-level `kind:` field for a
// document node, or "" if absent.
func yamlKind(n *yaml.Node) string {
	if n == nil || n.Kind != yaml.DocumentNode || len(n.Content) == 0 {
		return ""
	}
	root := n.Content[0]
	if root.Kind != yaml.MappingNode {
		return ""
	}
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value == "kind" {
			return root.Content[i+1].Value
		}
	}
	return ""
}

// pipelineNameFromYAML extracts metadata.name out of the (already-
// extracted) Pipeline document so the JWT carries the right
// pipeline_name claim. Best-effort — returns "" if the doc is missing
// the field, which lakekeeper accepts (informational).
func pipelineNameFromYAML(pipelineDoc []byte) string {
	var node yaml.Node
	if err := yaml.Unmarshal(pipelineDoc, &node); err != nil {
		return ""
	}
	if node.Kind != yaml.DocumentNode || len(node.Content) == 0 {
		return ""
	}
	root := node.Content[0]
	if root.Kind != yaml.MappingNode {
		return ""
	}
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value != "metadata" {
			continue
		}
		md := root.Content[i+1]
		if md.Kind != yaml.MappingNode {
			return ""
		}
		for j := 0; j+1 < len(md.Content); j += 2 {
			if md.Content[j].Value == "name" {
				return md.Content[j+1].Value
			}
		}
	}
	return ""
}

// rewriteYAMLWithRunToken takes a multi-document pipeline YAML path,
// injects `spec.runId` + `spec.runTokenRef.name` on every PipelineRun
// document, and writes the result back to the same path. ALSO updates
// `metadata.namespace` on every doc (Pipeline + PipelineRun) to the
// per-project namespace.
//
// Non-Pipeline / non-PipelineRun documents are passed through
// unchanged.
func rewriteYAMLWithRunToken(yamlPath string, runID uuid.UUID, tokenSecretName, namespace, lakekeeperProjectID string) error {
	data, err := os.ReadFile(yamlPath)
	if err != nil {
		return err
	}

	var docs []*yaml.Node
	dec := yaml.NewDecoder(bytes.NewReader(data))
	for {
		var node yaml.Node
		if err := dec.Decode(&node); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("decode %s: %w", yamlPath, err)
		}
		if node.Kind == yaml.DocumentNode && len(node.Content) > 0 {
			kind := yamlKind(&node)
			if namespace != "" && (kind == "Pipeline" || kind == "PipelineRun") {
				rewriteNamespace(&node, namespace)
			}
			if kind == "PipelineRun" {
				injectRunTokenIntoPipelineRun(&node, runID.String(), tokenSecretName)
			}
		}
		// Copy so downstream encode doesn't share state with decoder.
		n := node
		docs = append(docs, &n)
	}

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	for _, d := range docs {
		if err := enc.Encode(d); err != nil {
			enc.Close()
			return fmt.Errorf("encode %s: %w", yamlPath, err)
		}
	}
	if err := enc.Close(); err != nil {
		return err
	}
	return os.WriteFile(yamlPath, buf.Bytes(), 0o644)
}

// rewriteNamespace mutates a Pipeline / PipelineRun document so its
// metadata.namespace points at the per-project namespace. The
// scenarios in pipelines/k8s/*.yaml hardcode `namespace: datuplet`;
// the harness re-targets every doc to the per-project namespace it
// allocated at fixture time.
func rewriteNamespace(doc *yaml.Node, namespace string) {
	if doc == nil || len(doc.Content) == 0 {
		return
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return
	}
	md := mapLookupOrCreate(root, "metadata")
	setScalar(md, "namespace", namespace)
}

// injectRunTokenIntoPipelineRun mutates a PipelineRun document node in
// place, setting `.spec.runId` and `.spec.runTokenRef.name`. Any
// existing values are overwritten — the test framework owns the
// PipelineRun shape in e2e.
func injectRunTokenIntoPipelineRun(doc *yaml.Node, runID, tokenSecretName string) {
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return
	}
	spec := mapLookupOrCreate(root, "spec")
	setScalar(spec, "runId", runID)
	runTokenRef := mapLookupOrCreate(spec, "runTokenRef")
	setScalar(runTokenRef, "name", tokenSecretName)
}

// mapLookupOrCreate returns the child mapping at key `key` on mapping
// node `parent`, creating an empty mapping if absent.
func mapLookupOrCreate(parent *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(parent.Content); i += 2 {
		if parent.Content[i].Value == key {
			v := parent.Content[i+1]
			if v.Kind != yaml.MappingNode {
				v.Kind = yaml.MappingNode
				v.Tag = "!!map"
				v.Content = nil
				v.Value = ""
			}
			return v
		}
	}
	newKey := &yaml.Node{Kind: yaml.ScalarNode, Value: key}
	newVal := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	parent.Content = append(parent.Content, newKey, newVal)
	return newVal
}

// setScalar sets a string scalar child on a mapping node, creating or
// overwriting.
func setScalar(parent *yaml.Node, key, value string) {
	for i := 0; i+1 < len(parent.Content); i += 2 {
		if parent.Content[i].Value == key {
			parent.Content[i+1].Kind = yaml.ScalarNode
			parent.Content[i+1].Tag = "!!str"
			parent.Content[i+1].Value = value
			parent.Content[i+1].Content = nil
			return
		}
	}
	parent.Content = append(parent.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Value: key},
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value},
	)
}

// createRunTokenSecret creates a Secret `name` in namespace `ns` with
// data key `token` carrying the single per-run JWT.
// Overwrites an existing Secret with the same name so re-runs in the
// same session don't collide. Labels with `datuplet.io/run-id` so the
// reaper / cleanup finds it.
func createRunTokenSecret(ctx context.Context, ns, name, jwt, runID string) error {
	if ns == "" || name == "" || jwt == "" {
		return errors.New("createRunTokenSecret: ns/name/jwt are required")
	}
	_ = exec.CommandContext(ctx, "kubectl", "-n", ns, "delete", "secret", name, "--ignore-not-found").Run()
	// SECURITY NOTE: the JWT is passed as a kubectl command-line argument,
	// which is briefly visible in the OS process list (/proc/<pid>/cmdline,
	// `ps aux`). Acceptable for an e2e test harness in CI; the production
	// path uses controller-runtime's client.Create over the K8s API and
	// never exposes the token via shell.
	cmd := exec.CommandContext(ctx, "kubectl", "-n", ns, "create", "secret", "generic", name, //nolint:gosec
		"--from-literal="+pkg8s.RunTokenSecretKey+"="+jwt)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("create secret %s/%s: %w: %s", ns, name, err, strings.TrimSpace(string(out)))
	}
	if err := exec.CommandContext(ctx, "kubectl", "-n", ns, "label", "secret", name,
		"datuplet.io/run-id="+runID, "--overwrite").Run(); err != nil {
		return fmt.Errorf("label secret %s/%s: %w", ns, name, err)
	}
	return nil
}
