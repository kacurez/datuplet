package framework

import (
	"os"
	"text/template"
)

// TemplateVars holds variables available in pipeline YAML templates.
type TemplateVars struct {
	RunPrefix    string
	TestDataDir  string
	WarehouseDir string
}

// RenderPipeline reads a Go text/template pipeline YAML, executes it with the
// given vars, writes the result to a temp file, and returns the temp file path.
func RenderPipeline(templatePath string, vars TemplateVars) (string, error) {
	tmpl, err := template.ParseFiles(templatePath)
	if err != nil {
		return "", err
	}

	f, err := os.CreateTemp("", "e2e-pipeline-*.yaml")
	if err != nil {
		return "", err
	}
	defer f.Close()

	if err := tmpl.Execute(f, vars); err != nil {
		os.Remove(f.Name())
		return "", err
	}

	return f.Name(), nil
}
