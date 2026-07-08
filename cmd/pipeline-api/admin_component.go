package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
)

// runAdminComponent dispatches the `admin component` subcommands: register
// (apply a ComponentDefinition CR from a YAML file), list, and deprecate.
// Unlike the project-lifecycle subcommands (create-project et al.), these
// are K8s-only — no DB, no lakekeeper, no FGA — so runAdmin routes here
// before opening a DB connection (mirrors the keygen / lakekeeper-bootstrap
// precedent). REST admin mutation endpoints for the same operations are
// Phase 3 (need mustBeSuperadmin); this CLI is the only mutation surface
// for now.
func runAdminComponent(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("component requires a subcommand (register | list | deprecate)")
	}
	switch args[0] {
	case "register":
		return adminComponentRegister(args[1:])
	case "list":
		return adminComponentList(args[1:])
	case "deprecate":
		return adminComponentDeprecate(args[1:])
	default:
		return fmt.Errorf("unknown component subcommand: %q (valid: register | list | deprecate)", args[0])
	}
}

// componentK8sFlags registers the --kubeconfig / --in-cluster flags shared by
// every `admin component` subcommand, defaulting from the same envs
// pipelineapi.LoadConfig reads (KUBECONFIG / PIPELINE_API_IN_CLUSTER) so
// operators running this from a pipeline-api Pod/shell need no extra flags.
func componentK8sFlags(fs *flag.FlagSet) (kubeconfig *string, inCluster *bool) {
	kubeconfig = fs.String("kubeconfig", os.Getenv("KUBECONFIG"), "Path to kubeconfig (default from KUBECONFIG env)")
	inCluster = fs.Bool("in-cluster", os.Getenv("PIPELINE_API_IN_CLUSTER") == "true", "Use in-cluster config (default from PIPELINE_API_IN_CLUSTER env)")
	return kubeconfig, inCluster
}

func adminComponentRegister(args []string) error {
	fs := flag.NewFlagSet("component register", flag.ExitOnError)
	file := fs.String("file", "", "Path to a ComponentDefinition YAML manifest (required)")
	kubeconfig, inCluster := componentK8sFlags(fs)
	_ = fs.Parse(args)
	if *file == "" {
		return fmt.Errorf("--file is required")
	}
	data, err := os.ReadFile(*file)
	if err != nil {
		return fmt.Errorf("read %s: %w", *file, err)
	}
	c, err := pkg8s.NewClient(pkg8s.ClientOpts{KubeconfigPath: *kubeconfig, InCluster: *inCluster})
	if err != nil {
		return fmt.Errorf("k8s client: %w", err)
	}
	def, err := applyComponentDefinition(context.Background(), c, data)
	if err != nil {
		return err
	}
	fmt.Printf("Registered ComponentDefinition %q (%d version(s))\n", def.Name, len(def.Spec.Versions))
	fmt.Println("  the componentdefinition-controller validates it asynchronously — re-run `admin component list` to check phase=Valid")
	return nil
}

// applyComponentDefinition unmarshals defYAML into a ComponentDefinition and
// Creates or Updates it in K8s. Uses Get-then-Create/Update rather than
// true server-side apply, mirroring pkg/pipelineapi/k8s.ApplyPipelineCRD's
// documented rationale: SSA needs a FieldManager and behaves awkwardly
// against the fake client used in tests, so Get+Create/Update is the
// established equivalent for this codebase.
func applyComponentDefinition(ctx context.Context, c client.Client, defYAML []byte) (*datupletv1.ComponentDefinition, error) {
	def := &datupletv1.ComponentDefinition{}
	if err := yaml.Unmarshal(defYAML, def); err != nil {
		return nil, fmt.Errorf("unmarshal ComponentDefinition YAML: %w", err)
	}
	if def.Name == "" {
		return nil, errors.New("ComponentDefinition YAML has no metadata.name")
	}
	def.TypeMeta = metav1.TypeMeta{APIVersion: "datuplet.io/v1", Kind: "ComponentDefinition"}
	def.ResourceVersion = "" // Client.Create rejects a set RV.

	existing := &datupletv1.ComponentDefinition{}
	err := c.Get(ctx, types.NamespacedName{Name: def.Name}, existing)
	if apierrors.IsNotFound(err) {
		if createErr := c.Create(ctx, def); createErr != nil {
			return nil, fmt.Errorf("create ComponentDefinition: %w", createErr)
		}
		return def, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get ComponentDefinition: %w", err)
	}
	// Replace spec, preserve resourceVersion so the update succeeds.
	def.ResourceVersion = existing.ResourceVersion
	if err := c.Update(ctx, def); err != nil {
		return nil, fmt.Errorf("update ComponentDefinition: %w", err)
	}
	return def, nil
}

func adminComponentList(args []string) error {
	fs := flag.NewFlagSet("component list", flag.ExitOnError)
	kubeconfig, inCluster := componentK8sFlags(fs)
	_ = fs.Parse(args)
	c, err := pkg8s.NewClient(pkg8s.ClientOpts{KubeconfigPath: *kubeconfig, InCluster: *inCluster})
	if err != nil {
		return fmt.Errorf("k8s client: %w", err)
	}
	return listComponentDefinitions(context.Background(), c, os.Stdout)
}

// listComponentDefinitions renders the registered ComponentDefinitions to
// out, one line each: name, deprecation marker, validation phase, version
// count, and default version.
func listComponentDefinitions(ctx context.Context, c client.Client, out io.Writer) error {
	var list datupletv1.ComponentDefinitionList
	if err := c.List(ctx, &list); err != nil {
		return fmt.Errorf("list ComponentDefinitions: %w", err)
	}
	if len(list.Items) == 0 {
		fmt.Fprintln(out, "No ComponentDefinitions registered.")
		return nil
	}
	for _, d := range list.Items {
		phase := d.Status.Phase
		if phase == "" {
			phase = "Pending"
		}
		deprecated := ""
		if d.Spec.Deprecated {
			deprecated = " [deprecated]"
		}
		fmt.Fprintf(out, "%s%s  phase=%s  versions=%d  default=%s\n",
			d.Name, deprecated, phase, len(d.Spec.Versions), d.Spec.DefaultVersion)
	}
	return nil
}

func adminComponentDeprecate(args []string) error {
	fs := flag.NewFlagSet("component deprecate", flag.ExitOnError)
	kubeconfig, inCluster := componentK8sFlags(fs)
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		return fmt.Errorf("component deprecate requires exactly one positional NAME argument")
	}
	name := fs.Arg(0)
	c, err := pkg8s.NewClient(pkg8s.ClientOpts{KubeconfigPath: *kubeconfig, InCluster: *inCluster})
	if err != nil {
		return fmt.Errorf("k8s client: %w", err)
	}
	return deprecateComponentDefinition(context.Background(), c, name, os.Stdout)
}

// deprecateComponentDefinition sets spec.deprecated = true on the named
// ComponentDefinition. Idempotent: an already-deprecated component reports
// success without a redundant Update.
func deprecateComponentDefinition(ctx context.Context, c client.Client, name string, out io.Writer) error {
	def := &datupletv1.ComponentDefinition{}
	if err := c.Get(ctx, types.NamespacedName{Name: name}, def); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("ComponentDefinition %q not found", name)
		}
		return fmt.Errorf("get ComponentDefinition %q: %w", name, err)
	}
	if def.Spec.Deprecated {
		fmt.Fprintf(out, "ComponentDefinition %q is already deprecated.\n", name)
		return nil
	}
	def.Spec.Deprecated = true
	if err := c.Update(ctx, def); err != nil {
		return fmt.Errorf("update ComponentDefinition %q: %w", name, err)
	}
	fmt.Fprintf(out, "Deprecated ComponentDefinition %q\n", name)
	return nil
}
