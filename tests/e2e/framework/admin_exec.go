// Package framework — pipeline-api `admin` subcommand helpers (kubectl-exec)
// and Datuplet-project resolution for the HTTP run path.
//
// RFC 027 E2 review fix (Finding 1): pipeline-api resolves the URL {pid}
// against a real Datuplet `projects` row (projects.GetByID), NOT the lakekeeper
// project UUID. SetupFGABootstrap only ever provisions a *lakekeeper* Project
// for the suite, so the migrated CR-path scenarios must first bind a Datuplet
// projects-store row to that lakekeeper project and use its Postgres UUID as
// {pid}. That row is find-or-created exactly the way the working API scenarios
// (scenarios_secrets_test.go ensureSecretsLadderProject, scenarios_query_test.go
// getQueryProjectID) do it: `pipeline-api admin create-project` keyed to the
// harness's lakekeeper project NAME, then discovered by name over the REST API.
//
// These helpers duplicate — minimally — the test package's kubectl-exec
// pattern (execPipelineAPIAdmin / ensureSecretsLadderProject) into `framework`
// so K8sBackend can reach them without an e2e→framework import cycle.
package framework

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
)

// e2eControlNamespace is the namespace where pipeline-api / lakekeeper / openfga
// run for admin kubectl-exec. Matches deploy-local.sh (datuplet-e2e); override
// via DATUPLET_E2E_NAMESPACE. Kept in lockstep with signer.go's default.
func e2eControlNamespace() string {
	if ns := os.Getenv("DATUPLET_E2E_NAMESPACE"); ns != "" {
		return ns
	}
	return "datuplet-e2e"
}

// findPipelineAPIPodName returns the name of a running pipeline-api pod in the
// control namespace, or an error if none is found.
func findPipelineAPIPodName(ctx context.Context) (string, error) {
	ns := e2eControlNamespace()
	out, err := exec.CommandContext(ctx, "kubectl", "get", "pods", "-n", ns,
		"-l", "app.kubernetes.io/name=pipeline-api",
		"-o", "jsonpath={.items[0].metadata.name}").Output()
	if err != nil {
		return "", fmt.Errorf("find pipeline-api pod in %s: %w", ns, err)
	}
	name := strings.TrimSpace(string(out))
	if name == "" {
		return "", fmt.Errorf("no pipeline-api pod found in namespace %q", ns)
	}
	return name, nil
}

// execPipelineAPIAdmin runs `pipeline-api admin <args...>` inside the
// pipeline-api Pod (which already carries DATABASE_URL / SIGNING_KEY_FILE /
// OPENFGA_* env), returning the combined output.
func execPipelineAPIAdmin(ctx context.Context, args ...string) (string, error) {
	pod, err := findPipelineAPIPodName(ctx)
	if err != nil {
		return "", err
	}
	full := append([]string{"exec", pod, "-n", e2eControlNamespace(), "--",
		"/usr/local/bin/pipeline-api", "admin"}, args...)
	out, err := exec.CommandContext(ctx, "kubectl", full...).CombinedOutput()
	return string(out), err
}

// adminCreateProjectByName find-or-creates a Datuplet projects-store row bound
// to the lakekeeper project of the given NAME (the admin CLI's lakekeeper step
// probes by name before allocating, so passing the harness's lakekeeper project
// name reuses that project + its attached warehouse rather than provisioning a
// second one). Returns the Datuplet Postgres project UUID. Idempotent.
func adminCreateProjectByName(ctx context.Context, name, creatorEmail string) (string, error) {
	ns := e2eControlNamespace()
	lkURL := fmt.Sprintf("http://lakekeeper.%s.svc.cluster.local:8181", ns)
	fgaURL := fmt.Sprintf("http://openfga.%s.svc.cluster.local:8080", ns)
	out, err := execPipelineAPIAdmin(ctx, "create-project",
		"--name="+name,
		"--creator-email="+creatorEmail,
		"--lakekeeper-url="+lkURL,
		"--openfga-url="+fgaURL,
	)
	if err != nil {
		return "", fmt.Errorf("admin create-project %q: %w\noutput: %s", name, err, out)
	}
	id, perr := parseAdminIDLine(out)
	if perr != nil {
		return "", fmt.Errorf("parse create-project output: %w\noutput: %s", perr, out)
	}
	return id, nil
}

// adminCreateUser provisions a DB login user (email + password). Idempotent:
// `pipeline-api admin create-user` logs-and-ignores an already-existing email
// (exits 0), and a non-zero already-exists is tolerated defensively.
func adminCreateUser(ctx context.Context, email, password string) error {
	out, err := execPipelineAPIAdmin(ctx, "create-user",
		"--email="+email, "--password="+password)
	if err != nil {
		if strings.Contains(strings.ToLower(out), "already exists") {
			return nil
		}
		return fmt.Errorf("admin create-user %q: %w\noutput: %s", email, err, out)
	}
	return nil
}

// parseAdminIDLine extracts the "id: <uuid>" line adminCreateProject prints on
// both the fresh-create and idempotent-reuse branches (cmd/pipeline-api/admin.go).
func parseAdminIDLine(output string) (string, error) {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if id, ok := strings.CutPrefix(line, "id:"); ok {
			return strings.TrimSpace(id), nil
		}
	}
	return "", fmt.Errorf("no %q line found in output", "id:")
}

// datupletProjectCache memoizes the resolved Datuplet project UUID per
// lakekeeper project NAME for the test process, so the ~8 NewK8sBackend
// call-sites don't each re-run create-project + list.
var (
	datupletProjectCacheMu sync.Mutex
	datupletProjectCache   = map[string]string{}
)

// resolveDatupletProjectID find-or-creates the Datuplet projects-store row bound
// to the harness's lakekeeper project (by NAME) and returns its Postgres UUID —
// the value pipeline-api resolves {pid} against (projects.GetByID) and derives
// the run namespace from (datuplet-<uuid>, pkg/pipelineapi/k8s.NamespaceForProject).
// Cached per lakekeeper project name for the process.
//
// E5 VERIFICATION ASSUMPTION (no cluster here): `admin create-project
// --name=<h.LakekeeperProjectName>` binds the Datuplet row to the SAME
// lakekeeper project (find-by-name on the lakekeeper side) — and therefore to
// the warehouse SetupFGABootstrap already attached to it — so the resolved
// {pid}'s warehouse is the one the fixtures write to. This mirrors the proven
// ensureSecretsLadderProject binding; confirmed live at the E5 gate.
func resolveDatupletProjectID(ctx context.Context, h *FGAHarness) (string, error) {
	if h == nil {
		return "", errors.New("resolveDatupletProjectID: harness is required")
	}
	if h.LakekeeperProjectName == "" {
		return "", errors.New("resolveDatupletProjectID: harness has no LakekeeperProjectName")
	}

	datupletProjectCacheMu.Lock()
	if id := datupletProjectCache[h.LakekeeperProjectName]; id != "" {
		datupletProjectCacheMu.Unlock()
		return id, nil
	}
	datupletProjectCacheMu.Unlock()

	// Provision (idempotent) the Datuplet row bound to the harness's lakekeeper
	// project, then discover its UUID by name via an admin session (a non-admin
	// caller only sees projects it holds a relation on).
	if _, err := adminCreateProjectByName(ctx, h.LakekeeperProjectName, e2eAdminEmail); err != nil {
		return "", err
	}
	adminCookie, err := apiLogin(ctx, PipelineAPIBaseURL(), e2eAdminEmail, e2eAdminPassword)
	if err != nil {
		return "", fmt.Errorf("admin login for project resolution: %w", err)
	}
	id, err := apiFindProjectIDByName(ctx, PipelineAPIBaseURL(), adminCookie, h.LakekeeperProjectName)
	if err != nil {
		return "", fmt.Errorf("resolve Datuplet project %q: %w", h.LakekeeperProjectName, err)
	}

	datupletProjectCacheMu.Lock()
	datupletProjectCache[h.LakekeeperProjectName] = id
	datupletProjectCacheMu.Unlock()
	return id, nil
}

// relationForTestUser returns the FGA project relation SetupFGABootstrap seeds
// for a TestUser UUID ("project_admin" | "editor" | "viewer"), or "" if the
// user has no grant (dora) / is unknown. Used to thread the run's REAL
// (create-user-minted / seeded-admin) UUID into the same grant.
func relationForTestUser(id TestUserID) string {
	for _, u := range TestUsers {
		if u.ID == id {
			return u.Relation
		}
	}
	return ""
}
