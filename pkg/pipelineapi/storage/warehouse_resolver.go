package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// WarehouseResolverConfig configures NewLakekeeperWarehouseResolver.
type WarehouseResolverConfig struct {
	// LakekeeperURL is the catalog REST base URL (e.g.
	// "http://lakekeeper.<ns>.svc.cluster.local:8181/catalog"). The
	// resolver strips the "/catalog" suffix if present so it can call the
	// management API at "/management/v1/warehouse" on the same host.
	LakekeeperURL string

	// Minter mints an impersonation JWT for the authenticated user in ctx.
	// Reuse the same minter wired onto storage.Service.Minter — lakekeeper
	// validates the JWT, checks FGA on the project, and returns only
	// warehouses the user has visibility into.
	Minter func(ctx context.Context) (tokens.ImpersonationToken, error)

	// HTTPClient is optional. Defaults to http.DefaultClient if nil.
	HTTPClient *http.Client
}

// The trigger path reuses the impersonation resolver below — the triggering
// user already holds `data_admin` on the project (enforced before MintRun
// runs), which transitively grants `can_list_warehouses`. A service-identity
// variant was tried but rejected because a synthetic service identity has no
// FGA grants in lakekeeper (every call returned HTTP 403).

// NewLakekeeperWarehouseResolver returns a closure suitable for wiring
// onto storage.Service.WarehouseResolver. The closure calls lakekeeper's
// `GET /management/v1/warehouse` with the user's impersonation JWT + the
// `x-project-id` header, then returns the first warehouse name from the
// response.
//
// Picks the first warehouse the user can see for the given project.
// An explicit per-project warehouse selector is not yet implemented.
//
// Returns ("", error) when:
//   - The lakekeeper REST call fails or returns non-200.
//   - The response contains zero warehouses (operator hasn't registered
//     one yet for this project via `pipeline-api admin lakekeeper-bootstrap`).
//   - The minter returns an error.
func NewLakekeeperWarehouseResolver(cfg WarehouseResolverConfig) func(ctx context.Context, lakekeeperProjectID string) (string, error) {
	httpc := cfg.HTTPClient
	if httpc == nil {
		httpc = http.DefaultClient
	}
	// Strip "/catalog" suffix from the REST base URL — lakekeeper's
	// management endpoints live at the same host but a different path
	// prefix. The catalog client uses "/catalog/v1/..."; the management
	// API uses "/management/v1/...".
	base := strings.TrimSuffix(strings.TrimRight(cfg.LakekeeperURL, "/"), "/catalog")
	url := base + "/management/v1/warehouse"

	return func(ctx context.Context, lakekeeperProjectID string) (string, error) {
		if cfg.Minter == nil {
			return "", fmt.Errorf("warehouse resolver: Minter is required")
		}
		tok, err := cfg.Minter(ctx)
		if err != nil {
			return "", fmt.Errorf("mint impersonation token: %w", err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return "", err
		}
		req.Header.Set("authorization", "Bearer "+tok.Reveal())
		if lakekeeperProjectID != "" {
			req.Header.Set("x-project-id", lakekeeperProjectID)
		}
		resp, err := httpc.Do(req)
		if err != nil {
			return "", fmt.Errorf("list warehouses: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			return "", fmt.Errorf("list warehouses: HTTP %d: %s", resp.StatusCode, string(b))
		}
		var out struct {
			Warehouses []struct {
				Name string `json:"name"`
			} `json:"warehouses"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return "", fmt.Errorf("decode warehouses response: %w", err)
		}
		if len(out.Warehouses) == 0 {
			return "", fmt.Errorf("no warehouses registered for project %q (run `pipeline-api admin lakekeeper-bootstrap` first)", lakekeeperProjectID)
		}
		// Pick first (single-warehouse posture; multi-warehouse support not yet implemented).
		return out.Warehouses[0].Name, nil
	}
}
