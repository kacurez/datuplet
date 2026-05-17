package authz

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// ResolveStoreAndModel queries OpenFGA for the store_id (by name) and model_id
// (via the version-pin tuple `model_version:collaboration-<version>` → openfga_id → auth_model_id:<id>).
// Used at pipeline-api startup; the authz-bootstrap pre-install job
// ensures both exist before pipeline-api starts.
func ResolveStoreAndModel(ctx context.Context, fgaURL, apiKey, storeName, modelVersion string) (storeID, modelID string, err error) {
	client := &http.Client{Timeout: 10 * time.Second}
	base := strings.TrimRight(fgaURL, "/")

	// 1. Find store_id by name.
	req, err := http.NewRequestWithContext(ctx, "GET", base+"/stores", nil)
	if err != nil {
		return "", "", fmt.Errorf("build stores request: %w", err)
	}
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("list stores: %w", err)
	}
	defer resp.Body.Close()
	var sl struct {
		Stores []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"stores"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&sl); err != nil {
		return "", "", fmt.Errorf("decode stores: %w", err)
	}
	for _, s := range sl.Stores {
		if s.Name == storeName {
			storeID = s.ID
			break
		}
	}
	if storeID == "" {
		return "", "", fmt.Errorf("FGA store %q not found", storeName)
	}

	// 2. Read pin tuple for model version.
	pinObj := fmt.Sprintf("model_version:collaboration-%s", modelVersion)
	body, err := json.Marshal(map[string]any{
		"tuple_key": map[string]string{"relation": "openfga_id", "object": pinObj},
	})
	if err != nil {
		return "", "", fmt.Errorf("marshal pin request: %w", err)
	}
	req2, err := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("%s/stores/%s/read", base, storeID),
		bytes.NewReader(body))
	if err != nil {
		return "", "", fmt.Errorf("build read request: %w", err)
	}
	req2.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req2.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp2, err := client.Do(req2)
	if err != nil {
		return "", "", fmt.Errorf("read pin: %w", err)
	}
	defer resp2.Body.Close()
	var rr struct {
		Tuples []struct {
			Key struct {
				User string `json:"user"`
			} `json:"key"`
		} `json:"tuples"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&rr); err != nil {
		return "", "", fmt.Errorf("decode pin: %w", err)
	}
	if len(rr.Tuples) == 0 {
		return "", "", fmt.Errorf("FGA pin tuple for %s not found", pinObj)
	}
	modelID = strings.TrimPrefix(rr.Tuples[0].Key.User, "auth_model_id:")
	if modelID == "" {
		return "", "", fmt.Errorf("malformed pin tuple user: %q", rr.Tuples[0].Key.User)
	}
	return storeID, modelID, nil
}
