package http_test

import (
	"encoding/json"
	stdhttp "net/http"
	"net/http/httptest"
	"testing"

	apihttp "github.com/datuplet/datuplet/pkg/pipelineapi/http"
)

func TestVersionHandlerUnauthenticated(t *testing.T) {
	ts := httptest.NewServer(apihttp.NewServer(nil).Handler())
	defer ts.Close()

	resp, err := stdhttp.Get(ts.URL + "/api/v1/version")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["version"] == "" || body["commit"] == "" {
		t.Errorf("body = %v, want version and commit", body)
	}
}
