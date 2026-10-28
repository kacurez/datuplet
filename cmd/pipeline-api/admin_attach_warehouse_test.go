package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/datuplet/datuplet/pkg/pipelineapi/lakekeeper"
)

// TestAttachWarehouse_MissingRequiredFlags verifies that attach-warehouse errors
// out when either required flag (--project, --warehouse) is missing.
func TestAttachWarehouse_MissingRequiredFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"no args", []string{}},
		{"missing --project", []string{"--warehouse", "x"}},
		{"missing --warehouse", []string{"--project", "myproj"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := adminAttachWarehouse(context.Background(), nil, tc.args); err == nil {
				t.Fatal("expected error for missing required flag")
			}
		})
	}
}

// fakeEnsurer captures the (type, projectID, warehouseName, profile)
// passed to attachWarehouse so tests can assert the dispatch.
type fakeEnsurer struct {
	calledS3, calledGCS   bool
	gotS3                 lakekeeper.S3WarehouseProfile
	gotGCS                lakekeeper.GCSWarehouseProfile
	gotProject, gotWHName string
	retErr                error
}

func (f *fakeEnsurer) EnsureS3WarehouseInProject(ctx context.Context, projectID, warehouseName string, profile lakekeeper.S3WarehouseProfile) error {
	f.calledS3 = true
	f.gotS3 = profile
	f.gotProject, f.gotWHName = projectID, warehouseName
	return f.retErr
}

func (f *fakeEnsurer) EnsureGCSWarehouseInProject(ctx context.Context, projectID, warehouseName string, profile lakekeeper.GCSWarehouseProfile) error {
	f.calledGCS = true
	f.gotGCS = profile
	f.gotProject, f.gotWHName = projectID, warehouseName
	return f.retErr
}

// TestAttachWarehouse_GCSSystemIdentity asserts the GCS WIF branch builds
// a profile with CredentialType=system-identity and forwards Bucket /
// KeyPrefix / StsEnabled through.
func TestAttachWarehouse_GCSSystemIdentity(t *testing.T) {
	t.Setenv("GCS_SA_KEY_FILE", "") // make sure no env leaks in

	f := &fakeEnsurer{}
	err := attachWarehouse(context.Background(), f, "proj-1", "wh-gcs", "gcs",
		attachWarehouseS3Opts{},
		attachWarehouseGCSOpts{
			bucket:     "datuplet-gcs",
			keyPrefix:  "datuplet",
			credType:   "system-identity",
			stsEnabled: true,
		})
	if err != nil {
		t.Fatalf("attachWarehouse: %v", err)
	}
	if !f.calledGCS || f.calledS3 {
		t.Fatalf("expected GCS dispatch only; calledGCS=%v calledS3=%v", f.calledGCS, f.calledS3)
	}
	if f.gotGCS.Bucket != "datuplet-gcs" {
		t.Errorf("bucket: want datuplet-gcs, got %q", f.gotGCS.Bucket)
	}
	if f.gotGCS.KeyPrefix != "datuplet" {
		t.Errorf("key-prefix: want datuplet, got %q", f.gotGCS.KeyPrefix)
	}
	if f.gotGCS.CredentialType != "system-identity" {
		t.Errorf("credential-type: want system-identity, got %q", f.gotGCS.CredentialType)
	}
	if !f.gotGCS.StsEnabled {
		t.Error("expected sts-enabled=true")
	}
}

// TestAttachWarehouse_GCSServiceAccountKey asserts the SA-key branch reads
// the file from disk and forwards its content as ServiceAccountKeyJSON.
func TestAttachWarehouse_GCSServiceAccountKey(t *testing.T) {
	dir := t.TempDir()
	saPath := filepath.Join(dir, "sa.json")
	saContent := `{"type":"service_account","project_id":"p","private_key":"PRIV"}`
	if err := os.WriteFile(saPath, []byte(saContent), 0o600); err != nil {
		t.Fatalf("write sa.json: %v", err)
	}

	f := &fakeEnsurer{}
	err := attachWarehouse(context.Background(), f, "proj-1", "wh-gcs", "gcs",
		attachWarehouseS3Opts{},
		attachWarehouseGCSOpts{
			bucket:     "datuplet-gcs",
			credType:   "service-account-key",
			saKeyFile:  saPath,
			stsEnabled: true,
		})
	if err != nil {
		t.Fatalf("attachWarehouse: %v", err)
	}
	if !f.calledGCS {
		t.Fatal("expected GCS dispatch")
	}
	if f.gotGCS.CredentialType != "service-account-key" {
		t.Errorf("credential-type: want service-account-key, got %q", f.gotGCS.CredentialType)
	}
	if f.gotGCS.ServiceAccountKeyJSON != saContent {
		t.Errorf("SA key JSON not forwarded; got %q", f.gotGCS.ServiceAccountKeyJSON)
	}
}

// TestAttachWarehouse_S3DispatchUnchanged asserts the S3 path still routes
// to EnsureS3WarehouseInProject (regression: rename + signature shape).
func TestAttachWarehouse_S3DispatchUnchanged(t *testing.T) {
	t.Setenv("S3_ENDPOINT", "http://minio")
	t.Setenv("S3_ACCESS_KEY", "ak")
	t.Setenv("S3_SECRET_KEY", "sk")

	f := &fakeEnsurer{}
	err := attachWarehouse(context.Background(), f, "proj-1", "wh-s3", "s3",
		attachWarehouseS3Opts{
			bucket:    "datuplet",
			region:    "local-01",
			pathStyle: true,
		},
		attachWarehouseGCSOpts{})
	if err != nil {
		t.Fatalf("attachWarehouse: %v", err)
	}
	if !f.calledS3 || f.calledGCS {
		t.Fatalf("expected S3 dispatch only; calledS3=%v calledGCS=%v", f.calledS3, f.calledGCS)
	}
	if f.gotS3.Bucket != "datuplet" {
		t.Errorf("bucket: want datuplet, got %q", f.gotS3.Bucket)
	}
}

// TestAttachWarehouse_InvalidInputRejectedBeforeEnsurer asserts invalid
// --type / --gcs-bucket / --gcs-credential-type values error out without
// reaching the Manager.
func TestAttachWarehouse_InvalidInputRejectedBeforeEnsurer(t *testing.T) {
	tests := []struct {
		name         string
		whType       string
		gcs          attachWarehouseGCSOpts
		wantContains string
	}{
		{
			name:         "gcs bucket required",
			whType:       "gcs",
			gcs:          attachWarehouseGCSOpts{credType: "system-identity", stsEnabled: true},
			wantContains: "gcs-bucket",
		},
		{
			name:         "gcs unknown credential type echoes the value",
			whType:       "gcs",
			gcs:          attachWarehouseGCSOpts{bucket: "datuplet-gcs", credType: "bogus", stsEnabled: true},
			wantContains: "bogus",
		},
		{
			name:   "unknown type",
			whType: "azure",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeEnsurer{}
			err := attachWarehouse(context.Background(), f, "proj-1", "wh", tc.whType,
				attachWarehouseS3Opts{}, tc.gcs)
			if err == nil {
				t.Fatal("expected error")
			}
			if tc.wantContains != "" && !strings.Contains(err.Error(), tc.wantContains) {
				t.Errorf("expected error to mention %q; got: %v", tc.wantContains, err)
			}
			if f.calledGCS || f.calledS3 {
				t.Errorf("expected NO ensurer call; calledS3=%v calledGCS=%v", f.calledS3, f.calledGCS)
			}
		})
	}
}
