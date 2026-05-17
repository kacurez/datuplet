package docker

import (
	"github.com/datuplet/datuplet/pkg/lib/orchestrator"
	"github.com/datuplet/datuplet/pkg/lib/status"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"gopkg.in/yaml.v3"
)

const (
	// DefaultGatewayImage is the default image for the data gateway sidecar.
	DefaultGatewayImage = "datuplet/gateway:latest"

	// GatewayPort is the gRPC port the gateway listens on.
	GatewayPort = "50051"
)

// DockerOrchestrator runs components as Docker containers with gateway sidecars.
type DockerOrchestrator struct {
	client     *client.Client
	network    string
	gatewayImage string
	// runTokenHostPath is the host-side absolute path to the per-run JWT
	// file. When non-empty, every gateway sidecar and table-commit container
	// gets a :ro bind-mount of this file at dockerRunTokenSinglePath.
	// Set by SetRunTokenHostPath.
	runTokenHostPath string
}

// shortID returns the first 8 chars of id, or the whole string if shorter.
// Used to build container/job names; avoids panicking on unexpectedly short IDs.
func shortID(id string) string {
	if len(id) < 8 {
		return id
	}
	return id[:8]
}

// NewDockerOrchestrator creates a new Docker orchestrator.
func NewDockerOrchestrator(networkName string) (*DockerOrchestrator, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %w", err)
	}

	gatewayImage := os.Getenv("DATUPLET_GATEWAY_IMAGE")
	if gatewayImage == "" {
		gatewayImage = DefaultGatewayImage
	}

	return &DockerOrchestrator{
		client:     cli,
		network:    networkName,
		gatewayImage: gatewayImage,
	}, nil
}

// ExecuteComponent runs a component with a gateway sidecar and waits for completion.
// If spec.DirectMode is true, the component runs without a gateway sidecar.
func (d *DockerOrchestrator) ExecuteComponent(ctx context.Context, spec orchestrator.ComponentSpec) (*orchestrator.ExecutionResult, error) {
	result := &orchestrator.ExecutionResult{
		StartTime: time.Now(),
	}

	// DirectMode: run component without gateway sidecar
	if spec.DirectMode {
		return d.executeComponentDirect(ctx, spec)
	}

	// Pull images if needed
	if err := d.ensureImage(ctx, d.gatewayImage); err != nil {
		result.EndTime = time.Now()
		result.Error = fmt.Sprintf("failed to pull gateway image: %v", err)
		return result, err
	}

	if err := d.ensureImage(ctx, spec.Image); err != nil {
		result.EndTime = time.Now()
		result.Error = err.Error()
		return result, err
	}

	// Generate gateway config file
	configPath, err := d.generateGatewayConfig(spec)
	if err != nil {
		result.EndTime = time.Now()
		result.Error = fmt.Sprintf("failed to generate gateway config: %v", err)
		return result, err
	}
	defer os.Remove(configPath)

	// Start gateway sidecar
	gatewayContainerID, gatewayName, err := d.startGatewaySidecar(ctx, spec, configPath)
	if err != nil {
		result.EndTime = time.Now()
		result.Error = fmt.Sprintf("failed to start gateway sidecar: %v", err)
		return result, err
	}
	defer func() {
		d.client.ContainerStop(context.Background(), gatewayContainerID, container.StopOptions{})
		d.client.ContainerRemove(context.Background(), gatewayContainerID, container.RemoveOptions{Force: true})
	}()

	// Wait for gateway to be ready
	if err := d.waitForGatewayReady(ctx, gatewayContainerID); err != nil {
		result.EndTime = time.Now()
		result.Error = fmt.Sprintf("gateway failed to become ready: %v", err)
		return result, err
	}

	// Start component container
	componentResult, err := d.runComponent(ctx, spec, gatewayName)
	result.EndTime = time.Now()
	result.ExitCode = componentResult.ExitCode
	result.Success = componentResult.Success
	result.Logs = componentResult.Logs
	result.Error = componentResult.Error

	if err != nil {
		return result, err
	}

	return result, nil
}

// executeComponentDirect runs a component without a gateway sidecar.
// Used for sample mode where components read directly from sources.
func (d *DockerOrchestrator) executeComponentDirect(ctx context.Context, spec orchestrator.ComponentSpec) (*orchestrator.ExecutionResult, error) {
	result := &orchestrator.ExecutionResult{
		StartTime: time.Now(),
	}

	// Pull component image
	if err := d.ensureImage(ctx, spec.Image); err != nil {
		result.EndTime = time.Now()
		result.Error = err.Error()
		return result, err
	}

	// Build environment with config as JSON
	configJSON, err := json.Marshal(spec.Config)
	if err != nil {
		result.EndTime = time.Now()
		result.Error = fmt.Sprintf("failed to marshal config: %v", err)
		return result, err
	}

	env := []string{
		fmt.Sprintf("DATUPLET_CONFIG=%s", string(configJSON)),
	}

	// Add extra environment variables
	for key, value := range spec.ExtraEnv {
		env = append(env, fmt.Sprintf("%s=%s", key, value))
	}

	// Build volume bindings
	var binds []string
	for hostPath, containerPath := range spec.Volumes {
		binds = append(binds, fmt.Sprintf("%s:%s", hostPath, containerPath))
	}

	containerConfig := &container.Config{
		Image:  spec.Image,
		Env:    env,
		Labels: mergeLabels(spec.Labels),
	}

	hostConfig := &container.HostConfig{
		Binds: binds,
	}

	networkConfig := &network.NetworkingConfig{}
	if d.network != "" {
		networkConfig.EndpointsConfig = map[string]*network.EndpointSettings{
			d.network: {},
		}
	}

	containerName := fmt.Sprintf("datuplet-%s-%s", spec.Name, shortID(spec.ExecutionID))

	resp, err := d.client.ContainerCreate(ctx, containerConfig, hostConfig, networkConfig, nil, containerName)
	if err != nil {
		result.EndTime = time.Now()
		result.Error = fmt.Sprintf("failed to create container: %v", err)
		return result, err
	}

	if spec.OnContainerStarted != nil {
		spec.OnContainerStarted(resp.ID)
	}

	defer func() {
		d.client.ContainerRemove(context.Background(), resp.ID, container.RemoveOptions{Force: true})
	}()

	fmt.Printf("    Running component (direct mode): %s\n", containerName)
	if err := d.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		result.EndTime = time.Now()
		result.Error = fmt.Sprintf("failed to start container: %v", err)
		return result, err
	}

	// Wait for container to finish
	statusCh, errCh := d.client.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)

	select {
	case err := <-errCh:
		result.EndTime = time.Now()
		result.Error = fmt.Sprintf("error waiting for container: %v", err)
		return result, err
	case status := <-statusCh:
		result.ExitCode = int(status.StatusCode)
		result.Success = status.StatusCode == 0

		if status.Error != nil {
			result.Error = status.Error.Message
		}
	}

	// Collect logs
	logs, err := d.client.ContainerLogs(ctx, resp.ID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err == nil {
		logBytes, _ := io.ReadAll(logs)
		result.Logs = cleanDockerLogs(string(logBytes))
		logs.Close()
	}

	result.EndTime = time.Now()

	if !result.Success {
		result.FailureType = string(status.ClassifyExitCode(result.ExitCode))
		result.StatusMessage = status.ExtractStatusMessage(result.Logs, result.ExitCode)
		return result, fmt.Errorf("component failed (exit %d, %s): %s", result.ExitCode, result.FailureType, result.StatusMessage)
	}

	return result, nil
}

// generateGatewayConfig creates a temporary YAML config file for the gateway.
func (d *DockerOrchestrator) generateGatewayConfig(spec orchestrator.ComponentSpec) (string, error) {
	// Build gateway settings (shared between modes)
	gatewaySettings := map[string]any{
		"chunk_size":       spec.ChunkSize,
		"buffer_size":      spec.BufferSize,
		"row_group_size":   spec.RowGroupSize,
		"target_file_size": spec.TargetFileSize,
	}

	// Build output configuration
	// Prefer new bucket-based API (spec.OutputConfig) over legacy API (spec.Outputs)
	var outputsConfig any
	if spec.OutputConfig.DefaultBucket != "" || len(spec.OutputConfig.Buckets) > 0 || len(spec.OutputConfig.Tables) > 0 {
		// Use bucket-based API
		bucketConfig := map[string]any{}

		// Add default bucket if specified
		if spec.OutputConfig.DefaultBucket != "" {
			bucketConfig["default_bucket"] = spec.OutputConfig.DefaultBucket
			if spec.OutputConfig.DefaultWriteMode != "" {
				bucketConfig["default_write_mode"] = spec.OutputConfig.DefaultWriteMode
			} else {
				bucketConfig["default_write_mode"] = "APPEND"
			}
		}

		// Add explicit buckets if specified
		if len(spec.OutputConfig.Buckets) > 0 {
			buckets := make([]map[string]any, len(spec.OutputConfig.Buckets))
			for i, b := range spec.OutputConfig.Buckets {
				buckets[i] = map[string]any{
					"name":       b.Name,
					"write_mode": b.WriteMode,
				}
			}
			bucketConfig["buckets"] = buckets
		}

		// NOTE: Explicit tables are set at top-level via "output_tables" field (line ~377)
		// DO NOT set bucketConfig["tables"] here - it would cause duplicates!
		// The gateway command reads from both cfg.OutputTables (top-level) and
		// cfg.Component.Outputs.Tables (nested), appending both to the same array.

		// Add processors if specified
		if len(spec.OutputConfig.Processors) > 0 {
			bucketConfig["processors"] = spec.OutputConfig.Processors
		}

		outputsConfig = bucketConfig
	} else {
		// Fall back to legacy outputs API for backward compatibility
		outputs := make(map[string]any)
		for name, out := range spec.Outputs {
			if len(out.Processors) > 0 {
				// Full output config with processors
				outputs[name] = map[string]any{
					"path":       out.StagingPath,
					"processors": out.Processors,
				}
			} else {
				// Simple path-only format
				outputs[name] = out.StagingPath
			}
		}
		outputsConfig = outputs
	}

	// Use RunID if set, otherwise fall back to ExecutionID
	runID := spec.RunID
	if runID == "" {
		runID = spec.ExecutionID
	}

	// Build input_tables array from spec
	inputTables := make([]map[string]any, 0, len(spec.InputTables))
	for _, t := range spec.InputTables {
		tableEntry := map[string]any{
			"bucket": t.Bucket,
			"table":  t.Table,
		}
		// Add logical name if specified
		if t.As != "" {
			tableEntry["as"] = t.As
		}
		// Add incremental read config
		if t.SinceTimestampMs > 0 {
			tableEntry["since_timestamp_ms"] = t.SinceTimestampMs
		}
		if t.SinceSnapshot > 0 {
			tableEntry["since_snapshot"] = t.SinceSnapshot
		}
		if t.TimestampColumn != "" {
			tableEntry["timestamp_column"] = t.TimestampColumn
		}
		inputTables = append(inputTables, tableEntry)
	}

	// Build output_tables array from spec
	outputTables := make([]map[string]any, 0, len(spec.OutputConfig.Tables))
	for _, t := range spec.OutputConfig.Tables {
		entry := map[string]any{
			"name":       t.Name,
			"bucket":     t.Bucket,
			"write_mode": t.WriteMode,
		}
		if t.LogicalName != "" {
			entry["logical_name"] = t.LogicalName
		}
		if len(t.PartitionFields) > 0 {
			pfs := make([]map[string]string, len(t.PartitionFields))
			for i, pf := range t.PartitionFields {
				pfs[i] = map[string]string{
					"source_column": pf.SourceColumn,
					"transform":     pf.Transform,
				}
			}
			entry["partition_fields"] = pfs
		}
		outputTables = append(outputTables, entry)
	}

	// Build config structure
	// Use MinIO mode if: S3 config is provided, OR lakekeeper is configured
	// (lakekeeper-vended creds flow through the gateway sidecar's
	// per-write minio backend; the local-only mode is a tests-only path).
	var config map[string]any

	if spec.DataLake.Endpoint != "" || spec.LakekeeperURL != "" {
		// MinIO mode - gateway connects directly to MinIO
		config = map[string]any{
			"mode":             "minio",
			"run_id":         runID,
			"component_name": spec.Name,
			"gateway":        gatewaySettings,
			"component": map[string]any{
				"inputs":  spec.Inputs,
				"outputs": outputsConfig,
				"config":  spec.Config,
			},
			"minio": map[string]any{
				"endpoint":   spec.DataLake.Endpoint,
				"bucket":     spec.DataLake.Bucket,
				"access_key": spec.DataLake.AccessKey,
				"secret_key": spec.DataLake.SecretKey,
				"region":     spec.DataLake.Region,
				"use_ssl":    spec.DataLake.UseSSL,
			},
			// S3 credentials for StorageBootstrap (native S3 components like DuckDB)
			"s3_endpoint":    spec.DataLake.Endpoint,
			"s3_access_key":  spec.DataLake.AccessKey,
			"s3_secret_key":  spec.DataLake.SecretKey,
			"s3_region":      spec.DataLake.Region,
			"s3_bucket_name": spec.DataLake.Bucket,
		}

		// Add bucket-based fields if specified
		if len(spec.InputBuckets) > 0 {
			config["input_buckets"] = spec.InputBuckets
		}
		if len(spec.OutputBuckets) > 0 {
			config["output_buckets"] = spec.OutputBuckets
		}
		if len(inputTables) > 0 {
			config["input_tables"] = inputTables
		}
		if len(outputTables) > 0 {
			config["output_tables"] = outputTables
		}
		if spec.OutputConfig.DefaultBucket != "" {
			config["default_bucket"] = spec.OutputConfig.DefaultBucket
		}
		if spec.OutputConfig.DefaultWriteMode != "" {
			config["default_write_mode"] = spec.OutputConfig.DefaultWriteMode
		}

		// Add lakekeeper URL + warehouse name if configured. Both are
		// required when LakekeeperURL is set: iceberg-go's REST client
		// adds `?warehouse=<name>` to /v1/config and lakekeeper rejects
		// the call (`GetConfigNoWarehouseProvided`) when the param is
		// missing. WarehouseName mirrors what runner.go already plumbs
		// into TableCommitSpec.
		if spec.LakekeeperURL != "" {
			config["lakekeeper_url"] = spec.LakekeeperURL
		}
		if spec.WarehouseName != "" {
			config["lakekeeper_warehouse_name"] = spec.WarehouseName
		}
		// Route catalog calls to the correct lakekeeper project via x-project-id.
		if spec.LakekeeperProjectID != "" {
			config["lakekeeper_project_id"] = spec.LakekeeperProjectID
		}

		// Secrets mount: gateway resolves $[name] refs from this in-container path.
		if spec.SecretsDir != "" {
			config["secrets_dir"] = "/var/run/secrets/datuplet"
		}
	} else {
		// Local mode - for testing without MinIO
		config = map[string]any{
			"mode":             "local",
			"run_id":   runID,
			"data_dir": "/data",
			"gateway":          gatewaySettings,
			"component": map[string]any{
				"inputs":  spec.Inputs,
				"outputs": outputsConfig,
				"config":  spec.Config,
			},
			"local": map[string]any{
				"input_format":  "auto",
				"output_format": "csv",
			},
		}

		// Add bucket-based fields for local mode too
		if len(spec.InputBuckets) > 0 {
			config["input_buckets"] = spec.InputBuckets
		}
		if len(spec.OutputBuckets) > 0 {
			config["output_buckets"] = spec.OutputBuckets
		}
		if len(inputTables) > 0 {
			config["input_tables"] = inputTables
		}
		if len(outputTables) > 0 {
			config["output_tables"] = outputTables
		}
		if spec.OutputConfig.DefaultBucket != "" {
			config["default_bucket"] = spec.OutputConfig.DefaultBucket
		}
		if spec.OutputConfig.DefaultWriteMode != "" {
			config["default_write_mode"] = spec.OutputConfig.DefaultWriteMode
		}

		// Secrets mount: gateway resolves $[name] refs from this in-container path.
		if spec.SecretsDir != "" {
			config["secrets_dir"] = "/var/run/secrets/datuplet"
		}
	}

	// Write to temp file
	tmpFile, err := os.CreateTemp("", "gateway-config-*.yaml")
	if err != nil {
		return "", err
	}
	defer tmpFile.Close()

	encoder := yaml.NewEncoder(tmpFile)
	if err := encoder.Encode(config); err != nil {
		os.Remove(tmpFile.Name())
		return "", err
	}

	return tmpFile.Name(), nil
}

// dockerRunTokenMountPath is the in-container path where the gateway
// sidecar / table-commit container expects a per-run JWT map (legacy).
// Mirrors the K8s convention (datuplet-runtoken volume) so DG and
// table-commit binaries read the file from a single mode-agnostic
// location regardless of orchestrator.
const dockerRunTokenMountPath = "/var/run/secrets/datuplet-runtoken/tokens"

// dockerRunTokenSinglePath is the in-container path for the single
// per-run JWT. The gateway sidecar and iceberg-job both read
// --run-token-path (or RUN_TOKEN_PATH env) from this location.
// Matches the K8s Secret projection path so container entrypoints
// are mode-agnostic.
const dockerRunTokenSinglePath = "/var/run/secrets/datuplet-runtoken/token"

// runTokenBind returns the host:container:ro bind string for a per-run
// JWT map, or "" if hostPath is empty. Pure helper so tests can verify
// the bind shape without spinning up a Docker daemon.
func runTokenBind(hostPath string) string {
	if hostPath == "" {
		return ""
	}
	return fmt.Sprintf("%s:%s:ro", hostPath, dockerRunTokenMountPath)
}

// runTokenSingleBind returns the host:container:ro bind string for a
// single-bearer JWT file. Mounts hostPath at dockerRunTokenSinglePath.
// Returns "" when hostPath is empty.
func runTokenSingleBind(hostPath string) string {
	if hostPath == "" {
		return ""
	}
	return fmt.Sprintf("%s:%s:ro", hostPath, dockerRunTokenSinglePath)
}

// SetRunTokenHostPath overrides the host filesystem path used to
// bind-mount /var/run/secrets/datuplet-runtoken/token (singular)
// into every spawned container. In K8s deploys the token is projected
// by a Secret volume and this field stays empty; `datuplet run --remote`
// uses it to mount ~/.datuplet/token at the canonical in-container
// location. The path MUST be absolute (Docker bind-mount requirement).
// No-op when path is "".
func (d *DockerOrchestrator) SetRunTokenHostPath(path string) {
	d.runTokenHostPath = path
}

// startGatewaySidecar starts the gateway container.
func (d *DockerOrchestrator) startGatewaySidecar(ctx context.Context, spec orchestrator.ComponentSpec, configPath string) (string, string, error) {
	gatewayName := fmt.Sprintf("datuplet-gateway-%s-%s", spec.Name, shortID(spec.ExecutionID))

	// Determine gateway mode based on data lake config, lakekeeper, or
	// filesystem storage type.
	//
	// When StorageType is filesystem AND neither DataLake.Endpoint nor
	// LakekeeperURL is set, use --minio so the gateway starts with be=nil
	// (no static backend). NewServerV2 then detects
	// DATUPLET_STORAGE_TYPE=filesystem from the container env and wires the
	// SQLite resolver. Using --local here would create a LocalBackend
	// (non-nil) which fatals when a resolver is also present.
	gatewayMode := "--local"
	if spec.DataLake.Endpoint != "" || spec.LakekeeperURL != "" || spec.StorageType == "filesystem" {
		gatewayMode = "--minio"
	}

	// Gateway container environment variables.
	// In local-file mode DG reads DATUPLET_STORAGE_TYPE and
	// DATUPLET_STORAGE_ROOT from its process env (server_v2.go) to select
	// the SQLite resolver. Pass them explicitly so the container sees the
	// same values as the host process.
	var gatewayEnv []string
	if spec.StorageType == "filesystem" {
		gatewayEnv = append(gatewayEnv, "DATUPLET_STORAGE_TYPE=filesystem")
		if spec.StorageRoot != "" {
			gatewayEnv = append(gatewayEnv, fmt.Sprintf("DATUPLET_STORAGE_ROOT=%s", spec.StorageRoot))
		}
	}

	// Gateway container config.
	// When spec.RunTokenPath is set, forward `--run-token-path` so the
	// gateway loads the JWT and authenticates with lakekeeper. The K8s
	// path projects the token via a Secret volume; Docker bind-mounts
	// the host file directly.
	// When d.runTokenHostPath is set (via SetRunTokenHostPath), mount a
	// single bearer JWT at dockerRunTokenSinglePath and pass that path
	// as --run-token-path.
	cmd := []string{gatewayMode, "--config", "/config/gateway.yaml", "--addr", ":" + GatewayPort}
	if spec.RunTokenPath != "" {
		cmd = append(cmd, "--run-token-path", dockerRunTokenMountPath)
	} else if d.runTokenHostPath != "" {
		cmd = append(cmd, "--run-token-path", dockerRunTokenSinglePath)
	}
	containerConfig := &container.Config{
		Image:  d.gatewayImage,
		Cmd:    cmd,
		Env:    gatewayEnv,
		Labels: mergeLabels(spec.Labels),
	}

	// Mount config file and data directory
	absConfigPath, _ := filepath.Abs(configPath)

	hostConfig := &container.HostConfig{
		Binds: []string{
			fmt.Sprintf("%s:/config/gateway.yaml:ro", absConfigPath),
		},
	}

	// Add volume bindings from spec (for data access)
	for hostPath, containerPath := range spec.Volumes {
		hostConfig.Binds = append(hostConfig.Binds, fmt.Sprintf("%s:%s", hostPath, containerPath))
	}

	// Mount warehouse directory for filesystem mode (gateway needs direct
	// filesystem access). Mount at the host absolute path inside the
	// container so file:// URIs written by lakekeeper / TableCommit
	// resolve identically on host and in every sidecar.
	if spec.StorageType == "filesystem" && spec.StorageRoot != "" {
		hostConfig.Binds = append(hostConfig.Binds, fmt.Sprintf("%s:%s", spec.StorageRoot, spec.StorageRoot))
	}

	// Mount secrets directory for $[name] resolution (gateway sidecar only).
	if spec.SecretsDir != "" {
		hostConfig.Binds = append(hostConfig.Binds,
			fmt.Sprintf("%s:/var/run/secrets/datuplet:ro", spec.SecretsDir))
	}

	// Mount per-run token file for lakekeeper auth. Bind to the path the
	// gateway expects. Read-only.
	if b := runTokenBind(spec.RunTokenPath); b != "" {
		hostConfig.Binds = append(hostConfig.Binds, b)
	}
	// Single-bearer JWT mount for local-CLI mode.
	if spec.RunTokenPath == "" {
		if b := runTokenSingleBind(d.runTokenHostPath); b != "" {
			hostConfig.Binds = append(hostConfig.Binds, b)
		}
	}

	networkConfig := &network.NetworkingConfig{}
	if d.network != "" {
		networkConfig.EndpointsConfig = map[string]*network.EndpointSettings{
			d.network: {},
		}
	}

	resp, err := d.client.ContainerCreate(ctx, containerConfig, hostConfig, networkConfig, nil, gatewayName)
	if err != nil {
		return "", "", fmt.Errorf("failed to create gateway container: %w", err)
	}

	if spec.OnContainerStarted != nil {
		spec.OnContainerStarted(resp.ID)
	}

	fmt.Printf("    Starting gateway sidecar: %s\n", gatewayName)
	if err := d.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		d.client.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true})
		return "", "", fmt.Errorf("failed to start gateway container: %w", err)
	}

	return resp.ID, gatewayName, nil
}

// waitForGatewayReady waits for the gateway to be ready to accept connections.
func (d *DockerOrchestrator) waitForGatewayReady(ctx context.Context, containerID string) error {
	// Simple wait - check container is running and give it time to start
	deadline := time.Now().Add(30 * time.Second)

	for time.Now().Before(deadline) {
		inspect, err := d.client.ContainerInspect(ctx, containerID)
		if err != nil {
			return err
		}

		if !inspect.State.Running {
			return fmt.Errorf("gateway container exited: %s", inspect.State.Status)
		}

		// Check logs for "listening" message
		logs, err := d.client.ContainerLogs(ctx, containerID, container.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
		})
		if err == nil {
			logBytes, _ := io.ReadAll(logs)
			logs.Close()
			if strings.Contains(string(logBytes), "listening") {
				return nil // Gateway is ready
			}
		}

		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for gateway to be ready")
}

// runComponent runs the component container.
func (d *DockerOrchestrator) runComponent(ctx context.Context, spec orchestrator.ComponentSpec, gatewayName string) (*orchestrator.ExecutionResult, error) {
	result := &orchestrator.ExecutionResult{}

	// Build environment - only gateway address, not data lake credentials
	env := []string{
		fmt.Sprintf("DATUPLET_GATEWAY_ADDR=%s:%s", gatewayName, GatewayPort),
	}

	// Add extra environment variables
	for key, value := range spec.ExtraEnv {
		env = append(env, fmt.Sprintf("%s=%s", key, value))
	}

	// Build volume bindings
	var binds []string
	for hostPath, containerPath := range spec.Volumes {
		binds = append(binds, fmt.Sprintf("%s:%s", hostPath, containerPath))
	}

	// Mount warehouse directory for filesystem mode (component needs direct
	// access to parquet files). Mount at the host absolute path inside the
	// container so file:// URIs received from lakekeeper resolve without
	// any path translation.
	if spec.StorageType == "filesystem" && spec.StorageRoot != "" {
		binds = append(binds, fmt.Sprintf("%s:%s", spec.StorageRoot, spec.StorageRoot))
	}

	containerConfig := &container.Config{
		Image:  spec.Image,
		Env:    env,
		Labels: mergeLabels(spec.Labels),
	}

	hostConfig := &container.HostConfig{
		Binds: binds,
	}

	networkConfig := &network.NetworkingConfig{}
	if d.network != "" {
		networkConfig.EndpointsConfig = map[string]*network.EndpointSettings{
			d.network: {},
		}
	}

	containerName := fmt.Sprintf("datuplet-%s-%s", spec.Name, shortID(spec.ExecutionID))

	resp, err := d.client.ContainerCreate(ctx, containerConfig, hostConfig, networkConfig, nil, containerName)
	if err != nil {
		result.Error = fmt.Sprintf("failed to create container: %v", err)
		return result, err
	}

	if spec.OnContainerStarted != nil {
		spec.OnContainerStarted(resp.ID)
	}

	defer func() {
		d.client.ContainerRemove(context.Background(), resp.ID, container.RemoveOptions{Force: true})
	}()

	fmt.Printf("    Starting component: %s\n", containerName)
	if err := d.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		result.Error = fmt.Sprintf("failed to start container: %v", err)
		return result, err
	}

	// Wait for container to finish
	statusCh, errCh := d.client.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)

	select {
	case err := <-errCh:
		result.Error = fmt.Sprintf("error waiting for container: %v", err)
		return result, err
	case status := <-statusCh:
		result.ExitCode = int(status.StatusCode)
		result.Success = status.StatusCode == 0

		if status.Error != nil {
			result.Error = status.Error.Message
		}
	}

	// Collect logs
	logs, err := d.client.ContainerLogs(ctx, resp.ID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err == nil {
		logBytes, _ := io.ReadAll(logs)
		result.Logs = cleanDockerLogs(string(logBytes))
		logs.Close()
	}

	// Print logs
	if result.Logs != "" {
		fmt.Printf("    Component logs:\n")
		for _, line := range strings.Split(result.Logs, "\n") {
			if line != "" {
				fmt.Printf("      %s\n", line)
			}
		}
	}

	if !result.Success {
		result.FailureType = string(status.ClassifyExitCode(result.ExitCode))
		result.StatusMessage = status.ExtractStatusMessage(result.Logs, result.ExitCode)
		return result, fmt.Errorf("component failed (exit %d, %s): %s", result.ExitCode, result.FailureType, result.StatusMessage)
	}

	return result, nil
}

// ExecuteTableCommit runs a TableCommit job to commit a write session to an Iceberg table.
func (d *DockerOrchestrator) ExecuteTableCommit(ctx context.Context, spec orchestrator.TableCommitSpec) error {
	const tableCommitImage = "datuplet/iceberg-job:latest"

	// Pull image if needed
	if err := d.ensureImage(ctx, tableCommitImage); err != nil {
		return fmt.Errorf("failed to pull table-commit image: %w", err)
	}

	// Build environment
	env := []string{
		fmt.Sprintf("RUN_ID=%s", spec.RunID),
		fmt.Sprintf("WRITE_MODE=%s", spec.WriteMode),
		fmt.Sprintf("WAREHOUSE_PATH=%s", spec.WarehousePath),
		fmt.Sprintf("S3_ENDPOINT=%s", spec.DataLake.Endpoint),
		fmt.Sprintf("S3_BUCKET=%s", spec.DataLake.Bucket),
		fmt.Sprintf("S3_ACCESS_KEY=%s", spec.DataLake.AccessKey),
		fmt.Sprintf("S3_SECRET_KEY=%s", spec.DataLake.SecretKey),
	}

	// Forward the catalog endpoint, warehouse name, and warehouse root so
	// the binary can pick them up via --lakekeeper-url/--warehouse-name/
	// --warehouse-root. Empty values are pushed through verbatim — the CLI
	// hard-fails on missing required ones, surfacing misconfiguration early.
	if spec.LakekeeperURL != "" {
		env = append(env, fmt.Sprintf("LAKEKEEPER_URL=%s", spec.LakekeeperURL))
	}
	if spec.WarehouseName != "" {
		env = append(env, fmt.Sprintf("WAREHOUSE_NAME=%s", spec.WarehouseName))
	}
	if spec.WarehouseRoot != "" {
		env = append(env, fmt.Sprintf("WAREHOUSE_ROOT=%s", spec.WarehouseRoot))
	}
	// Forward the lakekeeper Project UUID so table-commit's catalog calls
	// land in the right project.
	if spec.LakekeeperProjectID != "" {
		env = append(env, fmt.Sprintf("LAKEKEEPER_PROJECT_ID=%s", spec.LakekeeperProjectID))
	}

	// Add bucket or table parameter based on what's provided
	if spec.Bucket != "" {
		// Bucket-based API (new)
		env = append(env, fmt.Sprintf("BUCKET=%s", spec.Bucket))
	} else if spec.Table != "" {
		// Legacy table-based API
		env = append(env, fmt.Sprintf("TABLE=%s", spec.Table))
	}
	if spec.DataLake.Region != "" {
		env = append(env, fmt.Sprintf("S3_REGION=%s", spec.DataLake.Region))
	}
	if spec.DataLake.UseSSL {
		env = append(env, "S3_USE_SSL=true")
	}
	if spec.DataLake.UsePathStyle {
		env = append(env, "S3_USE_PATH_STYLE=true")
	}

	// Forward RUN_TOKEN_PATH so the tablecommit binary attaches the JWT as
	// the Authorization header on lakekeeper calls. K8s projects the token
	// via a Secret volume; Docker bind-mounts the host file directly.
	if spec.RunTokenPath != "" {
		env = append(env, fmt.Sprintf("RUN_TOKEN_PATH=%s", dockerRunTokenMountPath))
	} else if d.runTokenHostPath != "" {
		// Single-bearer JWT for local-CLI mode.
		env = append(env, fmt.Sprintf("RUN_TOKEN_PATH=%s", dockerRunTokenSinglePath))
	}

	containerConfig := &container.Config{
		Image:  tableCommitImage,
		Env:    env,
		Labels: mergeLabels(spec.Labels),
	}

	networkConfig := &network.NetworkingConfig{}
	if d.network != "" {
		networkConfig.EndpointsConfig = map[string]*network.EndpointSettings{
			d.network: {},
		}
	}

	// Container name based on bucket or table
	targetName := spec.Bucket
	if targetName == "" {
		targetName = spec.Table
	}
	containerName := fmt.Sprintf("datuplet-table-commit-%s-%s",
		strings.ReplaceAll(targetName, "_", "-"),
		shortID(spec.RunID))

	hostConfig := &container.HostConfig{}

	// Mount warehouse directory for filesystem mode (tablecommit needs direct
	// filesystem access). Mount at the host absolute path on both sides so
	// file:// URIs (which DG, lakekeeper, and TableCommit all touch)
	// resolve identically on the host and in every container.
	if spec.StorageType == "filesystem" && spec.StorageRoot != "" {
		hostConfig.Binds = []string{
			fmt.Sprintf("%s:%s", spec.StorageRoot, spec.StorageRoot),
		}
	}

	// Bind-mount the per-run JWT at the standard runtoken path.
	if b := runTokenBind(spec.RunTokenPath); b != "" {
		hostConfig.Binds = append(hostConfig.Binds, b)
	}
	// Single-bearer JWT mount for local-CLI mode.
	if spec.RunTokenPath == "" {
		if b := runTokenSingleBind(d.runTokenHostPath); b != "" {
			hostConfig.Binds = append(hostConfig.Binds, b)
		}
	}

	resp, err := d.client.ContainerCreate(ctx, containerConfig, hostConfig, networkConfig, nil, containerName)
	if err != nil {
		return fmt.Errorf("failed to create table-commit container: %w", err)
	}

	if spec.OnContainerStarted != nil {
		spec.OnContainerStarted(resp.ID)
	}

	defer func() {
		d.client.ContainerRemove(context.Background(), resp.ID, container.RemoveOptions{Force: true})
	}()

	if spec.Bucket != "" {
		fmt.Printf("    Running TableCommit: %s -> bucket %s\n", spec.RunID, spec.Bucket)
	} else {
		fmt.Printf("    Running TableCommit: %s -> table %s\n", spec.RunID, spec.Table)
	}
	if err := d.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("failed to start table-commit container: %w", err)
	}

	// Wait for container to finish
	statusCh, errCh := d.client.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)

	select {
	case err := <-errCh:
		return fmt.Errorf("error waiting for table-commit: %w", err)
	case status := <-statusCh:
		if status.StatusCode != 0 {
			// Get logs for debugging
			logs, _ := d.client.ContainerLogs(ctx, resp.ID, container.LogsOptions{
				ShowStdout: true,
				ShowStderr: true,
			})
			if logs != nil {
				logBytes, _ := io.ReadAll(logs)
				logs.Close()
				return fmt.Errorf("table-commit failed (exit %d): %s", status.StatusCode, cleanDockerLogs(string(logBytes)))
			}
			return fmt.Errorf("table-commit failed with exit code %d", status.StatusCode)
		}
	}

	if spec.Bucket != "" {
		fmt.Printf("    TableCommit succeeded: bucket %s\n", spec.Bucket)
	} else {
		fmt.Printf("    TableCommit succeeded: table %s\n", spec.Table)
	}
	return nil
}

// ForceStop kills and removes a container by ID. Idempotent: not-found
// errors are swallowed. Used by LocalBackend on run cancel; the
// corresponding CLI path is always `Cleanup` at pipeline end.
func (d *DockerOrchestrator) ForceStop(ctx context.Context, containerID string) error {
	if containerID == "" {
		return nil
	}
	timeout := 0
	if err := d.client.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &timeout}); err != nil && !client.IsErrNotFound(err) {
		return fmt.Errorf("container stop: %w", err)
	}
	if err := d.client.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true}); err != nil && !client.IsErrNotFound(err) {
		return fmt.Errorf("container remove: %w", err)
	}
	return nil
}

// mergeLabels returns a fresh map containing the entries from the given
// labels map. Used at every ContainerCreate site so every container the
// orchestrator spawns carries the caller-supplied labels. Returns nil
// when there's nothing to set so Docker's Config.Labels stays unset.
func mergeLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		out[k] = v
	}
	return out
}

// Cleanup closes the Docker client.
func (d *DockerOrchestrator) Cleanup(ctx context.Context) error {
	return d.client.Close()
}

// EnsureNetwork creates the network if it doesn't exist.
func (d *DockerOrchestrator) EnsureNetwork(ctx context.Context) error {
	if d.network == "" {
		return nil
	}

	networks, err := d.client.NetworkList(ctx, network.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list networks: %w", err)
	}

	for _, n := range networks {
		if n.Name == d.network {
			return nil
		}
	}

	_, err = d.client.NetworkCreate(ctx, d.network, network.CreateOptions{
		Driver: "bridge",
	})
	if err != nil {
		return fmt.Errorf("failed to create network: %w", err)
	}

	return nil
}

// ensureImage pulls the image if it's not already present.
func (d *DockerOrchestrator) ensureImage(ctx context.Context, imageName string) error {
	_, err := d.client.ImageInspect(ctx, imageName)
	if err == nil {
		return nil // Image exists
	}

	fmt.Printf("    Pulling image: %s\n", imageName)
	reader, err := d.client.ImagePull(ctx, imageName, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("failed to pull image %s: %w", imageName, err)
	}
	io.Copy(io.Discard, reader)
	reader.Close()

	return nil
}

// cleanDockerLogs removes Docker log stream headers from the output.
func cleanDockerLogs(logs string) string {
	// Docker multiplexed stream has 8-byte header per line
	// Format: [stream_type:1][0:3][size:4][payload]
	var cleaned strings.Builder
	lines := strings.Split(logs, "\n")
	for _, line := range lines {
		if len(line) > 8 {
			// Skip the 8-byte header
			cleaned.WriteString(line[8:])
			cleaned.WriteString("\n")
		} else if len(line) > 0 {
			cleaned.WriteString(line)
			cleaned.WriteString("\n")
		}
	}
	return strings.TrimSpace(cleaned.String())
}

// Ensure DockerOrchestrator implements orchestrator.Orchestrator interface
var _ orchestrator.Orchestrator = (*DockerOrchestrator)(nil)
