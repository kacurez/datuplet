package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/datuplet/datuplet/pkg/lib/orchestrator"
	"github.com/datuplet/datuplet/pkg/lib/orchestrator/docker"
)

func testComponent(image, config, endpoint, bucket string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nReceived shutdown signal...")
		cancel()
	}()

	// Create Docker orchestrator with default network
	orch, err := docker.NewDockerOrchestrator("datuplet")
	if err != nil {
		return fmt.Errorf("failed to create docker orchestrator: %w", err)
	}
	defer orch.Cleanup(ctx)

	// Ensure network exists
	if err := orch.EnsureNetwork(ctx); err != nil {
		return fmt.Errorf("failed to ensure network: %w", err)
	}

	// Parse config JSON
	var configMap map[string]any
	if err := json.Unmarshal([]byte(config), &configMap); err != nil {
		return fmt.Errorf("invalid config JSON: %w", err)
	}

	// Extract volume mounts from config (for local files)
	volumes := extractVolumeMounts(configMap)

	// Build component spec
	spec := orchestrator.ComponentSpec{
		Name:        "test-component",
		Image:       image,
		ExecutionID: "test-" + fmt.Sprintf("%d", time.Now().Unix()),
		Config:      configMap,
		Volumes:     volumes,
		DataLake: orchestrator.DataLakeConfig{
			Endpoint:  endpoint,
			Bucket:    bucket,
			AccessKey: os.Getenv("MINIO_ACCESS_KEY"),
			SecretKey: os.Getenv("MINIO_SECRET_KEY"),
		},
		Outputs: map[string]orchestrator.OutputConfig{
			"output": {
				StagingPath: "staging/test/output/",
				FinalPath:   "test/output",
			},
		},
		ChunkSize: 1048576,
	}

	if spec.DataLake.AccessKey == "" {
		spec.DataLake.AccessKey = "minioadmin"
	}
	if spec.DataLake.SecretKey == "" {
		spec.DataLake.SecretKey = "minioadmin"
	}

	fmt.Printf("Testing component: %s\n", image)
	fmt.Printf("Config: %v\n", configMap)

	result, err := orch.ExecuteComponent(ctx, spec)
	if err != nil {
		return err
	}

	if result.Success {
		fmt.Println("\nComponent completed successfully!")
	} else {
		fmt.Printf("\nComponent failed: %s\n", result.Error)
	}

	return nil
}

func sampleComponent(image, config string, limit int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	// Create Docker orchestrator with default network
	orch, err := docker.NewDockerOrchestrator("datuplet")
	if err != nil {
		return fmt.Errorf("failed to create docker orchestrator: %w", err)
	}
	defer orch.Cleanup(ctx)

	// Ensure network exists
	if err := orch.EnsureNetwork(ctx); err != nil {
		return fmt.Errorf("failed to ensure network: %w", err)
	}

	// Parse config JSON
	var configMap map[string]any
	if err := json.Unmarshal([]byte(config), &configMap); err != nil {
		return fmt.Errorf("invalid config JSON: %w", err)
	}

	// Extract volume mounts from config (for local files)
	volumes := extractVolumeMounts(configMap)

	// Build component spec with sample mode (direct mode, no gateway)
	spec := orchestrator.ComponentSpec{
		Name:        "sample",
		Image:       image,
		ExecutionID: "sample-" + fmt.Sprintf("%d", time.Now().Unix()),
		Config:      configMap,
		Volumes:     volumes,
		DirectMode:  true, // Run without gateway sidecar
		ExtraEnv: map[string]string{
			"DATUPLET_MODE":         "sample",
			"DATUPLET_SAMPLE_LIMIT": fmt.Sprintf("%d", limit),
		},
	}

	result, err := orch.ExecuteComponent(ctx, spec)
	if err != nil {
		return err
	}

	if !result.Success {
		return fmt.Errorf("sample failed: %s", result.Error)
	}

	// The sample JSON is in the logs (stdout from the component)
	fmt.Println(result.Logs)
	return nil
}

// extractVolumeMounts scans config for file paths and creates volume mounts.
func extractVolumeMounts(config map[string]any) map[string]string {
	volumes := make(map[string]string)

	filePathKeys := []string{"source", "file", "path", "input_file"}

	for _, key := range filePathKeys {
		if val, ok := config[key]; ok {
			if strVal, ok := val.(string); ok {
				if strings.HasPrefix(strVal, "/") || strings.Contains(strVal, string(os.PathSeparator)) {
					absPath, err := filepath.Abs(strVal)
					if err != nil {
						continue
					}

					if _, err := os.Stat(absPath); os.IsNotExist(err) {
						continue
					}

					parentDir := filepath.Dir(absPath)
					volumes[parentDir] = "/data"

					// Update config to use container path
					config[key] = "/data/" + filepath.Base(absPath)
					break
				}
			}
		}
	}

	return volumes
}
