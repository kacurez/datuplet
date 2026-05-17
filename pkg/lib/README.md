# pkg/lib - Shared Infrastructure Libraries

> **Created 2026-01-20**: Refactored from flat pkg/ structure to separate domain-agnostic infrastructure

## Purpose

This directory contains **domain-agnostic infrastructure libraries** that are reused across multiple Datuplet services and commands. These are foundational abstractions that know nothing about Datuplet-specific domain logic (pipelines, Iceberg, proxies, etc.).

## Packages

### `datalake/` - Storage Abstraction
**What**: Simple MinIO/S3 client with 3 operations: Read, Write, List
**Used by**:
- `pkg/pipeline` (pipeline execution)
- `pkg/tablecommit` (files.json read path)
- `pkg/pipelineapi/storage` (catalog browse via lakekeeper)
- `cmd/datuplet` (CLI commands)

**Why in lib/**: Truly shared across all services, no Datuplet-specific logic

### `orchestrator/` - Container Execution Framework
**What**: Abstraction over Docker/Kubernetes for running containers
**Used by**:
- `cmd/datuplet` (creates Docker orchestrator)
- `pkg/pipeline` (uses orchestrator interface)

**Why in lib/**: Deployment-agnostic abstraction, supports Docker + K8s

**Implementations**:
- `docker/` - Docker implementation (current)
- `kubernetes/` - K8s implementation (Phase 10, future)

## Design Principles

### What Belongs in lib/

✅ **Domain-agnostic infrastructure**:
- Storage abstractions (S3, filesystems)
- Execution frameworks (Docker, K8s)
- Protocol implementations (gRPC, HTTP)
- Generic utilities (logging, metrics)

✅ **Shared by multiple services**:
- Used by 2+ services or commands
- Not specific to one component

✅ **No business logic**:
- No knowledge of Iceberg, pipelines, proxies
- Pure infrastructure/framework code

### What Does NOT Belong in lib/

❌ **Domain logic** (stays at `pkg/` top-level):
- `pkg/tablecommit` - Iceberg commit logic
- `pkg/datagateway` - Data transformation
- `pkg/pipeline` - Pipeline execution

Even though tablecommit is reused by CLI and operators, it contains Datuplet-specific domain logic and stays at top-level.

❌ **Single-use libraries**:
- If only one service uses it, nest under that service
- Example: `pkg/pipeline/config` (only pipeline uses it)

## Dependency Rules

### lib/ Packages Must

✅ **Have zero internal dependencies**:
- `pkg/lib/datalake` imports ZERO other pkg/ packages
- `pkg/lib/orchestrator` imports ZERO other pkg/ packages
- Only external dependencies allowed (stdlib, third-party)

✅ **Define clean interfaces**:
- `datalake.DataLake` interface
- `orchestrator.Orchestrator` interface

✅ **Support multiple implementations**:
- `datalake` could support: MinIO, S3, GCS, DuckDB
- `orchestrator` supports: Docker (now), Kubernetes (future)

### Services Can

✅ **Depend on lib/**:
```go
import "github.com/datuplet/datuplet/pkg/lib/datalake"
import "github.com/datuplet/datuplet/pkg/lib/orchestrator"
```

✅ **Depend on each other** (when needed):
```go
import "github.com/datuplet/datuplet/pkg/tablecommit"  // Domain logic
```

❌ **lib/ packages CANNOT import services**:
```go
// WRONG - lib/ importing domain logic
import "github.com/datuplet/datuplet/pkg/tablecommit"  // NO!
```

## Future Extensions

### Adding to lib/

Before adding a new lib/ package, ask:
1. **Is it domain-agnostic?** (No Datuplet-specific logic)
2. **Is it shared?** (Used by 2+ services)
3. **Is it infrastructure?** (Storage, execution, protocols)
4. **Has zero pkg/ dependencies?** (Only external deps)

If YES to all 4, it belongs in `pkg/lib/`.

### Examples of Future lib/ Packages

✅ **Good candidates**:
- `pkg/lib/metrics/` - Prometheus metrics abstraction
- `pkg/lib/logging/` - Structured logging
- `pkg/lib/storage/` - Generic storage interface (S3, GCS, Azure)
- `pkg/lib/cache/` - Generic caching (Redis, in-memory)

❌ **Bad candidates** (domain logic, stay at pkg/ top-level):
- `pkg/iceberg/` - Iceberg operations (use `pkg/tablecommit`)
- `pkg/datagateway/processor/` - Data transformations (use `pkg/datagateway/processor`)
- `pkg/catalog/` - Table catalog (Datuplet-specific)

## Related Documentation

- [pkg/lib/datalake/CLAUDE.md](datalake/CLAUDE.md) - Storage abstraction
- [pkg/lib/orchestrator/CLAUDE.md](orchestrator/CLAUDE.md) - Execution framework
- [pkg/pipeline/CLAUDE.md](../pipeline/CLAUDE.md) - Pipeline execution (uses lib/)
