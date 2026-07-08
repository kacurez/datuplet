# Enable Docker BuildKit for cache mounts and improved performance
export DOCKER_BUILDKIT=1

.PHONY: \
	build build-pipeline-api build-gateway build-iceberg-job build-services \
	build-components build-components-e2e build-component-data-generator build-operators \
	build-component-sql-transform build-component-datuplet-query \
	docker-build-operators docker-build-pipeline-api docker-build-pipeline-observer docker-build-k8s \
	clean clean-go-git-cache \
	test e2e e2e-k8s e2e-k8s-gcs e2e-all \
	deploy-local deploy-local-helm undeploy-local k8s-smoke \
	k8s-reload-crds k8s-rebuild-operators k8s-rebuild-services \
	k8s-retry-simple k8s-retry-duckdb k8s-retry-full \
	k8s-rebuild-retry-simple k8s-rebuild-all \
	port-forward-minio-k8s kill-port-forward-minio-k8s \
	prune-images prune-docker-cache \
	lint chart-render-check tidy all help

# =============================================================================
# Help
# =============================================================================

help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z][a-zA-Z0-9_-]*:.*?## / {printf "  %-35s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# =============================================================================
# Build
# =============================================================================

build: ## Build CLI binary (datuplet)
	go build -o bin/datuplet ./cmd/datuplet

build-pipeline-api: ## Build pipeline-api binary
	go build -o bin/pipeline-api ./cmd/pipeline-api

build-operators: ## Build pipeline-operator binary
	go build -o bin/pipeline-operator ./cmd/pipeline-operator

# Build the data gateway sidecar image
build-gateway: ## Build gateway Docker image
	docker build -t datuplet/gateway:latest -f utils/docker/Dockerfile.gateway .

# Build the iceberg job image
build-iceberg-job: ## Build iceberg-job Docker image
	docker build -t datuplet/iceberg-job:latest -f utils/docker/Dockerfile.iceberg-job .

build-services: build-iceberg-job ## Build service images (iceberg-job)

# Build all components (from root context)
build-components: build-gateway ## Build all component Docker images
	docker build -t datuplet/data-generator:latest -f components/data-generator/Dockerfile .
	docker build -t datuplet/sql-transform:latest -f components/sql-transform/Dockerfile .
	docker build -t datuplet/stdout-writer:latest -f components/stdout-writer/Dockerfile .
	docker build -t datuplet/http-json-extractor:latest -f components/http-json-extractor/Dockerfile .
	docker build -t datuplet/finnhub-extractor:latest -f components/finnhub-extractor/Dockerfile .
	docker build -t datuplet/query-worker:latest -f utils/docker/query-worker.Dockerfile .
#   docker build -t datuplet/pandas-transform:latest -f components/pandas-transform/Dockerfile .

# E2E-only component subset: skips finnhub-extractor (no scenario references it).
# Keeps gateway as a build dep (sidecar is required for every run). RFC 010
# scenarios (duckdb-etl, multi-table-join) need datuplet/sql-transform too.
# RFC 022 Task 2.7: query-worker is needed for the query e2e scenarios.
# RFC 026 Task R11: data-generator is additionally tagged v0.0.1 — a second,
# STABLE tag of the same local image — so the e2e ComponentDefinition
# bootstrap (tests/e2e/framework/components_bootstrap.go) has a real stable
# version to register alongside the mutable "dev" tag, giving the
# unpinned-resolution and schema-invalid scenarios something to pin against.
build-components-e2e: build-gateway build-component-sql-transform ## Build only the components actively used by e2e (data-generator + http-json-extractor + stdout-writer + sql-transform + query-worker)
	docker build -t datuplet/data-generator:latest -f components/data-generator/Dockerfile .
	docker tag datuplet/data-generator:latest datuplet/data-generator:v0.0.1
	docker build -t datuplet/http-json-extractor:latest -f components/http-json-extractor/Dockerfile .
	docker build -t datuplet/stdout-writer:latest -f components/stdout-writer/Dockerfile .
	docker build -t datuplet/query-worker:latest -f utils/docker/query-worker.Dockerfile .

build-component-sql-transform: ## Build sql-transform component image (RFC 010)
	docker build -t datuplet/sql-transform:latest -f components/sql-transform/Dockerfile .

# datuplet-query (RFC 022 Task 3.1): BYO-local ad-hoc SQL binary. SEPARATE
# install from the duckdb-free root `datuplet` CLI — the root binary cannot run
# DuckDB locally; this duckdb-tagged image/binary is required for `--local`.
build-component-datuplet-query: ## Build datuplet-query image (RFC 022, BYO-local SQL)
	docker build -t datuplet/datuplet-query:latest -f utils/docker/datuplet-query.Dockerfile .

build-component-data-generator: ## Build data-generator component image
	docker build -t datuplet/data-generator:latest -f components/data-generator/Dockerfile .

docker-build-operators: ## Build pipeline-operator Docker image
	docker build -t datuplet/pipeline-operator:latest -f utils/docker/Dockerfile.pipeline-operator .

docker-build-pipeline-api: ## Build pipeline-api Docker image
	DOCKER_BUILDKIT=1 docker build -f utils/docker/Dockerfile.pipeline-api -t datuplet/pipeline-api:latest .

docker-build-pipeline-observer: ## Build pipeline-observer Docker image (RFC 015)
	DOCKER_BUILDKIT=1 docker build -f utils/docker/Dockerfile.pipeline-observer -t datuplet/pipeline-observer:latest .

# Build all K8s images (operators + services)
docker-build-k8s: docker-build-operators build-gateway build-iceberg-job docker-build-pipeline-api docker-build-pipeline-observer ## Build all K8s images (operators + gateway + iceberg-job + pipeline-api + pipeline-observer)

# =============================================================================
# K8s UX umbrellas
# =============================================================================

# k8s-smoke — smoke against the OrbStack cluster via NodePort 30081.
# Probes /healthz, /api/v1/auth/jwks.json, and /.well-known/openid-configuration
# to confirm the cluster stack is functional end-to-end (lakekeeper OIDC
# validated by pipeline-api's JWKS). Minimal bar for "nothing is broken at
# the API layer".
k8s-smoke: ## Smoke the OrbStack cluster via NodePort 30081 (health + OIDC probes)
	@echo "=== k8s-smoke: probing NodePort :30081 ==="
	@if ! curl -fsS http://localhost:30081/healthz >/dev/null 2>&1; then \
		echo "ERROR: pipeline-api not reachable at http://localhost:30081"; \
		echo "  Run 'make k8s-up' first."; \
		exit 1; \
	fi
	@echo "  /healthz: OK"
	@if ! curl -fsS http://localhost:30081/.well-known/openid-configuration >/dev/null 2>&1; then \
		echo "ERROR: OIDC discovery not reachable at http://localhost:30081/.well-known/openid-configuration"; \
		exit 1; \
	fi
	@echo "  /.well-known/openid-configuration: OK"
	@if ! curl -fsS http://localhost:30081/api/v1/auth/jwks.json >/dev/null 2>&1; then \
		echo "ERROR: JWKS not reachable at http://localhost:30081/api/v1/auth/jwks.json"; \
		exit 1; \
	fi
	@echo "  /api/v1/auth/jwks.json: OK"
	@echo "OK: k8s-smoke passed (health + OIDC probes against NodePort :30081)"
	@echo "NOTE: Full pipeline smoke (trigger + storage browse) requires cluster-mode"
	@echo "  project provisioning + FGA grants. Use make e2e-k8s for full coverage."

# =============================================================================
# K8s MinIO port-forward
# =============================================================================

port-forward-minio-k8s: ## Port-forward MinIO from K8s cluster to localhost:9000
	kubectl port-forward svc/minio -n datuplet-e2e 9000:9000 &

kill-port-forward-minio-k8s: ## Kill the MinIO K8s port-forward
	-kill $(shell lsof -t -i:9000)

# =============================================================================
# Test
# =============================================================================

test: ## Run all unit tests
	go test -v ./...

e2e: e2e-k8s ## Run e2e (K8s tier — the only supported deployment surface)

e2e-k8s: docker-build-k8s build-components-e2e e2e-k8s-deploy ## Run e2e against helm-installed charts in namespace datuplet-e2e (build images + deploy + test + teardown)

# e2e-k8s-deploy: helm install + register + go test + teardown, NO image
# build. Used by the GitHub Actions e2e workflow which builds + `kind load`s
# images before invoking this target. Locally, prefer `make e2e-k8s`.
e2e-k8s-deploy: ## Deploy + test + teardown (images must already be present on the cluster nodes)
	helm dependency update charts/datuplet-operators
	helm dependency update charts/datuplet-infra
	helm dependency update charts/datuplet-app
	helm dependency update charts/datuplet-lakekeeper
	# Five-phase install: operators → infra → app → lakekeeper → register.
	# Sequential, each --wait --wait-for-jobs. Phases 2-4 are strict order:
	#   - infra owns CNPG + OpenFGA + MinIO + keygen (no Datuplet code)
	#   - app owns pipeline-api/observer/operator + authz-bootstrap (writes FGA pin)
	#   - lakekeeper installs after app finishes (its wait-for-fga-pin pre-install
	#     Job polls the pin tuple authz-bootstrap wrote)
	helm upgrade --install datuplet-operators charts/datuplet-operators \
	  -n datuplet-e2e --create-namespace \
	  -f tests/e2e/values-operators.yaml \
	  --wait --timeout 5m
	helm upgrade --install datuplet-infra charts/datuplet-infra \
	  -n datuplet-e2e \
	  -f tests/e2e/values-infra.yaml \
	  --wait --wait-for-jobs --timeout 10m
	helm upgrade --install datuplet-app charts/datuplet-app \
	  -n datuplet-e2e \
	  -f tests/e2e/values-app.yaml \
	  --wait --wait-for-jobs --timeout 10m
	helm upgrade --install datuplet-lakekeeper charts/datuplet-lakekeeper \
	  -n datuplet-e2e \
	  -f tests/e2e/values-lakekeeper.yaml \
	  --wait --wait-for-jobs --timeout 10m
	./scripts/register.sh --namespace datuplet-e2e
	# Run the suite with host-side port-forwards for the framework's
	# localhost endpoints (OpenFGA/Lakekeeper/pipeline-api/MinIO). Needed
	# on CI kind (no host port exposure); a no-op on OrbStack where the
	# NodePorts are already bound. Without this the FGA bootstrap can't
	# reach OpenFGA and every K8s scenario silently SKIPs.
	./scripts/e2e-port-forward.sh datuplet-e2e
	helm uninstall datuplet-lakekeeper -n datuplet-e2e || true
	helm uninstall datuplet-app -n datuplet-e2e || true
	helm uninstall datuplet-infra -n datuplet-e2e || true
	helm uninstall datuplet-operators -n datuplet-e2e || true
	kubectl delete namespace datuplet-e2e --wait=false || true

e2e-all: e2e-k8s ## Run all e2e tiers (K8s is the only supported surface; alias for e2e-k8s)

e2e-k8s-gcs: ## Run the GCS e2e scenario against fake-gcs-server (skips when no Ready nodes)
	./tests/e2e/scenarios/gcs-pipeline-k8s/run.sh

# =============================================================================
# Deploy (OrbStack K8s)
# =============================================================================

.PHONY: deploy-local deploy-local-helm undeploy-local
deploy-local: docker-build-k8s deploy-local-helm ## Build images + helm install all 4 charts + register.sh

# Helm-only deploy — skips image rebuild. Use this when images already exist
# in the local Docker daemon (OrbStack shares its image cache with K8s, so
# `make docker-build-k8s` once is enough; iterate on charts via this target).
deploy-local-helm: ## Helm install all 4 charts + register.sh (no docker build)
	helm dependency update charts/datuplet-operators
	helm dependency update charts/datuplet-infra
	helm dependency update charts/datuplet-app
	helm dependency update charts/datuplet-lakekeeper
	# RFC 015 5-phase install — see e2e-k8s target for ordering rationale.
	helm upgrade --install datuplet-operators charts/datuplet-operators \
	  -n datuplet --create-namespace --wait --timeout 5m
	helm upgrade --install datuplet-infra charts/datuplet-infra \
	  -n datuplet --wait --wait-for-jobs --timeout 10m
	helm upgrade --install datuplet-app charts/datuplet-app \
	  -n datuplet --wait --wait-for-jobs --timeout 10m
	helm upgrade --install datuplet-lakekeeper charts/datuplet-lakekeeper \
	  -n datuplet --wait --wait-for-jobs --timeout 10m
	./scripts/register.sh --namespace datuplet

# Symmetric tear-down for deploy-local-helm. Uninstalls in reverse install
# order (lakekeeper → app → infra → operators) so dependents are gone
# before their dependencies. Deletes the namespace last to clean up any
# lingering PVCs, kept Secrets (helm.sh/resource-policy: keep), and CRD
# instances. Idempotent — `-` prefix swallows "not found" errors on re-runs.
undeploy-local: ## Helm uninstall all 4 charts + delete datuplet namespace
	-helm uninstall datuplet-lakekeeper -n datuplet
	-helm uninstall datuplet-app -n datuplet
	-helm uninstall datuplet-infra -n datuplet
	-helm uninstall datuplet-operators -n datuplet
	-kubectl delete namespace datuplet --wait=false

# =============================================================================
# K8s (cluster ops — OrbStack only)
# =============================================================================

# Reload CRDs into cluster (apply updated CRD manifests).
# RFC 007 Slice 8: TableCommit CRD deleted — only Pipeline + PipelineRun remain.
k8s-reload-crds: ## Apply updated CRD manifests to cluster
	@echo "Reloading CRDs..."
	kubectl apply -f utils/deploy/k8s/crds/datuplet.io_pipelines.yaml
	kubectl apply -f utils/deploy/k8s/crds/datuplet.io_pipelineruns.yaml
	@echo "CRDs reloaded successfully"

# Rebuild operators: build images, apply CRDs & manifests, wait for rollout.
# OrbStack shares the host Docker daemon with its K8s node so no image load step is needed.
k8s-rebuild-operators: docker-build-operators ## Rebuild operator image + apply CRDs/RBAC + rollout restart
	@echo "Applying CRDs..."
	kubectl apply -f utils/deploy/k8s/crds/
	@echo "Applying RBAC..."
	kubectl apply -f utils/deploy/k8s/rbac/
	@echo "Applying operator deployments..."
	kubectl apply -f utils/deploy/k8s/operators.yaml
	kubectl rollout restart deployment/pipeline-operator -n datuplet-e2e
	@echo "Waiting for operators to be ready..."
	kubectl rollout status deployment/pipeline-operator -n datuplet-e2e --timeout=60s
	@echo "Operators rebuilt and ready!"

# Rebuild services: gateway, iceberg-job (RFC 007 Slice 9: TG retired)
k8s-rebuild-services: build-gateway build-iceberg-job ## Rebuild gateway + iceberg-job images
	@echo "Services rebuilt!"

# Clean up pipelineruns and retry the example (with CRD reload)
k8s-retry-simple: k8s-reload-crds ## Delete all PipelineRuns + re-apply simple-http-extract.yaml
	kubectl delete pipelinerun --all -n datuplet-e2e --ignore-not-found
	kubectl delete pipeline simple-pipeline -n datuplet-e2e --ignore-not-found
	kubectl apply -f examples/pipelines/simple-http-extract.yaml

k8s-retry-duckdb: k8s-reload-crds ## Delete all PipelineRuns + re-apply etl-duckdb.yaml
	kubectl delete pipelinerun --all -n datuplet-e2e --ignore-not-found
	kubectl delete pipeline duckdb-transform -n datuplet-e2e --ignore-not-found
	kubectl apply -f examples/pipelines/etl-duckdb.yaml

# Clean up pipelineruns and retry the example (with CRD reload)
k8s-retry-full: k8s-reload-crds ## Delete all PipelineRuns + re-apply full-etl.yaml
	kubectl delete pipelinerun --all -n datuplet-e2e --ignore-not-found
	kubectl delete pipeline full-pipeline -n datuplet-e2e --ignore-not-found
	kubectl apply -f examples/pipelines/full-etl.yaml

# Rebuild operators and retry example pipeline (comprehensive)
k8s-rebuild-retry-simple: k8s-rebuild-operators k8s-retry-simple ## Rebuild operators + retry simple-pipeline (comprehensive)

# Rebuild everything (operators + services) and retry
k8s-rebuild-all: k8s-rebuild-operators k8s-rebuild-services build-components ## Rebuild all K8s images (operators + services + components)

# Clean build artifacts
clean: ## Remove build artifacts (bin/ + go clean)
	rm -rf bin/
	go clean

clean-go-git-cache: ## Free disk space (Go build/test/fuzz cache + git gc; preserves module cache)
	go clean -cache -testcache -fuzzcache
	git gc --prune=now

# Run go mod tidy across every Go module in the monorepo. Tidying root
# in isolation drifts the others — their Docker builds + e2e go test
# enforce `go mod tidy` parity and fail CI. Always tidy the whole tree.
#
# Discovers modules dynamically rather than hardcoding paths so a new
# components/*/ or sdk/*/ module doesn't silently get missed.
#
# tests/integration/go.mod is excluded: its go.mod is missing a
# `replace github.com/datuplet/datuplet/sdk/go => ../../sdk/go` directive
# and won't tidy until that's fixed. It is NOT run in CI today (see
# .github/workflows/e2e.yml — only tests/e2e is exercised). Tracked as
# a pre-existing repo cleanup, not blocking this PR.
tidy: ## Run go mod tidy across every go.mod in the monorepo
	@echo "Tidying root..."
	@go mod tidy
	@# Exclude ./.claude/* so stale Claude Code worktrees (with broken
	@# replace directives pointing back at the parent repo) don't poison
	@# the tidy loop. Excluding .git / node_modules / vendor is hygiene.
	@for mod in $$(find . -name go.mod -not -path ./go.mod \
	    -not -path './.git/*' -not -path './node_modules/*' \
	    -not -path './vendor/*' -not -path './.claude/*' \
	    -not -path './tests/integration/go.mod'); do \
	  dir=$$(dirname $$mod); \
	  echo "Tidying $$dir..."; \
	  (cd $$dir && go mod tidy) || exit 1; \
	done
	@echo "✓ all modules tidy (tests/integration intentionally skipped)"

# Static analysis and dead code detection. RFC 019 §4.10 bearer-redaction
# is enforced via Stringer methods on the owned types (S3Creds, GCSCreds,
# vendedTokenSource, refreshingTokenSource) — see pkg/catalogwriter/creds.go.
lint: chart-render-check ## Run go vet + deadcode + chart golden-render diff
	@echo "Running go vet..."
	go vet ./...
	@echo "Running deadcode analysis..."
	go run golang.org/x/tools/cmd/deadcode@latest -test ./...

chart-render-check: ## Diff helm template output against golden renders in charts/datuplet-lakekeeper/_render/
	@# Pin encryptionKeySecret to suppress the randAlphaNum-generated Secret
	@# that would produce a different key on every run and cause false diffs.
	@helm template charts/datuplet-lakekeeper \
	  --set workloadIdentity.enabled=true \
	  --set workloadIdentity.gcpServiceAccount=test@example.iam.gserviceaccount.com \
	  --set platform.enableGcpSystemCredentials=true \
	  --set lakekeeper.secretBackend.postgres.encryptionKeySecret=golden-render \
	  > /tmp/datuplet-lakekeeper-wif-on.yaml && \
	diff -u charts/datuplet-lakekeeper/_render/wif-on.golden.yaml /tmp/datuplet-lakekeeper-wif-on.yaml \
	  || { echo "chart drift on wif-on"; exit 1; }
	@helm template charts/datuplet-lakekeeper \
	  --set lakekeeper.secretBackend.postgres.encryptionKeySecret=golden-render \
	  > /tmp/datuplet-lakekeeper-wif-off.yaml && \
	diff -u charts/datuplet-lakekeeper/_render/wif-off.golden.yaml /tmp/datuplet-lakekeeper-wif-off.yaml \
	  || { echo "chart drift on wif-off"; exit 1; }
	@echo "✓ all chart renders match golden"

# All-in-one: tidy, build, test
all: tidy build test ## Tidy + build + test (all-in-one)

prune-images: ## Prune unused Docker images and system volumes
	@echo "Pruning unused Docker images..."
	docker image prune -f
	docker system prune -a --volumes
	@echo "Docker images pruned."

# BuildKit layer + cache-mount caches accumulate per builder and are NOT
# touched by `docker system prune` (the named datuplet-builder once held
# 37GB and filled the disk, causing OrbStack VM resets + kubelet image GC).
# Keeps tagged images — safe to run while a cluster uses locally-built images.
prune-docker-cache: ## Purge ALL Docker build caches (every buildx builder + dangling images; keeps tagged images)
	@echo "Docker disk usage before:"
	@docker system df
	@# `buildx ls --format` emits builder AND node rows (e.g. datuplet-builder0);
	@# dedupe, and let prune no-op on node names via `|| true`.
	@for b in $$(docker buildx ls --format '{{.Name}}' | sort -u); do \
	  echo "--- pruning buildx builder: $$b"; \
	  docker buildx prune --builder $$b --all --force || true; \
	done
	@echo "--- pruning dangling images"
	docker image prune --force
	@echo "Docker disk usage after:"
	@docker system df
