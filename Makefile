# Enable Docker BuildKit for cache mounts and improved performance
export DOCKER_BUILDKIT=1

.PHONY: \
	build build-pipeline-api build-gateway build-iceberg-job build-services \
	build-components build-components-e2e build-component-data-generator build-operators \
	build-component-sql-transform \
	docker-build-operators docker-build-pipeline-api docker-build-pipeline-observer docker-build-k8s \
	clean clean-go-git-cache \
	test e2e e2e-k8s e2e-all \
	deploy-local deploy-local-helm undeploy-local k8s-smoke \
	k8s-reload-crds k8s-rebuild-operators k8s-rebuild-services \
	k8s-retry-simple k8s-retry-duckdb k8s-retry-full \
	k8s-rebuild-retry-simple k8s-rebuild-all \
	port-forward-minio-k8s kill-port-forward-minio-k8s \
	prune-images \
	lint lint-notokenlog tidy all help

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
#   docker build -t datuplet/pandas-transform:latest -f components/pandas-transform/Dockerfile .

# E2E-only component subset: skips finnhub-extractor (no scenario references it).
# Keeps gateway as a build dep (sidecar is required for every run). RFC 010
# scenarios (duckdb-etl, multi-table-join) need datuplet/sql-transform too.
build-components-e2e: build-gateway build-component-sql-transform ## Build only the components actively used by e2e (data-generator + http-json-extractor + stdout-writer + sql-transform)
	docker build -t datuplet/data-generator:latest -f components/data-generator/Dockerfile .
	docker build -t datuplet/http-json-extractor:latest -f components/http-json-extractor/Dockerfile .
	docker build -t datuplet/stdout-writer:latest -f components/stdout-writer/Dockerfile .

build-component-sql-transform: ## Build sql-transform component image (RFC 010)
	docker build -t datuplet/sql-transform:latest -f components/sql-transform/Dockerfile .

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
	cd tests/e2e && E2E_K8S=1 go test -v -count=1 -timeout 30m ./...
	helm uninstall datuplet-lakekeeper -n datuplet-e2e || true
	helm uninstall datuplet-app -n datuplet-e2e || true
	helm uninstall datuplet-infra -n datuplet-e2e || true
	helm uninstall datuplet-operators -n datuplet-e2e || true
	kubectl delete namespace datuplet-e2e --wait=false || true

e2e-all: e2e-k8s ## Run all e2e tiers (K8s is the only supported surface; alias for e2e-k8s)

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
k8s-retry-simple: k8s-reload-crds ## Delete all PipelineRuns + re-apply simple-pipeline.yaml
	kubectl delete pipelinerun --all -n datuplet-e2e --ignore-not-found
	kubectl delete pipeline simple-pipeline -n datuplet-e2e --ignore-not-found
	kubectl apply -f examples/k8s/simple-pipeline.yaml

k8s-retry-duckdb: k8s-reload-crds ## Delete all PipelineRuns + re-apply duckdb-pipeline.yaml
	kubectl delete pipelinerun --all -n datuplet-e2e --ignore-not-found
	kubectl delete pipeline duckdb-transform -n datuplet-e2e --ignore-not-found
	kubectl apply -f examples/k8s/duckdb-pipeline.yaml

# Clean up pipelineruns and retry the example (with CRD reload)
k8s-retry-full: k8s-reload-crds ## Delete all PipelineRuns + re-apply full-pipeline.yaml
	kubectl delete pipelinerun --all -n datuplet-e2e --ignore-not-found
	kubectl delete pipeline full-pipeline -n datuplet-e2e --ignore-not-found
	kubectl apply -f examples/k8s/full-pipeline.yaml

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

# Run go mod tidy
tidy: ## Run go mod tidy
	go mod tidy

# Static analysis and dead code detection
lint: lint-notokenlog ## Run go vet + deadcode + notokenlog (RFC 019 §4.10) analyzers
	@echo "Running go vet..."
	go vet ./...
	@echo "Running deadcode analysis..."
	go run golang.org/x/tools/cmd/deadcode@latest -test ./...

# RFC 019 §4.10: reject fmt-verb / log-arg uses of bearer-credential types
# (GCSCreds, *oauth2.Token, *vendedTokenSource). Source lives at
# tools/lint/notokenlog/.
lint-notokenlog: ## Run the notokenlog analyzer against ./... (RFC 019 §4.10)
	@echo "Running notokenlog analyzer..."
	go run ./tools/lint/notokenlog/cmd/notokenlog ./...

# All-in-one: tidy, build, test
all: tidy build test ## Tidy + build + test (all-in-one)

prune-images: ## Prune unused Docker images and system volumes
	@echo "Pruning unused Docker images..."
	docker image prune -f
	docker system prune -a --volumes
	@echo "Docker images pruned."
