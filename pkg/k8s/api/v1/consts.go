package v1

// ProjectSecretsName is the name of the single managed Secret that holds
// project-scoped user secrets (RFC 026 P1.5) — one per project namespace,
// keyed by the caller-supplied secret name under Data.
//
// It lives in this leaf CRD-types package (imported by both the operator
// controllers and pipeline-api) so neither side has to import the other just
// to name the Secret. pipeline-api's pkg/pipelineapi/k8s.ProjectSecretsName is
// an alias of this constant.
const ProjectSecretsName = "datuplet-project-secrets"
