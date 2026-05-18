// Package datupleticeio centralizes Datuplet's iceberg-go IO scheme
// registrations. Every binary that calls into iceberg-go (cmd/datuplet,
// cmd/pipeline-api, cmd/pipeline-operator) and every package that uses
// iceberg-go's IO factory directly (pkg/icebergjob, pkg/datagateway/lakekeeper,
// pkg/pipelineapi/storage) blank-imports this package so the registration
// order is deterministic and isolated package tests use the override.
//
// Today this package overrides only the `gs://` scheme. The default
// iceberg-go/io/gocloud factory reads gcs.keypath / gcs.jsonkey / ADC
// from props; the Datuplet factory reads gcs.oauth2.token (the
// lakekeeper-vended bearer) and wraps it in a refreshing TokenSource so
// long TableCommit transactions survive token expiry mid-write.
//
// See RFC 019 §4.5.
package datupleticeio
