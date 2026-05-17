// Package catalogwriter is a shared library that wraps Apache iceberg-go's
// REST catalog client (lakekeeper-compatible) and adds the supporting
// machinery DataGateway and TableCommit need to write Iceberg tables
// against a vended-credentials catalog.
//
// Components:
//
//   - Client (client.go): thin wrapper over iceberg-go's
//     `catalog/rest`.Catalog. Anonymous-imports
//     `github.com/apache/iceberg-go/io/gocloud` so that the
//     `s3://` / `s3a://` / `s3n://` schemes are registered for any
//     binary that links this package — required by lakekeeper-vended paths.
//
//   - VendedCreds (vended_creds.go): credential cache + auto-renewer
//     that fetches scoped STS credentials from lakekeeper via the
//     `GET /v1/{prefix}/namespaces/{ns}/tables/{tbl}` response's
//     `config` block. Renewal contract: renew when 50% of the issued TTL
//     has elapsed, with a 60-second hard floor between renewals.
//
//   - AWSCredentialsProvider (aws_provider.go): adapter that converts
//     a *VendedCreds into `aws.CredentialsProviderFunc`, so any
//     AWS-SDK-v2 S3 client gets transparent rotation per call.
//
//   - RetryOnConflict (retry.go): bounded retry-on-409 wrapper around
//     a commit closure (max 5 attempts, 100ms exponential backoff with
//     ±10% jitter).
//
// The package is intentionally narrow: it does NOT spin up DG's
// parquet writer, does NOT own the per-run-token map, and does NOT
// know about the K8s Secret shape. It exposes plumbing that DG and
// TableCommit compose on top of.
package catalogwriter
