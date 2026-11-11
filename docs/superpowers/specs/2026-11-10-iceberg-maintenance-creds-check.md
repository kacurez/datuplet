# Iceberg maintenance: vended-credential list/delete check (Task 0 gate)

Date: 2026-11-10. Lakekeeper v0.14.2.

## Live result (test `TestVendedCreds_ListAndDelete`)

Fixture: Lakekeeper v0.14.2 + silo (MinIO-compatible), warehouse `sts-enabled: true`, flavor `s3-compat`.
The vended credentials carry a **session token** (STS, expiring), so the run used STS credentials, not static keys.
Via `table.FS(ctx)`: `fs` implements `iceio.ListableIO`; `WalkDir(table location)` returns an uncommitted
orphan data file; `Remove(file)` succeeds; a second `WalkDir` no longer lists it. PASS.

Caveat: silo is not AWS. It does not necessarily enforce the downscoped session policy the way AWS STS does,
so this proves the plumbing (iceberg-go list/delete with vended creds), not the AWS policy contents.

## Policies (documentation, not live-verified)

- **S3 STS session policy.** The Lakekeeper docs do not list the session-policy actions; they say only that
  Lakekeeper assumes `sts-role-arn` "with a policy attached for the accessed table"
  (https://docs.lakekeeper.io/docs/latest/storage-s3/). Lakekeeper issue #1630 quotes the generated policy as
  `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on the bucket objects plus `s3:ListBucket` on the bucket
  (https://github.com/lakekeeper/lakekeeper/issues/1630, also seaweedfs discussion #8312). So delete and list are
  granted. Not confirmed: whether `ListBucket` carries an `s3:prefix` condition (it must include the table
  prefix for `WalkDir(table location)`; the live test covers this on silo only). The effective permission is
  also the intersection with the role's own policy, which the operator must grant `DeleteObject`/`ListBucket`.
- **GCS.** Docs: tokens are "downscoped to at most" `roles/storage.objectUser`, which covers list and delete
  (https://docs.lakekeeper.io/docs/latest/storage-gcs/, https://github.com/lakekeeper/lakekeeper/pull/2114).
  The exact Credential Access Boundary roles are not documented. **Documented, not live-verified** (no GKE).

## Verdict

- S3: GO (live on STS-enabled s3-compat; AWS policy per issue #1630). Confirm against real AWS in the Task 6 dry-run.
- GCS: GO conditional on documentation only; verify live on GKE before relying on it.
