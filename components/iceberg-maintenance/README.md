# iceberg-maintenance

Runs Iceberg table maintenance on each declared output table through the Data
Gateway. Config is nested per task (`compact`, `rewrite_manifests`,
`expire_snapshots`, `delete_orphan_files`); only `compact` is enabled by
default, and it bin-packs small data files into target-size files in one atomic
snapshot via the `CompactTable` RPC. `io: {inputs: none, outputs: required}`;
outputs must be declared as explicit `outputs.tables` (`defaultBucket`-only is
rejected).

Exit codes: `0` success (including nothing to do / superseded), `1` bad config,
no task enabled, unknown or undeclared table, `>=20` infrastructure errors. See
`docs/components.md` and the table-compaction spec.
