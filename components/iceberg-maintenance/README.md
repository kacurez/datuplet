# iceberg-maintenance

Runs Iceberg table maintenance on each declared output table through the Data
Gateway. Config is nested per task and tasks run per table in a fixed order:

1. `compact` (on by default): bin-packs small data files in one atomic snapshot
   (`CompactTable` RPC).
2. `rewrite_manifests` (off): merges small manifests (`RewriteManifests`).
3. `expire_snapshots` (off): expires old snapshots and deletes the files only
   they reference (`ExpireSnapshots`; floors: 24 h, retain at least 1).
4. `delete_orphan_files` (off): deletes unreferenced files under the table
   location (`DeleteOrphanFiles`; floor: 25 h).

`dry_run` makes every enabled task report without changing anything. A failed
task skips the remaining tasks for that table only. `io: {inputs: none,
outputs: required}`; outputs must be declared as explicit `outputs.tables`
(`defaultBucket`-only is rejected). The three destructive RPCs are served only
to this component.

Exit codes: `0` success (including `NOTHING_TO_DO`, `DRY_RUN`, `SUPERSEDED`),
`1` bad config, no task enabled, unknown or undeclared table, refused or
invalid request, `>=20` infrastructure errors. Config table, statuses and
examples: `docs/components.md`; safety assumptions: `docs/known-limitations.md`.
