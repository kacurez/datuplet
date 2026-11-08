# iceberg-compactor

Bin-packs the small data files of each declared output table into target-size
files in one atomic Iceberg snapshot, by calling the Data Gateway's
`CompactTable` RPC. `io: {inputs: none, outputs: required}`; outputs must be
declared as explicit `outputs.tables` (`defaultBucket`-only is rejected).

Exit codes: `0` success (including nothing to do / superseded), `1` bad config,
unknown or undeclared table, `>=20` infrastructure errors. See
`docs/components.md` and the table-compaction spec.
