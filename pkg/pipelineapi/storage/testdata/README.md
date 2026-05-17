# storage/testdata — Iceberg warehouse fixtures

This directory contains committed Iceberg fixtures used by the storage
package tests (walker / arrow-json serializer / HTTP handlers, landing
in A1-6..A1-8). The layout mirrors a real Datuplet warehouse:

    warehouse/
      orgs/myorg/
        projects/00000000-0000-0000-0000-000000000002/
          tables/
            public/
              simple/                11 metadata versions + version-hint=11
                metadata/
                  v1..v11.metadata.json
                  version-hint.text
                  snap-*.avro
                  *-m0.avro
                data/
                  part-00001.parquet (3 rows: id int64, name string)
              orphan/                proves walker skips unreadable files
                metadata/v99.metadata.json  (deliberately malformed JSON)
              empty/                 proves walker omits uninitialized tables
                metadata/.gitkeep

## Path portability

Iceberg metadata bakes absolute `file://` paths into `location`,
`metadata-log`, snapshot `manifest-list`, and data-file entries. The
committed files therefore only resolve on a checkout at the exact same
absolute path that was used to generate them (currently
`/Users/tomaskacur/mydevel/datuplet`). On any other checkout location,
the metadata still parses but scans over the data files will fail.

Tests that need the fixtures must regenerate into a temp dir at run
time. The approach documented for A1-6..A1-8 is to call the generator
from a `TestMain` (or a per-test helper) that writes into `t.TempDir()`
and passes that absolute path through iceberg-go. The committed files
exist only so contributors can inspect the expected shape and for a
quick smoke check (see `smoke.go`).

## Regenerating

```sh
# From the repo root:
go run ./pkg/pipelineapi/storage/testdata/gen.go

# Smoke-check that v11 parses + the snapshot is non-nil:
go run ./pkg/pipelineapi/storage/testdata/smoke.go
```

Both files carry `//go:build ignore` so they never compile into the
regular build. The generator wipes and recreates `warehouse/` on every
run; it is safe to re-run.
