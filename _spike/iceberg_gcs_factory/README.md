# RFC 019 Slice A0 — iceberg-go GCS factory spike

Throwaway harness for the five acceptance criteria in RFC 019 §4.5.4.

## Run

You need: a real GCP project, a real GCS bucket, a SA JSON key that can
list/get/put on the bucket, and a running Lakekeeper instance with a
GCS warehouse bootstrapped via `--gcs-credential-type=service-account-key`.

```
go run ./_spike/iceberg_gcs_factory \
  -criterion=all \
  -gcs-bucket=$GCS_BUCKET \
  -gcs-key-file=/tmp/datuplet-sa.json \
  -lakekeeper-url=$LK_URL \
  -warehouse=datuplet
```

Each criterion prints PASS or FAIL with a one-line reason. Findings
go to `docs/tmp/spikes/<date>-iceberg-gcs-factory.md`.

## Cleanup

Slice I deletes this directory entirely.
