# Building the component program

## Anatomy of `components/<name>/`

A Go component (the common case) is:

```
components/<name>/
├── main.go        # the program: config → do the job via the SDK → exit code
├── schema.json    # the config contract (see register-a-component.md)
├── Dockerfile     # multi-stage, non-root
├── go.mod         # module with a `replace` to the in-repo SDK
└── go.sum
```

Python components follow the same shape (`main.py`, `pyproject.toml`,
`schema.json`, `Dockerfile`) against `sdk/python`. Copy the closest existing
component and edit — don't start from a blank file.

## The SDK contract (Go — `github.com/datuplet/datuplet/sdk/go`)

The SDK hides the gateway. You get: connect, read your config, read input
tables, write output tables, log, and signal status.

- `client, err := sdk.New(ctx)` — connect to the gateway sidecar; `defer client.Close()`.
- `cfg := client.Config()` — run context (`cfg.ExecutionID`, declared `Inputs`,
  `InputTables`, etc.).
- `client.ParseConfig(&myStruct)` — decode the pipeline's `config` block into a
  typed struct (this is what your `schema.json` validates). A decode failure is a
  user error.
- **Write an output table:**
  `w, err := client.OpenWriter(ctx, "<table>")` (or
  `OpenWriterToBucket(ctx, bucket, table, opts...)`); `w.Write(ctx, chunkBytes)`
  in a loop; `w.Close(ctx)`. Options: `sdk.WithFormat(...)`, `sdk.WithSchema(...)`,
  `sdk.WithBatchSize(n)`.
- **Read an input table:**
  `r, err := client.OpenReader(ctx, bucket, table, opts...)`; loop
  `chunk, err := r.NextChunk()` until EOF; `r.Schema()` / `r.ColumnNames()`
  describe it. `sdk.WithIncrementalSince(snapshotID)` supports delta reads.
- `client.Log(ctx, "INFO", "...")` — structured logs.
- **Status / exit (critical):**
  - success → return normally / `os.Exit(0)`.
  - `sdk.ExitUserError("config.url is required")` → exit **1** (FailedUser).
  - `sdk.ExitAppError("failed to connect to gateway: ...")` → exit **≥20**
    (FailedApplication).
  These print the `DUPLET_STATUS_MESSAGE:` prefix and exit with the contract code
  so the run surfaces the right phase + message. (Python: `sdk/python/status.py`
  → `fail_user()` / `fail_application()`.)

## Minimal skeletons

**Source** (no inputs → one output table), modeled on `http-json-extractor`:

```go
func main() {
	ctx := context.Background()
	client, err := sdk.New(ctx)
	if err != nil {
		sdk.ExitAppError(fmt.Sprintf("connect gateway: %v", err))
	}
	defer client.Close()

	var cfg struct {
		URL   string `json:"url"`
		Token string `json:"apiKey"` // an x-datuplet-secret field, arrives resolved from $[…]
	}
	if err := client.ParseConfig(&cfg); err != nil {
		sdk.ExitUserError(fmt.Sprintf("bad config: %v", err))
	}
	if cfg.URL == "" {
		sdk.ExitUserError("config.url is required")
	}

	data, err := fetch(ctx, cfg) // your source logic → newline-JSON / parquet / arrow bytes
	if err != nil {
		sdk.ExitUserError(fmt.Sprintf("fetch failed: %v", err)) // upstream/user problem
	}

	w, err := client.OpenWriter(ctx, "data")
	if err != nil {
		sdk.ExitAppError(fmt.Sprintf("open writer: %v", err))
	}
	if err := w.Write(ctx, data); err != nil {
		sdk.ExitAppError(fmt.Sprintf("write: %v", err))
	}
	if _, err := w.Close(ctx); err != nil { // Close returns (*CloseResult, error)
		sdk.ExitAppError(fmt.Sprintf("close writer: %v", err))
	}
}
```

**Transform / sink** reads instead (or as well): open a reader per
`client.Config().InputTables`, stream `NextChunk()`, process, and (transform)
write to an output table or (sink) push to the external system. `sql-transform`
and `stdout-writer` are the reference implementations.

## Choosing the data format

The gateway handles storage format; components exchange chunk bytes in a
declared `DataFormat` (JSON/Arrow/Parquet via `sdk.WithFormat`). Match what your
source/target naturally produces and let the platform convert. For row-oriented
API data, newline-delimited JSON is the simplest starting point (see
`http-json-extractor`).

## Dockerfile pattern

Multi-stage, builds from the repo root (needed for the SDK `replace`), final
image runs **non-root**. Copy `components/http-json-extractor/Dockerfile`
verbatim and change the two component-name occurrences:

```dockerfile
FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY . .
WORKDIR /app/components/<name>
RUN go mod tidy
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -o /<name> .

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /<name> /app/<name>
RUN adduser -D -u 1000 datuplet
USER datuplet
ENTRYPOINT ["/app/<name>"]
```

Keep the component tiny — all the heavy lifting (storage, format, credentials,
retries on the storage side) belongs to the gateway, not here. If your `main.go`
is growing past a few hundred lines, most of it probably belongs in the source
system's own client library, not the component.
