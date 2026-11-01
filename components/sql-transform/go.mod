module github.com/datuplet/datuplet/components/sql-transform

go 1.27.3

require (
	github.com/datuplet/datuplet v0.0.0
	github.com/datuplet/datuplet/sdk/go v0.0.0
	github.com/duckdb/duckdb-go/v2 v2.20000.0
	github.com/grafana/pyroscope-go v1.3.0
)

require (
	github.com/apache/arrow-go/v18 v18.8.0 // indirect
	github.com/duckdb/duckdb-go-bindings v0.20000.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grafana/pyroscope-go/godeltaprof v0.1.10 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.29 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.duckdb.org/libduckdb/darwin-amd64 v0.20000.0 // indirect
	go.duckdb.org/libduckdb/darwin-arm64 v0.20000.0 // indirect
	go.duckdb.org/libduckdb/linux-amd64 v0.20000.0 // indirect
	go.duckdb.org/libduckdb/linux-arm64 v0.20000.0 // indirect
	go.duckdb.org/libduckdb/windows-amd64 v0.20000.0 // indirect
	golang.org/x/exp v0.0.0-20260603202125-055de637280b // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260819154853-08b0e4226688 // indirect
	google.golang.org/grpc v1.83.2 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/datuplet/datuplet/sdk/go => ../../sdk/go

replace github.com/datuplet/datuplet/sdk/go/arrow => ../../sdk/go/arrow

replace github.com/datuplet/datuplet => ../..
