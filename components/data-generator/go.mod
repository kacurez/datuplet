module github.com/datuplet/datuplet/components/data-generator

go 1.27.3

require (
	github.com/apache/arrow-go/v18 v18.8.0
	github.com/datuplet/datuplet v0.0.0
	github.com/datuplet/datuplet/sdk/go v0.0.0
	github.com/datuplet/datuplet/sdk/go/arrow v0.0.0
	github.com/google/uuid v1.6.0
)

require (
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.29 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
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
