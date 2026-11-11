module github.com/datuplet/datuplet/components/iceberg-maintenance

go 1.27.3

require (
	github.com/datuplet/datuplet/sdk/go v0.0.0
	google.golang.org/grpc v1.83.2
)

require (
	github.com/datuplet/datuplet v0.0.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260819154853-08b0e4226688 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/datuplet/datuplet/sdk/go => ../../sdk/go

replace github.com/datuplet/datuplet => ../..
