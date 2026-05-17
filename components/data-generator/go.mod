module github.com/datuplet/datuplet/components/data-generator

go 1.25.7

require (
	github.com/datuplet/datuplet/sdk/go v0.0.0
	github.com/google/uuid v1.6.0
)

require (
	github.com/datuplet/datuplet v0.0.0 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260319201613-d00831a3d3e7 // indirect
	google.golang.org/grpc v1.79.3 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/datuplet/datuplet/sdk/go => ../../sdk/go

replace github.com/datuplet/datuplet => ../..
