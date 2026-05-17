module github.com/datuplet/datuplet/components/stdout-writer

go 1.25.7

require (
	github.com/datuplet/datuplet v0.0.0
	github.com/datuplet/datuplet/sdk/go v0.0.0
)

require (
	golang.org/x/net v0.50.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/grpc v1.78.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/datuplet/datuplet/sdk/go => ../../sdk/go

replace github.com/datuplet/datuplet => ../..
