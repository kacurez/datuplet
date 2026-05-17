module example-component

go 1.25.7

require github.com/datuplet/datuplet/sdk/go v0.0.0

require (
	github.com/datuplet/datuplet v0.0.0 // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/grpc v1.78.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/datuplet/datuplet/sdk/go => ../../sdk/go

replace github.com/datuplet/datuplet => ../..
