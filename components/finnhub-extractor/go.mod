module github.com/datuplet/datuplet/components/finnhub-extractor

go 1.25.7

require (
	github.com/Finnhub-Stock-API/finnhub-go/v2 v2.0.20
	github.com/datuplet/datuplet v0.0.0
	github.com/datuplet/datuplet/sdk/go v0.0.0
)

require (
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260427160629-7cedc36a6bc4 // indirect
	google.golang.org/grpc v1.80.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/datuplet/datuplet/sdk/go => ../../sdk/go

replace github.com/datuplet/datuplet => ../..
