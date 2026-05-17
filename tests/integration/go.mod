module github.com/datuplet/datuplet/tests/integration

go 1.23.1

require (
	github.com/datuplet/datuplet v0.0.0
	github.com/stretchr/testify v1.10.0
)

require (
	github.com/apache/arrow-go/v18 v18.0.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	google.golang.org/grpc v1.69.4 // indirect
	google.golang.org/protobuf v1.36.3 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/datuplet/datuplet => ../..
