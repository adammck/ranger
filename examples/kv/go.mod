module github.com/adammck/ranger/examples/kv

go 1.16

// Temporary; just while iterating
replace github.com/adammck/ranger => ../..

require (
	github.com/adammck/ranger v0.0.0-00010101000000-000000000000
	github.com/golang/protobuf v1.5.2
	github.com/hashicorp/consul/api v1.11.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	google.golang.org/grpc v1.41.0
	google.golang.org/protobuf v1.27.1
)
