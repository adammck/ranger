module github.com/adammck/ranger/examples/kv

go 1.16

require (
	github.com/adammck/ranger v0.0.1
	github.com/golang/protobuf v1.5.2
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
	google.golang.org/grpc v1.41.0
	google.golang.org/protobuf v1.27.1
)

// Temporary; just while iterating
replace github.com/adammck/ranger => ../..
