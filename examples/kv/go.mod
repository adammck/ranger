module github.com/adammck/ranger/examples/kv

go 1.16

require github.com/gorilla/mux v1.8.0

require (
	github.com/adammck/ranger v0.0.1
	google.golang.org/grpc v1.41.0
)

// Temporary; just while iterating
replace github.com/adammck/ranger => ../..
