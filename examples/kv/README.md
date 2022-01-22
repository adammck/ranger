# Example: Key-Value Store

This is a very simple volatile in-memory key-value store to demonstrate the
Ranger API. It exposes an HTTP interface to GET and PUT things, and a gRPC
interface to move those things around.

## Usage

```console
$ cd ~/code/src/github.com/adammck/ranger/examples/kv
$ go build

$ # Run three nodes, to store data.
$ ./kv -node -addr ":8001"
$ ./kv -node -addr ":8002"
$ ./kv -node -addr ":8003"

$ # Run a proxy, to forward requests to the appropriate node(s).
$ ./kv -proxy -addr ":8000"

$ # Read and write some data.
$ bin/put.sh 8000 a aaaa
$ bin/get.sh 8000 a
```

## Tests

```console
$ bats test/test.bats
```
