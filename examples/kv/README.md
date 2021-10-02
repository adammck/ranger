# Example: Key-Value Store

This is a very simple volatile in-memory key-value store to demonstrate the
Ranger API. It exposes an HTTP interface to GET and PUT things, and a gRPC
interface to move those things around.

## Usage

Server:

```console
$ cd ~/code/src/github.com/adammck/ranger/examples/kv
$ go build

$ # Run three nodes.
$ ./kv -addr ":8001"
$ ./kv -addr ":8002"
$ ./kv -addr ":8003"
```

## Tests

```console
$ bats test/test.bats
```
