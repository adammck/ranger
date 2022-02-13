# Example: Key-Value Store

This is a very simple volatile in-memory key-value store to demonstrate the
Ranger API. It exposes an HTTP interface to GET and PUT things, and a gRPC
interface to move those things around.

## Usage

```console
$ cd ~/code/src/github.com/adammck/ranger/examples/kv
$ go build

$ # Start consul in the background.
$ brew services run consul

# Run a simple cluster (see Procfile)
$ foreman start -m controller=1,proxy=1,node=3
```

```console
$ # Read and write some data.
$ bin/put.sh 8000 a aaaa
$ bin/get.sh 8000 a

$ # Run a load test.
$ cd tools/hammer
$ go build
$ ./hammer -addr localhost:8000 -workers 10 -interval 100

$ # Move range 1 (the only range, for now) to n2.
$ bin/client.sh 9000 ranger.Balancer.Move '{"range": {"key": 1}, "node": "8002"}'
```

## Tests

```console
$ bats test/test.bats
```
