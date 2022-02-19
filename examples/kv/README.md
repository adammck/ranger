# Example: Key-Value Store

This is a very simple volatile in-memory key-value store to demonstrate the
Ranger API. It exposes an HTTP interface to GET and PUT things, and a gRPC
interface to move those things around.

## Dependencies

```console
$ brew bundle --file Brewfile
Homebrew Bundle complete! 2 Brewfile dependencies now installed.
```

## Usage

```console
$ cd ~/code/src/github.com/adammck/ranger/examples/kv
$ go build

$ # Start consul in the background.
$ brew services run consul

# Run a simple cluster (see Procfile)
$ bin/dev.sh
```

```console
$ # Read and write some data.to n1.
$ bin/put.sh 5200 a aaaa
$ bin/get.sh 5200 a

$ # Run a load test.
$ cd tools/hammer
$ go build
$ ./hammer -addr localhost:5100 -duration 30s

$ # Move R1 (the only range, for now) to N2.
$ bin/client.sh 5000 ranger.Balancer.Move '{"range": {"key": 1}, "node": "5201"}'

$ # Split R1 onto N1 and N3.
$ bin/client.sh 5000 ranger.Balancer.Split '{"range": {"key": 1}, "boundary": "'$(echo -n "M0000000" | base64)'", "node_left": "5200", "node_right": "5202"}'

$ # Join R1 and R3 back onto N2.
$ bin/client.sh 5000 ranger.Balancer.Join '{"range_left": {"key": 2}, "range_right": {"key": 3}, "node": "5201"}'
```

## Tests

```console
$ bats test
```
