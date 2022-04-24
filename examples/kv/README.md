# Example: Key-Value Store

This is a simple distributed in-memory key-value store using Ranger. You should
not use it for anything under any circumstances; there's no persistence, and it
totally ignores various important edge-cases. It's intended to demonstrate the
following specific use case:

- One or zero active replicas.
- Direct (node-to-node) state transfer.
- External proxy layer.

It has two components in addition to the controller:

- **node**:  
  This stateful service stores the actual data. It exposes a simple get/put
  interface over gRPC. It includes a _Rangelet_, which will coordinate workload
  assignment with the controller.
- **proxy**:  
  This stateless service connects to every node and watches the range
  assignments. It exposes the same get/put interface, but can transparently
  forward requests to the appropriate storage node.

----

## Deps

Install dependencies with Brew.  
Ths only works on macOS. (Sorry.)

```console
$ brew bundle --file Brewfile
Homebrew Bundle complete! 2 Brewfile dependencies now installed.
```


## Usage

Generate protos stubs:  
(I'll check these in once they're stable!)

```console
$ ../../bin/gen-proto.sh
$ bin/gen-proto.sh
```

Start consul in the background (for service discovery), and run a simple three
node cluster:  
(This uses Foreman to keep things simple, but you can also start up the services
in separate tabs or whatever, if you prefer.)

```console
$ brew services run consul
==> Successfully ran `consul` (label: homebrew.mxcl.consul)
$ cd ~/code/src/github.com/adammck/ranger/examples/kv
$ bin/dev.sh
23:15:00 controller.1 | started with pid 18951
23:15:00 proxy.1      | started with pid 18952
23:15:01 node.1       | started with pid 18953
23:15:01 node.2       | started with pid 18954
23:15:01 node.3       | started with pid 18955
```

Run a load test:  
(This hammer tool is specific to the kv example, and is kind of janky. It's
intended to demonstrate availability during range moves/splits/joins.)

```console
$ cd tools/hammer
$ go build
$ ./hammer -addr localhost:5100 -duration 60s
```

Move range 1 (the only range, for now) to node 2:

```console
$ rangerctl move 1 5201
```

Split range 1 onto nodes 1 and 3:

```console
$ rangerctl split 1 m 5200 5202
```

Join ranges 1 and 3 back onto node 2:

```console
$ rangerctl join 2 3 5021
```

## Tests

These aren't exactly working right now.

```console
$ bats test
```

## License

MIT.
