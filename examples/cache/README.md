# Example: Cache

This is a simple distributed cache using ranger. The work it performs is totally
pointless and deliberately expensive -- it recursively hashes incoming payloads
ten million times -- but is cached by key, so subsequent requests for the same
payload are fast. We can pretend that the workload is doing something expensive
but useful, like sending some RPCs, searching through a lot of data, or caching
some data from underlying storage.

This demonstrates the following features:

- **Separate data/control planes**:  
  Ranger traffic (from controller to node, and between nodes) is exchanged over
  gRPC, but the service exposes its own endpoint(s) over a separte HTTP server.
- **Dumb client**:  
  There's no custom client to send a request; it's just cURL.
- **Request Forwarding**:  
  Incoming requests are either handled, or forwarded (via HTTP 302) to the
  appropriate node. This is accomplished by every node maintaining a mirror of
  all range assignments.
- **Stateless**:  
  Cached data is not moved between nodes when range reassignments happen. It's
  just thrown away. The rangelet integration is therefore very simple.

## Usage

Start the thing using Foreman:  
(The port assignments are a huge hack.)

```console
$ brew services run consul
==> Successfully ran `consul` (label: homebrew.mxcl.consul)
$ cd ~/code/src/github.com/adammck/ranger/examples/cache
$ bin/dev.sh
18:56:00 controller.1 | started with pid 93093
18:56:00 node.1       | started with pid 93094
18:56:00 node.2       | started with pid 93095
18:56:00 node.3       | started with pid 93096
18:56:00 controller.1 | listening on: 127.0.0.1:5000
18:56:00 node.1       | grpc listening on: 127.0.0.1:15100
18:56:00 node.1       | http listening on: 127.0.0.1:25100
18:56:00 node.2       | grpc listening on: 127.0.0.1:15101
18:56:00 node.2       | http listening on: 127.0.0.1:25101
18:56:00 node.3       | grpc listening on: 127.0.0.1:15102
18:56:00 node.3       | http listening on: 127.0.0.1:25102
```

In a separate terminal, send some requests to node 1, which is currently
assigned the entire keyspace in a single range:

```console
$ curl -L http://localhost:25100/a
4851381cac0c5b0c2e4a6c7e5629c6ac6db47f2a15c31d40f242a6be39ffb97d

$ curl -L http://localhost:25100/b
3adbb65d20ee48ab81fc63063dc2ec38c31c7089782fc6f434627c3829eaf87c
```

Now send some requests to node 2, which is assigned nothing. It works, because
the request is forwarded to node 1, as can be seen by showing HTTP headers:

```console
$ curl -iL http://localhost:25101/c
HTTP/1.1 302 Found
Content-Type: text/plain
Location: http://127.0.0.1:25100/c
Date: Sun, 27 Nov 2022 00:57:00 GMT
Content-Length: 0

HTTP/1.1 200 OK
Content-Type: text/plain
Server: 127.0.0.1:25100
Date: Sun, 27 Nov 2022 00:57:00 GMT
Content-Length: 65

ca4f2be4e4c9604df3b971deae26f077841f0ec34ff9a77a534988c6352566f6
```

Use
[rangerctl](https://github.com/adammck/ranger/tree/master/cmd/rangerctl)
to query the state of the cluster (nodes and range assignments) and initiate
range operations (moves, splits, joins). This is built by `dev.sh`, so to see
usage, run:

```console
$ ./rangerctl
```

## License

MIT.
