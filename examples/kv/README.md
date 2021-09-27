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
$ ./kv -cp ":9001" -dp ":8001"
$ ./kv -cp ":9002" -dp ":8002"
$ ./kv -cp ":9003" -dp ":8003"
```

Client:

```console
$ # Try to read a key from node 1 which is not assigned.
$ curl http://localhost:8001/a
404: Not found
No such range

$ # Try to write same.
$ curl -X PUT -d "whatever" http://localhost:8001/a
400: Bad Request

$ # Assign the range [a,b) to node 1
$ grpcurl -d '{"range": {"ident": {"key": 1}, "start": "'$(echo -n a | base64)'", "end": "'$(echo -n b | base64)'"}}' -plaintext localhost:9001 ranger.Node.Give
{ }

$ # Try to read again. Different error.
$ curl http://localhost:8001/a
404: No such key

$ # Try to write same. Success!
$ curl -X PUT -d "whatever" http://localhost:8001/a
200: OK

$ # Read again. Success!
$ curl http://localhost:8001/a
whatever
```
