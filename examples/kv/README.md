# Example: Key-Value Store

This is a very simple volatile in-memory key-value store to demonstrate the
Ranger API. It exposes an HTTP interface to GET and PUT things, and a gRPC
interface to move those things around.

## Usage

Server:

```console
$ cd ~/code/src/github.com/adammck/ranger/examples/kv
$ go run main.go
```

Client:

```console
$ curl http://localhost:8000/a
404: Not found

$ curl -X PUT -d "whatever" http://localhost:8000/a
200: OK

$ curl http://localhost:8000/a
whatever
```
