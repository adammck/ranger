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

Client:

```console
$ alias kv=$(pwd)/bin/client.sh

$ # Try to read a key from node 1 which is not assigned.
$ kv 8001 kv.KV.Get '{"key": "a"}'
ERROR:
  Code: FailedPrecondition
  Message: no valid range

$ # Try to write same.
$ kv 8001 kv.KV.Put '{"key": "a", "value": "'$(echo -n aaa | base64)'"}'
ERROR:
  Code: FailedPrecondition
  Message: no valid range

$ # Assign the range [a,b) to node 1
$ kv 8001 ranger.Node.Give '{"range": {"ident": {"key": 1}, "start": "'$(echo -n a | base64)'", "end": "'$(echo -n b | base64)'"}}'
{
}

$ # Try to read again. Different error.
$ kv 8001 kv.KV.Get '{"key": "a"}'
ERROR:
  Code: NotFound
  Message: no such key

$ # Try to write again. Success!
$ kv 8001 kv.KV.Put '{"key": "a", "value": "'$(echo -n aaa | base64)'"}'
{
}

$ # Read again. Success! (n.b. it's base64 encoded)
$ kv 8001 kv.KV.Get '{"key": "a"}'
{
  "value": "YWFh"
}

$ # Try to dump the contents of range 1 from node 1
$ kv 8001 kv.KV.Dump '{"range": {"key": 1}}'
ERROR:
  Code: FailedPrecondition
  Message: can only dump ranges in the TAKEN state

$ # Take the range from node 1
$ kv 8001 ranger.Node.Take '{"range": {"key": 1}}'
{
}

$ # Try to dump again. Success!
$ kv 8001 kv.KV.Dump '{"range": {"key": 1}}'
{
  "pairs": [
    {
      "key": "a",
      "value": "YWFh"
    }
  ]
}

$ # Move that range to node 2
$ kv 8002 ranger.Node.Give '{"range": {"ident": {"key": 1}, "start": "'$(echo -n a | base64)'", "end": "'$(echo -n b | base64)'"}, "host": {"host": "localhost", "port": "8001"}}'
```
