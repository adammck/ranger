# Ranger

This is an experiment to define a generic interface for stateful range-sharded
distributed storage systems to implement, and a generic master to load-balance
keys between them.

## Development

Install development deps

```console
$ brew install protoc-gen-go
$ brew install protoc-gen-go-grpc
```

Install local runtime deps

```console
$ brew install consul
```

Regenerate the files in `pkg/proto`

```console
$ bin/gen-proto.sh
$ examples/kv/bin/gen-proto.sh
```

## State Machine

```graphviz
digraph G {
    subgraph cluster_0 {
        label=RANGE;
        
        rActive;
        rSubsuming;
        
        # The range has been subsumed and should no longer be placed on any
        # node. The controller should 
        rObsolete;
        
        # The balancer has decided to split or join this range.
        rActive -> rSubsuming;
        
        # The split/join failed.
        rSubsuming -> rActive;
        
        # The split/join was successful.
        rSubsuming -> rObsolete;
    }

    subgraph cluster_1 {
      label=PLACEMENT;

        # The placement exists, and a candidate node has been chosen, but we
        # haven't told the node yet.
        # The controller should send a Give RPC to the node.
        pPending;
        
        # The node has accepted the placement and is preparing to serve it by
        # loading relevant state (either from other nodes or cold storage).
        # The controller should ask the node whether it's finished yet.
        pLoading;
        
        # We've been waiting for the node to load the placement for too long.
        # The controller should send a Drop RPC to the node.
        pGiveUp;
        
        # The placement is fully active on a node. This is the steady state.
        # If the placement is marked as obsolete (by the Range state machine),
        # the controller should send a Drop RPC to the node.
        pReady;
        
        # The placement has been dropped from the node it was assigned to.
        # The controller should do nothing. The Range state machine will soon
        # destroy the placement.
        pDropped;
        
        
        # The node has accepted the placement.
        pPending -> pLoading;
        
        # The node rejected the placement, or the RPC timed out too many times.
        pPending -> pDropped;
        
        # The node has finished loading the placement!
        pLoading -> pReady;
        
        # The node took too long to load the placement.
        pLoading -> pGiveUp;
        
        # Either we told the node to Drop the placement and it succeeded, or we
        # noticed that the node is no longer reporting that it has it.
        pReady -> pDropped;
        
        # The Drop RPC was sent, and we waited a bit for a response. Whatever
        # the outcome, we move to dropped.
        pGiveUp -> pDropped;
    }
}
```

## Objects

### stateful

this stuff lives in a durable store, which must always be kept up to date. no
local state.

```text
keyspace
- next_id
- []range
  - id
  - [2]placement # current, next
    - id
    - node_id # <- where it is _expected_ to be
    - state # needed? can maybe be inferred
```

### volatile

this stuff is gathered at startup, and kept up to date during runtime.

```text
roster
- []node
  - id
  - host
  - port

  - utilization
    - cpu?
    - memory?
    - network?
    - disk?
  
  - vplacement
    - state
    - utilization # as above
```
