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
    graph [fontname="Sedgwick Ave" fontsize=20 color=grey];
    node [fontname="Handlee"];
    edge [fontname="Handlee"];

    subgraph cluster_0 {
        label=RANGE;
        
        Pending;
        Placing;
        PlaceError;
        Quarantine;
        Ready [penwidth=2];
        Moving;
        Splitting;
        Joining;
        Obsolete;
        
        Pending -> Placing;
        Placing -> Ready;
        Placing -> PlaceError;
        PlaceError -> Placing;
        PlaceError -> Quarantine;
        Quarantine -> Placing;
        Ready -> Moving;
        Ready -> Splitting;
        Ready -> Joining;

        Moving -> Ready;

        Splitting -> Obsolete;
        Joining -> Obsolete;
    }

    subgraph cluster_1 {
      label=PLACEMENT;

      xPending [label=Pending];
      xFetching [label=Fetching];
      xFetched [label=Fetched];
      xFetchFailed [label=FetchFailed];
      xReady [label=Ready penwidth=2];
      xTaken [label=Taken];
      xDropped [label=Dropped];

      xPending -> xReady [label=give];
      xPending -> xFetching [label=give];
      xFetching -> xFetched;
      xFetching -> xFetchFailed;
      xFetched -> xReady  [label=serve];
      xFetchFailed -> xPending;
      xReady -> xTaken [label=take];
      xTaken -> xDropped [label=drop];
      xTaken -> xReady [label=untake];
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
