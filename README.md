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
    edge [fontcolor=grey, fontsize=8];
    
    subgraph cluster_0 {
        label=Range;
        
        rActive [label=Active];
        rSubsuming [label=Subsuming];
        rObsolete [label=Obsolete];
        
        rActive -> rSubsuming;
        rSubsuming -> rActive;
        rSubsuming -> rObsolete;
    }

    subgraph cluster_2 {
      label=Placement;

        pPending
        [label=<Pending<br/><font point-size="8" color="#aaaaaa">Send <u>Give</u><br/><i>prepare_add_shard</i></font>>];
        
        pPreparing
        [label=<Preparing<br/><font point-size="8" color="#aaaaaa">If (something):<br/>Send <u>Serve</u><i><br/>add_shard</i></font>>];
        
        pGiveUp
        [label=<GiveUp> color=red];

        pReady
        [penwidth=2 label=<Ready<br/><font point-size="8" color="#aaaaaa">If (something):<br/>Send <u>Take</u><i><br/>prepare_remove_shard</i></font>>];
        
        pTaken
        [label=<Taken<br/><font point-size="8" color="#aaaaaa">If (something):<br/>Send <u>Take</u><i><br/>remove_shard</i></font>>];

        pDropped;
        
        pPending -> pPreparing
        [label="remote state is:\nPsPreparing"];
        
        pPreparing -> pReady
        [label="remote state is:\nPsReady"];
        
        pReady -> pTaken
        [label="remote state is:\nPsTaken"];
        
        pTaken -> pDropped
        [label="remote state is:\nPsDropped"];

        # Most states can move to GiveUp on failure. Some other placement will
        # hopefully pick up the workload.
        pPending -> pGiveUp;
        pPreparing -> pGiveUp;
        pReady -> pGiveUp;
        pTaken -> pGiveUp;
    
        pGiveUp -> pDropped;
        
        # Shortcuts for systems which don't need prepare steps.
        #pPending -> pReady [color="grey"];
        #pReady -> pDropped [color="grey"];
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
