# Ranger

This is an experiment to define a generic interface for stateful range-sharded
distributed storage systems to implement, and a generic master to load-balance
keys between them.

## Development

Regenerate the files in `pkg/proto`

```console
$ bin/gen-proto.sh
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
    }
}
```