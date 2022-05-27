# Ranger

This is an experiment to define a generic range-based sharding interface which
services can implement in order to have their workloads automatically balanced,
and a generic controller to perform that balancing. It's designed in particular
to support stateful workloads, which need to move around large amounts of data
in order to rebalance, but should be useful to stateless worksloads, too.

Ranger is just a toy today, with various critical features missing and hardly
any tests, so is not yet suitable for any purpose under any circumstances. I'm
working on it (on the side) because I'm acquainted with many systems which would
benefit from such a thing existing. Please drop me a line if you're interested
in collaborating.

## Examples

- [key-value store](examples/kv)

## Interface

Services implement [rangelet.Node](pkg/api/node.go):

- `GetLoadInfo(rID ranje.Ident) LoadInfo`
- `PrepareAddRange(rm RangeMeta, parents []Parent) error`
- `AddRange(rid RangeID) error`
- `PrepareDropRange(rid RangeID) error`
- `DropRange(rid RangeID) error`

This is a Go interface, but it's all gRPC+protobufs under the hood. There are no
other implementations today, but it's a goal to avoid doing anything which would
make it difficult to implement Rangelets in other languages.

## Design

![ranger-diagram-v1](https://user-images.githubusercontent.com/19543/167534758-82124dab-c12e-4920-869c-63165160dffb.png)

Here's how it works, at a high level.  
The main components are:

- **Keyspace**: Stores the desired state of ranges and placements. Provides an
  interface to create new ranges by splitting and joining. (Ranges cannot
  currently be destroyed; only obsoleted, in case the history is needed.)
  Provides an interface to create and destroy placements, in order to designate
  which node(s) each range should be placed on.
- **Roster**: Watches (external) service discovery to maintain a list of nodes
  (on which ranges can be placed). Relays messages from other components (e.g.
  the orchestrator) to the nodes. Periodically probes those nodes to monitor
  their health, and the state of the ranges placed on them. For now, provides an
  interface for other components to find a node suitable for range placement.
- **Orchestrator**: Reconciles the difference between the desired state (from
  the keyspace) and the current state (from the roster), somewhat like a
  Kubernetes controller.
- **Rangelet**: Runs inside of nodes. Receives RPCs from the roster, and calls
  methods of the rangelet.Node interface to notify nodes of changes to the set
  of ranges placed on them. Provides some useful helper methods to simplify node
  development.
- **Balancer**: External component. Simple implementation(s) provided, but can
  be replaced for more complex services. Fetches state of nodes, ranges, and
  placements from orchestrator, and sends split and join RPCs in order to spread
  ranges evenly across nodes.

Both **Persister** and **Discovery** are simple interfaces to pluggable storage
systems. Only Consul is supported for now, but adding support for other systems
(e.g. ZooKeeper, etcd) should be easy enough in future.

The **green boxes** are storage nodes. These are implemented entirely (except
the rangelet) by the service owner, to perform the _actual work_ that Ranger is
sharding and balancing. Services may receive their data via HTTP or RPC, and so
may provide a client library to route requests to the appropriate node(s), or
may forward requests between themselves. (Ranger doesn't provide any help with
that part today, but likely will in future.) Alternatively, services may pull
relevant work from e.g. a message queue.

For example node implementations, see the [examples](/examples) directory.  
For more complex examples, read the _Slicer_ and _Shard Manager_ papers.

## Client

Ranger includes a command line client, `rangerctl`, which is a thin wrapper
around the gRPC interface to the orchestrator. This is currently the primary
means of inspecting and balancing data across a cluster.

```console
$ ./rangerctl -h
Usage: ./rangerctl [-addr=host:port] <action> [<args>]

Action and args must be one of:
  - ranges
  - range <rangeID>
  - nodes
  - node <nodeID>
  - move <rangeID> [<nodeID>]
  - split <rangeID> <boundary> [<nodeID>] [<nodeID>]
  - join <rangeID> <rangeID> [<nodeID>]

Flags:
  -addr string
        controller address (default "localhost:5000")
  -request
        print gRPC request instead of sending it
```

## State Machines

TODO

## Related Work

I've taken ideas from most of these systems. I'll expand this doc soon to
clarify what came from each. But for now, here are some links:

- [Shard Manager](https://dl.acm.org/doi/pdf/10.1145/3477132.3483546) (Facebook, 2021)
- [Service Fabric](https://dl.acm.org/doi/pdf/10.1145/3190508.3190546) (Microsoft, 2018)
- [Slicer](https://www.usenix.org/system/files/conference/osdi16/osdi16-adya.pdf) (Google, 2016)
- [Ringpop](https://ringpop.readthedocs.io/en/latest/index.html) (Uber, 2016)
- [Helix](https://sci-hub.ru/10.1145/2391229.2391248) (LinkedIn, 2012)

## License

MIT
