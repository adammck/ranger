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

Services implement [rangelet.Node](pkg/rangelet/interface.go):

- `GetLoadInfo(rID ranje.Ident) LoadInfo`
- `PrepareAddRange(rm RangeMeta, parents []Parent) error`
- `AddRange(rid RangeID) error`
- `PrepareDropRange(rid RangeID) error`
- `DropRange(rid RangeID) error`

This is a Go interface, but it's all gRPC+protobufs under the hood. There are no
other implementations today, but it's a goal to avoid doing anything which would
make it difficult to implement Rangelets in other languages.

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

## Design

TODO

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
