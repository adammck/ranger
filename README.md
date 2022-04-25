# Ranger

This is an experiment to define a generic range-based sharding interface which
services can implement in order to have their workloads automatically balanced,
and a generic controller to perform that balancing. It's designed in particular
to support stateful workloads, which need to move around large amounts of data
in order to rebalance, but should be useful to stateless worksloads, too.

Ranger is just a toy today, with various critical features missing and hardly
any tests, so is not suitable for any purpose under any circumstances.

## Examples

- [key-value store](examples/kv)

## Interface

Services implement [rangelet.Node](pkg/rangelet/interface.go):

- `PrepareAddRange(rm RangeMeta, parents []Parent) error`
- `AddRange(rid RangeID) error`
- `PrepareDropRange(rid RangeID) error`
- `DropRange(rid RangeID) error`

This is a Go interface, but it's all gRPC+protobufs under the hood. There are no
other implementations today, but it's a goal to avoid doing anything which would
make it difficult to implement Rangelets in other languages.

## Design

TODO

## State Machines

TODO

## Related Work

I have shamelessly taken concepts from most of these. I will expand this doc
soon to clarify what was taken from each. For now, here are some links.

- [Shard Manager](https://dl.acm.org/doi/pdf/10.1145/3477132.3483546) (Facebook, 2021)
- [Service Fabric](https://dl.acm.org/doi/pdf/10.1145/3190508.3190546) (Microsoft, 2018)
- [Slicer](https://www.usenix.org/system/files/conference/osdi16/osdi16-adya.pdf) (Google, 2016)
- [Ringpop](https://ringpop.readthedocs.io/en/latest/index.html) (Uber, 2016)
- [Helix](https://sci-hub.ru/10.1145/2391229.2391248) (LinkedIn, 2012)

## License

MIT
