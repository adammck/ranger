package ranje

// TODO: Move this into the keyspace package.
type ReplicationConfig struct {

	// The number of active placements that a range should aim to have, when the
	// keyspace is stable. Whether the number of active placements is more or
	// fewer than this during operations depends on MinActive and MaxActive.
	TargetActive int

	// The minimum number of active placements that a range will ever be allowed
	// to voluntarily have. (Sometimes the number will be lower involuntarily,
	// because of e.g. nodes crashing.)
	MinActive int

	// The maximum number of active placements that a range will ever be allowed
	// to have.
	MaxActive int

	// TODO
	MinPlacements int

	// TODO
	MaxPlacements int
}

// TODO: Docs
func (rc *ReplicationConfig) Validate() error {
	return nil
}

// R1 is an example replication config for systems which want a single active
// placement of each key, and can tolerate an additional inactive placement.
var R1 = ReplicationConfig{
	TargetActive: 1,
	MinActive:    0,
	MaxActive:    1,
	//
	MinPlacements: 1,
	MaxPlacements: 2,
}

// R1 is an example replication config for high-availability systems which want
// to maintain three active placements of each key, can tolerate an additional
// two placements during operations, one of which can be active.
var R3 = ReplicationConfig{
	TargetActive: 3,
	MinActive:    3,
	MaxActive:    4,
	//
	MinPlacements: 3,
	MaxPlacements: 5, // Up to two spare
}
