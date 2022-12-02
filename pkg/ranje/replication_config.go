package ranje

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

// TODO
func (rc *ReplicationConfig) Validate() error {
	return nil
}
