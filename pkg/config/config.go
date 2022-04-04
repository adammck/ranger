package config

import "time"

// TODO: Change this to use the options pattern with sensible defaults.

// Config defines the behavior of the system, since the library can support a
// few different patterns. This is compile-time config, and should be identical
// between the different components in a system.
type Config struct {

	// Should nodes ask the controller to drain their ranges before shutting
	// down, and then wait for that? Or just vanish?
	DrainNodesBeforeShutdown bool

	// How long should the controller wait for a node to respond to a probe
	// before expiring it?
	NodeExpireDuration time.Duration

	// How many Ready placements should each range have?
	// TODO: This should be configurable for each range.
	Replication int

	//SkipPrepareState
	//SkipTakenState
}
