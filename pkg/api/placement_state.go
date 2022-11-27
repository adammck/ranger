package api

type PlacementState uint8

const (
	// Should never be in this state. Indicates an deserializing error.
	PsUnknown PlacementState = iota

	PsPending
	PsInactive
	PsActive
	PsMissing
	PsDropped
)

//go:generate stringer -type=PlacementState -output=zzz_placement_state.go
