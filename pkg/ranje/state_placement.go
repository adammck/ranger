package ranje

type StatePlacement uint8

const (
	// Should never be in this state. Indicates an deserializing error.
	SpUnknown StatePlacement = iota

	// Initial state. The placement exists, but we haven't done anything with it
	// yet.
	SpPending

	SpFetching
	SpFetched
	SpFetchFailed
	SpReady
	SpTaken
	SpDropped
)

//go:generate stringer -type=StatePlacement -output=zzz_state_placement_string.go
