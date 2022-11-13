package api

type RangeState uint8

const (
	RsUnknown RangeState = iota

	// The range is active, i.e. it should be placed on the appropriate number
	// of nodes and left alone until we decide to supersede it with another
	// range by joining or splitting.
	RsActive

	RsSubsuming

	// The range has finished being split or joined, has been dropped from all
	// nodes, and will never be placed on any node again.
	RsObsolete
)

//go:generate stringer -type=RangeState -output=zzz_range_state.go
