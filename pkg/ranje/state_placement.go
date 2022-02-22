package ranje

import pb "github.com/adammck/ranger/pkg/proto/gen"

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

	// Bad state. The controller expected the placement to placed on a node and
	// be in some other state, but the node doesn't know about the placement.
	SpGone
)

//go:generate stringer -type=StatePlacement -output=zzz_state_placement_string.go

func (s StatePlacement) ToProto() pb.PlacementState {
	switch s {
	case SpUnknown:
		return pb.PlacementState_PS_UNKNOWN
	case SpPending:
		return pb.PlacementState_PS_PENDING
	case SpFetching:
		return pb.PlacementState_PS_FETCHING
	case SpFetched:
		return pb.PlacementState_PS_FETCHED
	case SpFetchFailed:
		return pb.PlacementState_PS_FETCH_FAILED
	case SpReady:
		return pb.PlacementState_PS_READY
	case SpTaken:
		return pb.PlacementState_PS_TAKEN
	case SpDropped:
		return pb.PlacementState_PS_DROPPED
	case SpGone:
		return pb.PlacementState_PS_GONE
	}

	// Probably a state was added but this method wasn't updated.
	panic("unknown StatePlacement value!")
}
