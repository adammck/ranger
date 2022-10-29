package ranje

import (
	"log"

	pb "github.com/adammck/ranger/pkg/proto/gen"
)

type RangeState uint8

const (
	RsUnknown RangeState = iota

	// The range is active, i.e. it should be placed on the appropriate number
	// of nodes and left alone until we decide to supersede it with another
	// range by joining or splitting.
	RsActive RangeState = iota

	// The range is actively being split. It has two child ranges, which should
	// be placed and should be activated as soon as possible (unless Recall is
	// set).
	RsSplitting

	// The range is actively being joined with another to form a new (single)
	// child range, whhich should be placed and activated as soon as possible
	// (unless Recall is set).
	RsJoining

	// The range has finished being split or joined, has been dropped from all
	// nodes, and will never be placed on any node again.
	RsObsolete
)

type RangeStateTransition struct {
	from RangeState
	to   RangeState
}

var RangeStateTransitions []RangeStateTransition

func init() {
	RangeStateTransitions = []RangeStateTransition{
		{RsActive, RsSplitting},
		{RsSplitting, RsObsolete},
		{RsSplitting, RsObsolete},

		{RsActive, RsJoining},
		{RsJoining, RsObsolete},
		{RsJoining, RsObsolete},
	}
}

//go:generate stringer -type=RangeState -output=range_state_string.go

// TODO: Rename!
func FromProto(s *pb.RangeState) RangeState {
	switch *s {
	case pb.RangeState_RS_UNKNOWN:
		return RsUnknown
	case pb.RangeState_RS_ACTIVE:
		return RsActive
	case pb.RangeState_RS_SPLITTING:
		return RsSplitting
	case pb.RangeState_RS_JOINING:
		return RsJoining
	case pb.RangeState_RS_OBSOLETE:
		return RsObsolete
	}

	log.Printf("warn: got unknown state from proto: %s", *s)
	return RsUnknown
}

func (s RangeState) ToProto() pb.RangeState {
	switch s {
	case RsUnknown:
		return pb.RangeState_RS_UNKNOWN
	case RsActive:
		return pb.RangeState_RS_ACTIVE
	case RsSplitting:
		return pb.RangeState_RS_SPLITTING
	case RsJoining:
		return pb.RangeState_RS_JOINING
	case RsObsolete:
		return pb.RangeState_RS_OBSOLETE
	}

	// Probably a state was added but this method wasn't updated.
	panic("unknown RangeState value!")
}
