package ranje

import (
	"fmt"
	"log"

	pb "github.com/adammck/ranger/pkg/proto/gen"
)

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

type RangeStateTransition struct {
	from RangeState
	to   RangeState
}

var RangeStateTransitions []RangeStateTransition

func init() {
	RangeStateTransitions = []RangeStateTransition{
		{RsActive, RsSubsuming},
		{RsSubsuming, RsObsolete},
		{RsSubsuming, RsObsolete},

		{RsActive, RsSubsuming},
		{RsSubsuming, RsObsolete},
		{RsSubsuming, RsObsolete},
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
	case pb.RangeState_RS_SUBSUMING:
		return RsSubsuming
	case pb.RangeState_RS_OBSOLETE:
		return RsObsolete
	}

	log.Printf("warn: got unknown state from proto: %s", *s)
	return RsUnknown
}

func RangeStateToProto(s RangeState) pb.RangeState {
	switch s {
	case RsUnknown:
		return pb.RangeState_RS_UNKNOWN
	case RsActive:
		return pb.RangeState_RS_ACTIVE
	case RsSubsuming:
		return pb.RangeState_RS_SUBSUMING
	case RsObsolete:
		return pb.RangeState_RS_OBSOLETE
	}

	panic(fmt.Sprintf("unknown RangeState: %#v", s))
}
