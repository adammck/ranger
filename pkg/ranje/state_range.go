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

	// The range is actively being split or joined.
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
		{RsSubsuming, RsActive},
		{RsSubsuming, RsObsolete},
	}
}

//go:generate stringer -type=RangeState -output=zzz_state_range_string.go

func FromProto(s *pb.RangeState) RangeState {
	switch *s {
	case pb.RangeState_RS_UNKNOWN:
		return RsUnknown
	}

	log.Printf("warn: got unknown state from proto: %s", *s)
	return RsUnknown
}

func (s RangeState) ToProto() pb.RangeState {
	switch s {
	case RsUnknown:
		return pb.RangeState_RS_UNKNOWN
	}

	// Probably a state was added but this method wasn't updated.
	panic("unknown RangeState value!")
}
