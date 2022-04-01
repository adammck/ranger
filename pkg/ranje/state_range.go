package ranje

import (
	"log"

	pb "github.com/adammck/ranger/pkg/proto/gen"
)

type RangeState uint8

const (
	Unknown RangeState = iota
)

//go:generate stringer -type=RangeState -output=zzz_state_range_string.go

func FromProto(s *pb.RangeState) RangeState {
	switch *s {
	case pb.RangeState_RS_UNKNOWN:
		return Unknown
	}

	log.Printf("warn: got unknown state from proto: %s", *s)
	return Unknown
}

func (s RangeState) ToProto() pb.RangeState {
	switch s {
	case Unknown:
		return pb.RangeState_RS_UNKNOWN
	}

	// Probably a state was added but this method wasn't updated.
	panic("unknown RangeState value!")
}
