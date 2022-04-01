package ranje

import (
	"log"

	pb "github.com/adammck/ranger/pkg/proto/gen"
)

type StateLocal uint8

// TODO: Is joining and splitting the same state?

const (
	// ???
	Unknown StateLocal = iota
)

//go:generate stringer -type=StateLocal -output=zzz_state_local_string.go

func FromProto(s *pb.RangeState) StateLocal {
	switch *s {
	case pb.RangeState_RS_UNKNOWN:
		return Unknown
	}

	log.Printf("warn: got unknown state from proto: %s", *s)
	return Unknown
}

func (s StateLocal) ToProto() pb.RangeState {
	switch s {
	case Unknown:
		return pb.RangeState_RS_UNKNOWN
	}

	// Probably a state was added but this method wasn't updated.
	panic("unknown StateLocal value!")
}
