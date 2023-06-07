package orchestrator

import "github.com/adammck/ranger/pkg/api"

// TODO: Split this into Add, Remove
type OpMove struct {
	Range api.RangeID
	Src   api.NodeID
	Dest  api.NodeID
	Err   chan error
}

type OpSplit struct {
	Range api.RangeID
	Key   api.Key

	// TODO: Update this interface (and the proto) to accomodate replication.
	// Currently only the first placement can be placed deliberately. Others
	// just go wherever.
	Left  api.NodeID
	Right api.NodeID

	Err chan error
}

type OpJoin struct {
	Left  api.RangeID
	Right api.RangeID
	Dest  api.NodeID
	Err   chan error
}
